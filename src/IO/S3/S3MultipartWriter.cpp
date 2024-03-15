#include "config.h"

#if USE_AWS_S3
#include <IO/S3/S3MultipartWriter.h>
#include <IO/StdIStreamFromMemory.h>
#include <IO/WriteBufferFromS3TaskTracker.h>

#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/Throttler.h>
#include <Interpreters/Cache/FileCache.h>

#include <Common/Scheduler/ResourceGuard.h>
#include <IO/WriteHelpers.h>
#include <IO/S3Common.h>
#include <IO/S3/Requests.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/S3/BlobStorageLogWriter.h>

#include <aws/s3/model/StorageClass.h>

#include <utility>


namespace ProfileEvents
{
    //todo fix profile events
    extern const Event WriteBufferFromS3Bytes;
    extern const Event WriteBufferFromS3Microseconds;
    extern const Event WriteBufferFromS3RequestsErrors;
    extern const Event S3WriteBytes;

    extern const Event S3CreateMultipartUpload;
    extern const Event S3CompleteMultipartUpload;
    extern const Event S3AbortMultipartUpload;
    extern const Event S3UploadPart;
    extern const Event S3PutObject;

    extern const Event DiskS3CreateMultipartUpload;
    extern const Event DiskS3CompleteMultipartUpload;
    extern const Event DiskS3AbortMultipartUpload;
    extern const Event DiskS3UploadPart;
    extern const Event DiskS3PutObject;

    extern const Event RemoteWriteThrottlerBytes;
    extern const Event RemoteWriteThrottlerSleepMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
}

struct S3MultipartWriter::PartData
{
    Memory<> memory;
    size_t data_size = 0;

    std::shared_ptr<std::iostream> createAwsBuffer()
    {
        auto buffer = std::make_shared<StdIStreamFromMemory>(memory.data(), data_size);
        buffer->exceptions(std::ios::badbit);
        return buffer;
    }

    bool isEmpty() const
    {
        return data_size == 0;
    }
};


S3MultipartWriter::S3MultipartWriter(
    std::shared_ptr<const S3::Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    const S3Settings::RequestSettings & request_settings_,
    BlobStorageLogWriterPtr blob_log_,
    std::optional<std::map<String, String>> object_metadata_,
    ThreadPoolCallbackRunner<void> schedule_,
    const WriteSettings & write_settings_) :
     bucket(bucket_)
    , key(key_)
    , request_settings(request_settings_)
    , upload_settings(request_settings.getUploadSettings())
    , write_settings(write_settings_)
    , client_ptr(std::move(client_ptr_))
    , object_metadata(std::move(object_metadata_))
    , task_tracker(
          std::make_unique<TaskTracker>(
              std::move(schedule_),
              upload_settings.max_inflight_parts_for_one_file,
              limitedLog))
    , blob_log(std::move(blob_log_))
{
    LOG_TRACE(limitedLog, "Create S3MulipartWriter, {}", getShortLogDetails());
}

class S3MultipartWriter::WriteBufferWithMemoryCallback : public BufferWithOwnMemory<WriteBuffer>
{
public:
    WriteBufferWithMemoryCallback(size_t size_, std::function<void(Memory<>, size_t)> & memory_callback_)
    : BufferWithOwnMemory(size_ + 1025), size(size_), memory_callback(memory_callback_)
    {
    }

    void finalizeImpl() override
    {
        memory_callback(std::move(memory), size);
    } 
private: 
    void nextImpl() override 
    {
        if(offset() > size + 1025)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Static sized WriteBuffer expected {} bytes to be written, but {} was written",
                size,
                offset());
    }

    size_t size;
    std::function<void(Memory<>, size_t)> memory_callback;
};

std::unique_ptr<WriteBuffer> S3MultipartWriter::writeData(size_t size)
{
    std::function<void(Memory<>, size_t)> callback = [this](Memory<> memory, size_t size_)
    {
        // for now, if part is smaller than the min buffer size write it serially to the small file buffer 
        if(size_ > 5242880) //min size for part
        {
            //todo handle case where part is larger than max part size
            std::lock_guard lock(detacted_part_mutex);
            detached_part_data.push_back({std::move(memory), size_});
            writeMultipartUpload();
        }
        else //collect smaller files into a single buffer
        {
            //todo handle case when buffer reaches max part size
            std::lock_guard lock(small_file_buffer_mutex);
            while (small_file_buffer_offset + size_ > small_file_buffer.size())
            {
                small_file_buffer.resize(2 * small_file_buffer.size() + 1);
            }
            memcpy(small_file_buffer.data() + small_file_buffer_offset, memory.data(), size_);
            small_file_buffer_offset += size_;
        }
    };
    return std::make_unique<WriteBufferWithMemoryCallback>(size, callback);
}

String S3MultipartWriter::getVerboseLogDetails() const
{
    String multipart_upload_details;
    if (!multipart_upload_id.empty())
        multipart_upload_details = fmt::format(", upload id {}, upload has finished {}"
                                       , multipart_upload_id, multipart_upload_finished);

    return fmt::format("Details: bucket {}, key {}, total size {}, with pool: {}, {}",
                       bucket, key, total_size, task_tracker->isAsync(), multipart_upload_details);
}

String S3MultipartWriter::getShortLogDetails() const
{
    String multipart_upload_details;
    if (!multipart_upload_id.empty())
        multipart_upload_details = fmt::format(", upload id {}"
                                               , multipart_upload_id);

    return fmt::format("Details: bucket {}, key {}{}",
                       bucket, key, multipart_upload_details);
}

void S3MultipartWriter::tryToAbortMultipartUpload()
{
    try
    {
        task_tracker->safeWaitAll();
        abortMultipartUpload();
    }
    catch (...)
    {
        LOG_ERROR(log, "Multipart upload hasn't aborted. {}", getVerboseLogDetails());
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void S3MultipartWriter::finalize()
{
    {
        std::lock_guard lock(detacted_part_mutex);
        detached_part_data.push_back({std::move(small_file_buffer), small_file_buffer_offset});
        writeMultipartUpload();
    }
    task_tracker->safeWaitAll();
    completeMultipartUpload();
}

S3MultipartWriter::~S3MultipartWriter()
{
}
 
void S3MultipartWriter::writeMultipartUpload()
{
    if (multipart_upload_id.empty())
    {
        createMultipartUpload();
    }

    while (!detached_part_data.empty())
    {
        writePart(std::move(detached_part_data.front()));
        detached_part_data.pop_front();
    }
}

void S3MultipartWriter::createMultipartUpload()
{
    LOG_TEST(limitedLog, "Create multipart upload. {}", getShortLogDetails());

    S3::CreateMultipartUploadRequest req;

    req.SetBucket(bucket);
    req.SetKey(key);

    /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
    req.SetContentType("binary/octet-stream");

    if (object_metadata.has_value())
        req.SetMetadata(object_metadata.value());

    client_ptr->setKMSHeaders(req);

    ProfileEvents::increment(ProfileEvents::S3CreateMultipartUpload);
    if (write_settings.for_object_storage)
        ProfileEvents::increment(ProfileEvents::DiskS3CreateMultipartUpload);

    Stopwatch watch;
    auto outcome = client_ptr->CreateMultipartUpload(req);
    watch.stop();

    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Microseconds, watch.elapsedMicroseconds());
    if (blob_log)
        blob_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadCreate, bucket, key, {}, 0,
                           outcome.IsSuccess() ? nullptr : &outcome.GetError());

    if (!outcome.IsSuccess())
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);
        throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
    }

    multipart_upload_id = outcome.GetResult().GetUploadId();

    LOG_TRACE(limitedLog, "Multipart upload has created. {}", getShortLogDetails());
}

void S3MultipartWriter::abortMultipartUpload()
{
    if (multipart_upload_id.empty())
    {
        LOG_WARNING(log, "Nothing to abort. {}", getVerboseLogDetails());
        return;
    }

    LOG_WARNING(log, "Abort multipart upload. {}", getVerboseLogDetails());

    S3::AbortMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetUploadId(multipart_upload_id);

    ProfileEvents::increment(ProfileEvents::S3AbortMultipartUpload);
    if (write_settings.for_object_storage)
        ProfileEvents::increment(ProfileEvents::DiskS3AbortMultipartUpload);

    Stopwatch watch;
    auto outcome = client_ptr->AbortMultipartUpload(req);
    watch.stop();

    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Microseconds, watch.elapsedMicroseconds());

    if (blob_log)
        blob_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadAbort, bucket, key, {}, 0,
                           outcome.IsSuccess() ? nullptr : &outcome.GetError());

    if (!outcome.IsSuccess())
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);
        throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
    }

    LOG_WARNING(log, "Multipart upload has aborted successfully. {}", getVerboseLogDetails());
}

S3::UploadPartRequest S3MultipartWriter::getUploadRequest(size_t part_number, PartData & data)
{
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Bytes, data.data_size);

    S3::UploadPartRequest req;

    /// Setup request.
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetPartNumber(static_cast<int>(part_number));
    req.SetUploadId(multipart_upload_id);
    req.SetContentLength(data.data_size);
    req.SetBody(data.createAwsBuffer());
    /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
    req.SetContentType("binary/octet-stream");

    return req;
}

void S3MultipartWriter::writePart(S3MultipartWriter::PartData && data)
{
    if (data.data_size == 0)
    {
        LOG_TEST(limitedLog, "Skipping writing part as empty {}", getShortLogDetails());
        return;
    }

    multipart_tags.push_back({});
    size_t part_number = multipart_tags.size();
    LOG_TEST(limitedLog, "writePart {}, part size {}, part number {}", getShortLogDetails(), data.data_size, part_number);

    if (multipart_upload_id.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unable to write a part without multipart_upload_id, details: WriteBufferFromS3 created for bucket {}, key {}",
            bucket, key);

    if (part_number > upload_settings.max_part_number)
    {
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Part number exceeded {} while writing {} bytes to S3. Check min_upload_part_size = {}, max_upload_part_size = {}, "
            "upload_part_size_multiply_factor = {}, upload_part_size_multiply_parts_count_threshold = {}, max_single_part_upload_size = {}",
            upload_settings.max_part_number, data.data_size, upload_settings.min_upload_part_size, upload_settings.max_upload_part_size,
            upload_settings.upload_part_size_multiply_factor, upload_settings.upload_part_size_multiply_parts_count_threshold,
            upload_settings.max_single_part_upload_size);
    }

    if (data.data_size > upload_settings.max_upload_part_size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Part size exceeded max_upload_part_size. {}, part number {}, part size {}, max_upload_part_size {}",
            getShortLogDetails(),
            part_number,
            data.data_size,
            upload_settings.max_upload_part_size
            );
    }

    auto req = getUploadRequest(part_number, data);
    auto worker_data = std::make_shared<std::tuple<S3::UploadPartRequest, S3MultipartWriter::PartData>>(std::move(req), std::move(data));

    auto upload_worker = [&, worker_data, part_number] ()
    {
        auto & data_size = std::get<1>(*worker_data).data_size;

        LOG_TEST(limitedLog, "Write part started {}, part size {}, part number {}",
                 getShortLogDetails(), data_size, part_number);

        ProfileEvents::increment(ProfileEvents::S3UploadPart);
        if (write_settings.for_object_storage)
            ProfileEvents::increment(ProfileEvents::DiskS3UploadPart);

        auto & request = std::get<0>(*worker_data);

        ResourceCost cost = request.GetContentLength();
        ResourceGuard rlock(write_settings.resource_link, cost);
        Stopwatch watch;
        auto outcome = client_ptr->UploadPart(request);
        watch.stop();
        rlock.unlock(); // Avoid acquiring other locks under resource lock

        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Microseconds, watch.elapsedMicroseconds());

        if (blob_log)
        {
            blob_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadWrite,
                /* bucket = */ bucket, /* remote_path = */ key, /* local_path = */ {}, /* data_size */ data_size,
                outcome.IsSuccess() ? nullptr : &outcome.GetError());
        }

        if (!outcome.IsSuccess())
        {
            ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);
            write_settings.resource_link.accumulate(cost); // We assume no resource was used in case of failure
            throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
        }

        multipart_tags[part_number-1] = outcome.GetResult().GetETag();

        LOG_TEST(limitedLog, "Write part succeeded {}, part size {}, part number {}, etag {}",
                 getShortLogDetails(), data_size, part_number, multipart_tags[part_number-1]);
    };

    task_tracker->add(std::move(upload_worker));
}

void S3MultipartWriter::completeMultipartUpload()
{
    LOG_TEST(limitedLog, "Completing multipart upload. {}, Parts: {}", getShortLogDetails(), multipart_tags.size());

    if (multipart_tags.empty())
        throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Failed to complete multipart upload. No parts have uploaded");

    for (size_t i = 0; i < multipart_tags.size(); ++i)
    {
        const auto tag = multipart_tags.at(i);
        if (tag.empty())
            throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Failed to complete multipart upload. Part {} haven't been uploaded.", i);
    }

    S3::CompleteMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetUploadId(multipart_upload_id);

    Aws::S3::Model::CompletedMultipartUpload multipart_upload;
    for (size_t i = 0; i < multipart_tags.size(); ++i)
    {
        Aws::S3::Model::CompletedPart part;
        multipart_upload.AddParts(part.WithETag(multipart_tags[i]).WithPartNumber(static_cast<int>(i + 1)));
    }

    req.SetMultipartUpload(multipart_upload);

    size_t max_retry = std::max(request_settings.max_unexpected_write_error_retries, 1UL);
    for (size_t i = 0; i < max_retry; ++i)
    {
        ProfileEvents::increment(ProfileEvents::S3CompleteMultipartUpload);
        if (write_settings.for_object_storage)
            ProfileEvents::increment(ProfileEvents::DiskS3CompleteMultipartUpload);

        Stopwatch watch;
        auto outcome = client_ptr->CompleteMultipartUpload(req);
        watch.stop();

        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Microseconds, watch.elapsedMicroseconds());

        if (blob_log)
            blob_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadComplete, bucket, key, {}, 0,
                               outcome.IsSuccess() ? nullptr : &outcome.GetError());

        if (outcome.IsSuccess())
        {
            LOG_TRACE(limitedLog, "Multipart upload has completed. {}, Parts: {}", getShortLogDetails(), multipart_tags.size());
            return;
        }

        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);

        if (outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY)
        {
            /// For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests
            /// BTW, NO_SUCH_UPLOAD is expected error and we shouldn't retry it here, DB::S3::Client take care of it
            LOG_INFO(log, "Multipart upload failed with NO_SUCH_KEY error, will retry. {}, Parts: {}", getVerboseLogDetails(), multipart_tags.size());
        }
        else
        {
            throw S3Exception(
                outcome.GetError().GetErrorType(),
                "Message: {}, Key: {}, Bucket: {}, Tags: {}",
                outcome.GetError().GetMessage(), key, bucket, fmt::join(multipart_tags.begin(), multipart_tags.end(), " "));
        }
    }

    throw S3Exception(
        Aws::S3::S3Errors::NO_SUCH_KEY,
        "Message: Multipart upload failed with NO_SUCH_KEY error, retries {}, Key: {}, Bucket: {}",
        max_retry, key, bucket);
}

S3::PutObjectRequest S3MultipartWriter::getPutRequest(PartData & data)
{
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Bytes, data.data_size);

    S3::PutObjectRequest req;

    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetContentLength(data.data_size);
    req.SetBody(data.createAwsBuffer());
    if (object_metadata.has_value())
        req.SetMetadata(object_metadata.value());
    if (!upload_settings.storage_class_name.empty())
        req.SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(upload_settings.storage_class_name));

    /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
    req.SetContentType("binary/octet-stream");

    client_ptr->setKMSHeaders(req);

    return req;
}

}

#endif
