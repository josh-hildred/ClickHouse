
#include <IO/S3/copyS3File.h>

#if USE_AWS_S3

#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <Interpreters/Context.h>
#include <IO/LimitSeekableReadBuffer.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/StdStreamFromReadBuffer.h>
#include <IO/ReadBufferFromS3.h>

namespace ProfileEvents
{
    extern const Event WriteBufferFromS3Bytes;
    extern const Event WriteBufferFromS3Microseconds;
    extern const Event WriteBufferFromS3RequestsErrors;

    extern const Event S3CreateMultipartUpload;
    extern const Event S3CompleteMultipartUpload;
    extern const Event S3PutObject;
    extern const Event S3CopyObject;
    extern const Event S3UploadPart;
    extern const Event S3UploadPartCopy;

    extern const Event DiskS3CreateMultipartUpload;
    extern const Event DiskS3CompleteMultipartUpload;
    extern const Event DiskS3PutObject;
    extern const Event DiskS3CopyObject;
    extern const Event DiskS3UploadPart;
    extern const Event DiskS3UploadPartCopy;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
}

namespace
{
class CopyHelper
    {
    public:
        CopyHelper(
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & src_bucket_,
            std::vector<String> & src_keys_,
            const String & dest_bucket_,
            const String & dest_key_,
            const S3Settings::RequestSettings & request_settings_,
            const ReadSettings & read_settings_,
            ThreadPoolCallbackRunner<void> schedule_,
            bool for_disk_s3_,
            BlobStorageLogWriterPtr blob_storage_log_) : dest_bucket(dest_bucket_)
            , dest_key(dest_key_)
            , src_bucket(src_bucket_)
            , src_keys(src_keys_)
            , supports_multipart_copy(client_ptr_->supportsMultiPartCopy())
            , read_settings(read_settings_)
            , request_settings(request_settings_)
            , schedule(schedule_)
            , for_disk_s3(for_disk_s3_)
            , blob_storage_log(blob_storage_log_)
            , client_ptr(client_ptr_)
            , log(getLogger("S3MultiPartCopy"))
            , upload_settings(request_settings.getUploadSettings())

        {
        }

        void performCopy()
        {
            performMultipartCopy();

            if (request_settings.check_objects_after_upload)
                checkObjectAfterUpload();
        }

    private:
        const String & dest_bucket;
        const String & dest_key;
        const String & src_bucket;
        std::vector<String> & src_keys;
        bool supports_multipart_copy;
        const ReadSettings read_settings;
        const S3Settings::RequestSettings & request_settings;
        ThreadPoolCallbackRunner<void> schedule;
        bool for_disk_s3;
        BlobStorageLogWriterPtr blob_storage_log;
        const std::shared_ptr<const S3::Client> client_ptr;


        /// Represents a task copying a single part (file).
        /// Keep this struct small because there can be thousands of parts.
        struct UploadPartTask
        {
            size_t part_number;
            String key;
            String tag;
            bool is_finished = false;
            std::exception_ptr exception;
        };

        size_t num_parts;
        String multipart_upload_id;
        std::atomic<bool> multipart_upload_aborted = false;
        Strings part_tags;

        std::list<UploadPartTask> TSA_GUARDED_BY(bg_tasks_mutex) bg_tasks;
        size_t num_added_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        size_t num_finished_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        size_t num_finished_parts TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        std::mutex bg_tasks_mutex;
        std::condition_variable bg_tasks_condvar;
        const LoggerPtr log;
            const S3Settings::RequestSettings::PartUploadSettings & upload_settings;

        void fillCreateMultipartRequest(S3::CreateMultipartUploadRequest & request)
        {
            request.SetBucket(dest_bucket);
            request.SetKey(dest_key);

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request.SetContentType("binary/octet-stream");

            //if (object_metadata.has_value())
            //    request.SetMetadata(object_metadata.value());

            const auto & storage_class_name = upload_settings.storage_class_name;
            if (!storage_class_name.empty())
                request.SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(storage_class_name));

            client_ptr->setKMSHeaders(request);
        }

        void createMultipartUpload()
        {
            S3::CreateMultipartUploadRequest request;
            fillCreateMultipartRequest(request);

            ProfileEvents::increment(ProfileEvents::S3CreateMultipartUpload);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3CreateMultipartUpload);

            auto outcome = client_ptr->CreateMultipartUpload(request);
            if (blob_storage_log)
                blob_storage_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadCreate,
                                           dest_bucket, dest_key, /* local_path_ */ {}, /* data_size */ 0,
                                           outcome.IsSuccess() ? nullptr : &outcome.GetError());

            if (outcome.IsSuccess())
            {
                multipart_upload_id = outcome.GetResult().GetUploadId();
                LOG_TRACE(log, "Multipart upload has created. Bucket: {}, Key: {}, Upload id: {}", dest_bucket, dest_key, multipart_upload_id);
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);
                throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
            }
        }

        void completeMultipartUpload()
        {
            if (multipart_upload_aborted)
                return;

            LOG_TRACE(log, "Completing multipart upload. Bucket: {}, Key: {}, Upload_id: {}, Parts: {}", dest_bucket, dest_key, multipart_upload_id, part_tags.size());

            if (part_tags.empty())
                throw Exception(ErrorCodes::S3_ERROR, "Failed to complete multipart upload. No parts have uploaded");

            S3::CompleteMultipartUploadRequest request;
            request.SetBucket(dest_bucket);
            request.SetKey(dest_key);
            request.SetUploadId(multipart_upload_id);

            Aws::S3::Model::CompletedMultipartUpload multipart_upload;
            for (size_t i = 0; i < part_tags.size(); ++i)
            {
                Aws::S3::Model::CompletedPart part;
                multipart_upload.AddParts(part.WithETag(part_tags[i]).WithPartNumber(static_cast<int>(i + 1)));
            }

            request.SetMultipartUpload(multipart_upload);

            size_t max_retries = std::max(request_settings.max_unexpected_write_error_retries, 1UL);
            for (size_t retries = 1;; ++retries)
            {
                ProfileEvents::increment(ProfileEvents::S3CompleteMultipartUpload);
                if (for_disk_s3)
                    ProfileEvents::increment(ProfileEvents::DiskS3CompleteMultipartUpload);

                auto outcome = client_ptr->CompleteMultipartUpload(request);

                if (blob_storage_log)
                    blob_storage_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadComplete,
                                               dest_bucket, dest_key, /* local_path_ */ {}, /* data_size */ 0,
                                               outcome.IsSuccess() ? nullptr : &outcome.GetError());

                if (blob_storage_log)
                    blob_storage_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadComplete,
                                               dest_bucket, dest_key, /* local_path_ */ {}, /* data_size */ 0,
                                               outcome.IsSuccess() ? nullptr : &outcome.GetError());

                if (outcome.IsSuccess())
                {
                    LOG_TRACE(log, "Multipart upload has completed. Bucket: {}, Key: {}, Upload_id: {}, Parts: {}", dest_bucket, dest_key, multipart_upload_id, part_tags.size());
                    break;
                }

                if ((outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) && (retries < max_retries))
                {
                    /// For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests
                    /// BTW, NO_SUCH_UPLOAD is expected error and we shouldn't retry it
                    LOG_INFO(log, "Multipart upload failed with NO_SUCH_KEY error for Bucket: {}, Key: {}, Upload_id: {}, Parts: {}, will retry", dest_bucket, dest_key, multipart_upload_id, part_tags.size());
                    continue; /// will retry
                }
                ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);
                throw S3Exception(
                    outcome.GetError().GetErrorType(),
                    "Message: {}, Key: {}, Bucket: {}, Tags: {}",
                    outcome.GetError().GetMessage(), dest_key, dest_bucket, fmt::join(part_tags.begin(), part_tags.end(), " "));
            }
        }

        void abortMultipartUpload()
        {
            LOG_TRACE(log, "Aborting multipart upload. Bucket: {}, Key: {}, Upload_id: {}", dest_bucket, dest_key, multipart_upload_id);
            S3::AbortMultipartUploadRequest abort_request;
            abort_request.SetBucket(dest_bucket);
            abort_request.SetKey(dest_key);
            abort_request.SetUploadId(multipart_upload_id);
            auto outcome = client_ptr->AbortMultipartUpload(abort_request);
            if (blob_storage_log)
                blob_storage_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadAbort,
                                           dest_bucket, dest_key, /* local_path_ */ {}, /* data_size */ 0,
                                           outcome.IsSuccess() ? nullptr : &outcome.GetError());

            multipart_upload_aborted = true;
        }

        void checkObjectAfterUpload()
        {
            LOG_TRACE(log, "Checking object {} exists after upload", dest_key);
            S3::checkObjectExists(*client_ptr, dest_bucket, dest_key, {}, request_settings, {}, "Immediately after upload");
            LOG_TRACE(log, "Object {} exists after upload", dest_key);
        }

        void performMultipartCopy()
        {
            createMultipartUpload();
            try
            {
                int next_part_number = 0;
                for (std::string & src_key : src_keys)
                {
                    uploadPart(next_part_number++, src_key);
                }
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                // Multipart upload failed because it wasn't possible to schedule all the tasks.
                // To avoid execution of already scheduled tasks we abort MultipartUpload.
                abortMultipartUpload();
                waitForAllBackgroundTasks();
                throw;
            }

            waitForAllBackgroundTasks();
            completeMultipartUpload();
        }

        void uploadPart(size_t part_number, std::string & src_key)
        {
            //LOG_TRACE(log, "Multipart copy {}/{} to Bucket: {}, Key: {}, Upload_id: {} }", src_bucket, src_key, dest_bucket, dest_key, multipart_upload_id);

            if (schedule)
            {
                UploadPartTask * task = nullptr;

                {
                    std::lock_guard lock(bg_tasks_mutex);
                    task = &bg_tasks.emplace_back();
                    task->part_number = part_number;
                    task->key = src_key;
                    ++num_added_bg_tasks;
                }

                /// Notify waiting thread when task finished
                auto task_finish_notify = [this, task]()
                {
                    std::lock_guard lock(bg_tasks_mutex);
                    task->is_finished = true;
                    ++num_finished_bg_tasks;

                    /// Notification under mutex is important here.
                    /// Otherwise, WriteBuffer could be destroyed in between
                    /// Releasing lock and condvar notification.
                    bg_tasks_condvar.notify_one();
                };

                try
                {
                    schedule([this, task, task_finish_notify]()
                    {
                        try
                        {
                            processUploadTask(*task);
                        }
                        catch (...)
                        {
                            task->exception = std::current_exception();
                        }
                        task_finish_notify();
                    }, Priority{});
                }
                catch (...)
                {
                    task_finish_notify();
                    throw;
                }
            }
            else
            {
                UploadPartTask task;
                task.part_number = part_number;
                task.key = src_key;
                processUploadTask(task);
                part_tags.push_back(task.tag);
            }
        }

        void processUploadTask(UploadPartTask & task)
        {
            if (multipart_upload_aborted)
                return; /// Already aborted.

            auto request = makeUploadPartRequest(task.part_number, task.key);
            auto tag = processUploadPartRequest(*request);

            std::lock_guard lock(bg_tasks_mutex); /// Protect bg_tasks from race
            task.tag = tag;
            ++num_finished_parts;
            LOG_TRACE(log, "Finished writing part #{}. Bucket: {}, Key: {}, Upload_id: {}, Etag: {}, Finished parts: {} of {}",
                      task.part_number, dest_key, multipart_upload_id, task.tag, bg_tasks.size(), num_finished_parts, num_parts);
        }

        
        std::unique_ptr<Aws::AmazonWebServiceRequest> makeUploadPartRequest(size_t part_number, std::string & src_key ) const  
        {
            auto request = std::make_unique<S3::UploadPartCopyRequest>();

            /// Make a copy request to copy a part.
            request->SetCopySource(src_bucket + "/" + src_key);
            request->SetBucket(dest_bucket);
            request->SetKey(dest_key);
            request->SetUploadId(multipart_upload_id);
            request->SetPartNumber(static_cast<int>(part_number));
            //request->SetCopySourceRange(fmt::format("bytes={}-{}", part_offset, part_offset + part_size - 1));

            return request;
        }

        String processUploadPartRequest(Aws::AmazonWebServiceRequest & request) 
        {
            auto & req = typeid_cast<S3::UploadPartCopyRequest &>(request);

            ProfileEvents::increment(ProfileEvents::S3UploadPartCopy);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3UploadPartCopy);

            auto outcome = client_ptr->UploadPartCopy(req);
            if (!outcome.IsSuccess())
            {
                abortMultipartUpload();
                throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
            }

            return outcome.GetResult().GetCopyPartResult().GetETag();
        }

        void waitForAllBackgroundTasks()
        {
            if (!schedule)
                return;

            std::unique_lock lock(bg_tasks_mutex);
            /// Suppress warnings because bg_tasks_mutex is actually hold, but tsa annotations do not understand std::unique_lock
            bg_tasks_condvar.wait(lock, [this]() {return TSA_SUPPRESS_WARNING_FOR_READ(num_added_bg_tasks) == TSA_SUPPRESS_WARNING_FOR_READ(num_finished_bg_tasks); });

            auto & tasks = TSA_SUPPRESS_WARNING_FOR_WRITE(bg_tasks);
            for (auto & task : tasks)
            {
                if (task.exception)
                {
                    /// abortMultipartUpload() might be called already, see processUploadPartRequest().
                    /// However if there were concurrent uploads at that time, those part uploads might or might not succeed.
                    /// As a result, it might be necessary to abort a given multipart upload multiple times in order to completely free
                    /// all storage consumed by all parts.
                    abortMultipartUpload();

                    std::rethrow_exception(task.exception);
                }

                part_tags.push_back(task.tag);
            }
        }
    };
}

void concatS3Files(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & src_bucket,
    std::vector<String> & src_keys,
    const String & dest_bucket,
    const String & dest_key,
    const S3Settings::RequestSettings & settings,
    const ReadSettings & read_settings,
    BlobStorageLogWriterPtr blob_storage_log,
    //const std::optional<std::map<String, String>> & object_metadata = std::nullopt,
    ThreadPoolCallbackRunner<void> schedule_ = {},
    bool for_disk_s3 = false)
    {
        CopyHelper helper {s3_client, src_bucket, src_keys, dest_bucket, dest_key, settings, read_settings, schedule_, for_disk_s3, blob_storage_log};
        helper.performCopy();
    }

}

#endif
