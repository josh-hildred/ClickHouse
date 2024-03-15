#pragma once

#include "config.h"

#if USE_AWS_S3

#include <base/types.h>
#include <IO/WriteBufferFromS3TaskTracker.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <Storages/StorageS3Settings.h>
#include <Common/threadPoolCallbackRunner.h>
#include <IO/S3/BlobStorageLogWriter.h>

#include <memory>
#include <vector>
#include <list>

namespace DB
{

class S3MultipartWriter
{
public:
    S3MultipartWriter(
        std::shared_ptr<const S3::Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        const S3Settings::RequestSettings & request_settings_,
        BlobStorageLogWriterPtr blob_log_,
        std::optional<std::map<String, String>> object_metadata_ = std::nullopt,
        ThreadPoolCallbackRunner<void> schedule_ = {},
        const WriteSettings & write_settings_ = {});

    ~S3MultipartWriter();

    class WriteBufferWithMemoryCallback;

    std::unique_ptr<WriteBuffer> writeData(size_t size);
    void finalize();
private:
    String getVerboseLogDetails() const;
    String getShortLogDetails() const;

    struct PartData;
    //void hidePartialData();
   // void allocateFirstBuffer();
   // void reallocateFirstBuffer();
    //void detachBuffer();
    //void allocateBuffer();
    //void setFakeBufferWhenPreFinalized();

    S3::UploadPartRequest getUploadRequest(size_t part_number, PartData & data);
    void writePart(PartData && data);
    void writeMultipartUpload();
    void createMultipartUpload();
    void completeMultipartUpload();
    void abortMultipartUpload();
    void tryToAbortMultipartUpload();

    S3::PutObjectRequest getPutRequest(PartData & data);

    const String bucket;
    const String key;
    const S3Settings::RequestSettings request_settings;
    const S3Settings::RequestSettings::PartUploadSettings & upload_settings;
    const WriteSettings write_settings;
    const std::shared_ptr<const S3::Client> client_ptr;
    const std::optional<std::map<String, String>> object_metadata;
    LoggerPtr log = getLogger("WriteBufferFromS3");
    LogSeriesLimiterPtr limitedLog = std::make_shared<LogSeriesLimiter>(log, 1, 5);


    /// Upload in S3 is made in parts.
    /// We initiate upload, then upload each part and get ETag as a response, and then finalizeImpl() upload with listing all our parts.
    String multipart_upload_id;
    std::deque<String> multipart_tags;
    bool multipart_upload_finished = false;

    /// Track that prefinalize() is called only once
    bool is_prefinalized = false;

    /// First fully filled buffer has to be delayed
    /// There are two ways after:
    /// First is to call prefinalize/finalize, which leads to single part upload
    /// Second is to write more data, which leads to multi part upload
    std::mutex detacted_part_mutex;
    std::deque<PartData> detached_part_data;
    char fake_buffer_when_prefinalized[1] = {};

    //for now write small files serially into a single buffer
    std::mutex small_file_buffer_mutex;
    Memory<> small_file_buffer;
    size_t small_file_buffer_offset = 0;

    /// offset() and count() are unstable inside nextImpl
    /// For example nextImpl changes position hence offset() and count() is changed
    /// This vars are dedicated to store information about sizes when offset() and count() are unstable
    size_t total_size = 0;
    size_t hidden_size = 0;

    std::unique_ptr<TaskTracker> task_tracker;

    BlobStorageLogWriterPtr blob_log;
};

}

#endif
