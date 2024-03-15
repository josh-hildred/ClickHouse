#include "config.h"

#if USE_AWS_S3
#    include <Backups/BackupIO_S3GlacierMultipart.h>
#    include <Disks/IDisk.h>
#    include <IO/Archives/ParallelArchiveWriter.h>
#    include <IO/HTTPHeaderEntries.h>
#    include <IO/ReadBufferFromS3.h>
#    include <IO/S3/Client.h>
#    include <IO/S3/Credentials.h>
#    include <IO/S3/copyS3File.h>
#    include <IO/SharedThreadPools.h>
#    include <IO/WriteBufferFromS3.h>
#    include <Interpreters/Context.h>
#    include <Common/quoteString.h>
#    include <Common/threadPoolCallbackRunner.h>

#    include <Poco/Util/AbstractConfiguration.h>

#    include <aws/core/auth/AWSCredentials.h>

#    include <filesystem>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/*class BackupWriterS3GlacierMultipart::ParallelWriteBufferToWriteBuffer : public WriteBufferFromFileBase
{
public:
    ParallelWriteBufferToWriteBuffer(size_t size_, std::unique_ptr<WriteBuffer> write_buffer_)
        : WriteBufferFromFileBase(std::max(static_cast<unsigned long long>(size_ + 1025), DBMS_DEFAULT_BUFFER_SIZE), nullptr, 0)
        , size(size_)
        , write_buffer(std::move(write_buffer_))
    {
    }

    void finalizeImpl() override { next(); }

    void sync() override { }
    std::string getFileName() const override { return ""; }

private:
    void nextImpl() override
    {
        if (next_called)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "next() should be called only once for ParallelWriteBufferToWriteBuffer");
        next_called = true;
        if (offset() < size && size != 0)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "WriteBuffer expected {} bytes to be written, but {} was written when next() was called",
                size,
                offset());
        auto to_write = size == 0 ? offset() : size;
        write_buffer->write(working_buffer.begin(), to_write);
    }

    size_t size;
    std::unique_ptr<WriteBuffer> write_buffer;
    bool next_called = false;
};*/

BackupWriterS3GlacierMultipart::BackupWriterS3GlacierMultipart(
    const S3::URI & s3_uri_,
    const String & access_key_id_,
    const String & secret_access_key_,
    bool allow_s3_native_copy,
    const String & storage_class_name,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_,
    const String & archive_name_)
    : BackupWriterS3(
        s3_uri_, access_key_id_, secret_access_key_, allow_s3_native_copy, storage_class_name, read_settings_, write_settings_, context_)
    , archive_name(archive_name_)
{
    archive_write_buffer = std::make_shared<S3MultipartWriter>(
        client,
        s3_uri.bucket,
        fs::path(s3_uri.key) / archive_name,
        s3_settings.request_settings,
        blob_storage_log,
        std::nullopt,
        threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupWriterS3"),
        write_settings);
}

void BackupWriterS3GlacierMultipart::finalize()
{
    const char end_of_archive_blocks[1024] = {0};
    auto wb = archive_write_buffer->writeData(1024);
    wb->write(end_of_archive_blocks, 1024);
    wb->finalize();
    archive_write_buffer->finalize();
}

BackupWriterS3GlacierMultipart::~BackupWriterS3GlacierMultipart()
{
}

std::unique_ptr<WriteBuffer> BackupWriterS3GlacierMultipart::writeData(size_t size)
{
    return archive_write_buffer->writeData(size);
    //return std::make_unique<ParallelWriteBufferToWriteBuffer>(size, wb);
}

}

#endif
