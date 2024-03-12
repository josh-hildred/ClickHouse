#pragma once

#include "config.h"

#if USE_AWS_S3
#    include <Backups/BackupIO_S3.h>

namespace DB
{

class BackupWriterS3GlacierMultipart : public BackupWriterS3
{
public:
    BackupWriterS3GlacierMultipart(
        const S3::URI & s3_uri_,
        const String & access_key_id_,
        const String & secret_access_key_,
        bool allow_s3_native_copy,
        const String & storage_class_name,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const ContextPtr & context_,
        const String & archive_name);
    
    ~BackupWriterS3GlacierMultipart() override;
    
    std::unique_ptr<WriteBuffer> writeData(size_t size);
    class ParallelWriteBufferToWriteBuffer;

private:
    String archive_name;
    std::mutex wb_mutex;
    std::shared_ptr<WriteBuffer> archive_write_buffer;
};

}

#endif
