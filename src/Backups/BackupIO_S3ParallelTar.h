#pragma once

#include "config.h"

#if USE_AWS_S3
#include <Backups/BackupIO_Default.h>
#include <Disks/DiskType.h>
#include <IO/S3Common.h>
#include <Storages/StorageS3Settings.h>
#include <Interpreters/Context_fwd.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <mutex>
#include <vector>

namespace DB
{
/// This is an experimental implementation of https://github.com/awslabs/amazon-s3-tar-tool. It looks to reduce the time it takes to write backups as tar archives to s3 by leveraging multi-part uploads for increased parallelizability.
class BackupWriterS3ParallelTar : public BackupWriterDefault
{
public:
    BackupWriterS3ParallelTar(const S3::URI & s3_uri_, const String & access_key_id_, const String & secret_access_key_, bool allow_s3_native_copy, const String & storage_class_name, const ReadSettings & read_settings_, const WriteSettings & write_settings_, const ContextPtr & context_);
    ~BackupWriterS3ParallelTar() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;

    void copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length) override;
    void copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                          bool copy_encrypted, UInt64 start_pos, UInt64 length) override;

    void copyFile(const String & destination, const String & source, size_t size) override;

    void removeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;

    class ArchiveWriteBuffer;
private:
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override;
    //void removeFilesBatch(const Strings & file_names);


    const S3::URI s3_uri;
    const DataSourceDescription data_source_description;
    S3Settings s3_settings;
    std::shared_ptr<S3::Client> client;
    std::optional<bool> supports_batch_delete;
    std::mutex keys_mutex;
    std::vector<String> keys;

    BlobStorageLogWriterPtr blob_storage_log;
};
#endif
}
