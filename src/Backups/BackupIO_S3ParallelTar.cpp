#include<Backups/BackupIO_S3ParallelTar.h>

#if USE_AWS_S3
#include <IO/Archives/createArchiveWriter.h>
#include <IO/Archives/IArchiveWriter.h>
#include <Common/quoteString.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/Context.h>
#include <IO/SharedThreadPools.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/copyS3File.h>
#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/Archives/TarArchiveWriter.h>
#include <Disks/IDisk.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <aws/core/auth/AWSCredentials.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    std::shared_ptr<S3::Client> makeS3Client(
        const S3::URI & s3_uri,
        const String & access_key_id,
        const String & secret_access_key,
        const S3Settings & settings,
        const ContextPtr & context)
    {
        Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key);
        HTTPHeaderEntries headers;
        if (access_key_id.empty())
        {
            credentials = Aws::Auth::AWSCredentials(settings.auth_settings.access_key_id, settings.auth_settings.secret_access_key);
            headers = settings.auth_settings.headers;
        }

        const auto & request_settings = settings.request_settings;
        const Settings & global_settings = context->getGlobalContext()->getSettingsRef();
        const Settings & local_settings = context->getSettingsRef();

        S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
            settings.auth_settings.region,
            context->getRemoteHostFilter(),
            static_cast<unsigned>(global_settings.s3_max_redirects),
            static_cast<unsigned>(global_settings.s3_retry_attempts),
            global_settings.enable_s3_requests_logging,
            /* for_disk_s3 = */ false,
            request_settings.get_request_throttler,
            request_settings.put_request_throttler,
            s3_uri.uri.getScheme());

        client_configuration.endpointOverride = s3_uri.endpoint;
        client_configuration.maxConnections = static_cast<unsigned>(global_settings.s3_max_connections);
        /// Increase connect timeout
        client_configuration.connectTimeoutMs = 10 * 1000;
        /// Requests in backups can be extremely long, set to one hour
        client_configuration.requestTimeoutMs = 60 * 60 * 1000;

        S3::ClientSettings client_settings{
            .use_virtual_addressing = s3_uri.is_virtual_hosted_style,
            .disable_checksum = local_settings.s3_disable_checksum,
            .gcs_issue_compose_request = context->getConfigRef().getBool("s3.gcs_issue_compose_request", false),
        };

        return S3::ClientFactory::instance().create(
            client_configuration,
            client_settings,
            credentials.GetAWSAccessKeyId(),
            credentials.GetAWSSecretKey(),
            settings.auth_settings.server_side_encryption_customer_key_base64,
            settings.auth_settings.server_side_encryption_kms_config,
            std::move(headers),
            S3::CredentialsConfiguration
            {
                settings.auth_settings.use_environment_credentials.value_or(
                    context->getConfigRef().getBool("s3.use_environment_credentials", true)),
                settings.auth_settings.use_insecure_imds_request.value_or(
                    context->getConfigRef().getBool("s3.use_insecure_imds_request", false)),
                settings.auth_settings.expiration_window_seconds.value_or(
                    context->getConfigRef().getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
                settings.auth_settings.no_sign_request.value_or(
                    context->getConfigRef().getBool("s3.no_sign_request", false)),
            });
    }

    /*Aws::Vector<Aws::S3::Model::Object> listObjects(S3::Client & client, const S3::URI & s3_uri, const String & file_name)
    {
        S3::ListObjectsRequest request;
        request.SetBucket(s3_uri.bucket);
        request.SetPrefix(fs::path{s3_uri.key} / file_name);
        request.SetMaxKeys(1);
        auto outcome = client.ListObjects(request);
        if (!outcome.IsSuccess())
            throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
        return outcome.GetResult().GetContents();
    }

    bool isNotFoundError(Aws::S3::S3Errors error)
    {
        return error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND
            || error == Aws::S3::S3Errors::NO_SUCH_KEY;
    }*/
}

BackupWriterS3ParallelTar::BackupWriterS3ParallelTar(const S3::URI & s3_uri_, const String & access_key_id_, const String & secret_access_key_, bool allow_s3_native_copy, const String & storage_class_name, const ReadSettings & read_settings_, const WriteSettings & write_settings_, const ContextPtr & context_)
    : BackupWriterDefault(read_settings_, write_settings_, getLogger("BackupWriterS3"))
    , s3_uri(s3_uri_)
    , data_source_description{DataSourceType::ObjectStorage, ObjectStorageType::S3, MetadataStorageType::None, s3_uri.endpoint, false, false}
    , s3_settings(context_->getStorageS3Settings().getSettings(s3_uri.uri.toString(), context_->getUserName()))
{
    auto & request_settings = s3_settings.request_settings;
    request_settings.updateFromSettings(context_->getSettingsRef());
    request_settings.max_single_read_retries = context_->getSettingsRef().s3_max_single_read_retries; // FIXME: Avoid taking value for endpoint
    request_settings.allow_native_copy = allow_s3_native_copy;
    request_settings.setStorageClassName(storage_class_name);
    client = makeS3Client(s3_uri_, access_key_id_, secret_access_key_, s3_settings, context_);
    if (auto blob_storage_system_log = context_->getBlobStorageLog())
    {
        blob_storage_log = std::make_shared<BlobStorageLogWriter>(blob_storage_system_log);
        if (context_->hasQueryContext())
            blob_storage_log->query_id = context_->getQueryContext()->getCurrentQueryId();
    }
}

//todo do clean up of S3 temp dirs + copy tar parts
BackupWriterS3ParallelTar::~BackupWriterS3ParallelTar()
{

}


//todo cleanup + proper error handling
class BackupWriterS3ParallelTar::ArchiveWriteBuffer : public WriteBufferFromFileBase
{
public:
    ArchiveWriteBuffer(const String & filename_, std::shared_ptr<IArchiveWriter> archive_)
        : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0), filename(filename_), archive(archive_)
    {
    }
    
    void finalizeImpl() override
    {
        next();
        archive->finalize();
    }

    void sync() override { next(); }
    std::string getFileName() const override { return filename; }

private:
    void nextImpl() override
    {
        auto wb = archive->writeFile(filename, offset());
        wb->write(working_buffer.begin(), offset());
    }

    String filename;
    std::shared_ptr<IArchiveWriter> archive;
};

//might need a way to wait until all parts are uploaded    
std::unique_ptr<WriteBuffer> BackupWriterS3ParallelTar::writeFile(const String & file_name)
{
        String tmp_filename = file_name + ".tar";
        const std::lock_guard<std::mutex> lock(keys_mutex);
        keys.emplace_back(fs::path(s3_uri.key) / tmp_filename);
        return std::make_unique<ArchiveWriteBuffer>(file_name, 
            createArchiveWriter(tmp_filename, 
                std::make_unique<WriteBufferFromS3>(
                    client,
                    s3_uri.bucket,
                    fs::path(s3_uri.key) / tmp_filename,
                    DBMS_DEFAULT_BUFFER_SIZE,
                    s3_settings.request_settings,
                    blob_storage_log,
                    std::nullopt,
                    threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupWriterS3"),
                    write_settings)));
}

void BackupWriterS3ParallelTar::copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length)
{
    String tmp_filename = String(path_in_backup + ".tar");
    //todo create a tmp directory for the tar parts

    auto archive = createArchiveWriter(tmp_filename, std::make_unique<WriteBufferFromS3>(
        client,
        s3_uri.bucket,
        fs::path(s3_uri.key) / tmp_filename,
        DBMS_DEFAULT_BUFFER_SIZE,
        s3_settings.request_settings,
        blob_storage_log,
        std::nullopt,
        threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupWriterS3"),
        write_settings));
    
    auto read_buffer = create_read_buffer();

    auto archive_write_buffer = archive->writeFile(path_in_backup, length); 

    if (start_pos)
        read_buffer->seek(start_pos, SEEK_SET);

    auto write_buffer = writeFile(path_in_backup);

    copyData(*read_buffer, *archive_write_buffer, length);
    archive_write_buffer->finalize();
    const std::lock_guard<std::mutex> lock(keys_mutex);
    keys.emplace_back(fs::path(s3_uri.key) / tmp_filename);
}

void BackupWriterS3ParallelTar::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                        bool copy_encrypted, UInt64 start_pos, UInt64 length) 
{
    LOG_TRACE(log, "Copying file {} from disk {} through buffers", src_path, src_disk->getName());

    auto create_read_buffer = [src_disk, src_path, copy_encrypted, settings = read_settings.adjustBufferSize(start_pos + length)]
    {
        if (copy_encrypted)
            return src_disk->readEncryptedFile(src_path, settings);
        else
            return src_disk->readFile(src_path, settings);
    };

    copyDataToFile(path_in_backup, create_read_buffer, start_pos, length);
}

//this might not work due to tar header
void BackupWriterS3ParallelTar::copyFile(const String & destination, const String & source, size_t size)
{
        LOG_TRACE(log, "Copying file inside backup from {} to {} ", source, destination);
    copyS3File(
        client,
        /* src_bucket */ s3_uri.bucket,
        /* src_key= */ fs::path(s3_uri.key) / (source + ".tar"),
        0,
        size,
        s3_uri.bucket,
        fs::path(s3_uri.key) / (destination + ".tar"),
        s3_settings.request_settings,
        read_settings,
        blob_storage_log,
        {},
        threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupWriterS3"));

        const std::lock_guard<std::mutex> lock(keys_mutex);
        keys.emplace_back(fs::path(s3_uri.key) / (destination + ".tar"));
}

std::unique_ptr<ReadBuffer> BackupWriterS3ParallelTar::readFile([[maybe_unused]]const String & file_name, [[maybe_unused]]size_t expected_file_size)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not Implemented");
}

bool BackupWriterS3ParallelTar::fileExists([[maybe_unused]]const String & file_name)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not Implemented");
}
UInt64 BackupWriterS3ParallelTar::getFileSize([[maybe_unused]] const String & file_name)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not Implemented");
}  

void BackupWriterS3ParallelTar::removeFile([[maybe_unused]]const String & file_name)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not Implemented");
}
void BackupWriterS3ParallelTar::removeFiles([[maybe_unused]]const Strings & file_names)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not Implemented");
}
}

#endif

