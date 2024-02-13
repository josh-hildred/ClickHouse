#include <IO/Archives/ParallelTarArchiveWriter.h>

#include <filesystem>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>

#include <mutex>

#if USE_LIBARCHIVE

// this implemation follows the ZipArchiveWriter implemation as closely as possible.  

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PACK_ARCHIVE;
extern const int LOGICAL_ERROR;
extern const int CANNOT_READ_ALL_DATA;
extern const int UNSUPPORTED_METHOD;
extern const int NOT_IMPLEMENTED;
}

namespace
{
void checkResultCodeImpl(int code, const String & filename)
{
    if (code == ARCHIVE_OK)
        return;
    String message = "LibArchive Code = " + std::to_string(code);
    throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Couldn't pack archive: {}, filename={}", message, quoteString(filename));
}
}

// this is a thin wrapper for libarchive to be able to write the archive to a WriteBuffer
class ParallelTarArchiveWriter::StreamInfo
{
public:
    explicit StreamInfo(std::unique_ptr<WriteBuffer> archive_write_buffer_) : archive_write_buffer(std::move(archive_write_buffer_)) { }


    static ssize_t memory_write([[maybe_unused]] struct archive * a, void * client_data, const void * buff, size_t length)
    {
        auto * stream_info = reinterpret_cast<StreamInfo *>(client_data);
        stream_info->archive_write_buffer->write(reinterpret_cast<const char *>(buff), length);
        return length;
    }

    std::unique_ptr<WriteBuffer> archive_write_buffer;
};

class ParallelTarArchiveWriter::WriteBufferFromArchive : public WriteBufferFromFileBase
{
public:
    WriteBufferFromArchive(std::shared_ptr<ParallelTarArchiveWriter> archive_writer_, const String & filename_, const size_t & size_)
        : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0), archive_writer(archive_writer_), filename(filename_), size(size_)
    {
        archive_writer.lock()->archive_write_mutex.lock();
        startWritingFile();
        a = archive_writer_->getArchive();
        entry = nullptr;
    }


    ~WriteBufferFromArchive() override
    {
        try
        {
            closeFile(/* throw_if_error= */ false);
            endWritingFile();
        }
        catch (...)
        {
            tryLogCurrentException("WriteBufferFromTarArchive");
        }
        archive_writer.lock()->archive_write_mutex.unlock();
    }

    void finalizeImpl() override
    {
        next();
        closeFile(/* throw_if_error=*/true);
        endWritingFile();
    }

    void sync() override { next(); }
    std::string getFileName() const override { return filename; }


private:
    void nextImpl() override
    {
        
        if (!offset())
            return;
        if (entry == nullptr)
            writeEntry();
        ssize_t to_write = offset();
        ssize_t written = archive_write_data(a, working_buffer.begin(), offset());
        if (written != to_write)
        {
            throw Exception(
                ErrorCodes::CANNOT_PACK_ARCHIVE,
                "Couldn't pack tar archive: Failed to write all bytes, {} of {} , filename={}",
                written,
                to_write,
                quoteString(filename));
        }
    }


    void writeEntry()
    {
        expected_size = getSize();
        entry = archive_entry_new();
        archive_entry_set_pathname(entry, filename.c_str());
        archive_entry_set_size(entry, expected_size);
        archive_entry_set_filetype(entry, static_cast<__LA_MODE_T>(0100000));
        archive_entry_set_perm(entry, 0644);
        checkResult(archive_write_header(a, entry));
    }

    size_t getSize() const
    {
        if (size)
            return size;
        else
            return offset();
    }

    void closeFile([[maybe_unused]] bool throw_if_error)
    {
        if (entry)
        {
            archive_entry_free(entry);
            entry = nullptr;
        }
        if (throw_if_error and bytes != expected_size)
        {
            throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Couldn't pack tar archive: Wrote {} of expected {} , filename={}", bytes, expected_size, quoteString(filename)); 
        } 
    }

    void endWritingFile()
    {
        if (auto archive_writer_ptr = archive_writer.lock())
            archive_writer_ptr->endWritingFile();
    }

    void startWritingFile()
    {
        if (auto archive_writer_ptr = archive_writer.lock())
            archive_writer_ptr->startWritingFile();
    }

    void checkResult(int code) { checkResultCodeImpl(code, filename); }

    std::weak_ptr<ParallelTarArchiveWriter> archive_writer;
    const String filename;
    struct archive_entry * entry;
    struct archive * a;
    size_t size;
    size_t expected_size;
};

ParallelTarArchiveWriter::ParallelTarArchiveWriter(const String & path_to_archive_) : path_to_archive(path_to_archive_)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not Implemented");
}

ParallelTarArchiveWriter::ParallelTarArchiveWriter(const String & path_to_archive_, std::unique_ptr<WriteBuffer> archive_write_buffer_)
    : path_to_archive(path_to_archive_)
{
    a = archive_write_new();
    archive_write_set_format_pax_restricted(a);
    //this allows use to write directly to a writer buffer rather than an intermediate buffer in LibArchive
    //archive_write_set_bytes_per_block(a, 0);
    if (archive_write_buffer_)
    {
        stream_info = std::make_unique<StreamInfo>(std::move(archive_write_buffer_));
        archive_write_open2(a, &(*stream_info), nullptr, &StreamInfo::memory_write, nullptr, nullptr);
    }
    else
    {
        archive_write_open_filename(a, path_to_archive.c_str());
    }
}


ParallelTarArchiveWriter::~ParallelTarArchiveWriter()
{
    if (!finalized)
    {
        if (!std::uncaught_exceptions() && std::current_exception() == nullptr)
            chassert(false && "TarArchiveWriter is not finalized in destructor.");
    }

    if (a)
        archive_write_free(a);
}

std::unique_ptr<WriteBufferFromFileBase> ParallelTarArchiveWriter::writeFile(const String & filename, size_t size)
{
    return std::make_unique<WriteBufferFromArchive>(std::static_pointer_cast<ParallelTarArchiveWriter>(shared_from_this()), filename, size);
}

std::unique_ptr<WriteBufferFromFileBase> ParallelTarArchiveWriter::writeFile(const String & filename)
{
    return std::make_unique<WriteBufferFromArchive>(std::static_pointer_cast<ParallelTarArchiveWriter>(shared_from_this()), filename, 0);
}


void ParallelTarArchiveWriter::endWritingFile()
{
}

void ParallelTarArchiveWriter::startWritingFile()
{
}

void ParallelTarArchiveWriter::finalize()
{
    std::lock_guard lock{mutex};
    if (finalized)
        return;
    if (a)
        archive_write_close(a);
    if (stream_info)
    {
        stream_info->archive_write_buffer->finalize();
        stream_info.reset();
    }
    finalized = true;
}

void ParallelTarArchiveWriter::setCompression(const String & compression_method_, int compression_level)
{
    // throw an error unless setCompression is passed the defualt value
    if (compression_method_.size() == 0 and compression_level == -1)
    {
            return;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Tar archives are currenly supported without compression");
}

void ParallelTarArchiveWriter::setPassword([[maybe_unused]] const String & password_)
{
    if (password_ == "")
        return;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Setting a password is not currently supported for tar archives");
}

struct archive * ParallelTarArchiveWriter::getArchive()
{
    std::lock_guard lock{mutex};
    return a;
}
}
#endif
