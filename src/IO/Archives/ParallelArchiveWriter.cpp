#include <IO/Archives/ParallelArchiveWriter.h>

#if USE_LIBARCHIVE

#include <filesystem>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>

#include <mutex>


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
    size_t calculate_size(size_t size)
    {   
        /// tar block size is 512. 1 block for header + number of bytes / size blocks + 1 block for rest  
        size_t res = 512 * ((size / 512) + 2);
        return res; 
    }
}

//todo cleanup + proper error handling
class ParallelArchiveWriter::ArchiveWriteBuffer : public WriteBufferFromFileBase
{
public:
    ArchiveWriteBuffer(std::shared_ptr<IArchiveWriter> archive_, std::unique_ptr<WriteBuffer> archive_write_buffer_)
        : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0), archive(archive_), archive_write_buffer(std::move(archive_write_buffer_))
    {
    }
    
    void finalizeImpl() override
    {
        next();
        archive_write_buffer->finalize();
        archive->finalize();
    }

    void sync() override { next(); }
    std::string getFileName() const override { return filename; }

private:
    void nextImpl() override
    {
        archive_write_buffer->write(working_buffer.begin(), offset());
    }

    String filename;
    std::shared_ptr<IArchiveWriter> archive;
    std::unique_ptr<WriteBuffer> archive_write_buffer;
};

ParallelArchiveWriter::ParallelArchiveWriter(const String & path_to_archive_, const std::function<std::unique_ptr<WriteBuffer>(size_t)> & write_function_)
    : write_function(write_function_), path_to_archive(path_to_archive_)
{   
}


ParallelArchiveWriter::~ParallelArchiveWriter()
{
}

std::unique_ptr<WriteBufferFromFileBase> ParallelArchiveWriter::writeFile(const String & filename, size_t size)
{
        String tmp_filename = filename + ".tar";
        auto archive = std::make_shared<TarArchiveWriter>(tmp_filename, write_function(calculate_size(size)));
        return std::make_unique<ArchiveWriteBuffer>(archive, archive->writeFile(filename, size));
}

std::unique_ptr<WriteBufferFromFileBase> ParallelArchiveWriter::writeFile(const String & filename)
{
        String tmp_filename = filename + ".tar";
        auto archive = std::make_shared<TarArchiveWriter>(tmp_filename, write_function(0));
        return std::make_unique<ArchiveWriteBuffer>(archive, archive->writeFile(filename, 0));

}



void ParallelArchiveWriter::finalize()
{
}

void ParallelArchiveWriter::setCompression(const String & compression_method_, int compression_level)
{
    // throw an error unless setCompression is passed the defualt value
    if (compression_method_.size() == 0 and compression_level == -1)
    {
            return;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Tar archives are currenly supported without compression");
}

void ParallelArchiveWriter::setPassword([[maybe_unused]] const String & password_)
{
    if (password_ == "")
        return;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Setting a password is not currently supported for tar archives");
}
}
#endif
