#pragma once

#include "config.h"

#if USE_LIBARCHIVE
#    include <IO/Archives/ArchiveUtils.h>
#    include <IO/Archives/IArchiveWriter.h>
#    include <IO/Archives/TarArchiveWriter.h>
#    include <IO/WriteBufferFromFileBase.h>
#    include <base/defines.h>


namespace DB
{
class WriteBufferFromFileBase;

/// Interface for writing an archive.
class ParallelArchiveWriter : public IArchiveWriter
{
public:
    /// Constructs an archive that will be written as a file in the local filesystem.
    [[noreturn]] explicit ParallelArchiveWriter(const String & path_to_archive_);

    /// Constructs an archive that will be written as a file in the local filesystem.
    explicit ParallelArchiveWriter(
        const String & path_to_archive_,
        const std::function<std::unique_ptr<WriteBuffer>(size_t)> & write_function_,
        const std::function<void()> & finalize_callback_);

    /// Call finalize() before destructing IArchiveWriter.
    ~ParallelArchiveWriter() override;

    /// Starts writing a file to the archive. The function returns a write buffer,
    /// any data written to that buffer will be compressed and then put to the archive.
    /// You can keep only one such buffer at a time, a buffer returned by previous call
    /// of the function `writeFile()` should be destroyed before next call of `writeFile()`.
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & filename) override;
    /// LibArchive needs to know the size of the file being written. If the file size is not
    /// passed in the the archive writer tries to infer the size by looking at the available
    /// data in the buffer, if next is called before all data is written to the buffer
    /// an exception is thrown.
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & filename, size_t size) override;

    //todo fix this
    bool isWritingFile() const override { return false; }


    /// Finalizes writing of the archive. This function must be always called at the end of writing.
    /// (Unless an error appeared and the archive is in fact no longer needed.)
    void finalize() override;

    /// Sets compression method and level.
    /// Changing them will affect next file in the archive.
    void setCompression(const String & /* compression_method */, int /* compression_level */ = kDefaultCompressionLevel) override;

    /// Sets password. If the password is not empty it will enable encryption in the archive.
    void setPassword(const String & /* password */) override;


    bool supportsWritingInMultipleThreads() override { return true; }

    class ArchiveWriteBuffer;

private:
    const std::function<std::unique_ptr<WriteBuffer>(size_t)> write_function;
    const std::function<void()> finalize_callback;
    String path_to_archive;
};

}

#endif
