#pragma once

#include "config.h"

#if USE_LIBARCHIVE
#    include <IO/Archives/ArchiveUtils.h>
#    include <IO/Archives/IArchiveWriter.h>
#    include <IO/WriteBufferFromFileBase.h>
#    include <base/defines.h>


namespace DB
{
class WriteBufferFromFileBase;

/// Interface for writing an archive.
class ParallelTarArchiveWriter : public IArchiveWriter
{
public:
    /// Constructs an archive that will be written as a file in the local filesystem.
    [[noreturn]] explicit ParallelTarArchiveWriter(const String & path_to_archive_);

    /// Constructs an archive that will be written as a file in the local filesystem.
    explicit ParallelTarArchiveWriter(const String & path_to_archive_, std::unique_ptr<WriteBuffer> archive_write_buffer_);

    /// Call finalize() before destructing IArchiveWriter.
    ~ParallelTarArchiveWriter() override;

    /// Starts writing a file to the archive. The function returns a write buffer,
    /// any data written to that buffer will be compressed and then put to the archive.
    /// You can keep only one such buffer at a time, a buffer returned by previous call
    /// of the function `writeFile()` should be destroyed before next call of `writeFile()`.
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & filename) override;
    /// LibArchive needs to know the size of the file being written. If the file size is not
    /// passed in the the archive writer tries to infer the size by looking at the available
    /// data in the buffer, if next is called before all data is written to the buffer
    /// an exception is thrown.
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & filename, const size_t & size) override;

    //todo fix this
    bool isWritingFile() const override {return false;}



    /// Finalizes writing of the archive. This function must be always called at the end of writing.
    /// (Unless an error appeared and the archive is in fact no longer needed.)
    void finalize() override;

    static constexpr const int kDefaultCompressionLevel = -1;

    /// Sets compression method and level.
    /// Changing them will affect next file in the archive.
    void setCompression(const String & /* compression_method */, int /* compression_level */ = kDefaultCompressionLevel) override;

    /// Sets password. If the password is not empty it will enable encryption in the archive.
    void setPassword(const String & /* password */) override;


    bool supportsWritingInMultipleThreads() override { return true; }

private:
    class WriteBufferFromArchive;
    class StreamInfo;

    struct archive * getArchive();
    void startWritingFile();
    void endWritingFile();

    String path_to_archive;
    std::unique_ptr<StreamInfo> stream_info TSA_GUARDED_BY(mutex) = nullptr;
    struct archive * a TSA_GUARDED_BY(mutex) = nullptr;
    bool is_writing_file TSA_GUARDED_BY(mutex) = false;
    bool finalized TSA_GUARDED_BY(mutex) = false;
    mutable std::mutex mutex;
    std::mutex archive_write_mutex;
};

}

#endif
