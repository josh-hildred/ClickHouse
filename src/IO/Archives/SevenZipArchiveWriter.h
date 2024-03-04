#pragma once

#include "config.h"

#if USE_LIBARCHIVE

#include <IO/Archives/LibArchiveWriter.h>
namespace DB
{
class SevenZipArchiveWriter : public LibArchiveWriter
{
public:
    explicit SevenZipArchiveWriter(const String & path_to_archive_, std::unique_ptr<WriteBuffer> archive_write_buffer_)
    : LibArchiveWriter(path_to_archive_, std::move(archive_write_buffer_))
    {
        createArchive();
    }
    
    void setCompression(const String & compression_method_, int compression_level_) override;
private:
    void setFormatAndSettings() override;
    
    static constexpr const char kBzip2[] = "bzip2";
    static constexpr const char kLzma[] = "lzma";
    static constexpr const char kGzip[] = "gzip";
    static constexpr const char kNone[] = "none";
};
}
#endif
