#include <IO/Archives/SevenZipArchiveWriter.h>

#if USE_LIBARCHIVE
namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PACK_ARCHIVE;
extern const int NOT_IMPLEMENTED;
}
void SevenZipArchiveWriter::setCompression(const String & compression_method_, int compression_level_)
{
    // throw an error unless setCompression is passed the default value for compression level
    if (compression_level_ != -1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Using the compression_level option is not supported for 7z archives");
    if (compression_method_.empty())
        archive_write_add_filter_lzma(archive);    
    else if(compression_method_ == kGzip)
        archive_write_add_filter_gzip(archive);
    else if (compression_method_ == kBzip2)
        archive_write_add_filter_bzip2(archive);
    
     if (compression_method_ == kNone)
        archive_write_add_filter_none(archive);
    else if (compression_method_ == kLzma)
        archive_write_add_filter_lzma(archive);
    else
        throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Unknown compression method specified for a 7zip archive: {}", compression_method_);
}

void SevenZipArchiveWriter::setFormatAndSettings()
{
    archive_write_set_format_7zip(archive);   
}
}
#endif
