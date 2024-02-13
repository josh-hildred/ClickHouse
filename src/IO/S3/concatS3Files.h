#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/StorageS3Settings.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <base/types.h>
#include <functional>
#include <memory>


namespace DB
{

void concatS3Files(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & src_bucket,
    std::vector<String> & src_keys,
    const String & dest_bucket,
    const String & dest_key,
    const S3Settings::RequestSettings & settings,
    const ReadSettings & read_settings,
    BlobStorageLogWriterPtr blob_storage_log,
    //const std::optional<std::map<String, String>> & object_metadata = std::nullopt,
    ThreadPoolCallbackRunner<void> schedule_ = {},
    bool for_disk_s3 = false);

}

#endif