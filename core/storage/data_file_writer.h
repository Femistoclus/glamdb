#pragma once

#include "core/io/io_attempt.h"

#include <iceberg/result.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace arrow {
class RecordBatch;
}
namespace iceberg {
struct DataFile;
class FileIO;
struct TableMetadata;
}

namespace NGlamDB::NStorage {

struct TDataWriterLimits
{
    std::int64_t MaxInputBytes = 16LL << 20;
    std::int64_t MaxRows = 10000;
    std::int64_t MaxRowGroupBytes = 8LL << 20;
    std::int64_t MaxFileBytes = 32LL << 20;
};

struct TDataWriteResult
{
    std::shared_ptr<iceberg::DataFile> DataFile;
    NIO::TDurableFileReceipt Receipt;
    std::uint32_t FooterBytes;
};

// Batches own their buffers and remain immutable until this synchronous call returns.
iceberg::Result<TDataWriteResult> WriteDataFile(std::shared_ptr<iceberg::FileIO> fileIO,
    std::shared_ptr<NIO::TIOAttempt> attempt,
    const iceberg::TableMetadata& metadata,
    std::string path,
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
    TDataWriterLimits limits = {});

}
