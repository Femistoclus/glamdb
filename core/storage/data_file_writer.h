#pragma once

#include "core/io/io_attempt.h"

#include <arrow/util/type_fwd.h>
#include <iceberg/result.h>
#include <parquet/types.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
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

struct TParquetCompressionSpec
{
    arrow::Compression::type Codec{};
    // Absent: the codec's default level. Parquet does not record levels.
    std::optional<std::int32_t> Level{};
};

struct TParquetColumnSpec
{
    // Exact top-level field name.
    std::string Name;
    std::optional<TParquetCompressionSpec> Compression{};
    // Absent: the file default. Never true for bool columns.
    std::optional<bool> Dictionary{};
    // Encoding of non-dictionary pages; requires the column's dictionary off.
    std::optional<parquet::Encoding::type> Encoding{};
};

// Empty: the writer defaults (zstd at its default level; dictionaries for all
// but bool columns with a 1 MiB page limit and plain fallback; plain values
// otherwise), producing the same files as before write settings existed. The
// footer of every written file is checked against the result: codecs, the
// dictionary page, and data page encodings (dictionary pages may fall back to
// plain).
struct TParquetWriteSpec
{
    std::optional<TParquetCompressionSpec> Compression{};
    std::optional<bool> Dictionary{};
    std::optional<std::int64_t> DictionaryPageSizeLimit{};
    std::vector<TParquetColumnSpec> Columns{};
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
    TDataWriterLimits limits = {},
    TParquetWriteSpec writeSpec = {});

}
