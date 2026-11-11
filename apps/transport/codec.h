#pragma once

#include "apps/transport/errors.h"
#include "storage/engine.h"

#include <arrow/api.h>
#include <nlohmann/json.hpp>

namespace glamdb::NTransport {
using TJson = nlohmann::json;
inline constexpr size_t MaxJsonEvents = 5200000;
inline constexpr size_t MaxRequestBytes = 16ULL * 1024 * 1024;
inline constexpr size_t MaxResponseBytes = 16ULL * 1024 * 1024;

struct TFailure : std::exception
{
    ErrorDetail Detail;
    explicit TFailure(ErrorDetail detail)
        : Detail(std::move(detail))
    { }
};
void Require(bool condition, std::string_view message, std::string_view code = "INVALID_ARGUMENT");
template <class T>
T Unwrap(Result<T> result)
{
    if (!result) {
        throw TFailure(result.error());
    }
    return std::move(*result);
}
TJson ParseJson(std::string_view bytes);
void CheckKeys(const TJson& value, std::initializer_list<std::string_view> keys);
int64_t ParseInt64(const TJson& value);
SchemaSpec ParseSchema(const TJson& value);
std::shared_ptr<arrow::RecordBatch> ParseRows(const SchemaSpec& schema, const TJson& rows);
ScanRequest ParseScan(std::string collectionID, const SchemaSpec& schema, const TJson& body);
TJson EncodeSchema(const arrow::Schema& schema);
TJson EncodeRows(const arrow::RecordBatch& batch);
// Append rows to an unfinished JSON array (initially "["). Returns consumed
// rows; leaves an oversized next row for the next page. The first row may
// exceed the soft target, but never the hard response-envelope allowance.
struct TRowJsonBudget
{
    size_t MaxRows;
    size_t TargetBytes;
    size_t HardBytes;
};
size_t AppendRowsJson(const arrow::RecordBatch& batch, std::string& output, const TRowJsonBudget& budget);
TJson EncodeIdentity(const BaseIdentity& identity);
TJson EncodeHealth(const StoreHealth& health);
TJson EncodeError(const ErrorDetail& error);
int ErrorStatus(std::string_view code);
} // namespace glamdb::NTransport
