#pragma once

#include "apps/transport/errors.h"
#include "storage/engine.h"

#include <arrow/api.h>
#include <nlohmann/json.hpp>

#include <unordered_map>

namespace glamdb::NTransport {
using TJson = nlohmann::json;
inline constexpr size_t MaxJsonEvents = 10000 * (2 * MaxSchemaFields + 2) + 100;
inline constexpr size_t MaxRequestBytes = 16ULL * 1024 * 1024;
inline constexpr size_t MaxResponseBytes = 16ULL * 1024 * 1024;

struct TFailure : std::exception
{
    ErrorDetail Detail;
    explicit TFailure(ErrorDetail detail)
        : Detail(std::move(detail))
    { }
};
[[noreturn]] void Fail(std::string_view message, std::string_view code = "INVALID_ARGUMENT");
inline void Require(bool condition, std::string_view message, std::string_view code = "INVALID_ARGUMENT")
{
    if (!condition) [[unlikely]] {
        Fail(message, code);
    }
}
template <class T>
T Unwrap(Result<T> result)
{
    if (!result) {
        throw TFailure(result.error());
    }
    return std::move(*result);
}
TJson ParseJson(std::string_view bytes);
// Value of an append body's top-level "rows" member, kept as a compact preorder node list instead of a DOM.
// Object member keys are interned; Payload holds scalar bits, a string offset into Strings, or a container's
// end node index; Size holds a string length or a container's direct child count.
struct TJsonRows
{
    enum class EKind : uint8_t {
        Null,
        Bool,
        Integer,
        Unsigned,
        Float,
        String,
        Array,
        Object,
    };
    struct TNode
    {
        uint64_t Payload = 0;
        uint32_t Size = 0;
        uint32_t Key = 0;
        EKind Kind = EKind::Null;
    };
    bool Present = false;
    std::vector<TNode> Nodes;
    std::string Strings;
    std::unordered_map<std::string, uint32_t> Keys;
    std::vector<const std::string*> KeyNames;
};
// Same syntax, size, depth, event and duplicate-key checks as ParseJson, but the value of a top-level object's
// "rows" member is captured in `rows`; the returned DOM keeps a null placeholder under that key.
TJson ParseJson(std::string_view bytes, TJsonRows& rows);
void CheckKeys(const TJson& value, std::initializer_list<std::string_view> keys);
int64_t ParseInt64(const TJson& value);
SchemaSpec ParseSchema(const TJson& value);
// {"parquet": {"compression": {"codec", "level"}|null, "columns": [{"field", "compression"}]}}.
// Names only; the engine validates fields, codecs and levels.
WriteSettings ParseWriteSettings(const TJson& value);
std::shared_ptr<arrow::RecordBatch> ParseRows(const SchemaSpec& schema, const TJson& rows);
// Builds the same batch, or throws the same failure, as ParseRows over the equivalent DOM.
std::shared_ptr<arrow::RecordBatch> ParseRows(const SchemaSpec& schema, const TJsonRows& rows);
ScanRequest ParseScan(std::string collectionID, const SchemaSpec& schema, const TJson& body);
TJson EncodeSchema(const arrow::Schema& schema);
TJson EncodeRows(const arrow::RecordBatch& batch);
// Scalar metadata bounds use the same exact int64/timestamp wire values as rows.
TJson EncodeCell(const Cell& cell);
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
// The previous per-row DOM encoder: AppendRowsJson must stay byte-identical to it (tests and benchmarks).
size_t AppendRowsJsonReference(const arrow::RecordBatch& batch, std::string& output, const TRowJsonBudget& budget);
TJson EncodeIdentity(const BaseIdentity& identity);
TJson EncodeHealth(const StoreHealth& health);
TJson EncodeError(const ErrorDetail& error);
int ErrorStatus(std::string_view code);
} // namespace glamdb::NTransport
