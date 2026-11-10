#pragma once

#include "storage/types.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace arrow {
class RecordBatch;
class Schema;
} // namespace arrow

namespace glamdb {

enum class SnapshotSelectorKind : uint8_t { Latest, Id, InitialEmpty };

// Latest is resolved exactly once when a scan opens. InitialEmpty is the
// fixed-schema view before the first snapshot and stays empty after writes.
struct SnapshotSelector
{
    SnapshotSelectorKind kind = SnapshotSelectorKind::Latest;
    int64_t snapshot_id = 0;

    static SnapshotSelector Latest()
    {
        return {};
    }
    static SnapshotSelector Id(int64_t id)
    {
        return {SnapshotSelectorKind::Id, id};
    }
    static SnapshotSelector InitialEmpty()
    {
        return {SnapshotSelectorKind::InitialEmpty, 0};
    }
    bool operator==(const SnapshotSelector&) const = default;
};

enum class CompareOp : uint8_t { Eq, Ne, Lt, Le, Gt, Ge };

// A literal must have exactly the field type: no implicit casts.
using Literal = std::variant<std::string, bool, int64_t, double, TimestampUs>;

// Supported: TRUE and one comparison of a field with a literal. A NULL cell
// compares as UNKNOWN, so it is never returned. Strings use binary UTF-8 order.
class Predicate
{
public:
    enum class Kind : uint8_t { True, Compare };

    Predicate() = default;

    static Predicate True()
    {
        return {};
    }
    static Predicate Compare(std::string field, CompareOp op, Literal value)
    {
        Predicate predicate;
        predicate.kind_ = Kind::Compare;
        predicate.field_ = std::move(field);
        predicate.op_ = op;
        predicate.value_ = std::move(value);
        return predicate;
    }

    Kind kind() const
    {
        return kind_;
    }
    const std::string& field() const
    {
        return field_;
    }
    CompareOp op() const
    {
        return op_;
    }
    const Literal& value() const
    {
        return value_;
    }

private:
    Kind kind_ = Kind::True;
    std::string field_;
    CompareOp op_ = CompareOp::Eq;
    Literal value_;
};

struct ScanRequest
{
    std::string collection_id{};
    SnapshotSelector selector{};
    // nullopt selects every field in schema order.
    std::optional<std::vector<std::string>> projection{};
    Predicate predicate = Predicate::True();
    std::optional<int64_t> limit{};
    int64_t batch_rows = 65536;
};

// Immutable before the first Next(), including empty views and limit=0.
struct ScanInfo
{
    std::shared_ptr<const arrow::Schema> output_schema{};
    BaseIdentity identity{};
    // Latest resolves to Id(snapshot) or InitialEmpty when no snapshot exists.
    SnapshotSelector selector_resolved{};
};

class ScanCursor
{
public:
    virtual ~ScanCursor() = default;

    virtual const ScanInfo& Info() const = 0;
    // nullptr marks the end of the stream; a read failure is an error, never EOS.
    virtual Result<std::shared_ptr<arrow::RecordBatch>> Next() = 0;
    virtual void Cancel() = 0;
};

} // namespace glamdb
