#include "core/scan/scan_pruning.h"

#include "core/storage/scan_index.h"

#include <arrow/util/utf8.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <atomic>
#include <cmath>
#include <compare>
#include <cstring>
#include <exception>
#include <vector>

namespace glamdb::detail {
namespace {

std::atomic<bool> PruningEnabled{true};

TStatValue FromLiteral(const Literal& value)
{
    return std::visit(
        [](const auto& item) -> TStatValue {
            using T = std::decay_t<decltype(item)>;
            if constexpr (std::is_same_v<T, TimestampUs>) {
                return item.value;
            } else {
                return item;
            }
        },
        value);
}

// Unordered when the values have different types: nothing is concluded then.
std::partial_ordering Order(const TStatValue& left, const TStatValue& right)
{
    if (left.index() != right.index()) {
        return std::partial_ordering::unordered;
    }
    return std::visit(
        [&](const auto& value) -> std::partial_ordering {
            using T = std::decay_t<decltype(value)>;
            const auto& other = std::get<T>(right);
            if constexpr (std::is_same_v<T, std::string>) {
                // Unsigned byte order, as the predicate compares strings.
                const auto result = value.compare(other);
                return result < 0 ? std::partial_ordering::less
                    : result > 0  ? std::partial_ordering::greater
                                  : std::partial_ordering::equivalent;
            } else {
                return value <=> other;
            }
        },
        left);
}

std::partial_ordering LowerOrder(const TFieldStats& stats, const TStatValue& value)
{
    return stats.Lower ? Order(*stats.Lower, value) : std::partial_ordering::unordered;
}

std::partial_ordering UpperOrder(const TFieldStats& stats, const TStatValue& value)
{
    return stats.Upper ? Order(*stats.Upper, value) : std::partial_ordering::unordered;
}

// No row has a non-NULL value, so every comparison is unknown, never TRUE.
bool NoValues(const TFieldStats& stats)
{
    return stats.Rows <= 0 || (stats.Nulls && *stats.Nulls >= stats.Rows);
}

bool MayCompare(const TFieldStats& stats, CompareOp op, const Literal& literal)
{
    if (NoValues(stats)) {
        return false;
    }
    const auto value = FromLiteral(literal);
    const auto lower = LowerOrder(stats, value);
    const auto upper = UpperOrder(stats, value);
    switch (op) {
        case CompareOp::Eq:
            return !std::is_gt(lower) && !std::is_lt(upper);
        case CompareOp::Ne:
            // Every value equals the literal only when both bounds do.
            return !(std::is_eq(lower) && std::is_eq(upper));
        case CompareOp::Lt:
            return !std::is_gteq(lower);
        case CompareOp::Le:
            return !std::is_gt(lower);
        case CompareOp::Gt:
            return !std::is_lteq(upper);
        case CompareOp::Ge:
            return !std::is_lt(upper);
    }
    return true;
}

template <class T, class TMake>
bool MayContainAny(const TFieldStats& stats, const std::vector<T>& values, TMake&& make)
{
    for (const auto& item : values) {
        const auto value = make(item);
        if (!std::is_gt(LowerOrder(stats, value)) && !std::is_lt(UpperOrder(stats, value))) {
            return true;
        }
    }
    return false;
}

bool MayIn(const TFieldStats& stats, const TBoundInSet& set)
{
    // A NULL list item never makes IN TRUE.
    if (NoValues(stats)) {
        return false;
    }
    switch (set.Type) {
        case FieldType::String:
            return MayContainAny(stats, set.Strings, [](const std::string& value) {
                return TStatValue(value);
            });
        case FieldType::Bool:
            return MayContainAny(stats, set.Bools, [](uint8_t value) {
                return TStatValue(value != 0);
            });
        case FieldType::Int64:
        case FieldType::TimestampUsUtc:
            return MayContainAny(stats, set.Ints, [](int64_t value) {
                return TStatValue(value);
            });
        case FieldType::Float64:
            return MayContainAny(stats, set.Doubles, [](double value) {
                return TStatValue(value);
            });
    }
    return true;
}

std::optional<int64_t> ReadInt64(std::string_view bytes)
{
    if (bytes.size() != 8) {
        return std::nullopt;
    }
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value |= uint64_t{static_cast<uint8_t>(bytes[index])} << (8 * index);
    }
    return static_cast<int64_t>(value);
}

std::optional<TStatValue> DecodeBound(std::string_view bytes, FieldType type)
{
    switch (type) {
        case FieldType::Int64:
        case FieldType::TimestampUsUtc:
            if (const auto value = ReadInt64(bytes)) {
                return *value;
            }
            return std::nullopt;
        case FieldType::Float64:
            if (const auto value = ReadInt64(bytes)) {
                double number = 0;
                std::memcpy(&number, &*value, sizeof(number));
                if (std::isfinite(number)) {
                    return number;
                }
            }
            return std::nullopt;
        case FieldType::Bool:
            if (bytes.size() == 1 && (bytes[0] == 0 || bytes[0] == 1)) {
                return bytes[0] == 1;
            }
            return std::nullopt;
        case FieldType::String:
            // A bound that is not valid UTF-8 cannot order valid values.
            arrow::util::InitializeUTF8();
            if (!arrow::util::ValidateUTF8(bytes)) {
                return std::nullopt;
            }
            return std::string(bytes);
    }
    return std::nullopt;
}

// Statistics that contradict themselves are malformed: nothing may be
// concluded from them, so they prune nothing. `malformed` reports a bound that
// was present but could not be decoded.
TFieldStats Sanitize(TFieldStats stats, bool malformed)
{
    if (malformed) {
        return {.Rows = std::max<int64_t>(stats.Rows, 1)};
    }
    if (stats.Lower && stats.Upper && !std::is_lteq(Order(*stats.Lower, *stats.Upper))) {
        // Lower above upper (or values of different types).
        return {.Rows = std::max<int64_t>(stats.Rows, 1)};
    }
    if (stats.Nulls && *stats.Nulls >= stats.Rows && (stats.Lower || stats.Upper)) {
        // Bounds of non-NULL values in an all-NULL column.
        return {.Rows = std::max<int64_t>(stats.Rows, 1)};
    }
    return stats;
}

int64_t CodePoints(const std::string& bytes)
{
    int64_t count = 0;
    for (const auto byte : bytes) {
        count += (static_cast<uint8_t>(byte) & 0xC0) != 0x80 ? 1 : 0;
    }
    return count;
}

// Returns false when a present value cannot be converted (malformed).
template <class TStatistics, class TConvert>
bool TypedBounds(const parquet::Statistics& statistics, TFieldStats& stats, TConvert&& convert)
{
    const auto* typed = dynamic_cast<const TStatistics*>(&statistics);
    if (typed == nullptr) {
        return true;
    }
    std::optional<TStatValue> lower = convert(typed->min());
    std::optional<TStatValue> upper = convert(typed->max());
    if (!lower || !upper) {
        return false;
    }
    // Truncated (inexact) values are not used even though they still bound.
    if (statistics.is_min_value_exact() != std::optional(false)) {
        stats.Lower = std::move(lower);
    }
    if (statistics.is_max_value_exact() != std::optional(false)) {
        stats.Upper = std::move(upper);
    }
    return true;
}

} // namespace

bool MayMatch(const TBoundPredicate& predicate, std::span<const TFieldStats> fields)
{
    std::vector<bool> stack;
    stack.reserve(predicate.Ops.size());
    const auto field = [&](const TBoundOp& op) -> const TFieldStats* {
        return op.RefIndex < fields.size() ? &fields[op.RefIndex] : nullptr;
    };
    for (const auto& op : predicate.Ops) {
        switch (op.Kind) {
            case EBoundOpKind::PushTrue:
                stack.push_back(true);
                break;
            case EBoundOpKind::PushCompare: {
                const auto* stats = field(op);
                stack.push_back(!stats || MayCompare(*stats, op.Op, op.Value));
                break;
            }
            case EBoundOpKind::PushCompareNull:
                // A comparison with NULL is unknown for every row.
                stack.push_back(false);
                break;
            case EBoundOpKind::PushIn: {
                const auto* stats = field(op);
                stack.push_back(!stats || op.InSetIndex >= predicate.InSets.size() ||
                    MayIn(*stats, predicate.InSets[op.InSetIndex]));
                break;
            }
            case EBoundOpKind::PushIsNull: {
                const auto* stats = field(op);
                stack.push_back(!stats || (stats->Rows > 0 && !(stats->Nulls && *stats->Nulls == 0)));
                break;
            }
            case EBoundOpKind::PushIsNotNull: {
                const auto* stats = field(op);
                stack.push_back(!stats || !NoValues(*stats));
                break;
            }
            case EBoundOpKind::ApplyNot:
                if (stack.empty()) {
                    return true;
                }
                stack.back() = true;
                break;
            case EBoundOpKind::FoldAnd:
            case EBoundOpKind::FoldOr: {
                if (stack.size() < 2) {
                    return true;
                }
                const bool right = stack.back();
                stack.pop_back();
                const bool left = stack.back();
                stack.back() = op.Kind == EBoundOpKind::FoldAnd ? left && right : left || right;
                break;
            }
        }
    }
    return stack.size() != 1 || stack.front();
}

TFieldStats FileFieldStats(const NGlamDB::NStorage::TScanFieldMetrics* metrics,
    int64_t rows,
    FieldType type,
    int32_t truncateLength)
{
    TFieldStats stats{.Rows = rows};
    if (rows < 0) {
        return {.Rows = 1};
    }
    if (!metrics) {
        return stats;
    }
    if (metrics->NullCount && *metrics->NullCount >= 0 && *metrics->NullCount <= rows) {
        stats.Nulls = metrics->NullCount;
    }
    if (type == FieldType::Float64 && metrics->NanCount != 0) {
        return stats;
    }
    bool malformed = false;
    if (metrics->Lower) {
        // A truncated string lower bound is a prefix, still at most every value.
        stats.Lower = DecodeBound(*metrics->Lower, type);
        malformed = malformed || !stats.Lower;
    }
    if (metrics->Upper) {
        auto value = DecodeBound(*metrics->Upper, type);
        malformed = malformed || !value;
        if (value && type == FieldType::String && CodePoints(std::get<std::string>(*value)) >= truncateLength) {
            value.reset();
        }
        stats.Upper = std::move(value);
    }
    return Sanitize(std::move(stats), malformed);
}

TFieldStats
RowGroupFieldStats(const parquet::RowGroupMetaData& group, int column, FieldType type, bool floatsWithoutNaN)
{
    TFieldStats stats{.Rows = group.num_rows()};
    try {
        if (stats.Rows < 0 || column < 0 || column >= group.num_columns()) {
            return {.Rows = 1};
        }
        const auto chunk = group.ColumnChunk(column);
        const auto statistics = chunk->statistics();
        if (chunk->num_values() != stats.Rows || !statistics) {
            return stats;
        }
        if (statistics->HasNullCount() && statistics->null_count() >= 0 && statistics->null_count() <= stats.Rows) {
            stats.Nulls = statistics->null_count();
        }
        if (!statistics->HasMinMax() || statistics->descr() == nullptr) {
            return stats;
        }
        const auto physical = statistics->physical_type();
        const auto order = statistics->descr()->sort_order();
        bool converted = true;
        switch (type) {
            case FieldType::Int64:
            case FieldType::TimestampUsUtc:
                if (physical == parquet::Type::INT64 && order == parquet::SortOrder::SIGNED) {
                    converted = TypedBounds<parquet::Int64Statistics>(*statistics, stats, [](int64_t value) {
                        return TStatValue(value);
                    });
                }
                break;
            case FieldType::Float64:
                if (physical == parquet::Type::DOUBLE && order == parquet::SortOrder::SIGNED && floatsWithoutNaN) {
                    converted = TypedBounds<parquet::DoubleStatistics>(*statistics, stats, [](double value) {
                        return std::isfinite(value) ? std::optional<TStatValue>(value) : std::nullopt;
                    });
                }
                break;
            case FieldType::Bool:
                if (physical == parquet::Type::BOOLEAN) {
                    converted = TypedBounds<parquet::BoolStatistics>(*statistics, stats, [](bool value) {
                        return TStatValue(value);
                    });
                }
                break;
            case FieldType::String:
                if (physical == parquet::Type::BYTE_ARRAY && order == parquet::SortOrder::UNSIGNED) {
                    converted = TypedBounds<parquet::ByteArrayStatistics>(*statistics,
                        stats,
                        [](const parquet::ByteArray& value) {
                            return DecodeBound(std::string_view(reinterpret_cast<const char*>(value.ptr), value.len),
                                FieldType::String);
                        });
                }
                break;
        }
        return Sanitize(std::move(stats), !converted);
    } catch (const std::exception&) {
        return {.Rows = std::max<int64_t>(stats.Rows, 1)};
    }
}

void SetScanPruning(bool enabled)
{
    PruningEnabled.store(enabled);
}

bool ScanPruningEnabled()
{
    return PruningEnabled.load();
}

} // namespace glamdb::detail
