#include "core/scan/predicate.h"

#include "core/api/canonical.h"
#include "core/engine/storage_mapping.h"

#include <arrow/api.h>

#include <cmath>
#include <string>
#include <string_view>

namespace glamdb::detail {
namespace {

constexpr int64_t MinTimestampUs = -62135596800000000LL;
constexpr int64_t MaxTimestampUs = 253402300799999999LL;

ErrorDetail InvalidPredicate(std::string message)
{
    return {"INVALID_ARGUMENT", std::move(message), RetryHint::FixInput};
}

ErrorDetail Internal(std::string_view message)
{
    return {"INTERNAL", BoundedDiagnostic(message), RetryHint::OperatorAction};
}

Result<Literal> CheckLiteral(const FieldSpec& field, const Literal& value)
{
    switch (field.type) {
        case FieldType::String: {
            const auto* text = std::get_if<std::string>(&value);
            if (text == nullptr || !ValidUtf8(*text)) {
                break;
            }
            return value;
        }
        case FieldType::Bool:
            if (!std::holds_alternative<bool>(value)) {
                break;
            }
            return value;
        case FieldType::Int64:
            if (!std::holds_alternative<int64_t>(value)) {
                break;
            }
            return value;
        case FieldType::Float64: {
            const auto* number = std::get_if<double>(&value);
            if (number == nullptr || !std::isfinite(*number)) {
                break;
            }
            // Stored floats normalize negative zero, so the literal does too.
            return Literal{*number == 0.0 ? 0.0 : *number};
        }
        case FieldType::TimestampUsUtc: {
            const auto* timestamp = std::get_if<TimestampUs>(&value);
            if (timestamp == nullptr || timestamp->value < MinTimestampUs || timestamp->value > MaxTimestampUs) {
                break;
            }
            return value;
        }
    }
    return std::unexpected(InvalidPredicate("literal type or value does not match field " + field.name));
}

template <class T>
bool Matches(CompareOp op, const T& left, const T& right)
{
    switch (op) {
        case CompareOp::Eq:
            return left == right;
        case CompareOp::Ne:
            return left != right;
        case CompareOp::Lt:
            return left < right;
        case CompareOp::Le:
            return left <= right;
        case CompareOp::Gt:
            return left > right;
        case CompareOp::Ge:
            return left >= right;
    }
    return false;
}

template <class TArray, class TValue>
std::vector<int64_t> SelectTyped(const arrow::Array& column, CompareOp op, const TValue& literal)
{
    const auto& array = static_cast<const TArray&>(column);
    std::vector<int64_t> rows;
    for (int64_t row = 0; row < array.length(); ++row) {
        if (!array.IsNull(row) && Matches<TValue>(op, array.GetView(row), literal)) {
            rows.push_back(row);
        }
    }
    return rows;
}

template <class TArray, class TBuilder>
Result<std::shared_ptr<arrow::Array>> TakeTyped(const arrow::Array& column, std::span<const int64_t> rows)
{
    const auto& array = static_cast<const TArray&>(column);
    TBuilder builder(column.type(), arrow::default_memory_pool());
    if (const auto status = builder.Reserve(static_cast<int64_t>(rows.size())); !status.ok()) {
        return std::unexpected(Internal("cannot reserve selected rows: " + status.ToString()));
    }
    for (const auto row : rows) {
        if (row < 0 || row >= array.length()) {
            return std::unexpected(Internal("selected row is outside the batch"));
        }
        const auto status = array.IsNull(row) ? builder.AppendNull() : builder.Append(array.GetView(row));
        if (!status.ok()) {
            return std::unexpected(Internal("cannot copy selected row: " + status.ToString()));
        }
    }
    std::shared_ptr<arrow::Array> result;
    if (const auto status = builder.Finish(&result); !status.ok()) {
        return std::unexpected(Internal("cannot finish selected rows: " + status.ToString()));
    }
    return result;
}

} // namespace

Result<TBoundPredicate> BindPredicate(const Predicate& predicate, const SchemaSpec& schema)
{
    if (predicate.kind() == Predicate::Kind::True) {
        return TBoundPredicate{};
    }
    if (predicate.kind() != Predicate::Kind::Compare || predicate.op() > CompareOp::Ge) {
        return std::unexpected(InvalidPredicate("unsupported predicate"));
    }
    for (size_t index = 0; index < schema.fields.size(); ++index) {
        const auto& field = schema.fields[index];
        if (field.name != predicate.field()) {
            continue;
        }
        auto literal = CheckLiteral(field, predicate.value());
        if (!literal) {
            return std::unexpected(literal.error());
        }
        return TBoundPredicate{Predicate::Kind::Compare, index, field.type, predicate.op(), std::move(*literal)};
    }
    return std::unexpected(InvalidPredicate("predicate field does not exist"));
}

Result<std::vector<int64_t>> SelectRows(const TBoundPredicate& predicate, const arrow::Array& column)
{
    if (predicate.Kind == Predicate::Kind::True) {
        std::vector<int64_t> rows(static_cast<size_t>(column.length()));
        for (int64_t row = 0; row < column.length(); ++row) {
            rows[static_cast<size_t>(row)] = row;
        }
        return rows;
    }
    if (!column.type()->Equals(*MakeArrowType(predicate.Type))) {
        return std::unexpected(Internal("predicate column has an unexpected Arrow type"));
    }
    switch (predicate.Type) {
        case FieldType::String:
            return SelectTyped<arrow::StringArray, std::string_view>(column,
                predicate.Op,
                std::string_view(std::get<std::string>(predicate.Value)));
        case FieldType::Bool:
            return SelectTyped<arrow::BooleanArray, bool>(column, predicate.Op, std::get<bool>(predicate.Value));
        case FieldType::Int64:
            return SelectTyped<arrow::Int64Array, int64_t>(column, predicate.Op, std::get<int64_t>(predicate.Value));
        case FieldType::Float64:
            return SelectTyped<arrow::DoubleArray, double>(column, predicate.Op, std::get<double>(predicate.Value));
        case FieldType::TimestampUsUtc:
            return SelectTyped<arrow::TimestampArray, int64_t>(column,
                predicate.Op,
                std::get<TimestampUs>(predicate.Value).value);
    }
    return std::unexpected(Internal("unsupported predicate type"));
}

Result<std::shared_ptr<arrow::Array>> TakeRows(const arrow::Array& column, std::span<const int64_t> rows)
{
    switch (column.type_id()) {
        case arrow::Type::STRING:
            return TakeTyped<arrow::StringArray, arrow::StringBuilder>(column, rows);
        case arrow::Type::BOOL:
            return TakeTyped<arrow::BooleanArray, arrow::BooleanBuilder>(column, rows);
        case arrow::Type::INT64:
            return TakeTyped<arrow::Int64Array, arrow::Int64Builder>(column, rows);
        case arrow::Type::DOUBLE:
            return TakeTyped<arrow::DoubleArray, arrow::DoubleBuilder>(column, rows);
        case arrow::Type::TIMESTAMP:
            return TakeTyped<arrow::TimestampArray, arrow::TimestampBuilder>(column, rows);
        default:
            return std::unexpected(Internal("unsupported column type for row selection"));
    }
}

} // namespace glamdb::detail
