#include "apps/transport/codec.h"

#include <cstdio>
#include <type_traits>

namespace glamdb::NTransport {
namespace {
std::string FormatTimestamp(int64_t micros)
{
    const std::chrono::sys_time<std::chrono::microseconds> instant{std::chrono::microseconds(micros)};
    const auto day = std::chrono::floor<std::chrono::days>(instant);
    const std::chrono::year_month_day date{day};
    const std::chrono::hh_mm_ss time{instant - day};
    char buffer[40]{};
    std::snprintf(buffer,
        sizeof(buffer),
        "%04d-%02u-%02uT%02lld:%02lld:%02lld.%06lldZ",
        static_cast<int>(date.year()),
        static_cast<unsigned>(date.month()),
        static_cast<unsigned>(date.day()),
        static_cast<long long>(time.hours().count()),
        static_cast<long long>(time.minutes().count()),
        static_cast<long long>(time.seconds().count()),
        static_cast<long long>(time.subseconds().count()));
    return buffer;
}
TJson EncodeRow(const arrow::RecordBatch& batch, int64_t row)
{
    auto item = TJson::object();
    for (int column = 0; column < batch.num_columns(); ++column) {
        const auto& array = *batch.column(column);
        auto& value = item[batch.schema()->field(column)->name()];
        if (array.IsNull(row)) {
            value = nullptr;
            continue;
        }
        switch (array.type_id()) {
            case arrow::Type::STRING:
                value = static_cast<const arrow::StringArray&>(array).GetString(row);
                break;
            case arrow::Type::BOOL:
                value = static_cast<const arrow::BooleanArray&>(array).Value(row);
                break;
            case arrow::Type::INT64:
                value = std::to_string(static_cast<const arrow::Int64Array&>(array).Value(row));
                break;
            case arrow::Type::DOUBLE:
                value = static_cast<const arrow::DoubleArray&>(array).Value(row);
                break;
            case arrow::Type::TIMESTAMP:
                value = FormatTimestamp(static_cast<const arrow::TimestampArray&>(array).Value(row));
                break;
            default:
                throw TFailure({"INTERNAL", "unsupported output type", RetryHint::OperatorAction});
        }
    }
    return item;
}
} // namespace
TJson EncodeCell(const Cell& cell)
{
    return std::visit(
        [](const auto& value) -> TJson {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                return nullptr;
            } else if constexpr (std::is_same_v<T, int64_t>) {
                return std::to_string(value);
            } else if constexpr (std::is_same_v<T, TimestampUs>) {
                return FormatTimestamp(value.value);
            } else {
                return value;
            }
        },
        cell);
}
TJson EncodeRows(const arrow::RecordBatch& batch)
{
    auto rows = TJson::array();
    size_t bytes = 0;
    for (int64_t row = 0; row < batch.num_rows(); ++row) {
        auto item = EncodeRow(batch, row);
        bytes += item.dump().size() + 1;
        Require(bytes <= MaxResponseBytes, "JSON response exceeds transport limit", "INPUT_TOO_LARGE");
        rows.push_back(std::move(item));
    }
    return rows;
}
size_t AppendRowsJson(const arrow::RecordBatch& batch, std::string& output, const TRowJsonBudget& budget)
{
    size_t count = 0;
    const auto bound = std::min(budget.MaxRows, static_cast<size_t>(batch.num_rows()));
    for (; count < bound; ++count) {
        const auto row = EncodeRow(batch, static_cast<int64_t>(count)).dump();
        const bool comma = output.size() > 1;
        const auto size = output.size() + row.size() + static_cast<size_t>(comma);
        if (comma && size > budget.TargetBytes) {
            break;
        }
        Require(size <= budget.HardBytes, "individual JSON row exceeds the response byte limit");
        if (comma) {
            output += ',';
        }
        output += row;
    }
    return count;
}
} // namespace glamdb::NTransport
