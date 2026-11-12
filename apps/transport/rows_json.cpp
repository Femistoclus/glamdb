#include "apps/transport/codec.h"
#include "apps/transport/utf8.h"

#include <algorithm>
#include <charconv>
#include <cmath>
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

// Direct encoder: writes exactly what EncodeRow(batch, row).dump() produces.
// nlohmann objects are std::map, so members appear in byte order of their
// names and a repeated name keeps its last column's value.
class TRowWriter
{
public:
    explicit TRowWriter(const arrow::RecordBatch& batch)
    {
        std::vector<std::pair<std::string_view, int>> names;
        for (int column = 0; column < batch.num_columns(); ++column) {
            switch (batch.column(column)->type_id()) {
                case arrow::Type::STRING:
                case arrow::Type::BOOL:
                case arrow::Type::INT64:
                case arrow::Type::DOUBLE:
                case arrow::Type::TIMESTAMP:
                    break;
                default:
                    // EncodeRow encodes NULL cells of such columns and rejects values.
                    Supported_ = false;
                    return;
            }
            names.emplace_back(batch.schema()->field(column)->name(), column);
        }
        std::ranges::stable_sort(names, {}, &std::pair<std::string_view, int>::first);
        for (size_t i = 0; i < names.size(); ++i) {
            if (i + 1 < names.size() && names[i + 1].first == names[i].first) {
                continue;
            }
            const auto& array = *batch.column(names[i].second);
            TColumn column{.Array = &array};
            switch (array.type_id()) {
                case arrow::Type::STRING:
                    column.Kind = EKind::String;
                    break;
                case arrow::Type::BOOL:
                    column.Kind = EKind::Bool;
                    break;
                case arrow::Type::INT64:
                    column.Kind = EKind::Int64;
                    column.Int64 = static_cast<const arrow::Int64Array&>(array).raw_values();
                    break;
                case arrow::Type::DOUBLE:
                    column.Kind = EKind::Double;
                    column.Double = static_cast<const arrow::DoubleArray&>(array).raw_values();
                    break;
                case arrow::Type::TIMESTAMP:
                    column.Kind = EKind::Timestamp;
                    column.Int64 = static_cast<const arrow::TimestampArray&>(array).raw_values();
                    break;
                default:
                    break;
            }
            column.Prefix = Columns_.empty() ? "{" : ",";
            if (!AppendEscaped(column.Prefix, names[i].first)) {
                Supported_ = false;
                return;
            }
            column.Prefix += ':';
            Columns_.push_back(std::move(column));
        }
    }
    bool Supported() const
    {
        return Supported_;
    }
    // Returns false, possibly after partial output, when a string is not valid UTF-8.
    bool Append(int64_t row, std::string& output) const
    {
        if (Columns_.empty()) {
            output += "{}";
            return true;
        }
        for (const auto& column : Columns_) {
            output += column.Prefix;
            if (column.Array->IsNull(row)) {
                output += "null";
                continue;
            }
            switch (column.Kind) {
                case EKind::String:
                    if (!AppendEscaped(output, static_cast<const arrow::StringArray&>(*column.Array).GetView(row))) {
                        return false;
                    }
                    break;
                case EKind::Bool:
                    output += static_cast<const arrow::BooleanArray&>(*column.Array).Value(row) ? "true" : "false";
                    break;
                case EKind::Int64: {
                    char buffer[24];
                    buffer[0] = '"';
                    auto* end = std::to_chars(buffer + 1, buffer + sizeof(buffer), column.Int64[row]).ptr;
                    *end++ = '"';
                    output.append(buffer, static_cast<size_t>(end - buffer));
                    break;
                }
                case EKind::Double: {
                    const auto value = column.Double[row];
                    if (!std::isfinite(value)) {
                        output += "null";
                        break;
                    }
                    std::array<char, 64> buffer{};
                    const auto* end = nlohmann::detail::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
                    output.append(buffer.data(), static_cast<size_t>(end - buffer.data()));
                    break;
                }
                case EKind::Timestamp:
                    AppendTimestamp(output, column.Int64[row]);
                    break;
            }
        }
        output += '}';
        return true;
    }

private:
    enum class EKind : uint8_t {
        String,
        Bool,
        Int64,
        Double,
        Timestamp,
    };
    struct TColumn
    {
        const arrow::Array* Array = nullptr;
        const int64_t* Int64 = nullptr;
        const double* Double = nullptr;
        EKind Kind = EKind::String;
        std::string Prefix{};
    };
    std::vector<TColumn> Columns_;
    bool Supported_ = true;

    static bool AppendEscaped(std::string& output, std::string_view text)
    {
        output += '"';
        size_t run = 0;
        for (size_t i = 0; i < text.size();) {
            const auto byte = static_cast<uint8_t>(text[i]);
            if (byte >= 0x20 && byte != '"' && byte != '\\' && byte < 0x80) {
                ++i;
                continue;
            }
            if (byte >= 0x80) {
                const auto length = Utf8SequenceLength(text, i);
                if (length == 0) {
                    return false;
                }
                i += length;
                continue;
            }
            output.append(text.data() + run, i - run);
            switch (byte) {
                case '\b':
                    output += "\\b";
                    break;
                case '\t':
                    output += "\\t";
                    break;
                case '\n':
                    output += "\\n";
                    break;
                case '\f':
                    output += "\\f";
                    break;
                case '\r':
                    output += "\\r";
                    break;
                case '"':
                    output += "\\\"";
                    break;
                case '\\':
                    output += "\\\\";
                    break;
                default: {
                    constexpr std::string_view Hex = "0123456789abcdef";
                    const char escaped[] = {'\\', 'u', '0', '0', Hex[byte >> 4], Hex[byte & 0xF]};
                    output.append(escaped, sizeof(escaped));
                    break;
                }
            }
            run = ++i;
        }
        output.append(text.data() + run, text.size() - run);
        output += '"';
        return true;
    }
    static void AppendTimestamp(std::string& output, int64_t micros)
    {
        constexpr int64_t DayMicros = 86400000000LL;
        // Years 0000..9999; FormatTimestamp covers everything else.
        if (micros < -62167219200000000LL || micros > 253402300799999999LL) {
            output += '"';
            output += FormatTimestamp(micros);
            output += '"';
            return;
        }
        auto days = micros / DayMicros;
        auto rest = micros % DayMicros;
        if (rest < 0) {
            rest += DayMicros;
            --days;
        }
        const std::chrono::year_month_day date{std::chrono::sys_days{std::chrono::days{days}}};
        const auto year = static_cast<int>(date.year());
        const auto put = [](char* at, int64_t value, int width) {
            for (int i = width - 1; i >= 0; --i) {
                at[i] = static_cast<char>('0' + value % 10);
                value /= 10;
            }
        };
        char buffer[29] = "\"0000-00-00T00:00:00.000000Z";
        put(buffer + 1, year, 4);
        put(buffer + 6, static_cast<unsigned>(date.month()), 2);
        put(buffer + 9, static_cast<unsigned>(date.day()), 2);
        const auto seconds = rest / 1000000;
        put(buffer + 12, seconds / 3600, 2);
        put(buffer + 15, seconds / 60 % 60, 2);
        put(buffer + 18, seconds % 60, 2);
        put(buffer + 21, rest % 1000000, 6);
        buffer[28] = '"';
        output.append(buffer, sizeof(buffer));
    }
};
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
    size_t bytes = 2;
    for (int64_t row = 0; row < batch.num_rows(); ++row) {
        auto item = EncodeRow(batch, row);
        bytes += item.dump().size() + static_cast<size_t>(row != 0);
        Require(bytes <= MaxResponseBytes, "JSON response exceeds transport limit", "INPUT_TOO_LARGE");
        rows.push_back(std::move(item));
    }
    return rows;
}
size_t AppendRowsJsonReference(const arrow::RecordBatch& batch, std::string& output, const TRowJsonBudget& budget)
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
size_t AppendRowsJson(const arrow::RecordBatch& batch, std::string& output, const TRowJsonBudget& budget)
{
    const auto bound = std::min(budget.MaxRows, static_cast<size_t>(batch.num_rows()));
    if (bound == 0) {
        return 0;
    }
    const TRowWriter writer(batch);
    if (!writer.Supported()) {
        return AppendRowsJsonReference(batch, output, budget);
    }
    size_t count = 0;
    for (; count < bound; ++count) {
        const auto start = output.size();
        const bool comma = start > 1;
        if (comma) {
            output += ',';
        }
        if (!writer.Append(static_cast<int64_t>(count), output)) {
            // Invalid UTF-8: the reference encoder raises the serializer's exact error.
            output.resize(start);
            const auto row = EncodeRow(batch, static_cast<int64_t>(count)).dump();
            if (comma) {
                output += ',';
            }
            output += row;
        }
        const auto size = output.size();
        if (comma && size > budget.TargetBytes) {
            output.resize(start);
            break;
        }
        if (size > budget.HardBytes) {
            output.resize(start);
            Fail("individual JSON row exceeds the response byte limit");
        }
    }
    return count;
}
} // namespace glamdb::NTransport
