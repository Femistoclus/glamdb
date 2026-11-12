#include "storage/scalars.h"

#include <arrow/util/decimal.h>

#include <algorithm>
#include <chrono>
#include <format>

namespace glamdb {
namespace {

auto InvalidScalar(std::string message)
{
    return std::unexpected(ErrorDetail{"INVALID_ARGUMENT", std::move(message), RetryHint::FixInput});
}

bool IsDigit(char value)
{
    return value >= '0' && value <= '9';
}

} // namespace

Result<Decimal> ParseDecimal(std::string_view value, int32_t precision, int32_t scale)
{
    if (precision < 1 || precision > 38 || scale < 0 || scale > precision || value.empty() || value.size() > 128) {
        return InvalidScalar("Decimal requires precision 1..38, scale 0..precision and bounded plain decimal text.");
    }
    const bool negative = value.front() == '-';
    if (negative) {
        value.remove_prefix(1);
    }
    const auto point = value.find('.');
    const auto whole = value.substr(0, point);
    const auto fraction = point == value.npos ? std::string_view{} : value.substr(point + 1);
    if (whole.empty() || !std::ranges::all_of(whole, IsDigit) ||
        (point != value.npos && (fraction.empty() || !std::ranges::all_of(fraction, IsDigit))))
    {
        return InvalidScalar("Decimal requires plain decimal text.");
    }
    const auto width = static_cast<size_t>(scale);
    if (fraction.size() > width && !std::ranges::all_of(fraction.substr(width), [](char digit) {
            return digit == '0';
        }))
    {
        return InvalidScalar("Decimal rescaling would lose fractional digits.");
    }
    std::string digits(whole);
    digits.append(fraction.substr(0, width));
    if (fraction.size() < width) {
        digits.append(width - fraction.size(), '0');
    }
    const auto first = digits.find_first_not_of('0');
    digits = first == digits.npos ? "0" : digits.substr(first);
    if (digits.size() > static_cast<size_t>(precision)) {
        return InvalidScalar("Decimal coefficient exceeds its declared precision.");
    }
    if (negative && digits != "0") {
        digits.insert(digits.begin(), '-');
    }
    const auto coefficient = arrow::Decimal128::FromString(digits);
    if (!coefficient.ok()) {
        return InvalidScalar("Decimal coefficient is outside Decimal128.");
    }
    return Decimal{coefficient->high_bits(), coefficient->low_bits(), precision, scale};
}

std::string FormatDecimal(const Decimal& value)
{
    if (value.scale < 0 || value.scale > 38) {
        return {};
    }
    auto text = arrow::Decimal128(value.coefficient_high, value.coefficient_low).ToIntegerString();
    if (value.scale != 0) {
        const auto sign = text.front() == '-' ? size_t{1} : size_t{0};
        const auto scale = static_cast<size_t>(value.scale);
        if (text.size() - sign <= scale) {
            text.insert(sign, scale + 1 - (text.size() - sign), '0');
        }
        text.insert(text.size() - scale, 1, '.');
    }
    return text;
}

Result<Date> ParseDate(std::string_view value)
{
    if (value.size() != 10 || value[4] != '-' || value[7] != '-') {
        return InvalidScalar("Date requires YYYY-MM-DD text.");
    }
    int parts[3]{};
    size_t part = 0;
    for (size_t index = 0; index < value.size(); ++index) {
        if (index == 4 || index == 7) {
            ++part;
        } else if (IsDigit(value[index])) {
            parts[part] = parts[part] * 10 + value[index] - '0';
        } else {
            return InvalidScalar("Date requires YYYY-MM-DD text.");
        }
    }
    const std::chrono::year_month_day day{std::chrono::year{parts[0]},
        std::chrono::month{static_cast<unsigned>(parts[1])},
        std::chrono::day{static_cast<unsigned>(parts[2])}};
    if (parts[0] < 1 || !day.ok()) {
        return InvalidScalar("Date is outside valid calendar days in years 0001..9999.");
    }
    return Date{static_cast<int32_t>(std::chrono::sys_days(day).time_since_epoch().count())};
}

std::string FormatDate(Date value)
{
    const std::chrono::year_month_day day{std::chrono::sys_days{std::chrono::days{value.days_since_epoch}}};
    return std::format("{:04}-{:02}-{:02}",
        static_cast<int>(day.year()),
        static_cast<unsigned>(day.month()),
        static_cast<unsigned>(day.day()));
}

} // namespace glamdb
