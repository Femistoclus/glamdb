#pragma once

#include "storage/types.h"

#include <string_view>

namespace glamdb {

// Plain decimal text, exact rescaling only; no exponent, whitespace or binary float.
Result<Decimal> ParseDecimal(std::string_view value, int32_t precision, int32_t scale);
std::string FormatDecimal(const Decimal& value);

// Strict Gregorian YYYY-MM-DD in years 0001..9999, represented as Unix epoch days.
Result<Date> ParseDate(std::string_view value);
std::string FormatDate(Date value);

} // namespace glamdb
