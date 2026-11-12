#pragma once

#include <arrow/util/decimal.h>

namespace glamdb::detail {

inline bool FitsDecimalPrecision(const arrow::Decimal128& value, int32_t precision)
{
    if (precision < 1 || precision > 38) {
        return false;
    }
    const arrow::Decimal128 maximum(arrow::BasicDecimal128::GetMaxValue(precision));
    // Abs(INT128_MIN) wraps in the dependency's FitsInPrecision implementation.
    return value >= -maximum && value <= maximum;
}

} // namespace glamdb::detail
