#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace NGlamDB {

// Finite schema width; row, request and resource budgets apply independently.
inline constexpr std::size_t MaxSchemaFields = 128;

enum class EFieldType : std::uint8_t {
    String = 1,
    Bool = 2,
    Int64 = 3,
    Float64 = 4,
    TimestampUsUtc = 5,
    Decimal = 6,
    Date = 7
};

struct TFieldSpec
{
    std::string Name;
    EFieldType Type{};
    bool Nullable = false;
    std::int32_t Precision = 0;
    std::int32_t Scale = 0;
};

struct TSchemaSpec
{
    std::vector<TFieldSpec> Fields;
};

}
