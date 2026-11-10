#pragma once

#include "storage/scan.h"

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

namespace arrow {
class Array;
} // namespace arrow

namespace glamdb::detail {

struct TBoundPredicate
{
    Predicate::Kind Kind = Predicate::Kind::True;
    size_t FieldIndex = 0;
    FieldType Type = FieldType::String;
    CompareOp Op = CompareOp::Eq;
    Literal Value;
};

// Resolves the field by exact name and checks the literal type without casts.
Result<TBoundPredicate> BindPredicate(const Predicate& predicate, const SchemaSpec& schema);

// Returns rows where the comparison is TRUE. NULL cells compare as UNKNOWN and
// are not selected. The column must have the bound field's Arrow type.
Result<std::vector<int64_t>> SelectRows(const TBoundPredicate& predicate, const arrow::Array& column);

// Copies the selected rows, preserving NULLs, for the five supported types.
Result<std::shared_ptr<arrow::Array>> TakeRows(const arrow::Array& column, std::span<const int64_t> rows);

} // namespace glamdb::detail
