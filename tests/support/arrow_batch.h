#pragma once

#include <arrow/api.h>
#include <optional>
#include <stdexcept>
#include <vector>

namespace glamdb::test {

inline std::shared_ptr<arrow::RecordBatch> Batch(
    const std::vector<std::optional<int64_t>>& values = {1, std::nullopt, 1})
{
    arrow::Int64Builder builder;
    for (const auto value : values) {
        const auto status = value ? builder.Append(*value) : builder.AppendNull();
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }
    const auto array = builder.Finish();
    if (!array.ok()) {
        throw std::runtime_error(array.status().ToString());
    }
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("n", arrow::int64())}),
        static_cast<int64_t>(values.size()),
        {*array});
}

} // namespace glamdb::test
