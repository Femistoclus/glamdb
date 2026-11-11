#pragma once

#include "storage/resources.h"

#include <arrow/memory_pool.h>

#include <memory>

namespace glamdb::detail {
// Returned buffers must retain this pool: Arrow only borrows its address.
std::shared_ptr<arrow::MemoryPool> MakeResourcePool(std::shared_ptr<ResourceOwner> owner,
    std::shared_ptr<arrow::MemoryPool> upstream = {});
} // namespace glamdb::detail
