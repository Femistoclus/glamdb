#pragma once

#include "storage/types.h"

#include <mutex>

namespace iceberg {
class FileIO;
}

namespace glamdb::detail {

// Shared by all streams and rewinds of one bounded read operation.
class TReadBudget
{
public:
    explicit TReadBudget(uint64_t bytes,
        std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max(),
        std::shared_ptr<const std::atomic<bool>> cancellation = {});
    Status Charge(uint64_t requested);
    std::optional<ErrorDetail> Error() const;

private:
    mutable std::mutex Mutex_;
    uint64_t Remaining_;
    std::chrono::steady_clock::time_point Deadline_;
    std::shared_ptr<const std::atomic<bool>> Cancellation_;
    std::optional<ErrorDetail> Error_;
};

std::shared_ptr<iceberg::FileIO> BudgetReadOnlyFileIO(std::shared_ptr<iceberg::FileIO> base,
    std::shared_ptr<TReadBudget> budget);

} // namespace glamdb::detail
