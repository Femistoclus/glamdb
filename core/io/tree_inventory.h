#pragma once

#include "core/io/result.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

namespace NGlamDB::NIO {

struct ITreeInventory
{
    virtual ~ITreeInventory() = default;
    // Cancellation callbacks must be nonblocking and must not throw.
    virtual TResult<std::uint64_t> Measure(const std::function<TResult<std::uint64_t>()>& fullMeasure,
        std::uint64_t limit,
        std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max(),
        const std::function<bool()>& cancelled = {}) = 0;
    virtual void Invalidate() = 0;
};

TResult<std::shared_ptr<ITreeInventory>> MakeTreeInventory(int directory);

} // namespace NGlamDB::NIO
