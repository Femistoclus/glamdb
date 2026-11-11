#include "core/resources/arrow_pool.h"

#include <limits>
#include <map>
#include <mutex>
#include <new>

namespace glamdb::detail {
namespace {
class TResourcePool final : public arrow::ProxyMemoryPool
{
public:
    TResourcePool(std::shared_ptr<ResourceOwner> owner, std::shared_ptr<arrow::MemoryPool> upstream)
        : arrow::ProxyMemoryPool(upstream ? upstream.get() : arrow::default_memory_pool())
        , Owner_(std::move(owner))
        , Upstream_(std::move(upstream))
    { }

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override
    {
        if (size < 0 || alignment <= 0) {
            return arrow::Status::Invalid("invalid allocation size or alignment");
        }
        if (size == 0) {
            return arrow::ProxyMemoryPool::Allocate(size, alignment, out);
        }
        try {
            auto reservation = Owner_->Reserve(ResourceClass::Data, Charge({.Size = size, .Alignment = alignment}));
            if (!reservation) {
                return arrow::Status::OutOfMemory(reservation.error().message);
            }
            // Insert a node before allocating; no map growth can fail after a
            // successful upstream allocation or reallocation.
            std::map<uint8_t*, std::shared_ptr<ResourceReservation>> staging;
            staging.emplace(nullptr, std::move(*reservation));
            auto node = staging.extract(staging.begin());
            std::lock_guard lock(Mutex_);
            const auto status = arrow::ProxyMemoryPool::Allocate(size, alignment, out);
            if (!status.ok()) {
                return status;
            }
            node.key() = *out;
            Allocations_.insert(std::move(node));
            return status;
        } catch (const std::bad_alloc&) {
            return arrow::Status::OutOfMemory("cannot allocate resource accounting");
        }
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override
    {
        if (old_size == 0) {
            return Allocate(new_size, alignment, ptr);
        }
        if (new_size == 0) {
            uint8_t* empty = nullptr;
            const auto status = arrow::ProxyMemoryPool::Allocate(0, alignment, &empty);
            if (!status.ok()) {
                return status;
            }
            Free(*ptr, old_size, alignment);
            *ptr = empty;
            return status;
        }
        if (new_size < 0 || alignment <= 0) {
            return arrow::Status::Invalid("invalid reallocation size or alignment");
        }
        try {
            auto reservation = Owner_->Reserve(ResourceClass::Data, Charge({.Size = new_size, .Alignment = alignment}));
            if (!reservation) {
                return arrow::Status::OutOfMemory(reservation.error().message);
            }
            std::lock_guard lock(Mutex_);
            auto found = Allocations_.find(*ptr);
            if (found == Allocations_.end()) {
                return arrow::Status::Invalid("allocation belongs to another pool");
            }
            const auto status = arrow::ProxyMemoryPool::Reallocate(old_size, new_size, alignment, ptr);
            if (!status.ok()) {
                return status;
            }
            auto node = Allocations_.extract(found);
            node.key() = *ptr;
            node.mapped() = std::move(*reservation);
            Allocations_.insert(std::move(node));
            return status;
        } catch (const std::bad_alloc&) {
            return arrow::Status::OutOfMemory("cannot allocate resource accounting");
        }
    }

    void Free(uint8_t* buffer, int64_t size, int64_t alignment) override
    {
        std::lock_guard lock(Mutex_);
        arrow::ProxyMemoryPool::Free(buffer, size, alignment);
        if (size != 0) {
            Allocations_.erase(buffer);
        }
    }

private:
    const std::shared_ptr<ResourceOwner> Owner_;
    const std::shared_ptr<arrow::MemoryPool> Upstream_;
    std::mutex Mutex_;
    std::map<uint8_t*, std::shared_ptr<ResourceReservation>> Allocations_;

    struct TAllocation
    {
        int64_t Size;
        int64_t Alignment;
    };
    static uint64_t Charge(const TAllocation& allocation)
    {
        // Include alignment padding and one accounting node. The allocator may
        // retain freed arenas; this counter measures owned allocation capacity.
        const auto bytes = static_cast<uint64_t>(allocation.Size);
        const auto padding = static_cast<uint64_t>(allocation.Alignment) + 128;
        const auto max = std::numeric_limits<uint64_t>::max();
        return bytes > max - padding ? max : bytes + padding;
    }
};
} // namespace

std::shared_ptr<arrow::MemoryPool> MakeResourcePool(std::shared_ptr<ResourceOwner> owner,
    std::shared_ptr<arrow::MemoryPool> upstream)
{
    return std::make_shared<TResourcePool>(owner ? std::move(owner) : ResourceOwner::ProcessDefault(),
        std::move(upstream));
}
} // namespace glamdb::detail
