#pragma once

#include "storage/types.h"

#include <array>
#include <cstdint>
#include <memory>
#include <string>

namespace glamdb {

enum class ResourceClass : uint8_t { Data, Metadata, Control, Completion, Recovery };

struct ResourceLimits
{
    // Reservation envelopes, not a promise about process RSS. Protected classes
    // cannot be borrowed by ordinary requests. Zero disables a class.
    std::array<uint64_t, 5> memory_bytes{2ULL << 30, 8ULL << 30, 16ULL << 20, 64ULL << 20, 4ULL << 30};
    uint64_t device_reserved_bytes = 8ULL << 30;
    uint64_t device_reserved_inodes = 65536;
    uint64_t device_free_bytes = 1ULL << 30;
    uint64_t device_free_inodes = 128;
};

struct ResourceUsage
{
    std::array<uint64_t, 5> memory_bytes{};
    std::array<uint64_t, 5> memory_peak{};
    uint64_t device_reserved_bytes = 0;
    uint64_t device_reserved_inodes = 0;
    uint64_t device_peak_bytes = 0;
    uint64_t device_peak_inodes = 0;
};

class ResourceReservation;

// Supply the same owner to every store and transport in an embedding host.
// A missing owner selects the shared, finite process default.
class ResourceOwner
{
public:
    struct State;
    explicit ResourceOwner(ResourceLimits limits = {});
    Result<std::shared_ptr<ResourceReservation>> Reserve(ResourceClass resource, uint64_t bytes);
    // Paths on the same st_dev compete atomically. A not-yet-created store
    // resolves to its nearest existing parent. Availability is sampled again
    // for every admission; external ENOSPC/EIO still needs normal handling.
    Result<std::shared_ptr<ResourceReservation>>
    ReserveDevice(const std::string& absolute_path, uint64_t bytes, uint64_t inodes);
    ResourceUsage GetUsage() const;
    const ResourceLimits& GetLimits() const;
    static std::shared_ptr<ResourceOwner> ProcessDefault();

private:
    std::shared_ptr<State> State_;
};

// Shared ownership releases once, at the last buffer/attempt/context owner.
class ResourceReservation
{
public:
    ~ResourceReservation();
    uint64_t GetBytes() const
    {
        return Bytes_;
    }
    ResourceReservation(const ResourceReservation&) = delete;
    ResourceReservation& operator=(const ResourceReservation&) = delete;

private:
    friend class ResourceOwner;
    std::shared_ptr<ResourceOwner::State> State_;
    ResourceClass Class_ = ResourceClass::Data;
    uint64_t Bytes_ = 0, Inodes_ = 0, Device_ = 0;
    bool IsDevice_ = false;
    explicit ResourceReservation(std::shared_ptr<ResourceOwner::State> state);
};

} // namespace glamdb
