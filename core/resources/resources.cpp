#include "storage/resources.h"

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <limits>
#include <map>
#include <mutex>
#include <sys/stat.h>
#include <sys/statvfs.h>

namespace glamdb {
namespace {
ErrorDetail Exhausted()
{
    return {"RESOURCE_EXHAUSTED", "shared resource capacity is full", RetryHint::OperatorAction};
}

uint64_t Multiply(uint64_t first, uint64_t second)
{
    const auto max = std::numeric_limits<uint64_t>::max();
    return first != 0 && second > max / first ? max : first * second;
}

bool Fits(uint64_t used, uint64_t requested, uint64_t limit)
{
    return used <= limit && requested <= limit - used;
}
} // namespace

struct ResourceOwner::State
{
    struct TDevice
    {
        uint64_t Bytes = 0, Inodes = 0;
    };
    explicit State(ResourceLimits limits)
        : Limits(limits)
    { }
    const ResourceLimits Limits;
    mutable std::mutex Mutex;
    ResourceUsage Usage;
    std::map<uint64_t, TDevice> Devices;
};

ResourceOwner::ResourceOwner(ResourceLimits limits)
    : State_(std::make_shared<State>(limits))
{ }

Result<std::shared_ptr<ResourceReservation>> ResourceOwner::Reserve(ResourceClass resource, uint64_t bytes)
{
    const auto index = static_cast<size_t>(resource);
    if (index >= State_->Limits.memory_bytes.size()) {
        return std::unexpected(ErrorDetail{"INVALID_ARGUMENT", "unknown resource class", RetryHint::FixInput});
    }
    auto reservation = std::shared_ptr<ResourceReservation>(new ResourceReservation(State_));
    std::lock_guard lock(State_->Mutex);
    auto& usage = State_->Usage;
    if (!Fits(usage.memory_bytes[index], bytes, State_->Limits.memory_bytes[index])) {
        return std::unexpected(Exhausted());
    }
    reservation->Class_ = resource;
    reservation->Bytes_ = bytes;
    usage.memory_bytes[index] += bytes;
    usage.memory_peak[index] = std::max(usage.memory_peak[index], usage.memory_bytes[index]);
    return reservation;
}

Result<std::shared_ptr<ResourceReservation>>
ResourceOwner::ReserveDevice(const std::string& absolute_path, uint64_t bytes, uint64_t inodes)
{
    std::filesystem::path path(absolute_path);
    if (!path.is_absolute() || absolute_path.find('\0') != std::string::npos) {
        return std::unexpected(ErrorDetail{"INVALID_ARGUMENT", "device path must be absolute", RetryHint::FixInput});
    }
    auto reservation = std::shared_ptr<ResourceReservation>(new ResourceReservation(State_));
    // Serialize availability observation with every competing reservation.
    std::lock_guard lock(State_->Mutex);
    struct stat info
    { };
    while (stat(path.c_str(), &info) != 0) {
        if (errno != ENOENT || path == path.parent_path()) {
            return std::unexpected(
                ErrorDetail{"IO_ERROR", "cannot inspect reservation device", RetryHint::OperatorAction});
        }
        path = path.parent_path();
    }
    struct statvfs space
    { };
    if (statvfs(path.c_str(), &space) != 0) {
        return std::unexpected(ErrorDetail{"IO_ERROR", "cannot inspect device capacity", RetryHint::OperatorAction});
    }
    const auto device = static_cast<uint64_t>(info.st_dev);
    auto& pending = State_->Devices[device];
    const auto& limits = State_->Limits;
    const auto available = Multiply(space.f_bavail, space.f_frsize);
    const auto freeInodes = static_cast<uint64_t>(space.f_favail);
    if (!Fits(pending.Bytes, bytes, limits.device_reserved_bytes) ||
        !Fits(pending.Inodes, inodes, limits.device_reserved_inodes) ||
        !Fits(limits.device_free_bytes, pending.Bytes, available) ||
        !Fits(limits.device_free_bytes + pending.Bytes, bytes, available) ||
        !Fits(limits.device_free_inodes, pending.Inodes, freeInodes) ||
        !Fits(limits.device_free_inodes + pending.Inodes, inodes, freeInodes))
    {
        return std::unexpected(Exhausted());
    }
    auto& usage = State_->Usage;
    if (!Fits(usage.device_reserved_bytes, bytes, std::numeric_limits<uint64_t>::max()) ||
        !Fits(usage.device_reserved_inodes, inodes, std::numeric_limits<uint64_t>::max()))
    {
        return std::unexpected(Exhausted());
    }
    reservation->IsDevice_ = true;
    reservation->Device_ = device;
    reservation->Bytes_ = bytes;
    reservation->Inodes_ = inodes;
    pending.Bytes += bytes;
    pending.Inodes += inodes;
    usage.device_reserved_bytes += bytes;
    usage.device_reserved_inodes += inodes;
    usage.device_peak_bytes = std::max(usage.device_peak_bytes, usage.device_reserved_bytes);
    usage.device_peak_inodes = std::max(usage.device_peak_inodes, usage.device_reserved_inodes);
    return reservation;
}

ResourceUsage ResourceOwner::GetUsage() const
{
    std::lock_guard lock(State_->Mutex);
    return State_->Usage;
}

const ResourceLimits& ResourceOwner::GetLimits() const
{
    return State_->Limits;
}

std::shared_ptr<ResourceOwner> ResourceOwner::ProcessDefault()
{
    static const auto Owner = std::make_shared<ResourceOwner>();
    return Owner;
}

ResourceReservation::ResourceReservation(std::shared_ptr<ResourceOwner::State> state)
    : State_(std::move(state))
{ }

ResourceReservation::~ResourceReservation()
{
    std::lock_guard lock(State_->Mutex);
    auto& usage = State_->Usage;
    if (IsDevice_) {
        // Device entries are created before publishing a reservation and
        // remain in State for the lifetime of every lease.
        auto& pending = State_->Devices.find(Device_)->second;
        pending.Bytes -= Bytes_;
        pending.Inodes -= Inodes_;
        usage.device_reserved_bytes -= Bytes_;
        usage.device_reserved_inodes -= Inodes_;
    } else {
        usage.memory_bytes[static_cast<size_t>(Class_)] -= Bytes_;
    }
}

} // namespace glamdb
