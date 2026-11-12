#include "core/io/tree_inventory.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <set>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

#ifdef __linux__
#include <linux/magic.h>
#include <sys/inotify.h>
#include <sys/vfs.h>
#endif

namespace NGlamDB::NIO {
namespace {

#ifdef __linux__

constexpr std::size_t MaxDirectories = 1024;
constexpr std::size_t MaxEvents = 4096;
constexpr std::uint32_t WatchMask = IN_ATTRIB | IN_CLOSE_WRITE | IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MODIFY |
    IN_MOVE_SELF | IN_MOVED_FROM | IN_MOVED_TO | IN_ONLYDIR;
constexpr std::uint32_t RebuildMask =
    IN_Q_OVERFLOW | IN_IGNORED | IN_UNMOUNT | IN_DELETE_SELF | IN_MOVE_SELF | IN_MOVED_FROM | IN_MOVED_TO;

std::error_code LastError()
{
    return {errno, std::generic_category()};
}

std::error_code TrackingError()
{
    return errno == EMFILE || errno == ENFILE ? std::make_error_code(std::errc::not_supported) : LastError();
}

struct TInventoryControl
{
    std::chrono::steady_clock::time_point Deadline;
    const std::function<bool()>& Cancelled;
};

std::error_code CheckInterrupted(const TInventoryControl& control)
{
    if (control.Cancelled && control.Cancelled()) {
        return std::make_error_code(std::errc::operation_canceled);
    }
    return std::chrono::steady_clock::now() < control.Deadline ? std::error_code{}
                                                               : std::make_error_code(std::errc::timed_out);
}

class TDescriptor
{
public:
    explicit TDescriptor(int fd = -1)
        : Fd_(fd)
    { }

    ~TDescriptor()
    {
        Reset();
    }

    TDescriptor(const TDescriptor&) = delete;
    TDescriptor& operator=(const TDescriptor&) = delete;

    TDescriptor(TDescriptor&& other) noexcept
        : Fd_(std::exchange(other.Fd_, -1))
    { }

    TDescriptor& operator=(TDescriptor&& other) noexcept
    {
        if (this != &other) {
            Reset(std::exchange(other.Fd_, -1));
        }
        return *this;
    }

    int Get() const
    {
        return Fd_;
    }

    void Reset(int fd = -1)
    {
        if (Fd_ >= 0) {
            ::close(Fd_);
        }
        Fd_ = fd;
    }

private:
    int Fd_;
};

TResult<std::uint64_t> PhysicalBytes(const struct stat& info)
{
    if (info.st_size < 0 || info.st_blocks < 0 ||
        static_cast<std::uint64_t>(info.st_blocks) > std::numeric_limits<std::uint64_t>::max() / 512)
    {
        return std::errc::value_too_large;
    }
    const auto allocated = static_cast<std::uint64_t>(info.st_blocks) * 512;
    return S_ISREG(info.st_mode) ? std::max(allocated, static_cast<std::uint64_t>(info.st_size)) : allocated;
}

bool IsSupportedFilesystem(int fd)
{
    struct statfs info
    { };
    if (::fstatfs(fd, &info) != 0) {
        return false;
    }
    return info.f_type == EXT4_SUPER_MAGIC || info.f_type == XFS_SUPER_MAGIC || info.f_type == BTRFS_SUPER_MAGIC ||
        info.f_type == TMPFS_MAGIC;
}

class TTreeInventory final : public ITreeInventory
{
public:
    explicit TTreeInventory(TDescriptor root)
        : Root_(std::move(root))
    { }

    TResult<std::uint64_t> Measure(const std::function<TResult<std::uint64_t>()>& fullMeasure,
        std::uint64_t limit,
        std::chrono::steady_clock::time_point deadline,
        const std::function<bool()>& cancelled) override
    {
        const TInventoryControl control{deadline, cancelled};
        try {
            if (const auto error = CheckInterrupted(control)) {
                return error;
            }
            if (Unsupported_) {
                return FullMeasure(fullMeasure, limit, control);
            }
            if (Bytes_) {
                const auto error = ApplyEvents(control);
                if (!error && *Bytes_ <= limit) {
                    if (const auto interrupted = CheckInterrupted(control)) {
                        return Fail(interrupted);
                    }
                    return *Bytes_;
                }
                if (error && error != std::errc::state_not_recoverable && error != std::errc::value_too_large) {
                    return Fallback(error, fullMeasure, limit, control);
                }
            }
            return Refresh(fullMeasure, limit, control);
        } catch (const std::bad_alloc&) {
            return Fail(std::make_error_code(std::errc::not_enough_memory));
        }
    }

    void Invalidate() override
    {
        Directories_.clear();
        Events_.Reset();
        Bytes_.reset();
    }

private:
    struct TDirectory
    {
        TDescriptor Handle;
        std::uint64_t Bytes;
        dev_t Device;
        ino_t Inode;
    };

    struct TDiscovery
    {
        int Descriptor;
        dev_t Device;
        unsigned Depth;
    };

    const TDescriptor Root_;
    TDescriptor Events_;
    std::map<int, TDirectory> Directories_;
    std::optional<std::uint64_t> Bytes_;
    bool Unsupported_ = false;

    TResult<std::uint64_t> Fail(std::error_code error)
    {
        Invalidate();
        Unsupported_ = error == std::errc::not_supported;
        return error;
    }

    TResult<std::uint64_t> FullMeasure(const std::function<TResult<std::uint64_t>()>& fullMeasure,
        std::uint64_t limit,
        const TInventoryControl& control)
    {
        Invalidate();
        if (const auto error = CheckInterrupted(control)) {
            return error;
        }
        auto result = fullMeasure();
        if (const auto error = CheckInterrupted(control)) {
            return error;
        }
        if (result && result.Value() > limit) {
            return std::errc::no_space_on_device;
        }
        return result;
    }

    TResult<std::uint64_t> Fallback(std::error_code error,
        const std::function<TResult<std::uint64_t>()>& fullMeasure,
        std::uint64_t limit,
        const TInventoryControl& control)
    {
        if (error == std::errc::not_supported) {
            Unsupported_ = true;
            return FullMeasure(fullMeasure, limit, control);
        }
        return Fail(error);
    }

    TResult<std::uint64_t> Refresh(const std::function<TResult<std::uint64_t>()>& fullMeasure,
        std::uint64_t limit,
        const TInventoryControl& control)
    {
        Invalidate();
        Events_.Reset(::inotify_init1(IN_NONBLOCK | IN_CLOEXEC));
        if (Events_.Get() < 0) {
            return Fallback(std::make_error_code(std::errc::not_supported), fullMeasure, limit, control);
        }
        struct stat root
        { };
        if (::fstat(Root_.Get(), &root) != 0) {
            return Fail(LastError());
        }
        TDescriptor directory(::openat(Root_.Get(), ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
        if (directory.Get() < 0) {
            return Fallback(TrackingError(), fullMeasure, limit, control);
        }
        std::vector<TDiscovery> pending;
        pending.reserve(MaxDirectories);
        if (const auto error = TrackDirectory(std::move(directory), root, 0, control, pending)) {
            return Fallback(error, fullMeasure, limit, control);
        }
        for (std::size_t index = 0; index < pending.size(); ++index) {
            const auto current = pending[index];
            if (const auto error = TrackChildren(current, control, pending)) {
                return Fallback(error, fullMeasure, limit, control);
            }
        }
        if (const auto error = CheckInterrupted(control)) {
            return Fail(error);
        }
        auto measured = fullMeasure();
        if (!measured) {
            return Fail(measured.Error());
        }
        if (const auto error = CheckInterrupted(control)) {
            return Fail(error);
        }
        Bytes_ = measured.Value();
        for (auto& [watch, tracked] : Directories_) {
            tracked.Bytes = 0;
        }
        const auto error = ApplyEvents(control);
        if (error && error != std::errc::state_not_recoverable && error != std::errc::value_too_large) {
            return Fallback(error, fullMeasure, limit, control);
        }
        // Replaying changes after the baseline can overcharge. A fresh direct
        // measurement resolves both that case and another unstable watch set.
        if (error || *Bytes_ > limit) {
            return FullMeasure(fullMeasure, limit, control);
        }
        if (const auto interrupted = CheckInterrupted(control)) {
            return Fail(interrupted);
        }
        return *Bytes_;
    }

    std::error_code TrackDirectory(TDescriptor directory,
        const struct stat& expected,
        unsigned depth,
        const TInventoryControl& control,
        std::vector<TDiscovery>& pending)
    {
        if (const auto error = CheckInterrupted(control)) {
            return error;
        }
        if (Directories_.size() >= MaxDirectories) {
            return std::make_error_code(std::errc::not_supported);
        }
        struct stat info
        { };
        if (::fstat(directory.Get(), &info) != 0) {
            return LastError();
        }
        if (!S_ISDIR(info.st_mode) || info.st_dev != expected.st_dev || info.st_ino != expected.st_ino) {
            return std::make_error_code(std::errc::state_not_recoverable);
        }
        const auto bytes = PhysicalBytes(info);
        if (!bytes) {
            return bytes.Error();
        }
        const auto path = "/proc/self/fd/" + std::to_string(directory.Get());
        const auto watch = ::inotify_add_watch(Events_.Get(), path.c_str(), WatchMask);
        if (watch < 0) {
            return std::make_error_code(std::errc::not_supported);
        }
        const auto descriptor = directory.Get();
        if (!Directories_.emplace(watch, TDirectory{std::move(directory), bytes.Value(), info.st_dev, info.st_ino})
                 .second)
        {
            return std::make_error_code(std::errc::not_supported);
        }
        pending.push_back({descriptor, info.st_dev, depth});
        return {};
    }

    std::error_code
    TrackChildren(const TDiscovery& current, const TInventoryControl& control, std::vector<TDiscovery>& pending)
    {
        const auto fd = current.Descriptor;
        const auto scan = ::openat(fd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (scan < 0) {
            return TrackingError();
        }
        std::unique_ptr<DIR, decltype(&::closedir)> entries(::fdopendir(scan), ::closedir);
        if (!entries) {
            const auto error = LastError();
            ::close(scan);
            return error;
        }
        while (true) {
            if (const auto error = CheckInterrupted(control)) {
                return error;
            }
            errno = 0;
            const auto* entry = ::readdir(entries.get());
            if (!entry) {
                if (errno != 0) {
                    return LastError();
                }
                return ::closedir(entries.release()) == 0 ? std::error_code{} : LastError();
            }
            const std::string_view name(entry->d_name);
            if (name == "." || name == "..") {
                continue;
            }
            struct stat info
            { };
            if (::fstatat(fd, entry->d_name, &info, AT_SYMLINK_NOFOLLOW) != 0) {
                return LastError();
            }
            if (info.st_dev != current.Device) {
                return std::make_error_code(std::errc::cross_device_link);
            }
            if (S_ISREG(info.st_mode)) {
                if (info.st_nlink != 1) {
                    return std::make_error_code(std::errc::not_supported);
                }
            } else if (S_ISDIR(info.st_mode)) {
                if (current.Depth == 64) {
                    return std::make_error_code(std::errc::value_too_large);
                }
                TDescriptor child(::openat(fd, entry->d_name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
                if (child.Get() < 0) {
                    return TrackingError();
                }
                if (const auto error = TrackDirectory(std::move(child), info, current.Depth + 1, control, pending)) {
                    return error;
                }
            } else {
                return std::make_error_code(std::errc::invalid_argument);
            }
        }
    }

    std::error_code AddBytes(std::uint64_t bytes)
    {
        if (!Bytes_) {
            return std::make_error_code(std::errc::state_not_recoverable);
        }
        if (bytes > std::numeric_limits<std::uint64_t>::max() - *Bytes_) {
            return std::make_error_code(std::errc::value_too_large);
        }
        *Bytes_ += bytes;
        return {};
    }

    std::error_code ReadEvents(std::set<std::pair<int, std::string>>& dirty,
        std::set<int>& directories,
        const TInventoryControl& control)
    {
        alignas(inotify_event) std::array<char, 65536> buffer{};
        std::size_t count = 0;
        while (true) {
            if (const auto error = CheckInterrupted(control)) {
                return error;
            }
            const auto read = ::read(Events_.Get(), buffer.data(), buffer.size());
            if (read < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return errno == EAGAIN ? std::error_code{} : LastError();
            }
            if (read == 0) {
                return std::make_error_code(std::errc::state_not_recoverable);
            }
            const auto size = static_cast<std::size_t>(read);
            for (std::size_t offset = 0; offset < size;) {
                if (++count > MaxEvents || size - offset < sizeof(inotify_event)) {
                    return std::make_error_code(std::errc::state_not_recoverable);
                }
                if (count % 64 == 0) {
                    if (const auto error = CheckInterrupted(control)) {
                        return error;
                    }
                }
                const auto* event = reinterpret_cast<const inotify_event*>(buffer.data() + offset);
                if (event->len > size - offset - sizeof(inotify_event) || (event->mask & RebuildMask) != 0 ||
                    !Directories_.contains(event->wd))
                {
                    return std::make_error_code(std::errc::state_not_recoverable);
                }
                offset += sizeof(inotify_event) + event->len;
                directories.insert(event->wd);
                if ((event->mask & IN_ISDIR) != 0 && event->len != 0) {
                    return std::make_error_code(std::errc::state_not_recoverable);
                }
                if (event->len != 0) {
                    const auto length = ::strnlen(event->name, event->len);
                    if (length == 0 || length == event->len || length > NAME_MAX) {
                        return std::make_error_code(std::errc::state_not_recoverable);
                    }
                    dirty.emplace(event->wd, std::string(event->name, length));
                }
            }
        }
    }

    std::error_code ApplyEvents(const TInventoryControl& control)
    {
        std::set<std::pair<int, std::string>> dirty;
        std::set<int> directories;
        if (const auto error = ReadEvents(dirty, directories, control)) {
            return error;
        }
        for (const auto& [watch, name] : dirty) {
            if (const auto error = CheckInterrupted(control)) {
                return error;
            }
            const auto& directory = Directories_.at(watch);
            struct stat info
            { };
            if (::fstatat(directory.Handle.Get(), name.c_str(), &info, AT_SYMLINK_NOFOLLOW) != 0) {
                if (errno == ENOENT) {
                    continue;
                }
                return LastError();
            }
            if (!S_ISREG(info.st_mode) || info.st_dev != directory.Device) {
                return std::make_error_code(std::errc::state_not_recoverable);
            }
            if (info.st_nlink != 1) {
                return std::make_error_code(std::errc::not_supported);
            }
            const auto bytes = PhysicalBytes(info);
            if (!bytes) {
                return bytes.Error();
            }
            if (const auto error = AddBytes(bytes.Value())) {
                return error;
            }
        }
        for (const auto watch : directories) {
            if (const auto error = CheckInterrupted(control)) {
                return error;
            }
            auto& directory = Directories_.at(watch);
            struct stat info
            { };
            if (::fstat(directory.Handle.Get(), &info) != 0) {
                return LastError();
            }
            if (!S_ISDIR(info.st_mode) || info.st_dev != directory.Device || info.st_ino != directory.Inode) {
                return std::make_error_code(std::errc::state_not_recoverable);
            }
            const auto bytes = PhysicalBytes(info);
            if (!bytes) {
                return bytes.Error();
            }
            if (bytes.Value() > directory.Bytes) {
                if (const auto error = AddBytes(bytes.Value() - directory.Bytes)) {
                    return error;
                }
                directory.Bytes = bytes.Value();
            }
        }
        return {};
    }
};

#endif

} // namespace

TResult<std::shared_ptr<ITreeInventory>> MakeTreeInventory(int directory)
{
#ifdef __linux__
    if (!IsSupportedFilesystem(directory)) {
        return std::errc::not_supported;
    }
    TDescriptor root(::openat(directory, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (root.Get() < 0) {
        return LastError();
    }
    try {
        return std::shared_ptr<ITreeInventory>(std::make_shared<TTreeInventory>(std::move(root)));
    } catch (const std::bad_alloc&) {
        return std::errc::not_enough_memory;
    }
#else
    static_cast<void>(directory);
    return std::errc::not_supported;
#endif
}

} // namespace NGlamDB::NIO
