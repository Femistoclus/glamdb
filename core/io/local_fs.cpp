#include "core/io/local_fs.h"

#include <algorithm>
#include <cerrno>
#include <dirent.h>
#include <fcntl.h>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <utility>

namespace NGlamDB::NIO {
namespace {

struct TNativeHandle final : TFSHandle
{
    int Fd;
    bool Directory;
    std::mutex Mutex;

    TNativeHandle(int descriptor, bool isDirectory)
        : Fd(descriptor)
        , Directory(isDirectory)
    { }
    ~TNativeHandle() override
    {
        if (Fd >= 0) {
            ::close(Fd);
        }
    }
};

std::error_code LastError()
{
    return {errno, std::generic_category()};
}

bool ValidLeaf(std::string_view name)
{
    return !name.empty() && name != "." && name != ".." && name.find_first_of("/\0", 0, 2) == std::string_view::npos;
}

TResult<THandle> Adopt(int fd, bool directory)
{
    if (fd < 0) {
        return LastError();
    }
    try {
        return THandle{std::make_shared<TNativeHandle>(fd, directory)};
    } catch (const std::bad_alloc&) {
        ::close(fd);
        return std::errc::not_enough_memory;
    }
}

template <typename F>
auto WithHandle(const THandle& handle, std::optional<bool> directory, F&& operation) -> decltype(operation(0))
{
    const auto native = std::dynamic_pointer_cast<TNativeHandle>(handle);
    if (!native) {
        return std::make_error_code(std::errc::bad_file_descriptor);
    }
    const std::lock_guard lock(native->Mutex);
    if (native->Fd < 0) {
        return std::make_error_code(std::errc::bad_file_descriptor);
    }
    if (directory && native->Directory != *directory) {
        return std::make_error_code(*directory ? std::errc::not_a_directory : std::errc::invalid_argument);
    }
    return operation(native->Fd);
}

std::size_t SyscallSize(std::size_t size)
{
    return std::min(size, static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
}

std::error_code AddTreeBytes(const struct stat& info, std::uint64_t limit, std::uint64_t& total)
{
    if (info.st_size < 0 || info.st_blocks < 0 ||
        static_cast<std::uint64_t>(info.st_blocks) > std::numeric_limits<std::uint64_t>::max() / 512)
    {
        return std::make_error_code(std::errc::value_too_large);
    }
    const auto allocated = static_cast<std::uint64_t>(info.st_blocks) * 512;
    const auto bytes =
        S_ISREG(info.st_mode) ? std::max(allocated, static_cast<std::uint64_t>(info.st_size)) : allocated;
    if (bytes > limit - total) {
        return std::make_error_code(std::errc::no_space_on_device);
    }
    total += bytes;
    return {};
}

std::error_code
MeasureDirectory(int fd, const struct stat* expected, unsigned depth, std::uint64_t& total, std::uint64_t limit)
{
    const auto closeDirectory = [](DIR* value) noexcept {
        ::closedir(value);
    };
    std::unique_ptr<DIR, decltype(closeDirectory)> directory(::fdopendir(fd), closeDirectory);
    if (!directory) {
        const auto error = LastError();
        ::close(fd);
        return error;
    }
    struct stat parent
    { };
    if (::fstat(fd, &parent) != 0) {
        return LastError();
    }
    if (expected && (parent.st_dev != expected->st_dev || parent.st_ino != expected->st_ino)) {
        return std::make_error_code(std::errc::state_not_recoverable);
    }
    if (const auto error = AddTreeBytes(parent, limit, total)) {
        return error;
    }
    while (true) {
        errno = 0;
        const auto* entry = ::readdir(directory.get());
        if (!entry) {
            if (errno != 0) {
                return LastError();
            }
            return ::closedir(directory.release()) == 0 ? std::error_code{} : LastError();
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
        if (!S_ISREG(info.st_mode) && !S_ISDIR(info.st_mode)) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        if (info.st_dev != parent.st_dev) {
            return std::make_error_code(std::errc::cross_device_link);
        }
        if (S_ISREG(info.st_mode)) {
            if (const auto error = AddTreeBytes(info, limit, total)) {
                return error;
            }
            continue;
        }
        if (depth == 64) {
            return std::make_error_code(std::errc::value_too_large);
        }
        const auto child = ::openat(fd, entry->d_name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (child < 0) {
            return LastError();
        }
        if (const auto error = MeasureDirectory(child, &info, depth + 1, total, limit)) {
            return error;
        }
    }
}

}

TResult<THandle> TPosixLocalFS::OpenDir(const std::filesystem::path& absolutePath)
{
    if (!absolutePath.is_absolute() || absolutePath.native().find('\0') != std::string::npos) {
        return std::errc::invalid_argument;
    }
    auto directory = Adopt(::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW), true);
    for (const auto& component : absolutePath.relative_path()) {
        if (!directory) {
            break;
        }
        if (component.empty()) {
            continue;
        }
        directory = OpenDir(directory.Value(), component.native());
    }
    return directory;
}

TResult<THandle> TPosixLocalFS::OpenDir(const THandle& parent, std::string_view name)
{
    if (!ValidLeaf(name)) {
        return std::errc::invalid_argument;
    }
    const std::string leaf(name);
    return WithHandle(parent, true, [&](int fd) {
        return Adopt(::openat(fd, leaf.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW), true);
    });
}

std::error_code TPosixLocalFS::MakeDir(const THandle& parent, std::string_view name)
{
    if (!ValidLeaf(name)) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    const std::string leaf(name);
    return WithHandle(parent, true, [&](int fd) {
        return ::mkdirat(fd, leaf.c_str(), 0750) == 0 ? std::error_code{} : LastError();
    });
}

std::error_code TPosixLocalFS::Unlink(const THandle& parent, std::string_view name)
{
    if (!ValidLeaf(name)) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    const std::string leaf(name);
    return WithHandle(parent, true, [&](int fd) {
        return ::unlinkat(fd, leaf.c_str(), 0) == 0 ? std::error_code{} : LastError();
    });
}

TResult<THandle> TPosixLocalFS::CreateExclusive(const THandle& parent, std::string_view name, const TIOTag&)
{
    if (!ValidLeaf(name)) {
        return std::errc::invalid_argument;
    }
    const std::string leaf(name);
    return WithHandle(parent, true, [&](int fd) {
        return Adopt(::openat(fd, leaf.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0640), false);
    });
}

TResult<THandle> TPosixLocalFS::OpenRead(const THandle& parent, std::string_view name)
{
    if (!ValidLeaf(name)) {
        return std::errc::invalid_argument;
    }
    const std::string leaf(name);
    return WithHandle(parent, true, [&](int parentFd) -> TResult<THandle> {
        const int fd = ::openat(parentFd, leaf.c_str(), O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0) {
            return LastError();
        }
        struct stat info
        { };
        const bool failed = ::fstat(fd, &info) != 0;
        if (failed || !S_ISREG(info.st_mode)) {
            const auto error = failed ? LastError() : std::make_error_code(std::errc::invalid_argument);
            ::close(fd);
            return error;
        }
        return Adopt(fd, false);
    });
}

TResult<std::size_t> TPosixLocalFS::Write(const THandle& file, std::span<const std::byte> bytes)
{
    return WithHandle(file, false, [&](int fd) -> TResult<std::size_t> {
        const auto written = ::write(fd, bytes.data(), SyscallSize(bytes.size()));
        if (written < 0) {
            return LastError();
        }
        return static_cast<std::size_t>(written);
    });
}

TResult<std::size_t> TPosixLocalFS::PRead(const THandle& file, std::uint64_t offset, std::span<std::byte> bytes)
{
    if (offset > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
        return std::errc::value_too_large;
    }
    return WithHandle(file, false, [&](int fd) -> TResult<std::size_t> {
        const auto count = ::pread(fd, bytes.data(), SyscallSize(bytes.size()), static_cast<off_t>(offset));
        if (count < 0) {
            return LastError();
        }
        return static_cast<std::size_t>(count);
    });
}

TResult<TFileStat> TPosixLocalFS::Stat(const THandle& handle)
{
    return WithHandle(handle, std::nullopt, [](int fd) -> TResult<TFileStat> {
        struct stat info
        { };
        if (::fstat(fd, &info) != 0) {
            return LastError();
        }
        if (info.st_size < 0 || info.st_blocks < 0 ||
            static_cast<std::uint64_t>(info.st_blocks) > std::numeric_limits<std::uint64_t>::max() / 512)
        {
            return std::errc::value_too_large;
        }
        const auto type = S_ISREG(info.st_mode) ? EFileType::Regular
            : S_ISDIR(info.st_mode)             ? EFileType::Directory
                                                : EFileType::Other;
        return TFileStat{static_cast<std::uint64_t>(info.st_dev),
            static_cast<std::uint64_t>(info.st_ino),
            static_cast<std::uint64_t>(info.st_size),
            type,
            static_cast<std::uint64_t>(info.st_blocks) * 512};
    });
}

TResult<TSpaceInfo> TPosixLocalFS::StatFs(const THandle& directory)
{
    return WithHandle(directory, true, [](int fd) -> TResult<TSpaceInfo> {
        struct statvfs info
        { };
        if (::fstatvfs(fd, &info) != 0) {
            return LastError();
        }
        const auto blockSize = static_cast<std::uint64_t>(info.f_frsize);
        const auto maximum = std::numeric_limits<std::uint64_t>::max();
        if (blockSize == 0 || info.f_blocks > maximum / blockSize || info.f_bavail > maximum / blockSize) {
            return std::errc::value_too_large;
        }
        return TSpaceInfo{info.f_blocks * blockSize, info.f_bavail * blockSize};
    });
}

TResult<std::uint64_t> TPosixLocalFS::MeasureTree(const THandle& directory, std::uint64_t limit)
{
    // Only duplicate the directory under the handle lock: the walk itself runs
    // on the private descriptor, so concurrent opens through the same handle
    // (scan readers) do not wait for a whole-tree traversal.
    const auto scan = WithHandle(directory, true, [](int fd) -> TResult<int> {
        const auto duplicate = ::openat(fd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (duplicate < 0) {
            return LastError();
        }
        return duplicate;
    });
    if (!scan) {
        return scan.Error();
    }
    std::uint64_t total = 0;
    if (const auto error = MeasureDirectory(scan.Value(), nullptr, 0, total, limit)) {
        return error;
    }
    return total;
}

std::error_code TPosixLocalFS::SyncFile(const THandle& file)
{
    return WithHandle(file, false, [](int fd) {
#ifdef __APPLE__
        const int status = ::fcntl(fd, F_FULLFSYNC);
#else
        const int status = ::fsync(fd);
#endif
        return status == 0 ? std::error_code{} : LastError();
    });
}

std::error_code TPosixLocalFS::SyncDir(const THandle& directory)
{
    return WithHandle(directory, true, [](int fd) {
        return ::fsync(fd) == 0 ? std::error_code{} : LastError();
    });
}

std::error_code TPosixLocalFS::Close(const THandle& handle)
{
    const auto native = std::dynamic_pointer_cast<TNativeHandle>(handle);
    if (!native) {
        return std::make_error_code(std::errc::bad_file_descriptor);
    }
    const std::lock_guard lock(native->Mutex);
    if (native->Fd < 0) {
        return std::make_error_code(std::errc::bad_file_descriptor);
    }
    const int fd = std::exchange(native->Fd, -1);
    return ::close(fd) == 0 ? std::error_code{} : LastError();
}

std::string_view TPosixLocalFS::Profile() noexcept
{
#ifdef __APPLE__
    return "DEV-UNQUALIFIED";
#else
    return "LINUX-POSIX-UNQUALIFIED";
#endif
}

TResult<std::uint64_t> ILocalFS::MeasureTree(const THandle&, std::uint64_t)
{
    return std::errc::not_supported;
}

std::error_code ILocalFS::Unlink(const THandle&, std::string_view)
{
    return std::make_error_code(std::errc::not_supported);
}

}
