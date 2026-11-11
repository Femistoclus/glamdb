#include "core/engine/store_lease.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>

namespace glamdb::detail {
namespace {

void Check(bool success, std::string_view action, std::string_view code = "IO_ERROR")
{
    if (!success) {
        throw Failure(
            {std::string(code), std::string(action) + ": " + std::strerror(errno), RetryHint::OperatorAction});
    }
}

void ValidateRoot(const std::filesystem::path& root)
{
    Require(root.is_absolute() && root == root.lexically_normal() && root.has_filename() &&
            root.string().find('\0') == std::string::npos,
        "INVALID_ARGUMENT",
        "store root must be a normalized absolute directory path");
    std::error_code error;
    const auto canonical = std::filesystem::canonical(root.parent_path(), error);
    Require(!error && canonical == root.parent_path(), "STORE_MISSING", "parent is absent or uses a symlink alias");
}

std::string Identity(const StoreInfo& info)
{
    return "GLAMDB\n1\n" + info.store_id + "\n" + info.profile_id + "\n";
}

void CleanupRecoveryWorkspaces(const std::filesystem::path& root)
{
    // TRecoveryVisited uses mkstemp(".recovery-XXXXXX") on this device.
    // The caller holds exclusive ownership and has validated the store identity.
    constexpr std::string_view prefix = ".recovery-";
    for (const auto& entry : std::filesystem::directory_iterator(root)) {
        const auto name = entry.path().filename().string();
        if (name.size() != prefix.size() + 6 || !name.starts_with(prefix)) {
            continue;
        }
        const auto suffix = std::string_view(name).substr(prefix.size());
        const bool generated = std::ranges::all_of(suffix, [](char value) {
            return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9');
        });
        if (generated && std::filesystem::is_regular_file(entry.symlink_status())) {
            std::filesystem::remove(entry.path());
        }
    }
}

} // namespace

StoreLease::StoreLease(std::filesystem::path root)
    : root_(std::move(root))
{ }

StoreLease::~StoreLease()
{
    if (descriptor_ >= 0) {
        close(descriptor_);
    }
}

void StoreLease::Lock()
{
    descriptor_ = open(root_.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    Check(descriptor_ >= 0, "open store directory", "STORE_MISSING");
    Check(flock(descriptor_, LOCK_EX | LOCK_NB) == 0, "store is already owned", "STATE_IN_USE");
}

std::shared_ptr<StoreLease> StoreLease::Create(const std::filesystem::path& root, const StoreInfo& info)
{
    ValidateRoot(root);
    const int created = mkdir(root.c_str(), 0700);
    Check(created == 0, "create new store directory", errno == EEXIST ? "ALREADY_EXISTS" : "IO_ERROR");
    auto lease = std::shared_ptr<StoreLease>(new StoreLease(root));
    lease->Lock();
    lease->WriteMarker("store.identity", Identity(info));
    Check(mkdirat(lease->descriptor_, "warehouse", 0700) == 0, "create warehouse");
    const int warehouse = openat(lease->descriptor_, "warehouse", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    Check(warehouse >= 0, "open warehouse");
    const auto synced = fsync(warehouse);
    const int saved = errno;
    const auto closed = close(warehouse);
    if (synced != 0) {
        errno = saved;
        Check(false, "sync warehouse");
    }
    Check(closed == 0, "close warehouse");
    lease->SyncDirectories();
    return lease;
}

std::shared_ptr<StoreLease> StoreLease::Open(const std::filesystem::path& root, const StoreInfo& info)
{
    ValidateRoot(root);
    Require(std::filesystem::is_directory(std::filesystem::symlink_status(root)),
        "STORE_MISSING",
        "store root is missing or a symlink");
    auto lease = std::shared_ptr<StoreLease>(new StoreLease(root));
    lease->Lock();
    const auto identity = lease->ReadMarker("store.identity");
    Require(identity.starts_with("GLAMDB\n1\n"), "UNSUPPORTED_STORE_VERSION", "unknown store identity format");
    Require(identity == Identity(info),
        "STORE_IDENTITY_MISMATCH",
        "store identity or durability profile does not match");
    Require(lease->ReadMarker("store.ready") == Identity(info),
        "STORE_INCOMPLETE",
        "store initialization is incomplete");
    Require(std::filesystem::is_directory(std::filesystem::symlink_status(root / "warehouse")),
        "STORE_INCOMPLETE",
        "warehouse is missing or a symlink");
    CleanupRecoveryWorkspaces(root);
    return lease;
}

void StoreLease::WriteMarker(const char* name, std::string_view contents)
{
    const int file = openat(descriptor_, name, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    Check(file >= 0, "create immutable store marker");
    size_t offset = 0;
    while (offset < contents.size()) {
        const auto written = write(file, contents.data() + offset, contents.size() - offset);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            const int saved = errno;
            close(file);
            errno = saved;
            Check(false, "write store marker");
        }
        offset += static_cast<size_t>(written);
    }
    const int synced = fsync(file);
    const int saved = errno;
    const int closed = close(file);
    if (synced != 0) {
        errno = saved;
        Check(false, "sync store marker");
    }
    Check(closed == 0, "close store marker");
    SyncDirectories();
}

std::string StoreLease::ReadMarker(const char* name)
{
    const int file = openat(descriptor_, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    Check(file >= 0, "required store marker is missing", "STORE_INCOMPLETE");
    struct stat stat_buffer
    { };
    if (fstat(file, &stat_buffer) != 0 || !S_ISREG(stat_buffer.st_mode) || stat_buffer.st_size < 0 ||
        stat_buffer.st_size > 512)
    {
        close(file);
        throw Failure({"CORRUPTION", "invalid store marker", RetryHint::OperatorAction});
    }
    std::string contents(static_cast<size_t>(stat_buffer.st_size), '\0');
    size_t offset = 0;
    while (offset < contents.size()) {
        const auto read_bytes = read(file, contents.data() + offset, contents.size() - offset);
        if (read_bytes < 0 && errno == EINTR) {
            continue;
        }
        if (read_bytes <= 0) {
            close(file);
            throw Failure({"CORRUPTION", "truncated store marker", RetryHint::OperatorAction});
        }
        offset += static_cast<size_t>(read_bytes);
    }
    Check(close(file) == 0, "close store marker");
    return contents;
}

void StoreLease::SyncDirectories()
{
    auto directory = root_;
    while (!directory.empty()) {
        const int file = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        Check(file >= 0, "open durable ancestor directory");
        const int synced = fsync(file);
        const int saved = errno;
        const int closed = close(file);
        if (synced != 0) {
            errno = saved;
            Check(false, "sync ancestor directory");
        }
        Check(closed == 0, "close ancestor directory");
        if (directory == directory.root_path()) {
            break;
        }
        directory = directory.parent_path();
    }
}

void StoreLease::MarkReady(const StoreInfo& info)
{
    WriteMarker("store.ready", Identity(info));
}

std::string NewUuid()
{
    std::array<unsigned char, 16> bytes{};
    size_t offset = 0;
    while (offset < bytes.size()) {
        const auto count = getrandom(bytes.data() + offset, bytes.size() - offset, 0);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        Check(count > 0, "generate attempt UUID");
        offset += static_cast<size_t>(count);
    }
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
    for (size_t i = 0; i < 6; ++i) {
        bytes[i] = static_cast<unsigned char>((now_ms >> ((5 - i) * 8)) & 255U);
    }
    bytes[6] = static_cast<unsigned char>((bytes[6] & 15U) | 112U);
    bytes[8] = static_cast<unsigned char>((bytes[8] & 63U) | 128U);
    constexpr std::string_view hex = "0123456789abcdef";
    std::string uuid;
    for (size_t i = 0; i < bytes.size(); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            uuid.push_back('-');
        }
        uuid.push_back(hex[bytes[i] >> 4]);
        uuid.push_back(hex[bytes[i] & 15]);
    }
    return uuid;
}

} // namespace glamdb::detail
