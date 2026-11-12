#include "apps/server/drop_store.h"

#include "apps/transport/codec.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstdint>
#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace glamdb::NServer {
namespace {

[[noreturn]] void Fail(std::string_view message, std::string_view code = "PRECONDITION_FAILED")
{
    throw NTransport::TFailure({std::string(code), std::string(message), RetryHint::OperatorAction});
}

void Validate(bool condition, std::string_view message)
{
    if (!condition) {
        Fail(message);
    }
}

void CheckIO(bool condition, std::string_view message)
{
    if (!condition) {
        Fail(message, "IO_ERROR");
    }
}

class TDescriptor
{
public:
    explicit TDescriptor(int descriptor = -1)
        : Descriptor_(descriptor)
    { }

    ~TDescriptor()
    {
        if (Descriptor_ >= 0) {
            close(Descriptor_);
        }
    }

    TDescriptor(const TDescriptor&) = delete;
    TDescriptor& operator=(const TDescriptor&) = delete;
    TDescriptor(TDescriptor&& other) noexcept
        : Descriptor_(other.Release())
    { }

    TDescriptor& operator=(TDescriptor&& other) noexcept
    {
        std::swap(Descriptor_, other.Descriptor_);
        return *this;
    }

    int Get() const
    {
        return Descriptor_;
    }

    int Release()
    {
        return std::exchange(Descriptor_, -1);
    }

    void Close()
    {
        if (Descriptor_ >= 0) {
            CheckIO(close(Release()) == 0, "Cannot close managed database descriptor.");
        }
    }

private:
    int Descriptor_ = -1;
};

TDescriptor OpenDirectory(int parent, const char* name)
{
    const int descriptor = openat(parent, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    Validate(descriptor >= 0, "Cannot safely open managed database directory.");
    return TDescriptor(descriptor);
}

TDescriptor OpenServiceRoot(const std::filesystem::path& path)
{
    auto directory = OpenDirectory(AT_FDCWD, "/");
    for (const auto& component : path.relative_path()) {
        auto next = OpenDirectory(directory.Get(), component.c_str());
        directory.Close();
        directory = std::move(next);
    }
    return directory;
}

struct stat ReadStatus(int descriptor)
{
    struct stat status
    { };
    CheckIO(fstat(descriptor, &status) == 0, "Cannot inspect managed database descriptor.");
    return status;
}

std::optional<struct stat> ReadEntry(int directory, const char* name)
{
    struct stat status
    { };
    if (fstatat(directory, name, &status, AT_SYMLINK_NOFOLLOW) == 0) {
        return status;
    }
    if (errno == ENOENT) {
        return std::nullopt;
    }
    Fail("Cannot inspect managed database contents.", "IO_ERROR");
}

bool IsSameEntry(const struct stat& left, const struct stat& right)
{
    return left.st_dev == right.st_dev && left.st_ino == right.st_ino;
}

void ValidateDirectory(const struct stat& status, dev_t device)
{
    Validate(S_ISDIR(status.st_mode) && status.st_dev == device && status.st_uid == geteuid(),
        "Managed database directory ownership or type is unsafe.");
}

void ValidateFile(const struct stat& status, dev_t device)
{
    Validate(S_ISREG(status.st_mode) && status.st_dev == device && status.st_uid == geteuid() && status.st_nlink == 1,
        "Managed database file ownership or type is unsafe.");
}

class TDirectoryEntries
{
public:
    explicit TDirectoryEntries(int descriptor)
    {
        auto copied = OpenDirectory(descriptor, ".");
        Entries_ = fdopendir(copied.Get());
        CheckIO(Entries_ != nullptr, "Cannot enumerate managed database directory.");
        copied.Release();
    }

    ~TDirectoryEntries()
    {
        if (Entries_) {
            closedir(Entries_);
        }
    }

    TDirectoryEntries(const TDirectoryEntries&) = delete;
    TDirectoryEntries& operator=(const TDirectoryEntries&) = delete;

    std::optional<std::string> Read()
    {
        for (;;) {
            errno = 0;
            const auto* entry = readdir(Entries_);
            if (!entry) {
                CheckIO(errno == 0, "Cannot enumerate managed database directory.");
                return std::nullopt;
            }
            const std::string_view name = entry->d_name;
            if (name != "." && name != "..") {
                return std::string(name);
            }
        }
    }

    void Close()
    {
        CheckIO(closedir(std::exchange(Entries_, nullptr)) == 0, "Cannot close managed database directory listing.");
    }

private:
    DIR* Entries_ = nullptr;
};

bool IsUuid(std::string_view value)
{
    if (value.size() != 36) {
        return false;
    }
    for (size_t i = 0; i < value.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (value[i] != '-') {
                return false;
            }
        } else if (!((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f'))) {
            return false;
        }
    }
    return true;
}

bool IsDigits(std::string_view value)
{
    return !value.empty() && std::ranges::all_of(value, [](char character) {
        return character >= '0' && character <= '9';
    });
}

bool IsIdentityNumber(std::string_view value)
{
    uintmax_t number = 0;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), number);
    return parsed.ec == std::errc{} && parsed.ptr == value.data() + value.size() && std::to_string(number) == value;
}

bool IsRecoveryName(std::string_view name)
{
    constexpr std::string_view prefix = ".recovery-";
    return name.size() == prefix.size() + 6 && name.starts_with(prefix) &&
        std::ranges::all_of(name.substr(prefix.size()), [](char character) {
            return (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
                (character >= '0' && character <= '9');
        });
}

bool IsDataName(std::string_view name)
{
    return name.size() == 36 + 1 + 36 + std::string_view("-00000.parquet").size() && IsUuid(name.substr(0, 36)) &&
        name[36] == '-' && IsUuid(name.substr(37, 36)) && name.ends_with("-00000.parquet");
}

bool IsMetadataName(std::string_view name)
{
    if (name.ends_with(".metadata.json")) {
        name.remove_suffix(std::string_view(".metadata.json").size());
        const auto separator = name.find('-');
        return separator != std::string_view::npos && separator >= 5 && IsDigits(name.substr(0, separator)) &&
            IsUuid(name.substr(separator + 1));
    }
    if (!name.ends_with(".avro")) {
        return false;
    }
    name.remove_suffix(5);
    if (name.size() > 38 && IsUuid(name.substr(0, 36)) && name.substr(36, 2) == "-m") {
        return IsDigits(name.substr(38));
    }
    if (!name.starts_with("snap-") || name.size() < 5 + 1 + 1 + 1 + 1 + 36) {
        return false;
    }
    const auto uuidSeparator = name.size() - 37;
    if (name[uuidSeparator] != '-' || !IsUuid(name.substr(uuidSeparator + 1))) {
        return false;
    }
    const auto numbers = name.substr(5, uuidSeparator - 5);
    const auto attemptSeparator = numbers.rfind('-');
    if (attemptSeparator == std::string_view::npos || !IsDigits(numbers.substr(attemptSeparator + 1))) {
        return false;
    }
    const auto snapshot = numbers.substr(0, attemptSeparator);
    int64_t snapshotID = 0;
    const auto parsed = std::from_chars(snapshot.data(), snapshot.data() + snapshot.size(), snapshotID);
    return parsed.ec == std::errc{} && parsed.ptr == snapshot.data() + snapshot.size();
}

enum class ELayout : uint8_t { Store, Warehouse, Collection, Data, Metadata };
enum class EMarker : uint8_t { None, Identity, Deletion };

struct TEntryKind
{
    bool Directory = false;
    ELayout Layout = ELayout::Store;
    EMarker Marker = EMarker::None;
};

TEntryKind ClassifyEntry(ELayout layout, std::string_view name)
{
    if (layout == ELayout::Store) {
        if (name == "warehouse") {
            return {.Directory = true, .Layout = ELayout::Warehouse};
        }
        if (name == "store.identity" || name == "store.ready") {
            return {.Marker = EMarker::Identity};
        }
        if (name == "store.deleting") {
            return {.Marker = EMarker::Deletion};
        }
        if (name == "catalog.sqlite" || name == "catalog.sqlite-journal" || IsRecoveryName(name)) {
            return {};
        }
    } else if (layout == ELayout::Warehouse && IsUuid(name)) {
        return {.Directory = true, .Layout = ELayout::Collection};
    } else if (layout == ELayout::Collection && (name == "data" || name == "metadata")) {
        return {.Directory = true, .Layout = name == "data" ? ELayout::Data : ELayout::Metadata};
    } else if ((layout == ELayout::Data && IsDataName(name)) || (layout == ELayout::Metadata && IsMetadataName(name))) {
        return {};
    }
    Fail("Unexpected managed database contents require inspection.");
}

struct TLocation
{
    int Descriptor;
    const TLocation* Parent;
    std::string_view Name;
    struct stat Status;
};

using TCheckpoint = std::function<void(std::string_view)>;

void OnCheckpoint(const TCheckpoint& checkpoint, std::string_view name)
{
    if (checkpoint) {
        checkpoint(name);
    }
}

void SyncDescriptor(int descriptor, std::string_view message)
{
    int result = -1;
    do {
        result = fsync(descriptor);
    } while (result < 0 && errno == EINTR);
    CheckIO(result == 0, message);
}

void SyncDirectory(int descriptor, const TCheckpoint& checkpoint, std::string_view name)
{
    SyncDescriptor(descriptor, "Cannot synchronize managed database directory.");
    OnCheckpoint(checkpoint, name);
}

std::string GenerateDeletionToken()
{
    std::array<unsigned char, 16> bytes{};
    size_t offset = 0;
    while (offset < bytes.size()) {
        const auto count = getrandom(bytes.data() + offset, bytes.size() - offset, 0);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        CheckIO(count > 0, "Cannot generate managed database deletion identity.");
        offset += static_cast<size_t>(count);
    }
    bytes[6] = static_cast<unsigned char>((bytes[6] & 15U) | 64U);
    bytes[8] = static_cast<unsigned char>((bytes[8] & 63U) | 128U);
    constexpr std::string_view hex = "0123456789abcdef";
    std::string token;
    for (size_t i = 0; i < bytes.size(); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            token.push_back('-');
        }
        token.push_back(hex[bytes[i] >> 4]);
        token.push_back(hex[bytes[i] & 15]);
    }
    return token;
}

std::string ReadMarkerContents(int descriptor)
{
    const auto status = ReadStatus(descriptor);
    Validate(status.st_size > 0 && status.st_size <= 128, "Managed database marker is invalid.");
    std::string contents(static_cast<size_t>(status.st_size), '\0');
    size_t offset = 0;
    while (offset < contents.size()) {
        const auto count =
            pread(descriptor, contents.data() + offset, contents.size() - offset, static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        CheckIO(count > 0, "Cannot read managed database marker.");
        offset += static_cast<size_t>(count);
    }
    Validate(ReadStatus(descriptor).st_size == status.st_size, "Managed database marker changed during inspection.");
    return contents;
}

} // namespace

class TDropStore::TImpl
{
public:
    TImpl(std::filesystem::path serviceRoot,
        std::string id,
        const std::optional<TDropIdentity>& expected,
        int serviceRootDescriptor,
        int databasesDescriptor)
        : ServiceRoot_(std::move(serviceRoot))
        , ID_(std::move(id))
        , Marker_("GLAMDB\n1\n" + ID_ + "\nlocal-delete-extra-v1\n")
        , SealPrefix_("GLAMDB-DROP\n1\n" + ID_ + "\n")
        , RequireMarkers_(!expected || expected->Token.empty())
        , Prepared_(expected && !expected->Token.empty())
    {
        Validate(ServiceRoot_.is_absolute() && ServiceRoot_ == ServiceRoot_.lexically_normal() &&
                ServiceRoot_.has_filename() && ServiceRoot_.string().find('\0') == std::string::npos && IsUuid(ID_) &&
                serviceRootDescriptor >= -1 && databasesDescriptor >= -1,
            "Managed database location or identity is invalid.");
        if (expected) {
            Validate(IsIdentityNumber(expected->Device) && IsIdentityNumber(expected->Inode) &&
                    (expected->Token.empty() || IsUuid(expected->Token)),
                "Recorded managed database directory identity is invalid.");
        }
        Service_ =
            serviceRootDescriptor < 0 ? OpenServiceRoot(ServiceRoot_) : OpenDirectory(serviceRootDescriptor, ".");
        ServiceStatus_ = ReadStatus(Service_.Get());
        ValidateDirectory(ServiceStatus_, ServiceStatus_.st_dev);
        Parent_ = databasesDescriptor < 0 ? OpenDirectory(Service_.Get(), "databases")
                                          : OpenDirectory(databasesDescriptor, ".");
        ParentStatus_ = ReadStatus(Parent_.Get());
        ValidateDirectory(ParentStatus_, ServiceStatus_.st_dev);
        const auto root = ReadEntry(Parent_.Get(), ID_.c_str());
        if (!root) {
            if (!expected || expected->Token.empty()) {
                Fail("Managed database directory is absent.");
            }
            Identity_ = *expected;
            RootRemoved_ = true;
            ValidateRootBinding();
            return;
        }
        ValidateDirectory(*root, ParentStatus_.st_dev);
        Root_ = OpenDirectory(Parent_.Get(), ID_.c_str());
        RootStatus_ = ReadStatus(Root_.Get());
        Validate(IsSameEntry(*root, RootStatus_), "Managed database directory was replaced.");
        ValidateDirectory(RootStatus_, ParentStatus_.st_dev);
        if (flock(Root_.Get(), LOCK_EX | LOCK_NB) != 0) {
            Fail("Managed database directory is already owned.", "STATE_IN_USE");
        }
        Identity_ = {.Device = std::to_string(RootStatus_.st_dev),
            .Inode = std::to_string(RootStatus_.st_ino),
            .Token = expected ? expected->Token : std::string{}};
        Validate(!expected || (expected->Device == Identity_.Device && expected->Inode == Identity_.Inode),
            "Managed database directory was replaced.");
        Preflight();
        if (const auto token = ReadExistingSeal(/*sync*/ false)) {
            Identity_.Token = *token;
        }
    }

    TDropIdentity GetIdentity() const
    {
        return Identity_;
    }

    TDropIdentity Prepare()
    {
        ValidateRootBinding();
        if (RootRemoved_) {
            return Identity_;
        }
        Preflight();
        auto token = ReadExistingSeal(/*sync*/ true);
        if (!token) {
            token = GenerateDeletionToken();
            CreateSeal(*token);
        }
        ValidateRootBinding();
        SyncDirectory(Root_.Get(), {}, "seal-prepared");
        Identity_.Token = *token;
        Prepared_ = true;
        return Identity_;
    }

    void Remove(const TCheckpoint& checkpoint)
    {
        if (Finished_) {
            return;
        }
        ValidateRootBinding();
        if (!RootRemoved_) {
            Validate(Prepared_ && !Identity_.Token.empty(), "Managed database deletion seal has not been prepared.");
            Preflight();
            RequireMarkers_ = false;
            const TLocation root{Root_.Get(), nullptr, ID_, RootStatus_};
            Walk(root, ELayout::Store, /*remove*/ true, checkpoint);
            SyncDirectory(Root_.Get(), checkpoint, "directory-synced");
            RemoveMarker(root, "store.ready", EMarker::Identity, checkpoint);
            RemoveMarker(root, "store.identity", EMarker::Identity, checkpoint);
            SyncDirectory(Root_.Get(), checkpoint, "root-synced");
            RemoveMarker(root, "store.deleting", EMarker::Deletion, checkpoint);
            ValidateRootBinding();
            SyncDirectory(Root_.Get(), checkpoint, "seal-synced");
            ValidateRootBinding();
            CheckIO(unlinkat(Parent_.Get(), ID_.c_str(), AT_REMOVEDIR) == 0,
                "Cannot remove managed database directory.");
            RootRemoved_ = true;
            OnCheckpoint(checkpoint, "root-unlinked");
        }
        ValidateRootBinding();
        SyncDirectory(Parent_.Get(), checkpoint, "parent-synced");
        ValidateRootBinding();
        Root_.Close();
        Parent_.Close();
        Service_.Close();
        Finished_ = true;
    }

private:
    const std::filesystem::path ServiceRoot_;
    const std::string ID_;
    const std::string Marker_;
    const std::string SealPrefix_;

    TDescriptor Service_;
    TDescriptor Parent_;
    TDescriptor Root_;
    struct stat ServiceStatus_
    { };
    struct stat ParentStatus_
    { };
    struct stat RootStatus_
    { };
    TDropIdentity Identity_;
    bool RequireMarkers_ = true;
    bool Prepared_ = false;
    bool RootRemoved_ = false;
    bool Finished_ = false;

    void ValidateRootBinding() const
    {
        auto service = OpenServiceRoot(ServiceRoot_);
        const auto status = ReadStatus(service.Get());
        ValidateDirectory(status, ServiceStatus_.st_dev);
        Validate(IsSameEntry(status, ServiceStatus_), "Managed database service directory was replaced.");
        service.Close();
        const auto parent = ReadEntry(Service_.Get(), "databases");
        if (!parent) {
            Fail("Managed database parent directory is absent.");
        }
        Validate(IsSameEntry(*parent, ParentStatus_), "Managed database parent directory was replaced.");
        ValidateDirectory(*parent, ServiceStatus_.st_dev);
        const auto root = ReadEntry(Parent_.Get(), ID_.c_str());
        if (RootRemoved_) {
            Validate(!root, "Managed database directory reappeared after removal.");
        } else {
            if (!root) {
                Fail("Managed database directory is absent.");
            }
            Validate(IsSameEntry(*root, RootStatus_), "Managed database directory was replaced.");
            ValidateDirectory(*root, ParentStatus_.st_dev);
        }
    }

    void ValidateBinding(const TLocation& location) const
    {
        ValidateRootBinding();
        for (auto* current = &location; current->Parent; current = current->Parent) {
            const auto status = ReadEntry(current->Parent->Descriptor, std::string(current->Name).c_str());
            if (!status) {
                Fail("Managed database directory is absent.");
            }
            Validate(IsSameEntry(*status, current->Status), "Managed database directory was replaced.");
            ValidateDirectory(*status, RootStatus_.st_dev);
        }
    }

    void ValidateMarker(int descriptor) const
    {
        Validate(ReadMarkerContents(descriptor) == Marker_, "Managed database marker does not match.");
    }

    std::string ReadDeletionToken(int descriptor) const
    {
        const auto contents = ReadMarkerContents(descriptor);
        Validate(contents.size() == SealPrefix_.size() + 37 && contents.starts_with(SealPrefix_) &&
                contents.back() == '\n',
            "Managed database deletion seal is invalid.");
        const auto token = contents.substr(SealPrefix_.size(), 36);
        Validate(IsUuid(token) && (Identity_.Token.empty() || token == Identity_.Token),
            "Managed database deletion seal does not match.");
        return token;
    }

    std::optional<std::string> ReadExistingSeal(bool sync) const
    {
        const auto status = ReadEntry(Root_.Get(), "store.deleting");
        if (!status) {
            return std::nullopt;
        }
        ValidateFile(*status, RootStatus_.st_dev);
        TDescriptor file(openat(Root_.Get(), "store.deleting", O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        Validate(file.Get() >= 0, "Cannot safely open managed database deletion seal.");
        const auto opened = ReadStatus(file.Get());
        ValidateFile(opened, RootStatus_.st_dev);
        Validate(IsSameEntry(*status, opened), "Managed database deletion seal was replaced.");
        auto token = ReadDeletionToken(file.Get());
        if (sync) {
            ValidateRootBinding();
            SyncDescriptor(file.Get(), "Cannot synchronize managed database deletion seal.");
        }
        const auto current = ReadEntry(Root_.Get(), "store.deleting");
        Validate(current && IsSameEntry(*current, opened), "Managed database deletion seal was replaced.");
        file.Close();
        return token;
    }

    void CreateSeal(const std::string& token) const
    {
        ValidateRootBinding();
        TDescriptor file(
            openat(Root_.Get(), "store.deleting", O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
        CheckIO(file.Get() >= 0, "Cannot create managed database deletion seal.");
        const auto status = ReadStatus(file.Get());
        ValidateFile(status, RootStatus_.st_dev);
        const auto contents = SealPrefix_ + token + "\n";
        size_t offset = 0;
        while (offset < contents.size()) {
            ValidateRootBinding();
            const auto count = write(file.Get(), contents.data() + offset, contents.size() - offset);
            if (count < 0 && errno == EINTR) {
                continue;
            }
            CheckIO(count > 0, "Cannot write managed database deletion seal.");
            offset += static_cast<size_t>(count);
        }
        ValidateRootBinding();
        SyncDescriptor(file.Get(), "Cannot synchronize managed database deletion seal.");
        const auto current = ReadEntry(Root_.Get(), "store.deleting");
        Validate(current && IsSameEntry(*current, status), "Managed database deletion seal was replaced.");
        file.Close();
    }

    void InspectFile(const TLocation& location,
        const std::string& name,
        const struct stat& expected,
        EMarker marker,
        bool remove,
        const TCheckpoint& checkpoint) const
    {
        ValidateFile(expected, RootStatus_.st_dev);
        TDescriptor file(openat(location.Descriptor, name.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        Validate(file.Get() >= 0, "Cannot safely open managed database file.");
        const auto status = ReadStatus(file.Get());
        ValidateFile(status, RootStatus_.st_dev);
        Validate(IsSameEntry(status, expected), "Managed database file was replaced.");
        if (marker == EMarker::Identity) {
            ValidateMarker(file.Get());
        } else if (marker == EMarker::Deletion) {
            ReadDeletionToken(file.Get());
        }
        const auto current = ReadEntry(location.Descriptor, name.c_str());
        if (!current) {
            Fail("Managed database file is absent.");
        }
        Validate(IsSameEntry(*current, status), "Managed database file was replaced.");
        ValidateFile(*current, RootStatus_.st_dev);
        if (remove) {
            ValidateBinding(location);
            CheckIO(unlinkat(location.Descriptor, name.c_str(), 0) == 0, "Cannot remove managed database file.");
            OnCheckpoint(checkpoint, marker == EMarker::Deletion ? "seal-unlinked" : "file-unlinked");
        }
        file.Close();
    }

    unsigned Walk(const TLocation& location, ELayout layout, bool remove, const TCheckpoint& checkpoint) const
    {
        ValidateBinding(location);
        TDirectoryEntries entries(location.Descriptor);
        unsigned markers = 0;
        while (const auto name = entries.Read()) {
            const auto kind = ClassifyEntry(layout, *name);
            const auto status = ReadEntry(location.Descriptor, name->c_str());
            if (!status) {
                Fail("Managed database contents changed during deletion.");
            }
            if (kind.Directory) {
                InspectDirectory(location, *name, *status, kind.Layout, remove, checkpoint);
            } else {
                InspectFile(location, *name, *status, kind.Marker, remove && kind.Marker == EMarker::None, checkpoint);
                if (*name == "store.identity") {
                    markers |= 1;
                } else if (*name == "store.ready") {
                    markers |= 2;
                }
            }
        }
        entries.Close();
        return markers;
    }

    void InspectDirectory(const TLocation& location,
        const std::string& name,
        const struct stat& expected,
        ELayout layout,
        bool remove,
        const TCheckpoint& checkpoint) const
    {
        ValidateDirectory(expected, RootStatus_.st_dev);
        auto directory = OpenDirectory(location.Descriptor, name.c_str());
        const auto status = ReadStatus(directory.Get());
        ValidateDirectory(status, RootStatus_.st_dev);
        Validate(IsSameEntry(status, expected), "Managed database directory was replaced.");
        const TLocation child{directory.Get(), &location, name, status};
        Walk(child, layout, remove, checkpoint);
        if (remove) {
            SyncDirectory(directory.Get(), checkpoint, "directory-synced");
            ValidateBinding(child);
            CheckIO(unlinkat(location.Descriptor, name.c_str(), AT_REMOVEDIR) == 0,
                "Cannot remove managed database directory.");
            OnCheckpoint(checkpoint, "directory-unlinked");
        }
        directory.Close();
    }

    void Preflight() const
    {
        const TLocation root{Root_.Get(), nullptr, ID_, RootStatus_};
        const auto markers = Walk(root, ELayout::Store, /*remove*/ false, {});
        Validate(!RequireMarkers_ || markers == 3, "Managed database requires both exact identity markers.");
        Validate(!Prepared_ || ReadExistingSeal(/*sync*/ false).has_value(),
            "Managed database deletion seal is absent; cleanup requires inspection.");
    }

    void RemoveMarker(const TLocation& root, const char* name, EMarker marker, const TCheckpoint& checkpoint) const
    {
        const auto status = ReadEntry(root.Descriptor, name);
        if (status) {
            InspectFile(root, name, *status, marker, /*remove*/ true, checkpoint);
        } else {
            Validate(marker != EMarker::Deletion,
                "Managed database deletion seal is absent; cleanup requires inspection.");
        }
    }
};

TDropStore::TDropStore(const std::filesystem::path& serviceRoot,
    const std::string& id,
    const std::optional<TDropIdentity>& expected,
    int serviceRootDescriptor,
    int databasesDescriptor)
    : Impl_(std::make_unique<TImpl>(serviceRoot, id, expected, serviceRootDescriptor, databasesDescriptor))
{ }

TDropStore::~TDropStore() = default;

TDropIdentity TDropStore::GetIdentity() const
{
    return Impl_->GetIdentity();
}

TDropIdentity TDropStore::Prepare()
{
    return Impl_->Prepare();
}

void TDropStore::Remove(const std::function<void(std::string_view)>& checkpoint)
{
    Impl_->Remove(checkpoint);
}

} // namespace glamdb::NServer
