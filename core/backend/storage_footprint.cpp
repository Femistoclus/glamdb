#include "core/backend/local_storage_backend.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/io/store_dirs.h"
#include "core/storage/base_state.h"
#include "storage/resources.h"

#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <limits>
#include <map>
#include <set>

namespace glamdb::detail {
namespace {
constexpr size_t MaxEntries = 100000;
constexpr size_t MaxDepth = 16;
constexpr uint64_t InventoryMemory = 64ULL << 20;

struct TDescriptor
{
    int Value = -1;
    explicit TDescriptor(int value)
        : Value(value)
    { }
    ~TDescriptor()
    {
        if (Value >= 0) {
            ::close(Value);
        }
    }
    TDescriptor(const TDescriptor&) = delete;
};

void Add(uint64_t& target, uint64_t value)
{
    Require(value <= std::numeric_limits<uint64_t>::max() - target,
        "RESOURCE_EXHAUSTED",
        "physical inventory byte count overflow");
    target += value;
}

enum class ECategory { Data, Metadata, Catalog, Other, Shared };

ECategory Category(std::string_view path)
{
    if (path == "catalog.sqlite" || path == "catalog.sqlite-journal" || path == "catalog.sqlite-wal" ||
        path == "catalog.sqlite-shm")
    {
        return ECategory::Catalog;
    }
    if (path.starts_with("warehouse/") && path.find("/metadata/") != std::string_view::npos) {
        return ECategory::Metadata;
    }
    if (path.starts_with("warehouse/") && path.find("/data/") != std::string_view::npos && path.ends_with(".parquet")) {
        return ECategory::Data;
    }
    return ECategory::Other;
}

struct TEntry
{
    uint64_t Bytes;
    uint64_t Allocated;
    ECategory Category;
    bool Data = false;
    bool Metadata = false;
    bool Warehouse = false;
};

struct TInventory
{
    StorageFootprint Result;
    uint64_t Device = 0;
    std::string Prefix;
    std::map<std::string, uint64_t, std::less<>> Data;
    std::set<std::string, std::less<>> Metadata;
    std::map<std::pair<uint64_t, uint64_t>, TEntry> Inodes;

    void Walk(int descriptor, const std::string& path, size_t depth)
    {
        Require(depth <= MaxDepth, "RESOURCE_EXHAUSTED", "physical inventory depth budget exhausted");
        struct stat before
        { };
        Require(::fstat(descriptor, &before) == 0 && static_cast<uint64_t>(before.st_dev) == Device,
            "PRECONDITION_FAILED",
            "physical inventory directory changed");
        Add(Result.DatabaseAllocatedBytes, static_cast<uint64_t>(before.st_blocks) * 512);
        TDescriptor copy(::fcntl(descriptor, F_DUPFD_CLOEXEC, 0));
        Require(copy.Value >= 0, "IO_ERROR", "cannot duplicate physical inventory directory");
        DIR* stream = ::fdopendir(copy.Value);
        Require(stream != nullptr, "IO_ERROR", "cannot enumerate physical store");
        copy.Value = -1;
        struct TDirectoryGuard
        {
            DIR* Stream;
            ~TDirectoryGuard()
            {
                ::closedir(Stream);
            }
        } guard{stream};
        while (true) {
            errno = 0;
            const auto* entry = ::readdir(stream);
            if (entry == nullptr) {
                Require(errno == 0, "IO_ERROR", "physical store enumeration failed");
                break;
            }
            const std::string_view name(entry->d_name);
            if (name == "." || name == "..") {
                continue;
            }
            Require(++Result.Entries <= MaxEntries, "RESOURCE_EXHAUSTED", "physical inventory entry budget exhausted");
            struct stat item
            { };
            Require(::fstatat(descriptor, entry->d_name, &item, AT_SYMLINK_NOFOLLOW) == 0,
                "IO_ERROR",
                "physical store entry disappeared");
            Require(static_cast<uint64_t>(item.st_dev) == Device && !S_ISLNK(item.st_mode),
                "PRECONDITION_FAILED",
                "physical store contains a symlink or mount crossing");
            const auto relative = path.empty() ? std::string(name) : path + "/" + std::string(name);
            if (S_ISDIR(item.st_mode)) {
                TDescriptor child(::openat(descriptor, entry->d_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
                struct stat opened
                { };
                Require(child.Value >= 0 && ::fstat(child.Value, &opened) == 0 && opened.st_dev == item.st_dev &&
                        opened.st_ino == item.st_ino,
                    "PRECONDITION_FAILED",
                    "physical directory identity changed");
                Walk(child.Value, relative, depth + 1);
                continue;
            }
            Require(S_ISREG(item.st_mode) && item.st_size >= 0 && item.st_blocks >= 0,
                "PRECONDITION_FAILED",
                "physical store contains a special file");
            const auto uri = Prefix + relative;
            const auto known = Data.find(uri);
            const bool data = known != Data.end();
            if (data) {
                Require(known->second == static_cast<uint64_t>(item.st_size),
                    "CORRUPTION",
                    "committed data file length differs from verified membership");
                Data.erase(known);
            }
            const bool metadata = Metadata.erase(uri) != 0;
            TEntry record{static_cast<uint64_t>(item.st_size),
                static_cast<uint64_t>(item.st_blocks) * 512,
                Category(relative),
                data,
                metadata,
                relative.starts_with("warehouse/")};
            const auto [position, inserted] =
                Inodes.emplace(std::pair{static_cast<uint64_t>(item.st_dev), static_cast<uint64_t>(item.st_ino)},
                    record);
            if (!inserted) {
                auto& previous = position->second;
                Require(previous.Bytes == record.Bytes && previous.Allocated == record.Allocated,
                    "PRECONDITION_FAILED",
                    "hardlinked file changed during physical inventory");
                if (previous.Category != record.Category) {
                    previous.Category = ECategory::Shared;
                }
                previous.Data |= data;
                previous.Metadata |= metadata;
                previous.Warehouse |= record.Warehouse;
                ++Result.HardlinkPaths;
            }
        }
        struct stat after
        { };
        Require(::fstat(descriptor, &after) == 0 && before.st_mtime == after.st_mtime &&
                before.st_ctime == after.st_ctime,
            "DATABASE_BUSY",
            "physical directory changed during inventory");
    }
};
} // namespace

Result<StorageFootprint> TLocalStorageBackend::GetStorageFootprint()
{
    try {
        auto memory = Options_.Resources->Reserve(ResourceClass::Metadata, InventoryMemory);
        if (!memory) {
            return std::unexpected(memory.error());
        }
        Require(!Dirs_->CheckIdentity(), "PRECONDITION_FAILED", "owned store identity changed");
        TInventory inventory;
        inventory.Result.StoreID = Store_.store_id;
        inventory.Prefix = "store://" + Store_.store_id + "/";
        uint64_t pathBytes = 0;
        {
            std::lock_guard lock(Mutex_);
            Require(VerifiedGeneration_, "RECOVERY_REQUIRED", "physical inventory requires a verified generation");
            for (const auto& [id, state] : Collections_) {
                (void)id;
                const auto& base = state.Confirmed;
                Require(base != nullptr, "CORRUPTION", "verified membership is missing");
                Require(base->ScanIndex != nullptr || base->Metadata->snapshots.empty(),
                    "CORRUPTION",
                    "nonempty verified membership is missing");
                if (base->ScanIndex != nullptr) {
                    for (const auto& file : base->ScanIndex->Files) {
                        Require(file.SizeBytes >= 0, "CORRUPTION", "negative committed file length");
                        inventory.Data.emplace(file.Path, static_cast<uint64_t>(file.SizeBytes));
                        Add(pathBytes, file.Path.size() + 128);
                    }
                }
                inventory.Metadata.insert(base->MetadataLocation);
                for (const auto& snapshot : base->Metadata->snapshots) {
                    Require(snapshot != nullptr, "CORRUPTION", "snapshot metadata is missing");
                    inventory.Metadata.insert(snapshot->manifest_list);
                    Add(pathBytes, snapshot->manifest_list.size() + 128);
                }
                for (const auto& manifest : base->CurrentManifests) {
                    inventory.Metadata.insert(manifest.manifest_path);
                    Add(pathBytes, manifest.manifest_path.size() + 128);
                }
                Add(pathBytes, base->MetadataLocation.size() + 128);
                Require(pathBytes <= (32ULL << 20), "RESOURCE_EXHAUSTED", "physical inventory path budget exhausted");
            }
        }
        const auto expected = Dirs_->Fs()->Stat(Dirs_->Root());
        Require(static_cast<bool>(expected), "IO_ERROR", "cannot inspect the owned store root");
        TDescriptor root(::open(Dirs_->Path().c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        struct stat actual
        { };
        Require(root.Value >= 0 && ::fstat(root.Value, &actual) == 0 &&
                static_cast<uint64_t>(actual.st_dev) == expected.Value().Device &&
                static_cast<uint64_t>(actual.st_ino) == expected.Value().Inode,
            "PRECONDITION_FAILED",
            "physical store root identity differs");
        inventory.Device = expected.Value().Device;
        inventory.Walk(root.Value, "", 0);
        Require(inventory.Data.empty() && inventory.Metadata.empty(),
            "CORRUPTION",
            "verified storage files are absent from the owned root");
        Require(!Dirs_->CheckIdentity(), "PRECONDITION_FAILED", "owned store identity changed during inventory");
        auto& result = inventory.Result;
        for (const auto& [inode, file] : inventory.Inodes) {
            (void)inode;
            Add(result.DatabaseFileBytes, file.Bytes);
            Add(result.DatabaseAllocatedBytes, file.Allocated);
            ++result.Files;
            switch (file.Category) {
                case ECategory::Data:
                    Add(result.DataFileBytes, file.Bytes);
                    break;
                case ECategory::Metadata:
                    Add(result.IcebergMetadataFileBytes, file.Bytes);
                    break;
                case ECategory::Catalog:
                    Add(result.CatalogFileBytes, file.Bytes);
                    break;
                case ECategory::Other:
                    Add(result.OtherFileBytes, file.Bytes);
                    break;
                case ECategory::Shared:
                    Add(result.SharedFileBytes, file.Bytes);
                    break;
            }
            if (file.Data) {
                Add(result.CommittedDataFileBytes, file.Bytes);
            }
            if (file.Metadata) {
                Add(result.ReferencedMetadataFileBytes, file.Bytes);
            }
            if (file.Warehouse && !file.Data && !file.Metadata) {
                Add(result.UnreferencedWarehouseFileBytes, file.Bytes);
            }
        }
        return result;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::bad_alloc&) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", "physical inventory allocation failed", RetryHint::OperatorAction});
    }
}
} // namespace glamdb::detail
