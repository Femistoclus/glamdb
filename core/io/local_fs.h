#pragma once

#include "core/io/result.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace NGlamDB::NIO {

enum class EFileKind : std::uint8_t { Other, Data, Manifest, ManifestList, Metadata };

struct TIOTag
{
    EFileKind Kind = EFileKind::Other;
    std::string AttemptId;
};

class TFSHandle
{
public:
    virtual ~TFSHandle() = default;
};

using THandle = std::shared_ptr<TFSHandle>;

enum class EFileType : std::uint8_t { Regular, Directory, Other };

struct TFileStat
{
    std::uint64_t Device;
    std::uint64_t Inode;
    std::uint64_t Size;
    EFileType Type;
    // Allocated bytes (st_blocks * 512); zero when the adapter cannot tell.
    std::uint64_t Allocated = 0;
};

struct TSpaceInfo
{
    std::uint64_t Capacity;
    std::uint64_t Available;
};

struct ILocalFS
{
    virtual ~ILocalFS() = default;
    virtual TResult<THandle> OpenDir(const std::filesystem::path& absolutePath) = 0;
    virtual TResult<THandle> OpenDir(const THandle& parent, std::string_view name) = 0;
    virtual std::error_code MakeDir(const THandle& parent, std::string_view name) = 0;
    virtual TResult<THandle> CreateExclusive(const THandle& parent, std::string_view name, const TIOTag& tag = {}) = 0;
    virtual TResult<THandle> OpenRead(const THandle& parent, std::string_view name) = 0;
    virtual TResult<std::size_t> Write(const THandle& file, std::span<const std::byte> bytes) = 0;
    virtual TResult<std::size_t> PRead(const THandle& file, std::uint64_t offset, std::span<std::byte> bytes) = 0;
    virtual TResult<TFileStat> Stat(const THandle& handle) = 0;
    virtual TResult<TSpaceInfo> StatFs(const THandle& directory) = 0;
    virtual TResult<std::uint64_t> MeasureTree(const THandle&, std::uint64_t);
    // Removes one regular-file entry; used only for superseded metadata roots.
    virtual std::error_code Unlink(const THandle& parent, std::string_view name);
    virtual std::error_code SyncFile(const THandle& file) = 0;
    virtual std::error_code SyncDir(const THandle& directory) = 0;
    virtual std::error_code Close(const THandle& handle) = 0;
};

class TPosixLocalFS final : public ILocalFS
{
public:
    TResult<THandle> OpenDir(const std::filesystem::path& absolutePath) override;
    TResult<THandle> OpenDir(const THandle& parent, std::string_view name) override;
    std::error_code MakeDir(const THandle& parent, std::string_view name) override;
    TResult<THandle> CreateExclusive(const THandle& parent, std::string_view name, const TIOTag& tag = {}) override;
    TResult<THandle> OpenRead(const THandle& parent, std::string_view name) override;
    TResult<std::size_t> Write(const THandle& file, std::span<const std::byte> bytes) override;
    TResult<std::size_t> PRead(const THandle& file, std::uint64_t offset, std::span<std::byte> bytes) override;
    TResult<TFileStat> Stat(const THandle& handle) override;
    TResult<TSpaceInfo> StatFs(const THandle& directory) override;
    TResult<std::uint64_t> MeasureTree(const THandle& directory, std::uint64_t limit) override;
    std::error_code Unlink(const THandle& parent, std::string_view name) override;
    std::error_code SyncFile(const THandle& file) override;
    std::error_code SyncDir(const THandle& directory) override;
    std::error_code Close(const THandle& handle) override;
    static std::string_view Profile() noexcept;
};

}
