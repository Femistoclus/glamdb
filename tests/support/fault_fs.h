#pragma once

#include "core/io/local_fs.h"

#include <optional>
#include <vector>

namespace NGlamDB::NTesting {

enum class EIOOp : std::uint8_t {
    OpenDir,
    MakeDir,
    CreateExclusive,
    OpenRead,
    Write,
    PRead,
    Stat,
    StatFs,
    MeasureTree,
    SyncFile,
    SyncDir,
    Close,
    Publish
};

struct TIOEvent
{
    std::uint64_t Seq;
    EIOOp Op;
    std::string Path;
    NIO::TIOTag Tag;
    std::uint64_t Bytes = 0;
    std::error_code Error;
    std::vector<std::string> References;
};

struct TFaultRule
{
    EIOOp Op;
    std::string PathGlob = "*";
    std::uint64_t Occurrence = 1;
    std::uint64_t Repeat = 1;
    std::error_code Error;
    std::optional<std::size_t> MaxBytes;
};

class TFaultFS final : public NIO::ILocalFS
{
public:
    explicit TFaultFS(NIO::ILocalFS& underlying);
    ~TFaultFS() override;
    void AddRule(TFaultRule rule);
    std::vector<TIOEvent> Events() const;
    std::string JsonLines() const;
    void Publish(std::vector<std::string> references);

    NIO::TResult<NIO::THandle> OpenDir(const std::filesystem::path& path) override;
    NIO::TResult<NIO::THandle> OpenDir(const NIO::THandle& parent, std::string_view name) override;
    std::error_code MakeDir(const NIO::THandle& parent, std::string_view name) override;
    NIO::TResult<NIO::THandle>
    CreateExclusive(const NIO::THandle& parent, std::string_view name, const NIO::TIOTag& tag = {}) override;
    NIO::TResult<NIO::THandle> OpenRead(const NIO::THandle& parent, std::string_view name) override;
    NIO::TResult<std::size_t> Write(const NIO::THandle& file, std::span<const std::byte> bytes) override;
    NIO::TResult<std::size_t>
    PRead(const NIO::THandle& file, std::uint64_t offset, std::span<std::byte> bytes) override;
    NIO::TResult<NIO::TFileStat> Stat(const NIO::THandle& handle) override;
    NIO::TResult<NIO::TSpaceInfo> StatFs(const NIO::THandle& directory) override;
    NIO::TResult<std::uint64_t> MeasureTree(const NIO::THandle& directory, std::uint64_t limit) override;
    std::error_code SyncFile(const NIO::THandle& file) override;
    std::error_code SyncDir(const NIO::THandle& directory) override;
    std::error_code Close(const NIO::THandle& handle) override;

private:
    struct TState;
    std::unique_ptr<TState> State_;
};

}
