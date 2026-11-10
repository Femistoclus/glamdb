#pragma once

#include "core/io/io_attempt.h"
#include "core/io/store_dirs.h"

#include <iceberg/file_io.h>

namespace NGlamDB::NStorage {

struct TFileIOLimits
{
    std::uint64_t DataFileBytes = 32ULL << 20;
    std::uint64_t MetadataBytes = 16ULL << 20;
    std::uint64_t ManifestBytes = 8ULL << 20;
    std::uint64_t ManifestListBytes = 16ULL << 20;
};

struct TLocalFileIOState;

class TAttemptScope
{
public:
    TAttemptScope() = default;
    TAttemptScope(const TAttemptScope&) = delete;
    TAttemptScope& operator=(const TAttemptScope&) = delete;
    TAttemptScope(TAttemptScope&& other) noexcept;
    TAttemptScope& operator=(TAttemptScope&& other) noexcept;
    ~TAttemptScope();
    void Reset() noexcept;

private:
    std::shared_ptr<TLocalFileIOState> State_;
    std::shared_ptr<NIO::TIOAttempt> Attempt_;

    friend class TDurableLocalFileIO;
    TAttemptScope(std::shared_ptr<TLocalFileIOState> state, std::shared_ptr<NIO::TIOAttempt> attempt);
};

class TDurableLocalFileIO final : public iceberg::FileIO
{
public:
    static iceberg::Result<std::shared_ptr<TDurableLocalFileIO>>
    Make(std::shared_ptr<NIO::TStoreDirs> directories, std::string storeId, TFileIOLimits limits = {});
    iceberg::Result<TAttemptScope> BindAttempt(std::shared_ptr<NIO::TIOAttempt> attempt);
    // Implicit reads use the binding thread's attempt; scans retain an independent read-only view.
    iceberg::Result<std::shared_ptr<TDurableLocalFileIO>> ReadOnlyView() const;
    std::uint64_t DeniedDeleteCount() const noexcept;

    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string location) override;
    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string location,
        std::size_t length) override;
    iceberg::Result<std::unique_ptr<iceberg::OutputFile>> NewOutputFile(std::string location) override;
    iceberg::Result<std::string> ReadFile(const std::string& location, std::optional<std::size_t> length) override;
    iceberg::Status WriteFile(const std::string& location, std::string_view content) override;
    iceberg::Status DeleteFile(const std::string& location) override;
    iceberg::Status DeleteFiles(const std::vector<std::string>& locations) override;

private:
    std::shared_ptr<TLocalFileIOState> State_;

    explicit TDurableLocalFileIO(std::shared_ptr<TLocalFileIOState> state);
    iceberg::Result<std::unique_ptr<iceberg::InputFile>>
    Input(std::string location, std::optional<std::uint64_t> length, std::shared_ptr<NIO::TIOAttempt> attempt);
    iceberg::Result<std::string>
    Read(const std::string& location, std::optional<std::size_t> length, std::shared_ptr<NIO::TIOAttempt> attempt);
};

}
