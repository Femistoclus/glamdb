#include "core/io/durable_copy.h"

#include "tests/support/fault_fs.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>

namespace NGlamDB::NIO {
namespace {

using NTesting::EIOOp;
using NTesting::TFaultFS;
using NTesting::TIOEvent;

class TDurableCopyTest : public ::testing::Test
{
protected:
    std::filesystem::path Parent_;
    std::shared_ptr<TPosixLocalFS> Posix_ = std::make_shared<TPosixLocalFS>();
    std::shared_ptr<TFaultFS> Fs_ = std::make_shared<TFaultFS>(*Posix_);
    std::shared_ptr<TWriteLatch> Latch_ = std::make_shared<TWriteLatch>();
    TCopySource Source_;
    TCopyTarget Target_;

    void SetUp() override
    {
        auto name = (std::filesystem::temp_directory_path() / "glamdb-copy-XXXXXX").string();
        ASSERT_NE(::mkdtemp(name.data()), nullptr);
        Parent_ = std::filesystem::canonical(name);
        std::filesystem::create_directory(Parent_ / "source");
        std::filesystem::create_directory(Parent_ / "target");
        auto source = Fs_->OpenDir(Parent_ / "source");
        auto target = Fs_->OpenDir(Parent_ / "target");
        ASSERT_TRUE(source);
        ASSERT_TRUE(target);
        Source_ = {Fs_, source.Value(), "input"};
        Target_ = {Fs_, target.Value(), "output", Latch_};
        Put(Parent_ / "source/input", "abcdef");
    }

    void TearDown() override
    {
        Source_.Directory.reset();
        Target_.Directory.reset();
        std::error_code error;
        std::filesystem::remove_all(Parent_, error);
        EXPECT_FALSE(error);
    }

    static void Put(const std::filesystem::path& path, std::string_view bytes)
    {
        std::ofstream stream(path, std::ios::binary);
        stream.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        ASSERT_TRUE(stream.good());
    }

    std::size_t Count(EIOOp op, std::string_view name = {}) const
    {
        return std::ranges::count_if(Fs_->Events(), [&](const TIOEvent& event) {
            return event.Op == op && (name.empty() || event.Path.ends_with(name));
        });
    }

    void Inject(EIOOp op, std::errc error, std::string path = "*", std::uint64_t occurrence = 1)
    {
        Fs_->AddRule({op, std::move(path), occurrence, 1, std::make_error_code(error), std::nullopt});
    }

    void ExpectOrphan() const
    {
        EXPECT_TRUE(std::filesystem::is_regular_file(Parent_ / "target/output"));
        EXPECT_EQ(Count(EIOOp::Close, "input"), 1);
        EXPECT_EQ(Count(EIOOp::Close, "output"), 1);
    }
};

TEST_F(TDurableCopyTest, CopiesExactBytesWithCheckedSourceCloseBeforeDurability)
{
    const auto copied = CopyDurableFile(Source_, Target_, 6);
    ASSERT_TRUE(copied) << copied.Error();
    EXPECT_EQ(copied.Value().Path, "output");
    EXPECT_EQ(copied.Value().Length, 6);
    EXPECT_EQ(copied.Value().Kind, EFileKind::Other);
    EXPECT_TRUE(copied.Value().AttemptId.empty());
    EXPECT_EQ(HexDigest(copied.Value().Sha256), "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721");
    const auto events = Fs_->Events();
    const auto sourceClose = std::ranges::find_if(events, [](const auto& event) {
        return event.Op == EIOOp::Close && event.Path.ends_with("input");
    });
    const auto sync = std::ranges::find_if(events, [](const auto& event) {
        return event.Op == EIOOp::SyncFile;
    });
    ASSERT_NE(sourceClose, events.end());
    ASSERT_NE(sync, events.end());
    EXPECT_LT(sourceClose->Seq, sync->Seq);
    EXPECT_EQ(Count(EIOOp::SyncFile), 1);
    EXPECT_EQ(Count(EIOOp::SyncDir), 1);
    EXPECT_EQ(Count(EIOOp::Stat, "input"), 2);
    ExpectOrphan();
    std::ifstream output(Parent_ / "target/output", std::ios::binary);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(output), {}), "abcdef");
}

TEST_F(TDurableCopyTest, EmptyFileStillHasDurableReceipt)
{
    Put(Parent_ / "source/input", "");
    const auto copied = CopyDurableFile(Source_, Target_, 0);
    ASSERT_TRUE(copied);
    EXPECT_EQ(copied.Value().Length, 0);
    EXPECT_EQ(HexDigest(copied.Value().Sha256), HexDigest(TSha256{}.Finalize()));
    EXPECT_EQ(Count(EIOOp::PRead), 0);
    EXPECT_EQ(Count(EIOOp::SyncFile), 1);
    EXPECT_EQ(Count(EIOOp::Close), 2);
}

TEST_F(TDurableCopyTest, BoundedChunksRetryInterruptedAndShortReadsAndWrites)
{
    const std::string bytes(2 * 64 * 1024 + 3, 'x');
    Put(Parent_ / "source/input", bytes);
    Fs_->AddRule({EIOOp::PRead, "*", 1, 2, std::make_error_code(std::errc::interrupted), std::nullopt});
    Fs_->AddRule({EIOOp::PRead, "*", 3, 1, {}, 5});
    Fs_->AddRule({EIOOp::Write, "*", 1, 2, std::make_error_code(std::errc::interrupted), std::nullopt});
    Fs_->AddRule({EIOOp::Write, "*", 3, 1, {}, 2});
    const auto copied = CopyDurableFile(Source_, Target_, bytes.size());
    ASSERT_TRUE(copied);
    TSha256 hash;
    ASSERT_FALSE(hash.Update(std::as_bytes(std::span(bytes))));
    EXPECT_EQ(copied.Value().Sha256, hash.Finalize());
    EXPECT_EQ(copied.Value().Length, bytes.size());
    for (const auto& event : Fs_->Events()) {
        if (event.Op == EIOOp::PRead || event.Op == EIOOp::Write) {
            EXPECT_LE(event.Bytes, 64 * 1024);
        }
    }
    EXPECT_GT(Count(EIOOp::PRead), 3);
    EXPECT_GT(Count(EIOOp::Write), 3);
}

TEST_F(TDurableCopyTest, SourceAndDestinationMayUseIndependentFilesystems)
{
    auto otherPosix = std::make_shared<TPosixLocalFS>();
    auto otherFs = std::make_shared<TFaultFS>(*otherPosix);
    auto directory = otherFs->OpenDir(Parent_ / "target");
    ASSERT_TRUE(directory);
    Target_.Fs = otherFs;
    Target_.Directory = directory.Value();
    EXPECT_TRUE(CopyDurableFile(Source_, Target_, 6));
    EXPECT_EQ(Count(EIOOp::Write), 0);
    EXPECT_EQ(Count(EIOOp::Close), 1);
    EXPECT_TRUE(std::ranges::any_of(otherFs->Events(), [](const auto& event) {
        return event.Op == EIOOp::SyncDir;
    }));
}

TEST_F(TDurableCopyTest, RejectsInvalidArgumentsAndLatchedStoreWithoutIo)
{
    const auto events = Fs_->Events().size();
    for (const auto& name :
        {std::string{}, std::string("."), std::string(".."), std::string("dir/file"), std::string("a\0b", 3)})
    {
        auto source = Source_;
        source.Name = name;
        EXPECT_FALSE(CopyDurableFile(source, Target_, 6));
        auto target = Target_;
        target.Name = name;
        EXPECT_FALSE(CopyDurableFile(Source_, target, 6));
    }
    auto source = Source_;
    source.Fs.reset();
    EXPECT_FALSE(CopyDurableFile(source, Target_, 6));
    source = Source_;
    source.Directory.reset();
    EXPECT_FALSE(CopyDurableFile(source, Target_, 6));
    auto target = Target_;
    target.Fs.reset();
    EXPECT_FALSE(CopyDurableFile(Source_, target, 6));
    target = Target_;
    target.Directory.reset();
    EXPECT_FALSE(CopyDurableFile(Source_, target, 6));
    target = Target_;
    target.Latch.reset();
    EXPECT_FALSE(CopyDurableFile(Source_, target, 6));
    EXPECT_EQ(CopyDurableFile(Source_, Target_, std::numeric_limits<std::uint64_t>::max()).Error(),
        std::errc::value_too_large);
    Latch_->Block();
    EXPECT_FALSE(CopyDurableFile(Source_, Target_, 6));
    EXPECT_EQ(Fs_->Events().size(), events);
}

TEST_F(TDurableCopyTest, LengthMismatchDoesNotCreateDestination)
{
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 5).Error(), std::errc::state_not_recoverable);
    EXPECT_EQ(Count(EIOOp::CreateExclusive), 0);
    EXPECT_EQ(Count(EIOOp::Close, "input"), 1);
}

TEST_F(TDurableCopyTest, MissingSourceAndInitialStatFailureNeverCreateDestination)
{
    Source_.Name = "missing";
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), std::errc::no_such_file_or_directory);
    Source_.Name = "input";
    Inject(EIOOp::Stat, std::errc::permission_denied, "*/input");
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), std::errc::permission_denied);
    EXPECT_EQ(Count(EIOOp::CreateExclusive), 0);
    EXPECT_EQ(Count(EIOOp::Close, "input"), 1);
}

TEST_F(TDurableCopyTest, ExistingTargetAndSymlinksAreNeverOverwrittenOrFollowed)
{
    Put(Parent_ / "target/output", "original");
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), std::errc::file_exists);
    std::ifstream existing(Parent_ / "target/output");
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(existing), {}), "original");
    std::filesystem::create_symlink("input", Parent_ / "source/link");
    Source_.Name = "link";
    EXPECT_FALSE(CopyDurableFile(Source_, Target_, 6));
    Source_.Name = "input";
    std::filesystem::create_symlink("output", Parent_ / "target/link");
    Target_.Name = "link";
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), std::errc::file_exists);
    EXPECT_EQ(Count(EIOOp::Write), 0);
}

TEST_F(TDurableCopyTest, SourceReadFailurePreservesFirstErrorAndLeavesUnsyncedOrphan)
{
    Inject(EIOOp::PRead, std::errc::permission_denied);
    Inject(EIOOp::Close, std::errc::io_error, "*/input");
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), std::errc::permission_denied);
    ExpectOrphan();
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
    EXPECT_FALSE(Latch_->IsSet());
}

TEST_F(TDurableCopyTest, DestinationCleanupIoErrorLatchesWithoutReplacingTheReadError)
{
    Inject(EIOOp::PRead, std::errc::permission_denied);
    Inject(EIOOp::Close, std::errc::io_error, "*/output");
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), std::errc::permission_denied);
    ExpectOrphan();
    EXPECT_TRUE(Latch_->IsSet());
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
}

TEST_F(TDurableCopyTest, EarlyEofAndSourceCloseErrorPreventDestinationSync)
{
    Fs_->AddRule({EIOOp::PRead, "*", 1, 1, {}, 0});
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), std::errc::state_not_recoverable);
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
    Target_.Name = "second";
    Inject(EIOOp::Close, std::errc::interrupted, "*/input");
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), std::errc::interrupted);
    EXPECT_EQ(Count(EIOOp::Close, "input"), 2);
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
    EXPECT_FALSE(Latch_->IsSet());
}

class TCopyWriteFailureTest
    : public TDurableCopyTest
    , public ::testing::WithParamInterface<EIOOp>
{ };

TEST_P(TCopyWriteFailureTest, WriteErrorsFailCopyAndLatchOnlyForWriteIo)
{
    Inject(GetParam(), std::errc::io_error, GetParam() == EIOOp::SyncDir ? "*/target" : "*/output");
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), std::errc::io_error);
    EXPECT_TRUE(Latch_->IsSet());
    EXPECT_EQ(Count(EIOOp::Close, "input"), 1);
    if (GetParam() != EIOOp::CreateExclusive) {
        ExpectOrphan();
    }
}

INSTANTIATE_TEST_SUITE_P(WriteBoundaries,
    TCopyWriteFailureTest,
    ::testing::Values(EIOOp::CreateExclusive, EIOOp::Write, EIOOp::SyncFile, EIOOp::Close, EIOOp::SyncDir));

TEST_F(TDurableCopyTest, SecondSourceStatFailurePreventsDurability)
{
    Inject(EIOOp::Stat, std::errc::permission_denied, "*/input", 2);
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), std::errc::permission_denied);
    ExpectOrphan();
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
}

enum class ESourceFault : std::uint8_t { Size, Inode, Type, SystemError, BadAlloc, Unknown, Close };

class TSourceFaultFS final : public ILocalFS
{
public:
    TSourceFaultFS(std::shared_ptr<ILocalFS> fs, ESourceFault fault)
        : Fs_(std::move(fs))
        , Fault_(fault)
    { }

    TResult<THandle> OpenDir(const std::filesystem::path&) override
    {
        return std::errc::not_supported;
    }
    TResult<THandle> OpenDir(const THandle&, std::string_view) override
    {
        return std::errc::not_supported;
    }
    std::error_code MakeDir(const THandle&, std::string_view) override
    {
        return std::make_error_code(std::errc::not_supported);
    }
    TResult<THandle> CreateExclusive(const THandle&, std::string_view, const TIOTag&) override
    {
        return std::errc::not_supported;
    }
    TResult<THandle> OpenRead(const THandle& directory, std::string_view name) override
    {
        return Fs_->OpenRead(directory, name);
    }
    TResult<std::size_t> Write(const THandle&, std::span<const std::byte>) override
    {
        return std::errc::not_supported;
    }
    TResult<TSpaceInfo> StatFs(const THandle&) override
    {
        return std::errc::not_supported;
    }
    std::error_code SyncFile(const THandle&) override
    {
        return std::make_error_code(std::errc::not_supported);
    }
    std::error_code SyncDir(const THandle&) override
    {
        return std::make_error_code(std::errc::not_supported);
    }

    TResult<std::size_t> PRead(const THandle& file, std::uint64_t offset, std::span<std::byte> bytes) override
    {
        if (Fault_ == ESourceFault::SystemError) {
            throw std::system_error(std::make_error_code(std::errc::permission_denied));
        }
        if (Fault_ == ESourceFault::BadAlloc) {
            throw std::bad_alloc();
        }
        if (Fault_ == ESourceFault::Unknown) {
            throw std::runtime_error("injected copy read exception");
        }
        return Fs_->PRead(file, offset, bytes);
    }

    TResult<TFileStat> Stat(const THandle& file) override
    {
        auto info = Fs_->Stat(file);
        if (info && ++Stats_ == 2) {
            if (Fault_ == ESourceFault::Size) {
                ++info.Value().Size;
            } else if (Fault_ == ESourceFault::Inode) {
                ++info.Value().Inode;
            } else if (Fault_ == ESourceFault::Type) {
                info.Value().Type = EFileType::Other;
            }
        }
        return info;
    }

    std::error_code Close(const THandle& file) override
    {
        auto error = Fs_->Close(file);
        if (!error && Fault_ == ESourceFault::Close) {
            throw std::system_error(std::make_error_code(std::errc::interrupted));
        }
        return error;
    }

private:
    const std::shared_ptr<ILocalFS> Fs_;
    const ESourceFault Fault_;
    int Stats_ = 0;
};

class TCopySourceFaultTest
    : public TDurableCopyTest
    , public ::testing::WithParamInterface<ESourceFault>
{ };

TEST_P(TCopySourceFaultTest, SourceMutationOrExceptionCannotYieldReceipt)
{
    Source_.Fs = std::make_shared<TSourceFaultFS>(Fs_, GetParam());
    auto expected = std::errc::state_not_recoverable;
    switch (GetParam()) {
        case ESourceFault::SystemError:
            expected = std::errc::permission_denied;
            break;
        case ESourceFault::BadAlloc:
            expected = std::errc::not_enough_memory;
            break;
        case ESourceFault::Unknown:
            expected = std::errc::io_error;
            break;
        case ESourceFault::Close:
            expected = std::errc::interrupted;
            break;
        default:
            break;
    }
    EXPECT_EQ(CopyDurableFile(Source_, Target_, 6).Error(), expected);
    ExpectOrphan();
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
    EXPECT_FALSE(Latch_->IsSet());
}

INSTANTIATE_TEST_SUITE_P(SourceFailures,
    TCopySourceFaultTest,
    ::testing::Values(ESourceFault::Size,
        ESourceFault::Inode,
        ESourceFault::Type,
        ESourceFault::SystemError,
        ESourceFault::BadAlloc,
        ESourceFault::Unknown,
        ESourceFault::Close));

}
}
