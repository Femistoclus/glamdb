#include "core/io/durable_file.h"
#include "tests/support/durability_trace.h"
#include "tests/support/fault_fs.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <new>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <unistd.h>

namespace NGlamDB::NIO {
namespace {

using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;
using NGlamDB::NTesting::TFaultRule;
using NGlamDB::NTesting::TIOEvent;

constexpr auto KStore = "11111111-1111-4111-8111-111111111111";
constexpr auto KCollection = "22222222-2222-4222-8222-222222222222";
constexpr auto KAttempt = "33333333-3333-7333-8333-333333333333";
constexpr auto KOperation = "44444444-4444-4444-8444-444444444444";

std::span<const std::byte> Bytes(std::string_view text)
{
    return std::as_bytes(std::span(text.data(), text.size()));
}

class TDurableFileTest : public ::testing::Test
{
protected:
    std::filesystem::path Path_;
    std::shared_ptr<TPosixLocalFS> Posix_;
    std::shared_ptr<TFaultFS> Fs_;
    THandle Directory_;
    std::shared_ptr<TWriteLatch> Latch_;
    std::shared_ptr<TIOAttempt> Attempt_;
    std::string Leaf_;
    std::string Uri_;

    void SetUp() override
    {
        auto name = (std::filesystem::temp_directory_path() / "glamdb-durable-XXXXXX").string();
        ASSERT_NE(::mkdtemp(name.data()), nullptr);
        Path_ = std::filesystem::canonical(name);
        Posix_ = std::make_shared<TPosixLocalFS>();
        Fs_ = std::make_shared<TFaultFS>(*Posix_);
        auto directory = Fs_->OpenDir(Path_);
        ASSERT_TRUE(directory);
        Directory_ = std::move(directory).Value();
        Latch_ = std::make_shared<TWriteLatch>();
        auto attempt = TIOAttempt::Make({KStore, KCollection, KAttempt, EAttemptKind::Append, KOperation}, Latch_);
        ASSERT_TRUE(attempt);
        Attempt_ = std::move(attempt).Value();
        Leaf_ = std::string(KOperation) + "-" + KAttempt + "-00000.parquet";
        Uri_ = std::string("store://") + KStore + "/warehouse/" + KCollection + "/data/" + Leaf_;
        ASSERT_FALSE(Attempt_->DeclareDataFile(Uri_));
    }

    void TearDown() override
    {
        Directory_.reset();
        std::error_code error;
        std::filesystem::remove_all(Path_, error);
        EXPECT_FALSE(error);
    }

    std::shared_ptr<TDurableFile> Open()
    {
        auto file = TDurableFile::Create(Fs_, Directory_, Uri_, Latch_, Attempt_);
        EXPECT_TRUE(file) << file.Error();
        return file ? std::move(file).Value() : nullptr;
    }

    std::size_t Count(EIOOp op) const
    {
        const auto events = Fs_->Events();
        return std::ranges::count_if(events, [op](const TIOEvent& event) {
            return event.Op == op;
        });
    }

    void Inject(EIOOp op, std::errc error)
    {
        Fs_->AddRule({op, "*", 1, 1, std::make_error_code(error), std::nullopt});
    }
};

TEST_F(TDurableFileTest, ReceiptRequiresOrderedDurableCloseAndHasExactHash)
{
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Receipt());
    EXPECT_FALSE(file->Write(Bytes("abc")));
    EXPECT_FALSE(file->Write(Bytes("def")));
    const auto beforeFlush = Fs_->Events().size();
    EXPECT_FALSE(file->Flush());
    EXPECT_EQ(Fs_->Events().size(), beforeFlush);
    EXPECT_FALSE(file->Close());
    EXPECT_EQ(file->State(), EFileState::DirSynced);
    const auto receipt = file->Receipt();
    if (!receipt.has_value()) {
        FAIL() << "Missing durable receipt";
    }
    EXPECT_EQ(receipt->Path, Uri_);
    EXPECT_EQ(receipt->Kind, EFileKind::Data);
    EXPECT_EQ(receipt->Length, 6);
    EXPECT_EQ(receipt->AttemptId, KAttempt);
    EXPECT_EQ(HexDigest(receipt->Sha256), "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721");
    EXPECT_EQ(file->Position().Value(), 6);
    EXPECT_EQ(file->StoredLength(), 6);
    EXPECT_FALSE(Attempt_->Record(Uri_).Value().FormatVerified);

    std::vector<EIOOp> operations;
    for (const auto& event : Fs_->Events()) {
        operations.push_back(event.Op);
    }
    EXPECT_EQ(operations,
        (std::vector<EIOOp>{EIOOp::OpenDir,
            EIOOp::CreateExclusive,
            EIOOp::Write,
            EIOOp::Write,
            EIOOp::SyncFile,
            EIOOp::Stat,
            EIOOp::Close,
            EIOOp::SyncDir}));
    const auto beforeRepeat = Fs_->Events().size();
    EXPECT_FALSE(file->Close());
    EXPECT_EQ(Fs_->Events().size(), beforeRepeat);
    Fs_->Publish({(Path_ / Leaf_).string()});
    NGlamDB::NTesting::TDurabilityTrace trace({Path_.string()});
    for (const auto& event : Fs_->Events()) {
        EXPECT_FALSE(trace.Apply(event));
    }
}

TEST_F(TDurableFileTest, RetriesInterruptedAndShortWritesWithoutLosingBytes)
{
    Fs_->AddRule({EIOOp::Write, "*", 1, 2, std::make_error_code(std::errc::interrupted), std::nullopt});
    Fs_->AddRule({EIOOp::Write, "*", 3, 1, {}, 2});
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write(Bytes("abcdef")));
    EXPECT_FALSE(file->Close());
    const auto receipt = file->Receipt();
    if (!receipt.has_value()) {
        FAIL() << "Missing durable receipt";
    }
    EXPECT_EQ(file->StoredLength(), 6);
    EXPECT_EQ(HexDigest(receipt->Sha256), "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721");
    EXPECT_EQ(Count(EIOOp::Write), 4);
    EXPECT_EQ(Attempt_->Budget().NewFileBytes, 6);
    EXPECT_EQ(Attempt_->Budget().IoBytes, 22);
    EXPECT_EQ(Attempt_->Budget().ReservedNewFileBytes, 0);
}

TEST_F(TDurableFileTest, FailedWriteIsHiddenAndLaterBytesAreDiscarded)
{
    Inject(EIOOp::Write, std::errc::no_space_on_device);
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write(Bytes("abc")));
    EXPECT_EQ(file->Error(), std::errc::no_space_on_device);
    const auto events = Fs_->Events().size();
    EXPECT_FALSE(file->Write(Bytes("def")));
    EXPECT_FALSE(file->Flush());
    EXPECT_EQ(Fs_->Events().size(), events);
    EXPECT_EQ(file->Close(), std::errc::no_space_on_device);
    const auto afterClose = Fs_->Events().size();
    EXPECT_EQ(file->Close(), std::errc::no_space_on_device);
    EXPECT_FALSE(file->Write(Bytes("ghi")));
    EXPECT_FALSE(file->Flush());
    EXPECT_EQ(Fs_->Events().size(), afterClose);
    EXPECT_EQ(file->Position().Value(), 9);
    EXPECT_EQ(file->StoredLength(), 0);
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
    EXPECT_EQ(Count(EIOOp::Close), 1);
    EXPECT_EQ(Count(EIOOp::SyncDir), 0);
    EXPECT_EQ(file->State(), EFileState::Failed);
    EXPECT_FALSE(file->Receipt());
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_EQ(Attempt_->FirstError(), std::errc::no_space_on_device);
    EXPECT_FALSE(Latch_->IsSet());
}

TEST_F(TDurableFileTest, PartialWriteKeepsOnlyAcceptedBytesAndDigestAfterFailure)
{
    Fs_->AddRule({EIOOp::Write, "*", 1, 1, {}, 3});
    Fs_->AddRule({EIOOp::Write, "*", 2, 1, std::make_error_code(std::errc::no_space_on_device), std::nullopt});
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write(Bytes("abcdef")));
    EXPECT_EQ(file->Close(), std::errc::no_space_on_device);
    EXPECT_EQ(file->Position().Value(), 6);
    EXPECT_EQ(file->StoredLength(), 3);
    EXPECT_EQ(Attempt_->Budget().NewFileBytes, 3);
    EXPECT_EQ(Attempt_->Budget().ReservedNewFileBytes, 0);
    const auto record = Attempt_->Record(Uri_);
    ASSERT_TRUE(record);
    EXPECT_EQ(record.Value().Length, 3);
    const auto& digest = record.Value().Sha256;
    if (!digest.has_value()) {
        FAIL() << "Missing accepted-byte digest";
    }
    EXPECT_EQ(HexDigest(*digest), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
    EXPECT_EQ(std::filesystem::file_size(Path_ / Leaf_), 3);
    EXPECT_FALSE(file->Receipt());
}

class TWriteLatchBoundaryTest
    : public TDurableFileTest
    , public ::testing::WithParamInterface<std::tuple<EIOOp, std::errc>>
{ };

TEST_P(TWriteLatchBoundaryTest, NativeStorageFailuresPoisonTheStore)
{
    const auto [operation, error] = GetParam();
    Inject(operation, error);
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write(Bytes("abc")));
    EXPECT_EQ(file->Close(), error);
    EXPECT_TRUE(Latch_->IsSet());
    EXPECT_EQ(Attempt_->FirstError(), error);
    EXPECT_FALSE(file->Receipt());
}

INSTANTIATE_TEST_SUITE_P(NativeStorageErrors,
    TWriteLatchBoundaryTest,
    ::testing::Combine(::testing::Values(EIOOp::Write, EIOOp::SyncFile, EIOOp::SyncDir),
        ::testing::Values(std::errc::io_error, std::errc::read_only_file_system)));

TEST_F(TDurableFileTest, AbandonmentPreservesItsErrorAndObservesCleanupFailure)
{
    Inject(EIOOp::Close, std::errc::io_error);
    {
        auto file = Open();
        ASSERT_NE(file, nullptr);
    }
    EXPECT_EQ(Attempt_->FirstError(), std::errc::operation_canceled);
    EXPECT_TRUE(Latch_->IsSet());
    EXPECT_EQ(Count(EIOOp::Close), 1);
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
}

class TThrowingWriteFS final : public ILocalFS
{
public:
    int SyncCount = 0;
    int CloseCount = 0;

    explicit TThrowingWriteFS(std::errc error)
        : Error_(error)
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
        return std::make_shared<TFSHandle>();
    }
    TResult<THandle> OpenRead(const THandle&, std::string_view) override
    {
        return std::errc::not_supported;
    }
    TResult<std::size_t> Write(const THandle&, std::span<const std::byte>) override
    {
        if (Error_ == std::errc::not_enough_memory) {
            throw std::bad_alloc();
        }
        throw std::runtime_error("Injected writer exception");
    }
    TResult<std::size_t> PRead(const THandle&, std::uint64_t, std::span<std::byte>) override
    {
        return std::errc::not_supported;
    }
    TResult<TFileStat> Stat(const THandle&) override
    {
        return std::errc::not_supported;
    }
    TResult<TSpaceInfo> StatFs(const THandle&) override
    {
        return std::errc::not_supported;
    }
    std::error_code SyncFile(const THandle&) override
    {
        ++SyncCount;
        return {};
    }
    std::error_code SyncDir(const THandle&) override
    {
        ++SyncCount;
        return {};
    }
    std::error_code Close(const THandle&) override
    {
        ++CloseCount;
        return {};
    }

private:
    const std::errc Error_;
};

class TWriteExceptionTest
    : public TDurableFileTest
    , public ::testing::WithParamInterface<std::errc>
{ };

TEST_P(TWriteExceptionTest, OriginalExceptionIsRecordedBeforeReservationCleanup)
{
    auto throwingFs = std::make_shared<TThrowingWriteFS>(GetParam());
    auto result = TDurableFile::Create(throwingFs, std::make_shared<TFSHandle>(), Uri_, Latch_, Attempt_);
    ASSERT_TRUE(result);
    auto file = std::move(result).Value();
    EXPECT_FALSE(file->Write(Bytes("abc")));
    EXPECT_EQ(file->Error(), GetParam());
    EXPECT_EQ(Attempt_->FirstError(), GetParam());
    EXPECT_EQ(Attempt_->Budget().ReservedNewFileBytes, 0);
    EXPECT_EQ(file->Close(), GetParam());
    EXPECT_EQ(throwingFs->SyncCount, 0);
    EXPECT_EQ(throwingFs->CloseCount, 1);
    EXPECT_FALSE(Latch_->IsSet());
    EXPECT_FALSE(file->Receipt());
}

INSTANTIATE_TEST_SUITE_P(WriterExceptions,
    TWriteExceptionTest,
    ::testing::Values(std::errc::not_enough_memory, std::errc::io_error));

TEST_F(TDurableFileTest, ZeroProgressFailsAttemptWithoutPoisoningStore)
{
    Fs_->AddRule({EIOOp::Write, "*", 1, 1, {}, 0});
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write(Bytes("abc")));
    EXPECT_EQ(file->Close(), std::errc::io_error);
    EXPECT_EQ(file->Position().Value(), 3);
    EXPECT_EQ(file->StoredLength(), 0);
    EXPECT_FALSE(Latch_->IsSet());
    EXPECT_FALSE(file->Receipt());
}

class TDurableBoundaryTest
    : public TDurableFileTest
    , public ::testing::WithParamInterface<EIOOp>
{ };

TEST_P(TDurableBoundaryTest, FirstFailureSurvivesCloseAndNoReceiptIsIssued)
{
    Inject(GetParam(), std::errc::no_space_on_device);
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write(Bytes("abc")));
    EXPECT_EQ(file->Close(), std::errc::no_space_on_device);
    const auto afterClose = Fs_->Events().size();
    EXPECT_EQ(file->Close(), std::errc::no_space_on_device);
    EXPECT_EQ(Fs_->Events().size(), afterClose);
    EXPECT_EQ(Count(EIOOp::SyncFile), 1);
    EXPECT_EQ(Count(EIOOp::Close), 1);
    EXPECT_EQ(Count(EIOOp::SyncDir), GetParam() == EIOOp::SyncDir ? 1 : 0);
    EXPECT_FALSE(file->Receipt());
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_EQ(file->State(), EFileState::Failed);
}

INSTANTIATE_TEST_SUITE_P(CloseBoundaries,
    TDurableBoundaryTest,
    ::testing::Values(EIOOp::SyncFile, EIOOp::Stat, EIOOp::Close, EIOOp::SyncDir));

class TAfterDurableBoundaryTest
    : public TDurableFileTest
    , public ::testing::WithParamInterface<std::tuple<EFileKind, EIOOp>>
{ };

TEST_P(TAfterDurableBoundaryTest, SuccessfulNativeSyncCannotAuthorizeAnUnacknowledgedReceipt)
{
    const auto [kind, operation] = GetParam();
    std::string suffix;
    switch (kind) {
        case EFileKind::Data:
            suffix = "data/" + Leaf_;
            break;
        case EFileKind::Manifest:
            suffix = "metadata/" + std::string(KOperation) + "-m0.avro";
            break;
        case EFileKind::ManifestList:
            suffix = "metadata/snap-1-0-" + std::string(KOperation) + ".avro";
            break;
        case EFileKind::Metadata:
            suffix = "metadata/00000-" + std::string(KOperation) + ".metadata.json";
            break;
        default:
            FAIL() << "Unexpected file kind";
    }
    const auto uri = std::string("store://") + KStore + "/warehouse/" + KCollection + "/" + suffix;
    const auto physical = Path_ / std::filesystem::path(suffix).filename();
    auto injector = std::make_shared<TFaultFS>(static_cast<ILocalFS&>(*Fs_));
    const auto directory = injector->OpenDir(Path_);
    ASSERT_TRUE(directory);
    injector->AddRule({operation, "*", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt, true});
    const auto created = TDurableFile::Create(injector, directory.Value(), uri, Latch_, Attempt_);
    ASSERT_TRUE(created);
    const auto& file = created.Value();
    ASSERT_FALSE(file->Write(Bytes("durable orphan")));
    EXPECT_EQ(file->Close(), std::errc::io_error);

    const auto innerEvents = Fs_->Events();
    const auto outerEvents = injector->Events();
    const auto observedSync = std::ranges::find_if(innerEvents, [&](const auto& event) {
        return event.Op == operation;
    });
    ASSERT_NE(observedSync, innerEvents.end());
    EXPECT_FALSE(observedSync->Error);
    EXPECT_EQ(observedSync->Path, operation == EIOOp::SyncFile ? physical.string() : Path_.string());
    EXPECT_EQ(std::ranges::count(innerEvents, operation, &TIOEvent::Op), 1);
    const auto reportedSync = std::ranges::find(outerEvents, operation, &TIOEvent::Op);
    ASSERT_NE(reportedSync, outerEvents.end());
    EXPECT_EQ(reportedSync->Error, std::errc::io_error);
    EXPECT_EQ(std::ranges::count(innerEvents, EIOOp::Close, &TIOEvent::Op), 1);
    EXPECT_EQ(std::ranges::count(innerEvents, EIOOp::SyncDir, &TIOEvent::Op), operation == EIOOp::SyncDir ? 1 : 0);

    const auto record = Attempt_->Record(uri);
    ASSERT_TRUE(record);
    EXPECT_EQ(record.Value().Kind, kind);
    EXPECT_EQ(record.Value().State, EFileState::Failed);
    EXPECT_EQ(record.Value().FirstError, std::errc::io_error);
    EXPECT_FALSE(record.Value().Receipt);
    EXPECT_FALSE(record.Value().FormatVerified);
    EXPECT_FALSE(file->Receipt());
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_EQ(Attempt_->Seal(), std::errc::io_error);
    EXPECT_TRUE(Latch_->IsSet());
    EXPECT_FALSE(file->Write(Bytes("discarded")));
    EXPECT_EQ(file->Close(), std::errc::io_error);
    EXPECT_EQ(Attempt_->FirstError(), std::errc::io_error);
    EXPECT_EQ(TIOAttempt::Make({KStore, KCollection, KAttempt, EAttemptKind::Append, KOperation}, Latch_).Error(),
        std::errc::io_error);
    EXPECT_EQ(Fs_->Events().size(), innerEvents.size());
    EXPECT_EQ(injector->Events().size(), outerEvents.size());

    NTesting::TDurabilityTrace oracle({Path_.string()});
    for (const auto& event : outerEvents) {
        ASSERT_FALSE(oracle.Apply(event));
    }
    EXPECT_TRUE(oracle.Apply({outerEvents.size() + 1, EIOOp::Publish, {}, {}, 0, {}, {physical.string()}}));
    std::ifstream orphan(physical, std::ios::binary);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(orphan), {}), "durable orphan");
}

INSTANTIATE_TEST_SUITE_P(AllFileKinds,
    TAfterDurableBoundaryTest,
    ::testing::Combine(
        ::testing::Values(EFileKind::Data, EFileKind::Manifest, EFileKind::ManifestList, EFileKind::Metadata),
        ::testing::Values(EIOOp::SyncFile, EIOOp::SyncDir)));

TEST_F(TDurableFileTest, SyncAndCloseAreNotRetriedOnEintr)
{
    Inject(EIOOp::SyncFile, std::errc::interrupted);
    Inject(EIOOp::Close, std::errc::io_error);
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write(Bytes("abc")));
    EXPECT_EQ(file->Close(), std::errc::interrupted);
    EXPECT_EQ(file->Close(), std::errc::interrupted);
    EXPECT_EQ(Count(EIOOp::SyncFile), 1);
    EXPECT_EQ(Count(EIOOp::Close), 1);
    EXPECT_TRUE(Latch_->IsSet());
    EXPECT_EQ(Attempt_->FirstError(), std::errc::interrupted);
}

TEST_F(TDurableFileTest, InterruptedCloseDoesNotRetryOrSyncDirectory)
{
    Inject(EIOOp::Close, std::errc::interrupted);
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_EQ(file->Close(), std::errc::interrupted);
    EXPECT_EQ(file->Close(), std::errc::interrupted);
    EXPECT_EQ(Count(EIOOp::Close), 1);
    EXPECT_EQ(Count(EIOOp::SyncDir), 0);
    EXPECT_FALSE(file->Receipt());
}

TEST_F(TDurableFileTest, SizeMismatchAfterSyncCannotProduceReceipt)
{
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write(Bytes("abcdef")));
    ASSERT_EQ(::truncate((Path_ / Leaf_).c_str(), 1), 0);
    EXPECT_EQ(file->Close(), std::errc::state_not_recoverable);
    EXPECT_EQ(Count(EIOOp::Close), 1);
    EXPECT_EQ(Count(EIOOp::SyncDir), 0);
    EXPECT_FALSE(file->Receipt());
}

TEST_F(TDurableFileTest, AbandonedStreamClosesWithoutSyncOrReceipt)
{
    {
        auto file = Open();
        ASSERT_NE(file, nullptr);
        EXPECT_FALSE(file->Write(Bytes("abc")));
    }
    EXPECT_EQ(Count(EIOOp::Close), 1);
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
    EXPECT_EQ(Count(EIOOp::SyncDir), 0);
    EXPECT_EQ(Attempt_->FirstError(), std::errc::operation_canceled);
    EXPECT_EQ(Attempt_->Record(Uri_).Value().State, EFileState::Failed);
    EXPECT_TRUE(Attempt_->Receipts().empty());
}

TEST_F(TDurableFileTest, StoreLatchBlocksFurtherCreatesBeforeIo)
{
    Inject(EIOOp::CreateExclusive, std::errc::io_error);
    const auto file = TDurableFile::Create(Fs_, Directory_, Uri_, Latch_, Attempt_);
    EXPECT_FALSE(file);
    EXPECT_EQ(file.Error(), std::errc::io_error);
    EXPECT_TRUE(Latch_->IsSet());
    const auto events = Fs_->Events().size();
    EXPECT_FALSE(TDurableFile::Create(Fs_, Directory_, "store.identity", Latch_));
    EXPECT_EQ(Fs_->Events().size(), events);
}

TEST_F(TDurableFileTest, CollisionDoesNotOverwriteExistingFile)
{
    auto original = Fs_->CreateExclusive(Directory_, Leaf_);
    ASSERT_TRUE(original);
    ASSERT_TRUE(Fs_->Write(original.Value(), Bytes("original")));
    ASSERT_FALSE(Fs_->Close(original.Value()));
    const auto file = TDurableFile::Create(Fs_, Directory_, Uri_, Latch_, Attempt_);
    EXPECT_FALSE(file);
    EXPECT_EQ(file.Error(), std::errc::file_exists);
    EXPECT_EQ(std::filesystem::file_size(Path_ / Leaf_), 8);
    EXPECT_FALSE(Latch_->IsSet());
}

TEST_F(TDurableFileTest, StandaloneMarkerUsesTheSameDurableProtocol)
{
    auto result = TDurableFile::Create(Fs_, Directory_, "store.identity", Latch_);
    ASSERT_TRUE(result);
    auto file = std::move(result).Value();
    EXPECT_FALSE(file->Write(Bytes("identity")));
    EXPECT_FALSE(file->Close());
    const auto receipt = file->Receipt();
    if (!receipt.has_value()) {
        FAIL() << "Missing durable receipt";
    }
    EXPECT_EQ(receipt->Path, "store.identity");
    EXPECT_TRUE(receipt->AttemptId.empty());
    EXPECT_EQ(receipt->Kind, EFileKind::Other);
    EXPECT_EQ(receipt->Length, 8);
    EXPECT_EQ(Count(EIOOp::SyncFile), 1);
    EXPECT_EQ(Count(EIOOp::Close), 1);
    EXPECT_EQ(Count(EIOOp::SyncDir), 1);
}

TEST_F(TDurableFileTest, EmptyFileStillNeedsTheCompleteCloseChain)
{
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write({}));
    EXPECT_EQ(Count(EIOOp::Write), 0);
    EXPECT_FALSE(file->Close());
    const auto receipt = file->Receipt();
    if (!receipt.has_value()) {
        FAIL() << "Missing durable receipt";
    }
    EXPECT_EQ(receipt->Length, 0);
    EXPECT_EQ(HexDigest(receipt->Sha256), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST_F(TDurableFileTest, LateCloseCannotIssueReceiptForAnotherAttempt)
{
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write(Bytes("abc")));
    Attempt_->Deactivate();
    auto next = TIOAttempt::Make(
        {KStore, KCollection, "55555555-5555-7555-8555-555555555555", EAttemptKind::Append, KOperation},
        Latch_);
    ASSERT_TRUE(next);
    EXPECT_TRUE(file->Close());
    EXPECT_FALSE(file->Receipt());
    EXPECT_TRUE(next.Value()->Receipts().empty());
    EXPECT_FALSE(next.Value()->FirstError());
    EXPECT_EQ(Count(EIOOp::SyncFile), 0);
    EXPECT_EQ(Count(EIOOp::Close), 1);
}

TEST_F(TDurableFileTest, MutationAfterSealPoisonsAttemptWithoutIo)
{
    auto file = Open();
    ASSERT_NE(file, nullptr);
    ASSERT_FALSE(file->Close());
    ASSERT_FALSE(Attempt_->Seal());
    const auto events = Fs_->Events().size();
    EXPECT_FALSE(file->Write(Bytes("late")));
    EXPECT_EQ(Fs_->Events().size(), events);
    EXPECT_TRUE(Attempt_->FirstError());
    EXPECT_FALSE(file->Receipt());
    EXPECT_EQ(file->Position().Value(), 4);
}

TEST_F(TDurableFileTest, ClosedStreamRemainsIdempotentAfterSealAndDeactivation)
{
    auto file = Open();
    ASSERT_NE(file, nullptr);
    ASSERT_FALSE(file->Write(Bytes("abc")));
    ASSERT_FALSE(file->Close());
    ASSERT_FALSE(Attempt_->Seal());
    const auto events = Fs_->Events().size();
    EXPECT_FALSE(file->Close());
    EXPECT_FALSE(Attempt_->FirstError());
    ASSERT_TRUE(file->Receipt());
    Attempt_->Deactivate();
    EXPECT_FALSE(file->Close());
    EXPECT_FALSE(Attempt_->FirstError());
    EXPECT_EQ(file->State(), EFileState::DirSynced);
    const auto receipt = file->Receipt();
    if (!receipt.has_value()) {
        FAIL() << "Missing durable receipt";
    }
    EXPECT_EQ(receipt->Length, 3);
    EXPECT_EQ(Fs_->Events().size(), events);
}

TEST_F(TDurableFileTest, ClosedStreamRemainsIdempotentAfterAnotherWritePoisonsStore)
{
    auto file = Open();
    ASSERT_NE(file, nullptr);
    ASSERT_FALSE(file->Close());
    Latch_->Observe(std::make_error_code(std::errc::io_error));
    const auto events = Fs_->Events().size();
    EXPECT_FALSE(file->Close());
    EXPECT_FALSE(Attempt_->FirstError());
    EXPECT_TRUE(file->Receipt());
    EXPECT_EQ(Fs_->Events().size(), events);
}

TEST_F(TDurableFileTest, CapacityFailureHappensBeforeWrite)
{
    auto limited = TIOAttempt::Make({KStore, KCollection, KAttempt, EAttemptKind::Append, KOperation},
        Latch_,
        TAttemptLimits{5, 10});
    ASSERT_TRUE(limited);
    Attempt_ = std::move(limited).Value();
    ASSERT_FALSE(Attempt_->DeclareDataFile(Uri_));
    auto file = Open();
    ASSERT_NE(file, nullptr);
    EXPECT_FALSE(file->Write(Bytes("abcdef")));
    EXPECT_TRUE(file->Error());
    EXPECT_EQ(Count(EIOOp::Write), 0);
    EXPECT_EQ(file->StoredLength(), 0);
    EXPECT_EQ(file->Position().Value(), 6);
    EXPECT_TRUE(file->Close());
    EXPECT_TRUE(Attempt_->Receipts().empty());
}

TEST_F(TDurableFileTest, StreamRetainsItsOwnersUntilRelease)
{
    auto file = Open();
    ASSERT_NE(file, nullptr);
    std::weak_ptr<TFaultFS> weakFs = Fs_;
    std::weak_ptr<TWriteLatch> weakLatch = Latch_;
    Fs_.reset();
    Latch_.reset();
    Attempt_.reset();
    Directory_.reset();
    EXPECT_FALSE(weakFs.expired());
    EXPECT_FALSE(weakLatch.expired());
    EXPECT_FALSE(file->Write(Bytes("abc")));
    EXPECT_FALSE(file->Close());
    file.reset();
    EXPECT_TRUE(weakFs.expired());
    EXPECT_TRUE(weakLatch.expired());
}

TEST_F(TDurableFileTest, ConcurrentWritesHaveOneConsistentLength)
{
    auto file = Open();
    ASSERT_NE(file, nullptr);
    std::vector<std::thread> writers;
    writers.reserve(8);
    for (int i = 0; i < 8; ++i) {
        writers.emplace_back([&] {
            for (int j = 0; j < 20; ++j) {
                EXPECT_FALSE(file->Write(Bytes("x")));
            }
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }
    EXPECT_FALSE(file->Close());
    const auto receipt = file->Receipt();
    if (!receipt.has_value()) {
        FAIL() << "Missing durable receipt";
    }
    EXPECT_EQ(receipt->Length, 160);
    EXPECT_EQ(file->Position().Value(), 160);
    EXPECT_EQ(Attempt_->Budget().NewFileBytes, 160);
}

TEST_F(TDurableFileTest, InvalidArgumentsDoNotReachFilesystem)
{
    const auto events = Fs_->Events().size();
    EXPECT_FALSE(TDurableFile::Create({}, Directory_, "file", Latch_));
    EXPECT_FALSE(TDurableFile::Create(Fs_, {}, "file", Latch_));
    EXPECT_FALSE(TDurableFile::Create(Fs_, Directory_, "file", {}));
    for (const auto& path :
        {std::string{}, std::string("/"), std::string(".."), std::string("path/."), std::string("a\0b", 3)})
    {
        EXPECT_FALSE(TDurableFile::Create(Fs_, Directory_, path, Latch_));
    }
    EXPECT_EQ(Fs_->Events().size(), events);
}

TEST_F(TDurableFileTest, AttemptAndStreamMustShareTheStoreLatch)
{
    const auto events = Fs_->Events().size();
    const auto otherLatch = std::make_shared<TWriteLatch>();
    const auto file = TDurableFile::Create(Fs_, Directory_, Uri_, otherLatch, Attempt_);
    EXPECT_FALSE(file);
    EXPECT_EQ(file.Error(), std::errc::invalid_argument);
    EXPECT_EQ(Fs_->Events().size(), events);
    EXPECT_TRUE(Attempt_->Records().empty());
    EXPECT_EQ(Attempt_->FirstError(), std::errc::invalid_argument);
    EXPECT_FALSE(Latch_->IsSet());
    EXPECT_FALSE(otherLatch->IsSet());
}

}
}
