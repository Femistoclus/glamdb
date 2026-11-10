#include "core/storage/durable_local_file_io.h"
#include "tests/support/fault_fs.h"

#include <gtest/gtest.h>
#include <iceberg/file_writer.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/type.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <fstream>
#include <future>
#include <limits>
#include <new>
#include <utility>

namespace NGlamDB::NStorage {
namespace {

using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TIOEvent;

constexpr auto KStore = "11111111-1111-4111-8111-111111111111";
constexpr auto KCollection = "22222222-2222-4222-8222-222222222222";
constexpr auto KAttempt = "33333333-3333-7333-8333-333333333333";
constexpr auto KOperation = "44444444-4444-4444-8444-444444444444";
constexpr auto KOther = "55555555-5555-4555-8555-555555555555";

std::span<const std::byte> Bytes(std::string_view text)
{
    return std::as_bytes(std::span(text.data(), text.size()));
}

class TDurableLocalFileIOTest : public ::testing::Test
{
protected:
    std::filesystem::path Path_;
    std::shared_ptr<NIO::TPosixLocalFS> Posix_;
    std::shared_ptr<NGlamDB::NTesting::TFaultFS> Fs_;
    std::shared_ptr<NIO::TWriteLatch> Latch_;
    std::shared_ptr<NIO::TStoreDirs> Dirs_;
    std::shared_ptr<TDurableLocalFileIO> Io_;
    std::shared_ptr<NIO::TIOAttempt> Attempt_;
    std::string Prefix_;
    std::string Metadata_;
    std::string Data_;

    void SetUp() override
    {
        auto name = (std::filesystem::temp_directory_path() / "glamdb-fileio-XXXXXX").string();
        ASSERT_NE(::mkdtemp(name.data()), nullptr);
        Path_ = std::filesystem::canonical(name);
        Posix_ = std::make_shared<NIO::TPosixLocalFS>();
        Fs_ = std::make_shared<NGlamDB::NTesting::TFaultFS>(*Posix_);
        Latch_ = std::make_shared<NIO::TWriteLatch>();
        auto dirs = NIO::TStoreDirs::Create(Fs_, Path_ / "store", Latch_);
        ASSERT_TRUE(dirs);
        Dirs_ = std::move(dirs).Value();
        ASSERT_TRUE(Dirs_->CreateCollection(KCollection));
        auto adapter = TDurableLocalFileIO::Make(Dirs_, KStore);
        ASSERT_TRUE(adapter);
        Io_ = std::move(*adapter);
        Prefix_ = std::string("store://") + KStore + "/warehouse/" + KCollection;
        Metadata_ = Prefix_ + "/metadata/00000-" + KOther + ".metadata.json";
        Data_ = Prefix_ + "/data/" + KOperation + "-" + KAttempt + "-00000.parquet";
        Attempt_ = MakeAttempt();
    }

    void TearDown() override
    {
        Io_.reset();
        Dirs_.reset();
        std::error_code error;
        std::filesystem::remove_all(Path_, error);
        EXPECT_FALSE(error);
    }

    std::shared_ptr<NIO::TIOAttempt>
    MakeAttempt(NIO::TAttemptLimits limits = {}, const char* store = KStore, const char* collection = KCollection)
    {
        auto result =
            NIO::TIOAttempt::Make({store, collection, KAttempt, NIO::EAttemptKind::Append, KOperation}, Latch_, limits);
        EXPECT_TRUE(result);
        return result ? std::move(result).Value() : nullptr;
    }

    void Put(const std::string& uri, std::string_view bytes)
    {
        const auto relative = uri.substr(std::string("store://").size() + std::string(KStore).size() + 1);
        std::ofstream file(Dirs_->Path() / relative, std::ios::binary);
        file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        ASSERT_TRUE(file.good());
    }

    std::size_t Count(EIOOp op) const
    {
        return std::ranges::count_if(Fs_->Events(), [op](const TIOEvent& event) {
            return event.Op == op;
        });
    }

    void Inject(EIOOp op, std::errc error, std::string pattern = "*")
    {
        Fs_->AddRule({op, std::move(pattern), 1, 1, std::make_error_code(error), std::nullopt});
    }

    std::unique_ptr<iceberg::PositionOutputStream> Create(const std::string& uri)
    {
        auto file = Io_->NewOutputFile(uri);
        EXPECT_TRUE(file);
        if (!file) {
            return {};
        }
        EXPECT_EQ((*file)->location(), uri);
        auto stream = (*file)->Create();
        EXPECT_TRUE(stream) << (stream ? "" : stream.error().message);
        return stream ? std::move(*stream) : nullptr;
    }
};

TEST_F(TDurableLocalFileIOTest, RejectsInvalidFactoryAndBindingsBeforeIo)
{
    const auto events = Fs_->Events().size();
    EXPECT_FALSE(TDurableLocalFileIO::Make(nullptr, KStore));
    EXPECT_FALSE(TDurableLocalFileIO::Make(Dirs_, "invalid"));
    TFileIOLimits limits;
    limits.MetadataBytes = std::numeric_limits<std::uint64_t>::max();
    EXPECT_FALSE(TDurableLocalFileIO::Make(Dirs_, KStore, limits));
    EXPECT_FALSE(Io_->BindAttempt(nullptr));
    EXPECT_FALSE(Io_->BindAttempt(MakeAttempt({}, KOther)));
    auto otherLatch = NIO::TIOAttempt::Make({KStore, KCollection, KAttempt, NIO::EAttemptKind::Append, KOperation},
        std::make_shared<NIO::TWriteLatch>());
    ASSERT_TRUE(otherLatch);
    EXPECT_FALSE(Io_->BindAttempt(otherLatch.Value()));
    EXPECT_FALSE(Io_->NewOutputFile(Metadata_));
    EXPECT_FALSE(Io_->WriteFile(Metadata_, "{}"));
    EXPECT_EQ(Fs_->Events().size(), events);
}

TEST_F(TDurableLocalFileIOTest, ValidatesCollectionAndUriBeforeSyscalls)
{
    auto scope = Io_->BindAttempt(MakeAttempt({}, KStore, KOther));
    ASSERT_TRUE(scope);
    const auto events = Fs_->Events().size();
    EXPECT_FALSE(Io_->NewOutputFile(Metadata_));
    EXPECT_FALSE(Io_->NewInputFile(Metadata_));
    EXPECT_FALSE(Io_->NewInputFile("/tmp/foreign"));
    EXPECT_FALSE(Io_->NewInputFile(Prefix_ + "/metadata/../escape"));
    EXPECT_EQ(Fs_->Events().size(), events);
}

TEST_F(TDurableLocalFileIOTest, RejectsRebindingDeactivatedAndSealedAttempts)
{
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    scope->Reset();
    const auto events = Fs_->Events().size();
    EXPECT_FALSE(Io_->BindAttempt(Attempt_));
    EXPECT_EQ(Attempt_->FirstError(), std::errc::operation_canceled);
    auto sealed = MakeAttempt();
    ASSERT_FALSE(sealed->Seal());
    EXPECT_FALSE(Io_->BindAttempt(sealed));
    EXPECT_EQ(sealed->FirstError(), std::errc::operation_not_permitted);
    EXPECT_EQ(Fs_->Events().size(), events);
}

TEST_F(TDurableLocalFileIOTest, ConcurrentReadsDoNotUseTheWriterAttempt)
{
    ASSERT_TRUE(Dirs_->CreateCollection(KOther));
    const auto foreign =
        std::string("store://") + KStore + "/warehouse/" + KOther + "/metadata/00000-" + KOther + ".metadata.json";
    Put(Metadata_, "same");
    Put(foreign, "other");
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto same = std::async(std::launch::async, [&] {
        return Io_->ReadFile(Metadata_, std::nullopt);
    });
    auto other = std::async(std::launch::async, [&] {
        return Io_->ReadFile(foreign, std::nullopt);
    });
    const auto sameResult = same.get();
    const auto otherResult = other.get();
    ASSERT_TRUE(sameResult);
    ASSERT_TRUE(otherResult);
    EXPECT_EQ(*sameResult, "same");
    EXPECT_EQ(*otherResult, "other");
    EXPECT_EQ(Attempt_->Budget().IoBytes, 0);
    EXPECT_FALSE(Attempt_->FirstError());
}

TEST_F(TDurableLocalFileIOTest, BackgroundReaderOutlivesTheWriteScope)
{
    Put(Metadata_, "abc");
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto future =
        std::async(std::launch::async, [&]() -> iceberg::Result<std::unique_ptr<iceberg::SeekableInputStream>> {
            auto file = Io_->NewInputFile(Metadata_);
            if (!file) {
                return std::unexpected(file.error());
            }
            return (*file)->Open();
        });
    auto stream = future.get();
    ASSERT_TRUE(stream);
    scope->Reset();
    std::array<std::byte, 3> bytes{};
    EXPECT_TRUE((*stream)->ReadFully(0, bytes));
    EXPECT_TRUE((*stream)->Close());
    EXPECT_EQ(Attempt_->Budget().IoBytes, 0);
    EXPECT_FALSE(Attempt_->FirstError());
}

TEST_F(TDurableLocalFileIOTest, ExplicitReadOnlyViewHasIndependentLifetimeAndDeniesWrites)
{
    Put(Metadata_, "abc");
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto view = Io_->ReadOnlyView();
    ASSERT_TRUE(view);
    const auto events = Fs_->Events().size();
    EXPECT_FALSE((*view)->BindAttempt(Attempt_));
    EXPECT_FALSE((*view)->NewOutputFile(Metadata_));
    EXPECT_FALSE((*view)->WriteFile(Metadata_, "replacement"));
    EXPECT_EQ(Fs_->Events().size(), events);
    auto input = (*view)->NewInputFile(Metadata_);
    ASSERT_TRUE(input);
    auto stream = (*input)->Open();
    ASSERT_TRUE(stream);
    scope->Reset();
    Io_.reset();
    Dirs_.reset();
    std::array<std::byte, 3> bytes{};
    EXPECT_TRUE((*stream)->ReadFully(0, bytes));
    EXPECT_TRUE((*stream)->Close());
    auto content = (*view)->ReadFile(Metadata_, std::nullopt);
    ASSERT_TRUE(content);
    EXPECT_EQ(*content, "abc");
    EXPECT_EQ(Attempt_->Budget().IoBytes, 0);
    EXPECT_FALSE(Attempt_->FirstError());
    auto nestedView = (*view)->ReadOnlyView();
    ASSERT_TRUE(nestedView);
    EXPECT_FALSE((*nestedView)->BindAttempt(MakeAttempt()));
}

TEST_F(TDurableLocalFileIOTest, OwnerThreadReadsKeepTheirCapturedAttempt)
{
    Put(Metadata_, "abc");
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    EXPECT_TRUE(Io_->ReadFile(Metadata_, std::nullopt));
    EXPECT_EQ(Attempt_->Budget().IoBytes, 3);
    auto file = Io_->NewInputFile(Metadata_);
    ASSERT_TRUE(file);
    auto stream = (*file)->Open();
    ASSERT_TRUE(stream);
    scope->Reset();
    auto next = MakeAttempt();
    auto nextScope = Io_->BindAttempt(next);
    ASSERT_TRUE(nextScope);
    std::array<std::byte, 3> bytes{};
    EXPECT_FALSE((*stream)->ReadFully(0, bytes));
    EXPECT_EQ(next->Budget().IoBytes, 0);
    EXPECT_FALSE(next->FirstError());
    EXPECT_TRUE((*stream)->Close());
}

TEST_F(TDurableLocalFileIOTest, CrossThreadWriteFileReadbackStillChargesItsAttempt)
{
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto written = std::async(std::launch::async, [&] {
        return Io_->WriteFile(Metadata_, "{}");
    });
    EXPECT_TRUE(written.get());
    EXPECT_EQ(Attempt_->Budget().NewFileBytes, 2);
    EXPECT_EQ(Attempt_->Budget().IoBytes, 4);
    EXPECT_FALSE(Attempt_->FirstError());
}

TEST_F(TDurableLocalFileIOTest, ScanReadersRejectActiveOpenFilesWithoutPoisoningTheWriter)
{
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto view = Io_->ReadOnlyView();
    ASSERT_TRUE(view);
    auto nested = (*view)->ReadOnlyView();
    ASSERT_TRUE(nested);
    auto stream = Create(Metadata_);
    ASSERT_NE(stream, nullptr);
    ASSERT_TRUE(stream->Write(Bytes("{}")));
    const auto opens = Count(EIOOp::OpenRead);
    auto background = std::async(std::launch::async, [&] {
        return Io_->NewInputFile(Metadata_);
    });
    EXPECT_FALSE(background.get());
    EXPECT_FALSE((*view)->NewInputFile(Metadata_));
    EXPECT_FALSE((*nested)->NewInputFile(Metadata_));
    EXPECT_EQ(Count(EIOOp::OpenRead), opens);
    EXPECT_FALSE(Attempt_->FirstError());
    EXPECT_TRUE(stream->Close());
    const auto budget = Attempt_->Budget().IoBytes;
    EXPECT_TRUE((*view)->ReadFile(Metadata_, std::nullopt));
    EXPECT_TRUE((*nested)->ReadFile(Metadata_, std::nullopt));
    auto after = std::async(std::launch::async, [&] {
        return Io_->ReadFile(Metadata_, std::nullopt);
    });
    EXPECT_TRUE(after.get());
    EXPECT_EQ(Attempt_->Budget().IoBytes, budget);
    EXPECT_FALSE(Attempt_->FirstError());
}

TEST_F(TDurableLocalFileIOTest, WritesDurableMetadataAndReadsExactBytes)
{
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    ASSERT_TRUE(Io_->WriteFile(Metadata_, "{\"value\":1}"));
    const auto record = Attempt_->Record(Metadata_);
    ASSERT_TRUE(record);
    const auto& receipt = record.Value().Receipt;
    if (!receipt) {
        FAIL() << "Expected a durable metadata receipt";
    }
    EXPECT_EQ(receipt->Kind, NIO::EFileKind::Metadata);
    EXPECT_EQ(receipt->Length, 11);
    EXPECT_FALSE(record.Value().FormatVerified);
    EXPECT_EQ(Attempt_->Budget().NewFileBytes, 11);
    EXPECT_EQ(Attempt_->Budget().IoBytes, 22);
    auto actual = Io_->ReadFile(Metadata_, 11);
    ASSERT_TRUE(actual);
    EXPECT_EQ(*actual, "{\"value\":1}");
    EXPECT_EQ(Count(EIOOp::SyncFile), 1);
    EXPECT_EQ(Count(EIOOp::CreateExclusive), 1);
}

TEST_F(TDurableLocalFileIOTest, ScopeMovesAndRejectsLateOutputWithoutTouchingNextAttempt)
{
    auto bound = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(bound);
    EXPECT_FALSE(Io_->BindAttempt(MakeAttempt()));
    auto deferred = Io_->NewOutputFile(Metadata_);
    ASSERT_TRUE(deferred);
    TAttemptScope moved = std::move(*bound);
    TAttemptScope assigned;
    assigned = std::move(moved);
    assigned.Reset();
    assigned.Reset();
    auto next = MakeAttempt();
    auto scope = Io_->BindAttempt(next);
    ASSERT_TRUE(scope);
    const auto events = Fs_->Events().size();
    EXPECT_FALSE((*deferred)->Create());
    EXPECT_EQ(Fs_->Events().size(), events);
    EXPECT_EQ(Attempt_->FirstError(), std::errc::operation_canceled);
    EXPECT_FALSE(next->FirstError());
}

TEST_F(TDurableLocalFileIOTest, LeakedStreamCannotCompleteIntoAnotherAttempt)
{
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto stream = Create(Metadata_);
    ASSERT_NE(stream, nullptr);
    ASSERT_TRUE(stream->Write(Bytes("{}")));
    scope->Reset();
    auto next = MakeAttempt();
    auto nextScope = Io_->BindAttempt(next);
    ASSERT_TRUE(nextScope);
    EXPECT_FALSE(stream->Close());
    EXPECT_TRUE(next->Receipts().empty());
    EXPECT_FALSE(next->FirstError());
    EXPECT_TRUE(Attempt_->Receipts().empty());
}

TEST_F(TDurableLocalFileIOTest, CreateOrOverwriteNeverTruncatesExistingFile)
{
    Put(Metadata_, "original");
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto output = Io_->NewOutputFile(Metadata_);
    ASSERT_TRUE(output);
    auto created = (*output)->CreateOrOverwrite();
    ASSERT_FALSE(created);
    EXPECT_EQ(created.error().kind, iceberg::ErrorKind::kAlreadyExists);
    scope->Reset();
    auto actual = Io_->ReadFile(Metadata_, std::nullopt);
    ASSERT_TRUE(actual);
    EXPECT_EQ(*actual, "original");
    EXPECT_EQ(Count(EIOOp::Write), 0);
}

TEST_F(TDurableLocalFileIOTest, DeletesAreDeniedWithoutIoOrPoisoningAttempt)
{
    Put(Metadata_, "{}");
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    const auto events = Fs_->Events().size();
    EXPECT_FALSE(Io_->DeleteFile(Metadata_));
    EXPECT_FALSE(Io_->DeleteFile("missing"));
    EXPECT_FALSE(Io_->DeleteFiles({Metadata_}));
    EXPECT_FALSE(Io_->DeleteFiles({}));
    EXPECT_EQ(Io_->DeniedDeleteCount(), 4);
    EXPECT_EQ(Fs_->Events().size(), events);
    EXPECT_FALSE(Attempt_->FirstError());
    EXPECT_TRUE(Io_->ReadFile(Metadata_, std::nullopt));
}

TEST_F(TDurableLocalFileIOTest, IndependentReadersUsePreadAndRepeatableClose)
{
    Put(Metadata_, "abcdef");
    auto input = Io_->NewInputFile(Metadata_, 6);
    ASSERT_TRUE(input);
    EXPECT_EQ((*input)->location(), Metadata_);
    auto first = (*input)->Open();
    auto second = (*input)->Open();
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    std::array<std::byte, 3> bytes{};
    EXPECT_EQ((*first)->Read(bytes).value(), 3);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size()), "abc");
    EXPECT_EQ((*first)->Position().value(), 3);
    EXPECT_EQ((*second)->Position().value(), 0);
    ASSERT_TRUE((*second)->ReadFully(3, bytes));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size()), "def");
    EXPECT_EQ((*second)->Read(bytes).value(), 0);
    EXPECT_TRUE((*second)->Seek(0));
    EXPECT_FALSE((*second)->Seek(-1));
    EXPECT_FALSE((*second)->Seek(7));
    EXPECT_FALSE((*second)->ReadFully(5, bytes));
    EXPECT_TRUE((*first)->Close());
    const auto calls = Count(EIOOp::Close);
    EXPECT_TRUE((*first)->Close());
    EXPECT_EQ(Count(EIOOp::Close), calls);
    EXPECT_FALSE((*first)->Read(bytes));
    EXPECT_FALSE((*first)->Position());
}

TEST_F(TDurableLocalFileIOTest, SizeAlwaysStatsAndRejectsHintsAndMutation)
{
    Put(Metadata_, "abc");
    EXPECT_FALSE(Io_->NewInputFile(Metadata_, 2));
    auto input = Io_->NewInputFile(Metadata_, 3);
    ASSERT_TRUE(input);
    const auto stats = Count(EIOOp::Stat);
    EXPECT_EQ((*input)->Size().value(), 3);
    EXPECT_EQ((*input)->Size().value(), 3);
    EXPECT_EQ(Count(EIOOp::Stat), stats + 2);
    Put(Metadata_, "abcd");
    EXPECT_FALSE((*input)->Size());
    EXPECT_FALSE((*input)->Open());
}

TEST_F(TDurableLocalFileIOTest, EnforcesAllFileKindLimitsBeforeReturningSize)
{
    const auto manifest = Prefix_ + "/metadata/" + KOther + "-m0.avro";
    const auto list = Prefix_ + "/metadata/snap-1-0-" + KOther + ".avro";
    auto bounded = TDurableLocalFileIO::Make(Dirs_, KStore, {2, 2, 2, 2});
    ASSERT_TRUE(bounded);
    for (const auto& path : {Metadata_, Data_, manifest, list}) {
        Put(path, "abc");
        EXPECT_FALSE((*bounded)->NewInputFile(path));
    }
}

TEST_F(TDurableLocalFileIOTest, RetriesInterruptedShortReadsAndChargesEveryRequest)
{
    Put(Metadata_, "abcdef");
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    Fs_->AddRule({EIOOp::PRead, "*", 1, 2, std::make_error_code(std::errc::interrupted), std::nullopt});
    Fs_->AddRule({EIOOp::PRead, "*", 3, 1, {}, 2});
    auto content = Io_->ReadFile(Metadata_, std::nullopt);
    ASSERT_TRUE(content);
    EXPECT_EQ(*content, "abcdef");
    EXPECT_EQ(Count(EIOOp::PRead), 4);
    EXPECT_EQ(Attempt_->Budget().IoBytes, 22);
}

TEST_F(TDurableLocalFileIOTest, EarlyEofAndReadBudgetFailWithoutAcceptingPartialData)
{
    Put(Metadata_, "abcdef");
    Fs_->AddRule({EIOOp::PRead, "*", 1, 1, {}, 0});
    auto read = Io_->ReadFile(Metadata_, std::nullopt);
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error().kind, iceberg::ErrorKind::kInvalid);
    auto attempt = MakeAttempt({256, 5});
    auto scope = Io_->BindAttempt(attempt);
    ASSERT_TRUE(scope);
    const auto before = Count(EIOOp::PRead);
    EXPECT_FALSE(Io_->ReadFile(Metadata_, std::nullopt));
    EXPECT_EQ(Count(EIOOp::PRead), before);
    EXPECT_EQ(attempt->FirstError(), std::errc::no_buffer_space);
}

TEST_F(TDurableLocalFileIOTest, ReadAndReaderCloseErrorsAreReported)
{
    Put(Metadata_, "abc");
    Inject(EIOOp::PRead, std::errc::io_error);
    EXPECT_FALSE(Io_->ReadFile(Metadata_, std::nullopt));
    EXPECT_FALSE(Latch_->IsSet());
    Inject(EIOOp::Close, std::errc::no_space_on_device);
    auto input = Io_->NewInputFile(Metadata_);
    ASSERT_TRUE(input);
    auto stream = (*input)->Open();
    ASSERT_TRUE(stream);
    EXPECT_FALSE((*stream)->Close());
    const auto calls = Count(EIOOp::Close);
    EXPECT_FALSE((*stream)->Close());
    EXPECT_EQ(Count(EIOOp::Close), calls);
}

TEST_F(TDurableLocalFileIOTest, HidesWriteAndFlushErrorsUntilCloseWithLogicalPosition)
{
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto stream = Create(Metadata_);
    ASSERT_NE(stream, nullptr);
    Inject(EIOOp::Write, std::errc::no_space_on_device);
    EXPECT_TRUE(stream->Write(Bytes("abc")));
    EXPECT_TRUE(stream->Flush());
    const auto events = Fs_->Events().size();
    EXPECT_TRUE(stream->Write(Bytes("def")));
    EXPECT_EQ(Fs_->Events().size(), events);
    EXPECT_EQ(stream->Position().value(), 6);
    EXPECT_EQ(stream->StoredLength().value(), 0);
    EXPECT_FALSE(stream->Close());
    EXPECT_FALSE(stream->Close());
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_FALSE(Latch_->IsSet());
}

TEST_F(TDurableLocalFileIOTest, OutputLimitDropsBytesAndStillAdvancesLogicalPosition)
{
    auto bounded = TDurableLocalFileIO::Make(Dirs_, KStore, {2, 2, 2, 2});
    ASSERT_TRUE(bounded);
    Io_ = *bounded;
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto stream = Create(Metadata_);
    ASSERT_NE(stream, nullptr);
    EXPECT_TRUE(stream->Write(Bytes("abc")));
    EXPECT_EQ(stream->Position().value(), 3);
    EXPECT_EQ(Count(EIOOp::Write), 0);
    EXPECT_FALSE(stream->Close());
    EXPECT_EQ(Attempt_->FirstError(), std::errc::file_too_large);
}

TEST_F(TDurableLocalFileIOTest, OpenFilesCannotBeRead)
{
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto stream = Create(Metadata_);
    ASSERT_NE(stream, nullptr);
    EXPECT_TRUE(stream->Write(Bytes("{}")));
    const auto before = Count(EIOOp::OpenRead);
    EXPECT_FALSE(Io_->NewInputFile(Metadata_));
    EXPECT_EQ(Count(EIOOp::OpenRead), before);
    EXPECT_FALSE(stream->Close());
}

TEST_F(TDurableLocalFileIOTest, ReadOpenAndStatFailuresAreReturned)
{
    Put(Metadata_, "abc");
    for (auto operation : {EIOOp::OpenDir, EIOOp::OpenRead, EIOOp::Stat}) {
        Inject(operation, std::errc::permission_denied);
        EXPECT_FALSE(Io_->NewInputFile(Metadata_));
    }
    EXPECT_TRUE(Io_->NewInputFile(Metadata_));
}

TEST_F(TDurableLocalFileIOTest, WriteLatchRejectsNextBindingButPermitsReads)
{
    Put(Metadata_, "abc");
    auto next = MakeAttempt();
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    const auto second = Prefix_ + "/metadata/00001-" + KOther + ".metadata.json";
    Inject(EIOOp::CreateExclusive, std::errc::read_only_file_system);
    auto output = Io_->NewOutputFile(second);
    ASSERT_TRUE(output);
    EXPECT_FALSE((*output)->Create());
    EXPECT_TRUE(Latch_->IsSet());
    scope->Reset();
    EXPECT_FALSE(Io_->BindAttempt(next));
    auto content = Io_->ReadFile(Metadata_, std::nullopt);
    ASSERT_TRUE(content);
    EXPECT_EQ(*content, "abc");
}

TEST_F(TDurableLocalFileIOTest, RootReplacementBlocksCreation)
{
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto output = Io_->NewOutputFile(Metadata_);
    ASSERT_TRUE(output);
    std::filesystem::rename(Dirs_->Path(), Path_ / "old-store");
    ASSERT_TRUE(std::filesystem::create_directory(Dirs_->Path()));
    EXPECT_FALSE((*output)->Create());
    EXPECT_TRUE(Latch_->IsSet());
    EXPECT_EQ(Count(EIOOp::CreateExclusive), 0);
}

class TThrowingReadFS final : public NIO::ILocalFS
{
public:
    explicit TThrowingReadFS(int exception)
        : Exception_(exception)
    { }
    NIO::TResult<NIO::THandle> OpenDir(const std::filesystem::path& path) override
    {
        return Fs_.OpenDir(path);
    }
    NIO::TResult<NIO::THandle> OpenDir(const NIO::THandle& parent, std::string_view name) override
    {
        return Fs_.OpenDir(parent, name);
    }
    std::error_code MakeDir(const NIO::THandle& parent, std::string_view name) override
    {
        return Fs_.MakeDir(parent, name);
    }
    NIO::TResult<NIO::THandle>
    CreateExclusive(const NIO::THandle& parent, std::string_view name, const NIO::TIOTag& tag) override
    {
        return Fs_.CreateExclusive(parent, name, tag);
    }
    NIO::TResult<NIO::THandle> OpenRead(const NIO::THandle&, std::string_view) override
    {
        if (Exception_ == 0) {
            throw std::system_error(std::make_error_code(std::errc::permission_denied));
        }
        if (Exception_ == 1) {
            throw std::bad_alloc();
        }
        throw 42;
    }
    NIO::TResult<std::size_t> Write(const NIO::THandle& file, std::span<const std::byte> bytes) override
    {
        return Fs_.Write(file, bytes);
    }
    NIO::TResult<std::size_t> PRead(const NIO::THandle& file, std::uint64_t offset, std::span<std::byte> bytes) override
    {
        return Fs_.PRead(file, offset, bytes);
    }
    NIO::TResult<NIO::TFileStat> Stat(const NIO::THandle& file) override
    {
        return Fs_.Stat(file);
    }
    NIO::TResult<NIO::TSpaceInfo> StatFs(const NIO::THandle& directory) override
    {
        return Fs_.StatFs(directory);
    }
    std::error_code SyncFile(const NIO::THandle& file) override
    {
        return Fs_.SyncFile(file);
    }
    std::error_code SyncDir(const NIO::THandle& directory) override
    {
        return Fs_.SyncDir(directory);
    }
    std::error_code Close(const NIO::THandle& file) override
    {
        return Fs_.Close(file);
    }

private:
    NIO::TPosixLocalFS Fs_;
    int Exception_;
};

TEST_F(TDurableLocalFileIOTest, ExceptionBoundaryRecordsAttemptFailure)
{
    const std::array errors{std::errc::permission_denied, std::errc::not_enough_memory, std::errc::io_error};
    for (int exception = 0; exception < 3; ++exception) {
        auto fs = std::make_shared<TThrowingReadFS>(exception);
        auto dirs = NIO::TStoreDirs::Open(fs, Dirs_->Path(), Latch_);
        ASSERT_TRUE(dirs);
        auto adapter = TDurableLocalFileIO::Make(dirs.Value(), KStore);
        ASSERT_TRUE(adapter);
        auto attempt = MakeAttempt();
        auto scope = (*adapter)->BindAttempt(attempt);
        ASSERT_TRUE(scope);
        EXPECT_FALSE((*adapter)->NewInputFile(Metadata_));
        EXPECT_EQ(attempt->FirstError(), errors[exception]);
        EXPECT_FALSE(Latch_->IsSet());
    }
}

TEST_F(TDurableLocalFileIOTest, GenericCloseProducesUnverifiedReceiptAndSurvivesSeal)
{
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    auto stream = Create(Metadata_);
    ASSERT_NE(stream, nullptr);
    EXPECT_TRUE(stream->Write(Bytes("{}")));
    EXPECT_TRUE(stream->Close());
    EXPECT_FALSE(Attempt_->Seal());
    const auto events = Fs_->Events().size();
    EXPECT_TRUE(stream->Close());
    EXPECT_EQ(Fs_->Events().size(), events);
    ASSERT_EQ(Attempt_->Receipts().size(), 1);
    EXPECT_FALSE(Attempt_->Record(Metadata_).Value().FormatVerified);
}

TEST_F(TDurableLocalFileIOTest, RealParquetWriterCannotEraseFirstCloseFailureByRetry)
{
    iceberg::parquet::RegisterAll();
    ASSERT_FALSE(Attempt_->DeclareDataFile(Data_));
    auto scope = Io_->BindAttempt(Attempt_);
    ASSERT_TRUE(scope);
    iceberg::WriterOptions options;
    options.path = Data_;
    options.io = Io_;
    options.schema = std::make_shared<iceberg::Schema>(
        std::vector{iceberg::SchemaField::MakeRequired(1, "value", iceberg::int64())});
    options.properties = iceberg::WriterProperties::FromMap({{"write.parquet.compression-codec", "uncompressed"}});
    auto writer = iceberg::WriterFactoryRegistry::Open(iceberg::FileFormatType::kParquet, options);
    ASSERT_TRUE(writer) << writer.error().message;
    Inject(EIOOp::Close, std::errc::no_space_on_device, "*.parquet");
    EXPECT_FALSE((*writer)->Close());
    EXPECT_TRUE((*writer)->Close());
    EXPECT_EQ(Attempt_->FirstError(), std::errc::no_space_on_device);
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_FALSE(Io_->NewInputFile(Data_));
    EXPECT_EQ(Count(EIOOp::Close), 1);
}

}
}
