#include "storage/resources.h"
#include "tests/support/checked.h"
#include "tests/support/fault_fs.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include <chrono>
#include <future>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace vertical;

class TGeneratedBulkSource final : public BulkAppendSource
{
public:
    int64_t Rows = 20'000;
    int64_t ChunkRows = 1000;
    size_t KeyBytes = 900;
    int Pass = 0;
    int FailPass = 0;
    bool ChangeSecondPass = false;
    std::function<void()> OnNext;

    Status Rewind() override
    {
        Position_ = 0;
        ++Pass;
        return {};
    }

    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        if (OnNext) {
            OnNext();
        }
        if (Pass == FailPass && Position_ >= ChunkRows) {
            return std::unexpected(ErrorDetail{"IO_ERROR", "Source interrupted.", RetryHint::OperatorAction});
        }
        if (Position_ == Rows) {
            return nullptr;
        }
        const auto count = std::min(ChunkRows, Rows - Position_);
        std::vector<TInputRow> rows;
        for (int64_t i = 0; i < count; ++i) {
            const auto value = (Position_ + i) % 7;
            rows.push_back({std::string(KeyBytes, static_cast<char>('a' + value)),
                value + (ChangeSecondPass && Pass > 1 ? 1 : 0),
                -0.0,
                true,
                std::nullopt});
        }
        Position_ += count;
        return Batch(rows);
    }

private:
    int64_t Position_ = 0;
};

class TRepeatedBulkCapSource final : public BulkAppendSource
{
public:
    explicit TRepeatedBulkCapSource(int64_t rows)
        : Rows_(rows)
    {
        arrow::StringBuilder builder;
        const std::string value(4096, 'x');
        for (int row = 0; row < 1000; ++row) {
            const auto appended = builder.Append(value);
            if (!appended.ok()) {
                throw std::runtime_error(appended.ToString());
            }
        }
        const auto array = builder.Finish();
        if (!array.ok()) {
            throw std::runtime_error(array.status().ToString());
        }
        Batch_ =
            arrow::RecordBatch::Make(arrow::schema({arrow::field("payload", arrow::utf8(), false)}), 1000, {*array});
    }

    Status Rewind() override
    {
        Position_ = 0;
        return {};
    }

    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        if (Position_ == Rows_) {
            return nullptr;
        }
        const auto count = std::min<int64_t>(1000, Rows_ - Position_);
        Position_ += count;
        return Batch_->Slice(0, count);
    }

private:
    int64_t Rows_;
    int64_t Position_ = 0;
    std::shared_ptr<arrow::RecordBatch> Batch_;
};

class TBulkAppendTest : public ::testing::Test
{
protected:
    test::JournalFaultVfs Vfs_;
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<Engine> Engine_;
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<NGlamDB::NTesting::TFaultFS> Faults_ = std::make_shared<NGlamDB::NTesting::TFaultFS>(Posix_);
    std::shared_ptr<THookBackend> Hooks_;

    void SetUp() override
    {
        detail::TLocalStorageOptions options;
        options.Fs = Faults_;
        const auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()},
            THookBackend::Factory(Hooks_, options),
            Vfs_.Name());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = *created;
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    BulkAppendRequest Request(const std::shared_ptr<TGeneratedBulkSource>& source)
    {
        return {.collection_id = test::collection_id,
            .operation_id = FirstOperation,
            .rows = source->Rows,
            .source = source};
    }

    int64_t Read(SnapshotSelector selector = SnapshotSelector::Latest(), size_t keyBytes = 900)
    {
        ScanRequest request;
        request.collection_id = test::collection_id;
        request.selector = selector;
        const auto cursor = Engine_->Scan(request);
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().message);
        if (!cursor) {
            return -1;
        }
        int64_t count = 0;
        std::array<int64_t, 7> occurrences{};
        for (;;) {
            const auto batch = (*cursor)->Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            const auto& keys = static_cast<const arrow::StringArray&>(*(*batch)->column(0));
            const auto& values = static_cast<const arrow::Int64Array&>(*(*batch)->column(1));
            for (int64_t row = 0; row < (*batch)->num_rows(); ++row) {
                const auto value = values.Value(row);
                EXPECT_GE(value, 0);
                EXPECT_LT(value, 7);
                if (value >= 0 && value < 7) {
                    EXPECT_EQ(keys.GetView(row), std::string(keyBytes, static_cast<char>('a' + value)));
                    ++occurrences[static_cast<size_t>(value)];
                }
            }
            count += (*batch)->num_rows();
        }
        for (size_t value = 0; value < occurrences.size(); ++value) {
            EXPECT_EQ(occurrences[value], count / 7 + (static_cast<int64_t>(value) < count % 7 ? 1 : 0));
        }
        return count;
    }
};

TEST_F(TBulkAppendTest, StreamsBoundedFilesIntoOneSnapshotAndPreservesOldReceipts)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    auto request = Request(source);
    const auto result = Engine_->BulkAppendDurable(request);
    ASSERT_EQ(result.disposition, CommitDisposition::Committed)
        << (result.error ? test::Present(result.error).code + ": " + test::Present(result.error).message
                         : std::string());
    ASSERT_TRUE(result.result);
    EXPECT_EQ(test::Present(result.result).rows_appended, source->Rows);
    EXPECT_GT(test::Present(result.result).data_files_added, 1);
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 1);
    EXPECT_EQ(Read(), source->Rows);
    const auto snapshot = test::Present(result.result).snapshot_id;
    const auto later = Engine_->AppendDurable(
        {test::collection_id, SecondOperation, {}, {Batch({{std::string(900, 'a'), 0, -0.0, true, std::nullopt}})}});
    ASSERT_EQ(later.disposition, CommitDisposition::Committed);
    EXPECT_EQ(Read(SnapshotSelector::Id(snapshot)), source->Rows);
    EXPECT_EQ(Read(SnapshotSelector::InitialEmpty()), 0);
    source->ChunkRows = 777;
    request.expected_snapshot = {SnapshotPreconditionKind::Equals, -1};
    EXPECT_EQ(Engine_->BulkAppendDurable(request), result);
    EXPECT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = *reopened;
    EXPECT_EQ(Read(SnapshotSelector::Id(snapshot)), source->Rows);
    EXPECT_EQ(Engine_->BulkAppendDurable(request), result);
    EXPECT_EQ(*Engine_->FindAppendOutcome(test::collection_id, FirstOperation), result);
}

TEST_F(TBulkAppendTest, CollectionCompressionAppliesToEveryBulkFileAndRetriesKeepOriginalSettings)
{
    const WriteSettings settings{.compression = ParquetCompression{ParquetCodec::Gzip, 6},
        .columns = {{.field = "key", .compression = ParquetCompression{ParquetCodec::Uncompressed, std::nullopt}}}};
    ASSERT_TRUE(Engine_->UpdateWriteSettings({test::collection_id, 0, settings}));
    const auto source = std::make_shared<TGeneratedBulkSource>();
    const auto request = Request(source);
    const auto committed = Engine_->BulkAppendDurable(request);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed)
        << (committed.error ? test::Present(committed.error).message : std::string());
    ASSERT_TRUE(committed.result);
    const auto& result = test::Present(committed.result);
    ASSERT_GT(result.data_files_added, 1);
    const auto verifyFooters = [&] {
        int64_t files = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(Root_ / "warehouse")) {
            if (entry.path().extension() != ".parquet" ||
                entry.path().filename().string().find(FirstOperation) == std::string::npos)
            {
                continue;
            }
            ++files;
            const auto reader = parquet::ParquetFileReader::OpenFile(entry.path().string());
            const auto footer = reader->metadata();
            ASSERT_GT(footer->num_row_groups(), 0);
            for (int group = 0; group < footer->num_row_groups(); ++group) {
                for (int column = 0; column < footer->num_columns(); ++column) {
                    const auto expected = footer->schema()->Column(column)->name() == "key"
                        ? arrow::Compression::UNCOMPRESSED
                        : arrow::Compression::GZIP;
                    EXPECT_EQ(footer->RowGroup(group)->ColumnChunk(column)->compression(), expected);
                }
            }
        }
        EXPECT_EQ(files, result.data_files_added);
    };
    verifyFooters();
    const WriteSettings laterSettings{.compression = ParquetCompression{ParquetCodec::Snappy, std::nullopt}};
    ASSERT_TRUE(Engine_->UpdateWriteSettings({test::collection_id, 1, laterSettings}));
    EXPECT_EQ(Engine_->BulkAppendDurable(request), committed);
    EXPECT_EQ(Read(), source->Rows);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = *reopened;
    EXPECT_EQ(Engine_->BulkAppendDurable(request), committed);
    EXPECT_EQ(Read(), source->Rows);
    verifyFooters();
}

TEST_F(TBulkAppendTest, RetainedFileMetricsStayChargedAcrossReconciliationAndReopen)
{
    const auto rootBytes = [&] {
        uint64_t bytes = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(Root_ / "warehouse")) {
            if (entry.path().filename().string().ends_with(".metadata.json")) {
                bytes = std::max(bytes, entry.file_size());
            }
        }
        return bytes;
    };
    const auto owner = Engine_->GetResourceOwner();
    const auto before = owner->GetUsage().memory_bytes[1];
    const auto oldRootBytes = rootBytes();
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->Rows = 28'000;
    const auto committed = Engine_->BulkAppendDurable(Request(source));
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed)
        << (committed.error ? test::Present(committed.error).message : std::string());
    ASSERT_TRUE(committed.result);
    ASSERT_EQ(test::Present(committed.result).data_files_added, 3);
    const auto secondSource = std::make_shared<TGeneratedBulkSource>();
    secondSource->Rows = 14'000;
    auto secondRequest = Request(secondSource);
    secondRequest.operation_id = SecondOperation;
    const auto second = Engine_->BulkAppendDurable(secondRequest);
    ASSERT_EQ(second.disposition, CommitDisposition::Committed)
        << (second.error ? test::Present(second.error).message : std::string());
    ASSERT_TRUE(second.result);
    ASSERT_EQ(test::Present(second.result).data_files_added, 2);
    const auto files = test::Present(committed.result).data_files_added + test::Present(second.result).data_files_added;
    // Every persisted file retains at least the column-size and value-count
    // map values for every field, beyond the existing JSON root envelope.
    const auto minimumMaps =
        static_cast<uint64_t>(files) * Schema.fields.size() * 2 * sizeof(std::pair<int32_t, int64_t>);
    const auto minimumCharge = before + (rootBytes() - oldRootBytes) * 32 + minimumMaps;
    EXPECT_GE(owner->GetUsage().memory_bytes[1], minimumCharge);
    auto pressure =
        owner->Reserve(ResourceClass::Metadata, owner->GetLimits().memory_bytes[1] - owner->GetUsage().memory_bytes[1]);
    ASSERT_TRUE(pressure) << pressure.error().message;
    const auto reconciled = Engine_->Reconcile();
    ASSERT_TRUE(reconciled) << reconciled.error().message;
    pressure->reset();
    EXPECT_GE(owner->GetUsage().memory_bytes[1], minimumCharge);
    EXPECT_EQ(Engine_->BulkAppendDurable(Request(source)), committed);
    EXPECT_EQ(Engine_->BulkAppendDurable(secondRequest), second);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    Hooks_.reset();
    const auto reopened = OpenExisting({Root_.string(), test::store_id, "local-delete-extra-v1", {}, owner});
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = *reopened;
    EXPECT_GE(owner->GetUsage().memory_bytes[1], minimumCharge);
    EXPECT_EQ(Engine_->BulkAppendDurable(Request(source)), committed);
    EXPECT_EQ(Engine_->BulkAppendDurable(secondRequest), second);
    EXPECT_EQ(Read(), source->Rows + secondSource->Rows);
}

TEST_F(TBulkAppendTest, ChangedProductionStreamHasPermanentFailureAndNoPartialVisibility)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->ChangeSecondPass = true;
    const auto request = Request(source);
    const auto failed = Engine_->BulkAppendDurable(request);
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).code, "INVALID_ARGUMENT");
    EXPECT_EQ(Read(), 0);
    EXPECT_TRUE(Engine_->ListSnapshots(test::collection_id)->snapshots.empty());
    source->ChangeSecondPass = false;
    EXPECT_EQ(Engine_->BulkAppendDurable(request), failed);
}

TEST_F(TBulkAppendTest, InterruptedValidationDoesNotBindTheOperation)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->FailPass = 1;
    const auto request = Request(source);
    const auto failed = Engine_->BulkAppendDurable(request);
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).code, "IO_ERROR");
    EXPECT_FALSE(Engine_->FindAppendOutcome(test::collection_id, FirstOperation)->has_value());
    source->FailPass = 0;
    EXPECT_EQ(Engine_->BulkAppendDurable(request).disposition, CommitDisposition::Committed);
}

TEST_F(TBulkAppendTest, CancellationAtPhysicalAdmissionDoesNotBindOrProduceFiles)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->Rows = 3;
    const auto stop = std::make_shared<std::atomic<bool>>(false);
    auto request = Request(source);
    request.cancellation = stop;
    bool enteredAdmission = false;
    Hooks_->BeforeAppendAdmission = [&](std::chrono::steady_clock::time_point) {
        enteredAdmission = true;
        stop->store(true);
    };
    const auto files = CountRegularFiles(Root_ / "warehouse");
    const auto failed = Engine_->BulkAppendDurable(request);
    EXPECT_TRUE(enteredAdmission);
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).code, "CANCELLED");
    const auto outcome = Engine_->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(outcome) << outcome.error().message;
    EXPECT_FALSE(outcome->has_value());
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), files);
    EXPECT_EQ(Read(), 0);
}

TEST_F(TBulkAppendTest, CancellationDuringProductionRecordsFailure)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    const auto stop = std::make_shared<std::atomic<bool>>(false);
    auto request = Request(source);
    request.cancellation = stop;
    source->OnNext = [&] {
        if (source->Pass == 2) {
            stop->store(true);
        }
    };
    const auto failed = Engine_->BulkAppendDurable(request);
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(Read(), 0);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).code, "CANCELLED");
    EXPECT_EQ(*Engine_->FindAppendOutcome(test::collection_id, FirstOperation), failed);
}

TEST_F(TBulkAppendTest, ValidationBoundsConsumeNoIdentity)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->Rows = 3;
    auto request = Request(source);
    request.rows = 4;
    EXPECT_EQ(Engine_->BulkAppendDurable(request).disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(Engine_->FindAppendOutcome(test::collection_id, FirstOperation)->has_value());
    request.rows = BulkAppendLimits::MaxRows + 1;
    EXPECT_EQ(test::Present(Engine_->BulkAppendDurable(request).error).code, "INVALID_ARGUMENT");
    EXPECT_FALSE(Engine_->FindAppendOutcome(test::collection_id, FirstOperation)->has_value());
    request.rows = source->Rows;
    EXPECT_EQ(Engine_->BulkAppendDurable(request).disposition, CommitDisposition::Committed);
}

TEST_F(TBulkAppendTest, MatchingOrdinaryReceiptIsReturnedBeforeBulkExecution)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->Rows = 3;
    ASSERT_TRUE(source->Rewind());
    const auto batch = source->Next();
    ASSERT_TRUE(batch);
    const auto ordinary = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {*batch}});
    ASSERT_EQ(ordinary.disposition, CommitDisposition::Committed);
    auto request = Request(source);
    request.expected_snapshot = {SnapshotPreconditionKind::Equals, -1};
    EXPECT_EQ(Engine_->BulkAppendDurable(request), ordinary);
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 1);
    source->ChangeSecondPass = true;
    const auto conflict = Engine_->BulkAppendDurable(request);
    ASSERT_TRUE(conflict.error);
    EXPECT_EQ(test::Present(conflict.error).code, "OPERATION_CONFLICT");
    EXPECT_EQ(*Engine_->FindAppendOutcome(test::collection_id, FirstOperation), ordinary);
}

TEST_F(TBulkAppendTest, OrdinaryAppendStillRefusesBulkInput)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->ChunkRows = source->Rows;
    ASSERT_TRUE(source->Rewind());
    const auto batch = source->Next();
    ASSERT_TRUE(batch);
    const auto refused = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {*batch}});
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "INVALID_ARGUMENT");
    EXPECT_FALSE(Engine_->FindAppendOutcome(test::collection_id, FirstOperation)->has_value());
    EXPECT_EQ(Read(), 0);
}

TEST_F(TBulkAppendTest, FailureOfSecondFileLeavesNoSnapshotAndSurvivesReopen)
{
    using NGlamDB::NTesting::EIOOp;
    Faults_->AddRule({EIOOp::SyncFile, "*.parquet", 2, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    const auto source = std::make_shared<TGeneratedBulkSource>();
    const auto failed = Engine_->BulkAppendDurable(Request(source));
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).code, "IO_ERROR");
    EXPECT_EQ(Read(), 0);
    EXPECT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    Engine_ = *opened;
    EXPECT_EQ(Engine_->BulkAppendDurable(Request(source)), failed);
    EXPECT_EQ(Read(), 0);
}

TEST_F(TBulkAppendTest, LostOuterCommitReplyResolvesAllFilesTogether)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    const auto request = Request(source);
    Vfs_.Arm(2, true);
    const auto unknown = Engine_->BulkAppendDurable(request);
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(unknown.recovery);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    Vfs_.Disarm();
    ASSERT_TRUE(Engine_->Reconcile());
    const auto receipt = Engine_->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(receipt && receipt->has_value());
    EXPECT_EQ(test::Present(*receipt).disposition, CommitDisposition::Committed);
    EXPECT_EQ(Engine_->BulkAppendDurable(request), test::Present(*receipt));
    EXPECT_EQ(Read(), source->Rows);
}

TEST_F(TBulkAppendTest, CancellationAfterOuterCommitKeepsTheCommittedReceipt)
{
    const auto stop = std::make_shared<std::atomic<bool>>(false);
    Hooks_->AfterPublication = [&] {
        stop->store(true);
    };
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->Rows = 3;
    auto request = Request(source);
    request.cancellation = stop;
    const auto committed = Engine_->BulkAppendDurable(request);
    EXPECT_TRUE(stop->load());
    EXPECT_EQ(committed.disposition, CommitDisposition::Committed);
    EXPECT_EQ(*Engine_->FindAppendOutcome(test::collection_id, FirstOperation), committed);
}

TEST_F(TBulkAppendTest, CancellationDuringClosurePreventsTheOuterCommit)
{
    const auto stop = std::make_shared<std::atomic<bool>>(false);
    Hooks_->BeforeVerifyClosure = [&](std::string_view) {
        stop->store(true);
    };
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->Rows = 3;
    auto request = Request(source);
    request.cancellation = stop;
    const auto failed = Engine_->BulkAppendDurable(request);
    ASSERT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).code, "CANCELLED");
    EXPECT_EQ(*Engine_->FindAppendOutcome(test::collection_id, FirstOperation), failed);
    EXPECT_TRUE(Engine_->ListSnapshots(test::collection_id)->snapshots.empty());
    EXPECT_EQ(Read(), 0);
}

TEST_F(TBulkAppendTest, OddChunkingRollsOverAtTheFileRowLimit)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->Rows = 200'003;
    source->ChunkRows = 9999;
    source->KeyBytes = 1;
    const auto result = Engine_->BulkAppendDurable(Request(source));
    ASSERT_EQ(result.disposition, CommitDisposition::Committed)
        << (result.error ? test::Present(result.error).message : std::string());
    EXPECT_EQ(test::Present(result.result).data_files_added, 3);
    EXPECT_EQ(Read(SnapshotSelector::Latest(), 1), source->Rows);
}

TEST_F(TBulkAppendTest, TinySourceChunksAreCoalescedIntoOneFile)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->Rows = 2051;
    source->ChunkRows = 1;
    source->KeyBytes = 1;
    const auto result = Engine_->BulkAppendDurable(Request(source));
    ASSERT_EQ(result.disposition, CommitDisposition::Committed)
        << (result.error ? test::Present(result.error).message : std::string());
    EXPECT_EQ(test::Present(result.result).data_files_added, 1);
    EXPECT_EQ(Read(SnapshotSelector::Latest(), 1), source->Rows);
}

TEST_F(TBulkAppendTest, CommittedRetryPrecedesLowerInputAndDeviceAdmission)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->Rows = 3;
    auto request = Request(source);
    const auto committed = Engine_->BulkAppendDurable(request);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    EXPECT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    ResourceLimits resourceLimits;
    resourceLimits.device_reserved_bytes = 1ULL << 30;
    auto resources = std::make_shared<ResourceOwner>(resourceLimits);
    Limits limits;
    limits.max_owned_input_bytes = 1;
    const auto opened = OpenExisting({Root_.string(), test::store_id, "local-delete-extra-v1", limits, resources});
    ASSERT_TRUE(opened) << opened.error().message;
    Engine_ = *opened;
    request.expected_snapshot = {SnapshotPreconditionKind::Equals, -1};
    EXPECT_EQ(Engine_->BulkAppendDurable(request), committed);
    request.operation_id = SecondOperation;
    const auto refused = Engine_->BulkAppendDurable(request);
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_FALSE(Engine_->FindAppendOutcome(test::collection_id, SecondOperation)->has_value());
}

TEST_F(TBulkAppendTest, ShutdownDuringProductionRecordsFailureWithoutPublishing)
{
    const auto source = std::make_shared<TGeneratedBulkSource>();
    source->Rows = 3;
    bool stopped = false;
    source->OnNext = [&] {
        if (source->Pass == 2 && !stopped) {
            stopped = true;
            auto shutdown = std::async(std::launch::async, [&] {
                return Engine_->Shutdown(0ms);
            });
            EXPECT_FALSE(shutdown.get());
        }
    };
    const auto failed = Engine_->BulkAppendDurable(Request(source));
    EXPECT_TRUE(stopped);
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).code, "CANCELLED");
    EXPECT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    Engine_ = *opened;
    EXPECT_EQ(Engine_->BulkAppendDurable(Request(source)), failed);
    EXPECT_EQ(Read(), 0);
}

// Manual capacity campaigns hash several GiB; keep them outside normal CI.
TEST_F(TBulkAppendTest, DISABLED_CanonicalByteCapRejectsBeforeBinding)
{
    ASSERT_EQ(Engine_->CreateCollection({SecondCollection, "payloads", {{{"payload", FieldType::String, false}}}})
                  .disposition,
        CommitDisposition::Committed);
    constexpr auto Rows = BulkAppendLimits::MaxDecodedBytes / (1 + 4 + 4096) + 1;
    const auto source = std::make_shared<TRepeatedBulkCapSource>(Rows);
    const auto refused = Engine_->BulkAppendDurable({SecondCollection, FirstOperation, {}, Rows, source});
    EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(refused.phase, CommitPhase::Validation);
    EXPECT_EQ(test::Present(refused.error).message, "bulk source exceeds its canonical byte limit");
    EXPECT_FALSE(Engine_->FindAppendOutcome(SecondCollection, FirstOperation)->has_value());
    EXPECT_TRUE(Engine_->ListSnapshots(SecondCollection)->snapshots.empty());
    EXPECT_TRUE(Engine_->GetHealth().write_ready);
}

TEST_F(TBulkAppendTest, DISABLED_FileCapRejectsAtomicallyAndReopensThePermanentFailure)
{
    ASSERT_EQ(Engine_->CreateCollection({SecondCollection, "payloads", {{{"payload", FieldType::String, false}}}})
                  .disposition,
        CommitDisposition::Committed);
    constexpr int64_t Rows = 800'000;
    static_assert(Rows * (1 + 4 + 4096) < BulkAppendLimits::MaxDecodedBytes);
    const auto source = std::make_shared<TRepeatedBulkCapSource>(Rows);
    const auto failed = Engine_->BulkAppendDurable({SecondCollection, FirstOperation, {}, Rows, source});
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(failed.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(failed.phase, CommitPhase::Data);
    EXPECT_NE(test::Present(failed.error).message.find("operation exceeds its data file limit"), std::string::npos);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse" / SecondCollection / "data"), 256U);
    EXPECT_EQ(test::Present(*Engine_->FindAppendOutcome(SecondCollection, FirstOperation)), failed);
    EXPECT_TRUE(Engine_->ListSnapshots(SecondCollection)->snapshots.empty());
    EXPECT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    Engine_ = *opened;
    EXPECT_EQ(test::Present(*Engine_->FindAppendOutcome(SecondCollection, FirstOperation)), failed);
    EXPECT_TRUE(Engine_->ListSnapshots(SecondCollection)->snapshots.empty());
    const auto cursor = Engine_->Scan({.collection_id = SecondCollection});
    ASSERT_TRUE(cursor);
    EXPECT_EQ(*(*cursor)->Next(), nullptr);
}

} // namespace
} // namespace glamdb
