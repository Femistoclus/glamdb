// Verified scans reuse membership; metadata APIs still read authoritative files.
// Environmental read failures preserve their classification and readiness.

#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <system_error>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;

class ScanReadErrorClassificationTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    // Declared before the engine: the fault FS wraps it and the backend keeps the
    // fault FS alive until the engine is released in TearDown.
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<TFaultFS> Faults_ = std::make_shared<TFaultFS>(Posix_);
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        const auto faults = Faults_;
        const detail::BackendFactory factory =
            [faults](const std::filesystem::path& path,
                const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = faults;
            auto backend = detail::TLocalStorageBackend::Open(path, info, options);
            if (!backend) {
                return std::unexpected(backend.error());
            }
            return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
        };
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, factory);
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        const auto appended = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
        ASSERT_EQ(appended.disposition, CommitDisposition::Committed)
            << (appended.error ? appended.error->code + ": " + appended.error->message : std::string());
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
            Engine_.reset();
        }
    }

    int64_t CountRows()
    {
        const auto cursor = Engine_->Scan({.collection_id = test::collection_id});
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().code + ": " + cursor.error().message);
        if (!cursor) {
            return -1;
        }
        int64_t rows = 0;
        while (true) {
            const auto batch = (*cursor)->Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().code + ": " + batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            rows += (*batch)->num_rows();
        }
        return rows;
    }
};

TEST_F(ScanReadErrorClassificationTest, VerifiedScansDoNotRereadRoots)
{
    for (const auto error : {std::errc::io_error, std::errc::too_many_files_open, std::errc::not_enough_memory}) {
        SCOPED_TRACE(std::make_error_code(error).message());
        Faults_->AddRule({EIOOp::OpenRead, "*.metadata.json", 1, 1, std::make_error_code(error), std::nullopt});
        EXPECT_EQ(CountRows(), static_cast<int64_t>(FirstRows.size()));
        // The metadata API still reads the authoritative root and consumes the fault.
        const auto history = Engine_->ListSnapshots(test::collection_id);
        ASSERT_FALSE(history);
        EXPECT_NE(history.error().code, "CORRUPTION");
        EXPECT_TRUE(Engine_->GetHealth().read_available);
        EXPECT_TRUE(Engine_->ListSnapshots(test::collection_id));
    }
}

TEST_F(ScanReadErrorClassificationTest, VerifiedScansDoNotRereadManifests)
{
    Faults_->AddRule({EIOOp::PRead, "*-m[0-9].avro", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    EXPECT_EQ(CountRows(), static_cast<int64_t>(FirstRows.size()));
    const auto statistics = Engine_->GetStatistics(test::collection_id);
    ASSERT_FALSE(statistics);
    EXPECT_EQ(statistics.error().code, "IO_ERROR");
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    EXPECT_TRUE(Engine_->GetStatistics(test::collection_id));
}

TEST_F(ScanReadErrorClassificationTest, MetadataFailuresPreserveClassificationAndExplainReadsNoData)
{
    for (const int method : {0, 1, 2}) {
        SCOPED_TRACE(method);
        Faults_->AddRule(
            {EIOOp::OpenRead, "*.metadata.json", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
        const auto failure = method == 0 ? Status(Engine_->GetStatistics(test::collection_id).transform([](auto) {
        }))
            : method == 1                ? Status(Engine_->ListSnapshots(test::collection_id).transform([](auto) {
              }))
                          : Status(Engine_->ExplainScan({.collection_id = test::collection_id}).transform([](auto) {
                            }));
        ASSERT_FALSE(failure);
        EXPECT_EQ(failure.error().code, "IO_ERROR");
        EXPECT_TRUE(Engine_->GetHealth().read_available);
    }
    // If explain or statistics opens Parquet, this persistent fault rejects it.
    Faults_->AddRule({EIOOp::OpenRead, "*.parquet", 1, 100, std::make_error_code(std::errc::io_error), std::nullopt});
    EXPECT_TRUE(Engine_->GetStatistics(test::collection_id));
    EXPECT_TRUE(Engine_->ExplainScan({.collection_id = test::collection_id}));
    const auto cursor = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor);
    const auto failed = (*cursor)->Next();
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, "IO_ERROR");
    EXPECT_EQ((*cursor)->Profile().state, ScanState::Failed);
    EXPECT_EQ((*cursor)->Profile().data_files_read.value, 0);
    EXPECT_EQ((*cursor)->Profile().rows_returned.value, 0);
}

TEST_F(ScanReadErrorClassificationTest, MissingMetadataClosesReadiness)
{
    Faults_->AddRule({EIOOp::OpenRead,
        "*.metadata.json",
        1,
        1,
        std::make_error_code(std::errc::no_such_file_or_directory),
        std::nullopt});
    const auto result = Engine_->ListSnapshots(test::collection_id);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, "CORRUPTION");
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    const auto blocked = Engine_->GetStatistics(test::collection_id);
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error().code, "RECOVERY_REQUIRED");
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_TRUE(Engine_->GetStatistics(test::collection_id));
}

TEST_F(ScanReadErrorClassificationTest, FileDetailsClassifyFailuresAndReadOnlyFooterRanges)
{
    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing) << listing.error().message;
    ASSERT_EQ(listing->Files.size(), 1U);
    const auto& file = listing->Files.front();
    const auto firstEvent = Faults_->Events().size();
    const auto details = Engine_->GetFileDetails(test::collection_id, file.FileID);
    ASSERT_TRUE(details) << details.error().message;
    const auto events = Faults_->Events();
    uint64_t dataBytes = 0;
    for (size_t index = firstEvent; index < events.size(); ++index) {
        if (events[index].Op == EIOOp::PRead && events[index].Path.ends_with(".parquet")) {
            dataBytes += events[index].Bytes;
        }
    }
    EXPECT_GT(dataBytes, 12U);
    EXPECT_LT(dataBytes, static_cast<uint64_t>(file.CompressedBytes));

    for (const auto operation : {EIOOp::OpenRead, EIOOp::PRead, EIOOp::Close}) {
        Faults_->AddRule({operation, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
        const auto failed = Engine_->GetFileDetails(test::collection_id, file.FileID);
        ASSERT_FALSE(failed);
        EXPECT_EQ(failed.error().code, "IO_ERROR");
        EXPECT_TRUE(Engine_->GetHealth().read_available);
        EXPECT_TRUE(Engine_->GetFileDetails(test::collection_id, file.FileID));
    }
    Faults_->AddRule({EIOOp::OpenRead, "*.parquet", 1, 100, std::make_error_code(std::errc::io_error), std::nullopt});
    EXPECT_TRUE(Engine_->ListFiles(test::collection_id));
    const auto rejected = Engine_->GetFileDetails(test::collection_id, std::string(64, '0'));
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, "NOT_FOUND");
}

TEST_F(ScanReadErrorClassificationTest, MissingSelectedDataFileClosesReadiness)
{
    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing);
    ASSERT_EQ(listing->Files.size(), 1U);
    Faults_->AddRule(
        {EIOOp::OpenRead, "*.parquet", 1, 1, std::make_error_code(std::errc::no_such_file_or_directory), std::nullopt});
    const auto failed = Engine_->GetFileDetails(test::collection_id, listing->Files.front().FileID);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, "CORRUPTION");
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    ASSERT_TRUE(Engine_->Reconcile());
}

} // namespace
} // namespace glamdb
