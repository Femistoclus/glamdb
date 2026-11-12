// A scan reads each data file through bounded positioned reads: the Parquet
// magic, trailer and footer, then one read per column chunk of a projected or
// filtered field. Counts are checked against the physical reads observed by a
// fault-injecting filesystem and against the file layout reported by the
// independent file-details API.

#include "core/scan/scan_cursor.h"
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

std::vector<TInputRow> ManyRows(int count)
{
    std::vector<TInputRow> rows;
    for (int row = 0; row < count; ++row) {
        rows.push_back({"key-" + std::to_string(row),
            row % 7 == 0 ? std::nullopt : std::optional<int64_t>(row),
            0.5 * row,
            row % 2 == 0,
            int64_t{1000} * row});
    }
    return rows;
}

class ScanRangeReadTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<TFaultFS> Faults_ = std::make_shared<TFaultFS>(Posix_);
    std::shared_ptr<Engine> Engine_;
    std::vector<TInputRow> Rows_ = ManyRows(3000);

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
        const auto appended = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(Rows_)}});
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

    struct TLayout
    {
        int64_t FileBytes = 0;
        int64_t FooterBytes = 0;
        // Compressed bytes of each field's chunks (field ID - 1), summed over row groups.
        std::vector<int64_t> FieldBytes;
        int64_t MaxChunkBytes = 0;
        int64_t RowGroups = 0;
    };

    TLayout Layout()
    {
        TLayout layout;
        layout.FieldBytes.resize(Schema.fields.size());
        const auto listing = Engine_->ListFiles(test::collection_id);
        EXPECT_TRUE(listing);
        EXPECT_EQ(listing->Files.size(), 1U);
        const auto details = Engine_->GetFileDetails(test::collection_id, listing->Files.front().FileID);
        EXPECT_TRUE(details);
        layout.FileBytes = details->File.CompressedBytes;
        for (const auto& group : details->RowGroups) {
            ++layout.RowGroups;
            for (const auto& column : group.Columns) {
                layout.FieldBytes.at(static_cast<size_t>(column.FieldID - 1)) += column.CompressedBytes;
                layout.MaxChunkBytes = std::max(layout.MaxChunkBytes, column.CompressedBytes);
            }
        }
        for (const auto& entry : std::filesystem::recursive_directory_iterator(Root_)) {
            if (entry.path().extension() == ".parquet") {
                const auto bytes = ReadBytes(entry.path());
                EXPECT_EQ(static_cast<int64_t>(bytes.size()), layout.FileBytes);
                uint32_t footer = 0;
                for (size_t index = 0; index < 4; ++index) {
                    footer |= uint32_t{static_cast<uint8_t>(bytes[bytes.size() - 8 + index])} << (8 * index);
                }
                layout.FooterBytes = footer;
            }
        }
        return layout;
    }

    struct TScanResult
    {
        std::vector<std::string> Rows;
        ScanProfile Profile;
        detail::TScanReadStats Stats;
        uint64_t PhysicalCalls = 0;
        uint64_t PhysicalBytes = 0;
    };

    TScanResult Drain(const ScanRequest& request)
    {
        TScanResult result;
        const auto firstEvent = Faults_->Events().size();
        const auto cursor = Engine_->Scan(request);
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().message);
        if (!cursor) {
            return result;
        }
        while (true) {
            const auto batch = (*cursor)->Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().code + ": " + batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            const auto rendered = Render(**batch);
            result.Rows.insert(result.Rows.end(), rendered.begin(), rendered.end());
        }
        result.Profile = (*cursor)->Profile();
        result.Stats = detail::GetScanReadStats(**cursor).value();
        const auto events = Faults_->Events();
        for (size_t index = firstEvent; index < events.size(); ++index) {
            if (events[index].Op == EIOOp::PRead && events[index].Path.ends_with(".parquet")) {
                ++result.PhysicalCalls;
                result.PhysicalBytes += events[index].Bytes;
            }
        }
        return result;
    }
};

TEST_F(ScanRangeReadTest, ProjectionReadsOnlyFooterAndItsColumnChunks)
{
    const auto layout = Layout();
    ASSERT_EQ(layout.RowGroups, 1);
    // Magic (4 bytes), length + magic trailer (8 bytes), footer.
    const auto footerBytes = 12 + layout.FooterBytes;

    const auto narrow = Drain({.collection_id = test::collection_id, .projection = std::vector<std::string>{"score"}});
    EXPECT_EQ(static_cast<int64_t>(narrow.Rows.size()), static_cast<int64_t>(Rows_.size()));
    EXPECT_EQ(narrow.Profile.data_files_read.value, 1);
    EXPECT_EQ(narrow.Profile.data_bytes_read.value, footerBytes + layout.FieldBytes[2]);
    EXPECT_EQ(narrow.Profile.data_bytes_read.value, static_cast<int64_t>(narrow.PhysicalBytes));
    EXPECT_LT(*narrow.Profile.data_bytes_read.value * 3, layout.FileBytes);
    // Three footer reads and one column chunk.
    EXPECT_EQ(narrow.Stats.ReadCalls, 4);
    EXPECT_EQ(narrow.Stats.ReadCalls, static_cast<int64_t>(narrow.PhysicalCalls));
    EXPECT_EQ(narrow.Stats.MaxReadBytes, std::max(layout.FooterBytes, layout.FieldBytes[2]));
    EXPECT_LE(narrow.Stats.PeakHeldBytes, layout.FieldBytes[2]);

    // A filter-only field is read too, and only the projected one is returned.
    const auto filtered = Drain({.collection_id = test::collection_id,
        .projection = std::vector<std::string>{"score"},
        .predicate = Predicate::Compare("value", CompareOp::Lt, int64_t{10})});
    EXPECT_EQ(filtered.Profile.data_bytes_read.value, footerBytes + layout.FieldBytes[1] + layout.FieldBytes[2]);
    EXPECT_EQ(filtered.Stats.ReadCalls, 5);
    EXPECT_EQ(Sorted(filtered.Rows),
        Sorted({"0.500000", "1.000000", "1.500000", "2.000000", "2.500000", "3.000000", "4.000000", "4.500000"}));

    const auto full = Drain({.collection_id = test::collection_id});
    EXPECT_EQ(Sorted(full.Rows), Sorted(Render(*Batch(Rows_))));
    int64_t allChunks = 0;
    for (const auto bytes : layout.FieldBytes) {
        allChunks += bytes;
    }
    EXPECT_EQ(full.Profile.data_bytes_read.value, footerBytes + allChunks);
    EXPECT_EQ(full.Profile.data_bytes_read.value, static_cast<int64_t>(full.PhysicalBytes));
    EXPECT_LE(*full.Profile.data_bytes_read.value, layout.FileBytes);
    EXPECT_EQ(full.Stats.ReadCalls, 3 + static_cast<int64_t>(Schema.fields.size()));
    EXPECT_LE(full.Stats.MaxReadBytes, std::max(layout.FooterBytes, layout.MaxChunkBytes));
    EXPECT_LE(full.Stats.PeakHeldBytes, allChunks);
}

TEST_F(ScanRangeReadTest, ColumnChunkReadFailureIsAnIOErrorThatKeepsReadiness)
{
    // The fourth data-file read is the first column chunk, after the footer.
    Faults_->AddRule({EIOOp::PRead, "*.parquet", 4, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    const auto cursor = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor);
    const auto failed = (*cursor)->Next();
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, "IO_ERROR") << failed.error().message;
    EXPECT_EQ((*cursor)->Profile().state, ScanState::Failed);
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    const auto retried = Drain({.collection_id = test::collection_id});
    EXPECT_EQ(retried.Rows.size(), Rows_.size());

    // A failed footer read is an I/O error too.
    Faults_->AddRule({EIOOp::PRead, "*.parquet", 2, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    const auto footer = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(footer);
    const auto footerFailure = (*footer)->Next();
    ASSERT_FALSE(footerFailure);
    EXPECT_EQ(footerFailure.error().code, "IO_ERROR") << footerFailure.error().message;
    EXPECT_TRUE(Engine_->GetHealth().read_available);

    // Closing the data file after decoding is checked like the former whole-file read.
    Faults_->AddRule({EIOOp::Close, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    const auto closing = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(closing);
    Result<std::shared_ptr<arrow::RecordBatch>> next;
    do {
        next = (*closing)->Next();
    } while (next && *next);
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "IO_ERROR") << next.error().message;
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    EXPECT_EQ(Drain({.collection_id = test::collection_id}).Rows.size(), Rows_.size());
}

TEST_F(ScanRangeReadTest, LimitedScansReportDataFileCloseFailures)
{
    const auto closeFails = [&] {
        Faults_->AddRule({EIOOp::Close, "*.parquet", 1, 100, std::make_error_code(std::errc::io_error), std::nullopt});
    };
    const auto clearFaults = [&] {
        // A rule stops matching after its repeats; consume them with a full scan
        // that is allowed to fail.
        for (int attempt = 0; attempt < 200; ++attempt) {
            const auto cursor = Engine_->Scan({.collection_id = test::collection_id, .limit = 0});
            ASSERT_TRUE(cursor);
            ASSERT_TRUE((*cursor)->Next());
            auto probe = Engine_->Scan({.collection_id = test::collection_id});
            ASSERT_TRUE(probe);
            Result<std::shared_ptr<arrow::RecordBatch>> next;
            do {
                next = (*probe)->Next();
            } while (next && *next);
            if (next) {
                return;
            }
        }
        FAIL() << "close faults did not drain";
    };
    const auto expectCloseFailure = [&](std::shared_ptr<ScanCursor> cursor, int64_t expectedRows) {
        int64_t rows = 0;
        Result<std::shared_ptr<arrow::RecordBatch>> next;
        while ((next = cursor->Next()) && *next) {
            rows += (*next)->num_rows();
        }
        EXPECT_EQ(rows, expectedRows);
        ASSERT_FALSE(next) << "a failed close was reported as EOS";
        EXPECT_EQ(next.error().code, "IO_ERROR") << next.error().message;
        EXPECT_EQ(cursor->Profile().state, ScanState::Failed);
        EXPECT_TRUE(Engine_->GetHealth().read_available);
    };

    // The limit ends the scan inside the file.
    closeFails();
    expectCloseFailure(*Engine_->Scan({.collection_id = test::collection_id, .limit = 1}), 1);
    clearFaults();
    // The limit ends the scan exactly at the end of the file, before its EOF was read.
    closeFails();
    expectCloseFailure(
        *Engine_->Scan({.collection_id = test::collection_id, .limit = static_cast<int64_t>(Rows_.size())}),
        static_cast<int64_t>(Rows_.size()));
    clearFaults();
    // A zero limit opens no data file, so nothing can fail to close.
    closeFails();
    const auto zero = Engine_->Scan({.collection_id = test::collection_id, .limit = 0});
    ASSERT_TRUE(zero);
    const auto empty = (*zero)->Next();
    ASSERT_TRUE(empty);
    EXPECT_EQ(*empty, nullptr);
    EXPECT_EQ((*zero)->Profile().state, ScanState::Complete);
    clearFaults();
    // Cancellation after the limit is still reported as cancellation; the
    // close is best effort there.
    closeFails();
    const auto cancelled = Engine_->Scan({.collection_id = test::collection_id, .limit = 1});
    ASSERT_TRUE(cancelled);
    ASSERT_TRUE((*cancelled)->Next());
    (*cancelled)->Cancel();
    const auto stopped = (*cancelled)->Next();
    ASSERT_FALSE(stopped);
    EXPECT_EQ(stopped.error().code, "CANCELLED");
    clearFaults();

    // The limit is reached on a later file: the newest file (two rows) is read
    // to its end and closed, then the limit ends the scan inside this fixture's file.
    const auto appended = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
    ASSERT_EQ(appended.disposition, CommitDisposition::Committed);
    const auto later = Engine_->Scan({.collection_id = test::collection_id,
        .limit = static_cast<int64_t>(SecondRows.size()) + 1,
        .batch_rows = static_cast<int64_t>(SecondRows.size())});
    ASSERT_TRUE(later);
    int64_t rows = 0;
    // The first batch is the newest file; the second opens the older one.
    for (int batch = 0; batch < 2; ++batch) {
        const auto next = (*later)->Next();
        ASSERT_TRUE(next && *next) << (next ? std::string("EOS") : next.error().message);
        rows += (**next).num_rows();
    }
    EXPECT_EQ(rows, static_cast<int64_t>(SecondRows.size()) + 1);
    closeFails();
    expectCloseFailure(*later, 0);
    clearFaults();
    EXPECT_EQ(Drain({.collection_id = test::collection_id}).Rows.size(), Rows_.size() + SecondRows.size());
}

TEST_F(ScanRangeReadTest, ResourceErrorUnderTheDecoderKeepsItsClassification)
{
    Faults_->AddRule(
        {EIOOp::PRead, "*.parquet", 4, 1, std::make_error_code(std::errc::not_enough_memory), std::nullopt});
    const auto cursor = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor);
    const auto failed = (*cursor)->Next();
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, "RESOURCE_EXHAUSTED") << failed.error().message;
    EXPECT_TRUE(Engine_->GetHealth().read_available);
}

} // namespace
} // namespace glamdb
