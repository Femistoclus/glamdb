#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include <arrow/util/byte_size.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <optional>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace vertical;

// Issue #102 fixtures: required key and payload strings.
const SchemaSpec PayloadSchema{{
    {"key", FieldType::String, false},
    {"payload", FieldType::String, true},
}};

struct TPayloadRow
{
    std::string Key;
    std::optional<std::string> Payload;
};

std::shared_ptr<arrow::RecordBatch> PayloadBatch(const std::vector<TPayloadRow>& rows)
{
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<std::string>> payloads;
    for (const auto& row : rows) {
        keys.emplace_back(row.Key);
        payloads.push_back(row.Payload);
    }
    return arrow::RecordBatch::Make(
        arrow::schema({arrow::field("key", arrow::utf8(), false), arrow::field("payload", arrow::utf8())}),
        static_cast<int64_t>(rows.size()),
        {Column<arrow::StringBuilder>(keys, arrow::utf8()), Column<arrow::StringBuilder>(payloads, arrow::utf8())});
}

std::string Key(size_t row)
{
    char text[16];
    std::snprintf(text, sizeof(text), "k%05zu", row);
    return text;
}

// Both occupy 1,058,575 payload bytes in 10,000 rows.
std::vector<TPayloadRow> UniformRows()
{
    std::vector<TPayloadRow> rows;
    for (size_t row = 0; row < 10000; ++row) {
        rows.push_back({Key(row), std::string(row < 8575 ? 106 : 105, 'x')});
    }
    return rows;
}

std::vector<TPayloadRow> SkewedRows()
{
    std::vector<TPayloadRow> rows;
    for (size_t row = 0; row < 10000; ++row) {
        rows.push_back({Key(row), std::string(row == 0 ? (1U << 20) : 1U, 'x')});
    }
    return rows;
}

// Duplicate keys, NULL payloads and three 40 KiB values at the start, middle
// and end; about 3.5 times a 64 KiB row group bound.
std::vector<TPayloadRow> MixedRows()
{
    std::vector<TPayloadRow> rows;
    for (size_t row = 0; row < 3000; ++row) {
        std::optional<std::string> payload;
        if (row == 0 || row == 1500 || row == 2999) {
            payload = std::string(40 << 10, static_cast<char>('a' + row % 26));
        } else if (row % 5 != 0) {
            payload = std::string(row % 13, static_cast<char>('a' + row % 26));
        }
        rows.push_back({"dup-" + std::to_string(row % 7), std::move(payload)});
    }
    return rows;
}

// Documented writer accounting: per string field a validity byte, four offset
// bytes and the value bytes; plus one trailing offset per string field per group.
int64_t RowBytes(const TPayloadRow& row)
{
    return 5 + static_cast<int64_t>(row.Key.size()) + 5 + (row.Payload ? static_cast<int64_t>(row.Payload->size()) : 0);
}

struct TFooter
{
    std::vector<int64_t> GroupRows;
    int64_t FooterBytes = 0;
    int64_t FileBytes = 0;
};

TFooter ReadFooter(const std::filesystem::path& path)
{
    const auto footer = parquet::ParquetFileReader::OpenFile(path.string())->metadata();
    TFooter result;
    result.FooterBytes = footer->size();
    result.FileBytes = static_cast<int64_t>(std::filesystem::file_size(path));
    for (int group = 0; group < footer->num_row_groups(); ++group) {
        result.GroupRows.push_back(footer->RowGroup(group)->num_rows());
    }
    return result;
}

void ExpectGroupsWithin(const std::vector<TPayloadRow>& rows, const std::vector<int64_t>& groups, int64_t cap)
{
    size_t next = 0;
    for (size_t group = 0; group < groups.size(); ++group) {
        int64_t bytes = 8;
        for (int64_t row = 0; row < groups[group]; ++row) {
            bytes += RowBytes(rows.at(next++));
        }
        EXPECT_LE(bytes, cap) << "row group " << group;
    }
    EXPECT_EQ(next, rows.size());
}

class RowGroupLayoutTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<THookBackend> Hooks_;
    std::shared_ptr<Engine> Engine_;
    detail::TLocalStorageOptions Options_;

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    void Create(const SchemaSpec& schema = PayloadSchema)
    {
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()},
            THookBackend::Factory(Hooks_, Options_));
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", schema}).disposition,
            CommitDisposition::Committed);
    }

    void Reopen()
    {
        ASSERT_TRUE(Engine_->Shutdown(1000ms));
        Engine_.reset();
        auto opened =
            detail::EngineFactory::OpenWith({.absolute_root = Root_.string(), .expected_store_id = test::store_id},
                THookBackend::Factory(Hooks_, Options_));
        ASSERT_TRUE(opened) << opened.error().message;
        Engine_ = std::move(*opened);
    }

    std::filesystem::path OnlyDataFile() const
    {
        const auto files = FilesIn(Root_ / "warehouse" / test::collection_id / "data");
        EXPECT_EQ(files.size(), 1U);
        return files.empty() ? std::filesystem::path() : files.front();
    }

    // Rows in scan order; one data file is read in row group order.
    std::vector<std::string> ScanInOrder()
    {
        auto cursor = Engine_->Scan({.collection_id = test::collection_id, .batch_rows = 997});
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().message);
        std::vector<std::string> rows;
        while (cursor) {
            const auto batch = (*cursor)->Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            const auto rendered = Render(**batch);
            rows.insert(rows.end(), rendered.begin(), rendered.end());
        }
        return rows;
    }
};

TEST_F(RowGroupLayoutTest, EqualByteUniformAndSkewedAppendsEachFormOneGroup)
{
    const auto uniform = PayloadBatch(UniformRows());
    const auto skewed = PayloadBatch(SkewedRows());
    ASSERT_EQ(arrow::util::TotalBufferSize(*uniform), arrow::util::TotalBufferSize(*skewed));
    ASSERT_NO_FATAL_FAILURE(Create());
    ASSERT_EQ(Engine_->CreateCollection({SecondCollection, "skewed", PayloadSchema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {uniform}}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Engine_->AppendDurable({SecondCollection, FirstOperation, {}, {skewed}}).disposition,
        CommitDisposition::Committed);
    const auto cap = Options_.Writer.MaxRowGroupBytes;
    for (const auto* collection : {test::collection_id, SecondCollection}) {
        const auto files = FilesIn(Root_ / "warehouse" / collection / "data");
        ASSERT_EQ(files.size(), 1U);
        const auto footer = ReadFooter(files.front());
        EXPECT_EQ(footer.GroupRows, std::vector<int64_t>{10000}) << collection;
        ExpectGroupsWithin(collection == SecondCollection ? SkewedRows() : UniformRows(), footer.GroupRows, cap);
        // Previously 278,488 footer bytes for the skewed file.
        EXPECT_LT(footer.FooterBytes, 4096) << collection;
    }
}

// Exact rows, order, duplicates, NULLs and manifest statistics survive scan,
// reopen and an old-operation retry with a multi-group adaptive layout.
TEST_F(RowGroupLayoutTest, MixedLayoutSurvivesScanReopenAndRetry)
{
    Options_.Writer.MaxRowGroupBytes = 64 << 10;
    ASSERT_NO_FATAL_FAILURE(Create());
    const auto rows = MixedRows();
    const auto batch = PayloadBatch(rows);
    const AppendRequest request{test::collection_id, FirstOperation, {}, {batch->Slice(0, 1000), batch->Slice(1000)}};
    const auto committed = Engine_->AppendDurable(request);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed)
        << (committed.error ? committed.error->message : std::string());

    const auto footer = ReadFooter(OnlyDataFile());
    // The widest-row rule produced one group per row here.
    EXPECT_GE(footer.GroupRows.size(), 2U);
    EXPECT_LE(footer.GroupRows.size(), 4U);
    ExpectGroupsWithin(rows, footer.GroupRows, Options_.Writer.MaxRowGroupBytes);

    const auto expected = Render(*batch);
    EXPECT_EQ(ScanInOrder(), expected);

    const auto files = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(files) << files.error().message;
    ASSERT_EQ(files->Files.size(), 1U);
    const auto& file = files->Files.front();
    EXPECT_EQ(file.RecordCount, 3000);
    ASSERT_EQ(file.FieldMetrics.size(), 2U);
    EXPECT_EQ(file.FieldMetrics[0].ValueCount, 3000);
    EXPECT_EQ(file.FieldMetrics[0].NullCount, 0);
    const auto bytes = [](std::string_view text) {
        return std::vector<uint8_t>(text.begin(), text.end());
    };
    EXPECT_EQ(file.FieldMetrics[0].LowerBound, bytes("dup-0"));
    EXPECT_EQ(file.FieldMetrics[0].UpperBound, bytes("dup-6"));
    EXPECT_EQ(file.FieldMetrics[1].ValueCount, 3000);
    EXPECT_EQ(file.FieldMetrics[1].NullCount, std::ranges::count_if(rows, [](const auto& row) {
        return !row.Payload;
    }));
    const auto details = Engine_->GetFileDetails(test::collection_id, file.FileID);
    ASSERT_TRUE(details) << details.error().message;
    ASSERT_EQ(details->RowGroups.size(), footer.GroupRows.size());
    for (size_t group = 0; group < footer.GroupRows.size(); ++group) {
        EXPECT_EQ(details->RowGroups[group].RecordCount, footer.GroupRows[group]);
    }

    EXPECT_EQ(Engine_->AppendDurable(request), committed);
    ASSERT_NO_FATAL_FAILURE(Reopen());
    EXPECT_EQ(Engine_->AppendDurable(request), committed);
    EXPECT_EQ(ScanInOrder(), expected);
    const auto reopened = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(reopened);
    EXPECT_EQ(reopened->Files, files->Files);
    EXPECT_EQ(FilesIn(Root_ / "warehouse" / test::collection_id / "data").size(), 1U);
}

// The backend admission and writer still reject a row whose estimate plus
// string offsets exceeds the bound, without binding the operation.
TEST_F(RowGroupLayoutTest, RowOverTheBoundIsStillRefused)
{
    std::vector<TPayloadRow> rows{{"a", "x"}, {"b", std::string(1000, 'y')}};
    Options_.Writer.MaxRowGroupBytes = RowBytes(rows[1]) + 8 - 1;
    ASSERT_NO_FATAL_FAILURE(Create());
    const auto refused = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {PayloadBatch(rows)}});
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(refused.phase, CommitPhase::Validation);
    const auto found = Engine_->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(found);
    EXPECT_FALSE(found->has_value());
    rows[1].Payload->pop_back();
    const auto committed = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {PayloadBatch(rows)}});
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    EXPECT_EQ(ReadFooter(OnlyDataFile()).GroupRows, (std::vector<int64_t>{1, 1}));
}

// Manual measurement, excluded from CTest. Run with
//   glamdb_vertical --gtest_also_run_disabled_tests --gtest_filter='*RowGroupLayoutMeasurement*'
// GLAMDB_BENCH_RUNS sets the repetitions (default 7).
TEST(RowGroupLayoutMeasurement, DISABLED_AppendAndFullScan)
{
    const auto* env = std::getenv("GLAMDB_BENCH_RUNS");
    const int runs = env ? std::atoi(env) : 7;
    std::vector<TInputRow> ordinary;
    for (int64_t row = 0; row < 10000; ++row) {
        ordinary.push_back({"key-" + std::to_string(row % 4000),
            row % 3 == 0 ? std::nullopt : std::optional<int64_t>(row),
            row % 7 == 0 ? std::nullopt : std::optional<double>(row * 0.5),
            row % 2 == 0,
            row % 11 == 0 ? std::nullopt : std::optional<int64_t>(row * 1000)});
    }
    std::vector<TPayloadRow> wide;
    for (size_t row = 0; row < 10000; ++row) {
        wide.push_back({Key(row), std::string(700 + (row * 7919) % 300, static_cast<char>('a' + row % 26))});
    }
    struct TFixture
    {
        const char* Name;
        SchemaSpec Schema;
        std::shared_ptr<arrow::RecordBatch> Batch;
    };
    const std::vector<TFixture> fixtures{{"uniform", PayloadSchema, PayloadBatch(UniformRows())},
        {"skewed", PayloadSchema, PayloadBatch(SkewedRows())},
        {"ordinary", Schema, Batch(ordinary)},
        {"wide-varied", PayloadSchema, PayloadBatch(wide)}};
    const auto median = [](std::vector<double> values) {
        std::ranges::sort(values);
        return values[values.size() / 2];
    };
    for (const auto& fixture : fixtures) {
        std::vector<double> writes;
        std::vector<double> scans;
        TFooter footer;
        for (int run = 0; run < runs; ++run) {
            test::TempDir temporary;
            const auto root = temporary.path / "store";
            auto created = CreateStore({test::store_id, root.string()});
            ASSERT_TRUE(created) << created.error().message;
            auto engine = *created;
            ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", fixture.Schema}).disposition,
                CommitDisposition::Committed);
            const auto started = std::chrono::steady_clock::now();
            const auto outcome = engine->AppendDurable({test::collection_id, FirstOperation, {}, {fixture.Batch}});
            const auto written = std::chrono::steady_clock::now();
            ASSERT_EQ(outcome.disposition, CommitDisposition::Committed);
            auto cursor = engine->Scan({.collection_id = test::collection_id});
            ASSERT_TRUE(cursor);
            int64_t scanned = 0;
            while (true) {
                const auto batch = (*cursor)->Next();
                ASSERT_TRUE(batch);
                if (!*batch) {
                    break;
                }
                scanned += (*batch)->num_rows();
            }
            const auto finished = std::chrono::steady_clock::now();
            ASSERT_EQ(scanned, fixture.Batch->num_rows());
            writes.push_back(std::chrono::duration<double, std::milli>(written - started).count());
            scans.push_back(std::chrono::duration<double, std::milli>(finished - written).count());
            footer = ReadFooter(FilesIn(root / "warehouse" / test::collection_id / "data").front());
            ASSERT_TRUE(engine->Shutdown(1000ms));
        }
        std::string samples;
        for (int run = 0; run < runs; ++run) {
            samples += std::format(" {:.1f}/{:.1f}", writes[run], scans[run]);
        }
        std::printf("MEASURE fixture=%s rows=%lld arrow_bytes=%lld groups=%zu group0_rows=%lld footer_bytes=%lld "
                    "file_bytes=%lld write_ms_median=%.2f scan_ms_median=%.2f runs=%d samples(write/scan ms):%s\n",
            fixture.Name,
            static_cast<long long>(fixture.Batch->num_rows()),
            static_cast<long long>(arrow::util::TotalBufferSize(*fixture.Batch)),
            footer.GroupRows.size(),
            static_cast<long long>(footer.GroupRows.front()),
            static_cast<long long>(footer.FooterBytes),
            static_cast<long long>(footer.FileBytes),
            median(writes),
            median(scans),
            runs,
            samples.c_str());
    }
}

}
}
