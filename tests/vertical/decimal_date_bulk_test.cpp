#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include <array>

namespace glamdb {
namespace {

class TNativeScalarBulkSource final : public BulkAppendSource
{
public:
    explicit TNativeScalarBulkSource(std::shared_ptr<arrow::RecordBatch> batch)
        : Batch_(std::move(batch))
    { }

    int64_t ChunkRows = 997;

    Status Rewind() override
    {
        Position_ = 0;
        return {};
    }

    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        if (Position_ == Batch_->num_rows()) {
            return nullptr;
        }
        const auto count = std::min(ChunkRows, Batch_->num_rows() - Position_);
        auto batch = Batch_->Slice(Position_, count);
        Position_ += count;
        return batch;
    }

private:
    std::shared_ptr<arrow::RecordBatch> Batch_;
    int64_t Position_ = 0;
};

TEST(TDecimalDateBulkTest, ExactValuesAndReceiptsSurviveMultipleFilesRechunkingAndReopen)
{
    using namespace std::chrono_literals;
    constexpr int64_t rows = 100'004;
    constexpr std::array<const char*, 3> amounts{"-999999999999999999999999999999999999.99",
        "999999999999999999999999999999999999.99",
        "0.00"};
    constexpr std::array<int32_t, 3> days{-719162, 2932896, 0};
    std::vector<std::optional<arrow::Decimal128>> decimals;
    std::vector<std::optional<int32_t>> dates;
    for (int64_t row = 0; row < rows; ++row) {
        const auto index = static_cast<size_t>(row % 4);
        if (index == 3) {
            decimals.emplace_back(std::nullopt);
            dates.emplace_back(std::nullopt);
        } else {
            const auto parsed = arrow::Decimal128::FromString(amounts[index]);
            ASSERT_TRUE(parsed.ok());
            decimals.emplace_back(*parsed);
            dates.emplace_back(days[index]);
        }
    }
    const auto decimalType = arrow::decimal128(38, 2);
    const auto batch = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("amount", decimalType), arrow::field("day", arrow::date32())}),
        rows,
        {vertical::Column<arrow::Decimal128Builder>(decimals, decimalType),
            vertical::Column<arrow::Date32Builder>(dates, arrow::date32())});
    const auto source = std::make_shared<TNativeScalarBulkSource>(batch);
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto created = CreateStore({test::store_id, root.string()});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    const SchemaSpec schema{{{"amount", FieldType::Decimal, true, 38, 2}, {"day", FieldType::Date, true}}};
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "native_bulk", schema}).disposition,
        CommitDisposition::Committed);
    const BulkAppendRequest request{.collection_id = test::collection_id,
        .operation_id = vertical::FirstOperation,
        .rows = rows,
        .source = source};
    const auto committed = engine->BulkAppendDurable(request);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed)
        << (committed.error ? test::Present(committed.error).code + ": " + test::Present(committed.error).message
                            : std::string());
    ASSERT_TRUE(committed.result);
    EXPECT_EQ(test::Present(committed.result).receipt_version, 2);
    EXPECT_EQ(test::Present(committed.result).rows_appended, rows);
    EXPECT_EQ(test::Present(committed.result).data_files_added, 2);
    source->ChunkRows = 777;
    EXPECT_EQ(engine->BulkAppendDurable(request), committed);
    ASSERT_TRUE(engine->Shutdown(1000ms));
    engine.reset();
    const auto reopened = OpenExisting({root.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    engine = *reopened;
    EXPECT_EQ(engine->BulkAppendDurable(request), committed);
    EXPECT_EQ(*engine->FindAppendOutcome(test::collection_id, vertical::FirstOperation), committed);
    const auto cursor = engine->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor) << cursor.error().message;
    int64_t seen = 0;
    std::array<int64_t, 4> occurrences{};
    for (;;) {
        const auto next = (*cursor)->Next();
        ASSERT_TRUE(next) << next.error().message;
        if (!*next) {
            break;
        }
        ASSERT_TRUE((*next)->column(0)->type()->Equals(*decimalType));
        ASSERT_TRUE((*next)->column(1)->type()->Equals(*arrow::date32()));
        const auto& values = static_cast<const arrow::Decimal128Array&>(*(*next)->column(0));
        const auto& calendar = static_cast<const arrow::Date32Array&>(*(*next)->column(1));
        for (int64_t row = 0; row < (*next)->num_rows(); ++row) {
            if (values.IsNull(row)) {
                ASSERT_TRUE(calendar.IsNull(row));
                ++occurrences[3];
            } else {
                ASSERT_FALSE(calendar.IsNull(row));
                const auto found = std::ranges::find(amounts, values.FormatValue(row));
                ASSERT_NE(found, amounts.end());
                const auto index = static_cast<size_t>(std::distance(amounts.begin(), found));
                EXPECT_EQ(calendar.Value(row), days[index]);
                ++occurrences[index];
            }
        }
        seen += (*next)->num_rows();
    }
    EXPECT_EQ(seen, rows);
    for (const auto count : occurrences) {
        EXPECT_EQ(count, rows / 4);
    }
    EXPECT_EQ(engine->ListSnapshots(test::collection_id)->snapshots.size(), 1);
    EXPECT_TRUE(engine->Shutdown(1000ms));
}

} // namespace
} // namespace glamdb
