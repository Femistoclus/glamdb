#include "tests/vertical/vertical_support.h"

#include <chrono>

namespace glamdb {
namespace {

using namespace std::chrono_literals;

TEST(WideSchema, NativeAppendScanReopenAndRetryPreserve105Columns)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    SchemaSpec schema;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    constexpr int64_t FirstValue = 9007199254740993LL;
    for (size_t index = 0; index < 105; ++index) {
        const auto name = index == 0 ? "WatchID" : index == 104 ? "CLID" : "c" + std::to_string(index);
        schema.fields.push_back({name, FieldType::Int64, false});
        fields.push_back(arrow::field(name, arrow::int64(), false));
        arrow::Int64Builder builder;
        ASSERT_TRUE(builder.Append(FirstValue + static_cast<int64_t>(index)).ok());
        ASSERT_TRUE(builder.Append(FirstValue + static_cast<int64_t>(index)).ok());
        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        columns.push_back(std::move(array));
    }
    const auto input = arrow::RecordBatch::Make(arrow::schema(fields), 2, columns);
    auto createdStore = CreateStore({test::store_id, root.string()});
    ASSERT_TRUE(createdStore) << createdStore.error().message;
    auto engine = std::move(*createdStore);
    const auto collection = engine->CreateCollection({test::collection_id, "hits", schema});
    ASSERT_EQ(collection.disposition, CommitDisposition::Committed)
        << (collection.error ? collection.error->message : std::string());
    const auto appended = engine->AppendDurable({test::collection_id, vertical::FirstOperation, {}, {input}});
    ASSERT_EQ(appended.disposition, CommitDisposition::Committed)
        << (appended.error ? appended.error->message : std::string());
    ASSERT_TRUE(appended.result);
    const auto snapshot = appended.result->snapshot_id;

    const auto check = [&](bool projected) {
        ScanRequest request;
        request.collection_id = test::collection_id;
        request.selector = SnapshotSelector::Id(snapshot);
        request.batch_rows = 1;
        if (projected) {
            request.projection = std::vector<std::string>{"CLID", "WatchID"};
            request.predicate = Predicate::Compare("CLID", CompareOp::Eq, FirstValue + 104);
        }
        auto opened = engine->Scan(request);
        ASSERT_TRUE(opened) << opened.error().message;
        auto cursor = std::move(*opened);
        int64_t count = 0;
        while (true) {
            auto next = cursor->Next();
            ASSERT_TRUE(next) << next.error().message;
            if (!*next) {
                break;
            }
            const auto& batch = **next;
            ASSERT_EQ(batch.num_columns(), projected ? 2 : 105);
            for (int column = 0; column < batch.num_columns(); ++column) {
                const auto source = projected ? (column == 0 ? 104 : 0) : column;
                const auto values = std::static_pointer_cast<arrow::Int64Array>(batch.column(column));
                for (int64_t row = 0; row < batch.num_rows(); ++row) {
                    EXPECT_EQ(values->Value(row), FirstValue + source);
                }
            }
            count += batch.num_rows();
        }
        EXPECT_EQ(count, 2);
    };
    check(false);
    check(true);
    ASSERT_TRUE(engine->Shutdown(5s));
    engine.reset();
    auto reopened = OpenExisting({root.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    engine = std::move(*reopened);
    check(false);
    check(true);
    const auto retry = engine->AppendDurable({test::collection_id, vertical::FirstOperation, {}, {input}});
    ASSERT_EQ(retry.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(retry.result);
    EXPECT_EQ(retry.result->snapshot_id, snapshot);
    check(false);
    ASSERT_TRUE(engine->Shutdown(5s));
}

} // namespace
} // namespace glamdb
