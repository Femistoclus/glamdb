#include "core/api/codec.h"
#include "storage/write_session.h"
#include "tests/support/staging_fault_vfs.h"
#include "tests/vertical/storage_fault_support.h"
#include <cstdlib>

namespace glamdb {
namespace {
using namespace vertical;
TWriteSessions::TNext ProcessRows()
{
    return [done = false]() mutable -> Result<std::shared_ptr<arrow::RecordBatch>> {
        if (std::exchange(done, true)) {
            return nullptr;
        }
        return Batch({{"restart", 7, 0.5, true, 10}, {"restart", 7, 0.5, true, 10}});
    };
}
TEST(WriteSessionChild, Step)
{
    const auto* root = std::getenv("GLAMDB_STORAGE_FAULT_ROOT");
    const auto* setting = std::getenv("GLAMDB_STORAGE_FAULT_COMMITTED");
    if (!root || !setting) {
        GTEST_SKIP();
    }
    const std::string mode(setting);
    test::JournalFaultVfs vfs;
    const auto factory = [](const std::filesystem::path& path, const StoreInfo& store) {
        return detail::TLocalStorageBackend::Open(path, store);
    };
    auto engine = detail::EngineFactory::OpenWith({root, test::store_id}, factory, vfs.Name());
    ASSERT_TRUE(engine) << engine.error().message;
    test::TStagingFaultVfs stageFault;
    auto opened =
        TWriteSessions::Open(*engine, (std::filesystem::path(root).parent_path() / "stage").string(), "alice");
    ASSERT_TRUE(opened) << opened.error().message;
    const auto sessions = *opened;
    const TWriteIdentity id{test::collection_id, FirstOperation};
    if (mode == "begin") {
        ASSERT_TRUE(sessions->BeginWrite({id}));
    } else if (mode == "upload-crash") {
        auto status = sessions->GetWriteStatus(id);
        ASSERT_TRUE(status);
        sessions->PutPart(id, {0, 0, 0}, [first = true]() mutable -> Result<std::shared_ptr<arrow::RecordBatch>> {
            if (std::exchange(first, false)) {
                return Batch({{"partial", 99, {}, {}, {}}});
            }
            std::_Exit(73);
        });
        FAIL() << "upload must exit inside active SQLite transaction";
    } else if (mode == "part") {
        auto part = sessions->GetPartReceipt(id, {0, 0, 0});
        ASSERT_TRUE(part);
        EXPECT_FALSE(*part);
        ASSERT_TRUE(sessions->PutPart(id, {0, 0, 0}, ProcessRows()));
    } else if (mode == "seal" || mode == "seal-crash") {
        auto part = sessions->GetPartReceipt(id, {0, 0, 0});
        ASSERT_TRUE(part);
        ASSERT_TRUE(*part);
        EXPECT_EQ((*part)->Rows, 2);
        if (mode == "seal-crash") {
            stageFault.CrashAfterNextJournalSync();
        }
        ASSERT_TRUE(sessions->SealWrite(id, {{0, 0, 0}}));
    } else if (mode.starts_with("publish")) {
        const bool after = mode == "publish-after";
        vfs.Arm(2, after);
        auto outcome = sessions->CommitWrite(id);
        ASSERT_TRUE(outcome);
        EXPECT_EQ(outcome->disposition, CommitDisposition::CommitUnknown);
        EXPECT_EQ(vfs.FailureCount(), 1);
        EXPECT_FALSE(sessions->AbortWrite(id));
        ASSERT_TRUE(sessions->Cleanup());
        EXPECT_EQ(sessions->GetWriteStatus(id)->State, EWriteState::CommitStarted);
    } else if (mode == "commit") {
        auto outcome = sessions->CommitWrite(id);
        ASSERT_TRUE(outcome);
        EXPECT_EQ(outcome->disposition, CommitDisposition::Committed);
    } else if (mode == "resolve-committed" || mode == "resolve-failed") {
        auto status = sessions->GetWriteStatus(id);
        ASSERT_TRUE(status);
        EXPECT_EQ(status->Rows, 2);
        auto outcome = sessions->GetWriteOutcome(id);
        ASSERT_TRUE(outcome);
        ASSERT_TRUE(*outcome);
        const bool committed = mode == "resolve-committed";
        EXPECT_EQ((*outcome)->disposition,
            committed ? CommitDisposition::Committed : CommitDisposition::DefinitelyNotCommitted);
        const auto retry = sessions->CommitWrite(id);
        ASSERT_TRUE(retry);
        EXPECT_EQ(*retry, **outcome);
        ASSERT_TRUE(sessions->Cleanup());
        auto final = sessions->CommitWrite(id);
        ASSERT_TRUE(final);
        EXPECT_EQ(*final, **outcome);
        auto cursor = (*engine)->Scan({.collection_id = test::collection_id});
        ASSERT_TRUE(cursor);
        auto batch = (*cursor)->Next();
        ASSERT_TRUE(batch);
        if (committed) {
            ASSERT_TRUE(*batch);
            EXPECT_EQ(Render(**batch),
                (std::vector<std::string>{"restart|7|0.500000|true|ts:10", "restart|7|0.500000|true|ts:10"}));
        } else {
            EXPECT_FALSE(*batch);
        }
    }
    std::_Exit(::testing::Test::HasFailure() ? 74 : 73);
}
TEST(WriteSessionBoundaryChild, Step)
{
    const auto* root = std::getenv("GLAMDB_STORAGE_FAULT_ROOT");
    const auto* setting = std::getenv("GLAMDB_STORAGE_FAULT_COMMITTED");
    if (!root || !setting) {
        GTEST_SKIP();
    }
    const std::string mode(setting);
    const std::string value((1 << 20) - 31, 'x');
    auto engine = OpenExisting({root, test::store_id});
    ASSERT_TRUE(engine) << engine.error().message;
    auto opened =
        TWriteSessions::Open(*engine, (std::filesystem::path(root).parent_path() / "stage").string(), "boundary");
    ASSERT_TRUE(opened) << opened.error().message;
    const auto sessions = *opened;
    const TWriteIdentity id{test::collection_id, FirstOperation};
    if (mode == "put") {
        ASSERT_TRUE(sessions->BeginWrite({id}));
        arrow::StringBuilder keys, values;
        for (int row = 0; row < 16; ++row) {
            ASSERT_TRUE(keys.Append("k").ok());
            ASSERT_TRUE(values.Append(value).ok());
        }
        auto keyArray = keys.Finish();
        auto valueArray = values.Finish();
        ASSERT_TRUE(keyArray.ok());
        ASSERT_TRUE(valueArray.ok());
        auto batch = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("key", arrow::utf8(), false), arrow::field("value", arrow::utf8(), false)}),
            16,
            {*keyArray, *valueArray});
        auto part = sessions->PutPart(id,
            {0, 0, 0},
            [batch = std::move(batch)]() mutable -> Result<std::shared_ptr<arrow::RecordBatch>> {
                return std::exchange(batch, nullptr);
            });
        ASSERT_TRUE(part) << part.error().message;
        EXPECT_EQ(part->Rows, 16);
    } else if (mode == "seal") {
        auto part = sessions->GetPartReceipt(id, {0, 0, 0});
        ASSERT_TRUE(part);
        ASSERT_TRUE(*part);
        EXPECT_EQ((*part)->Rows, 16);
        auto sealed = sessions->SealWrite(id, {{0, 0, 0}});
        ASSERT_TRUE(sealed) << sealed.error().code << ": " << sealed.error().message;
        EXPECT_EQ(sealed->Rows, 16);
    } else {
        auto committed = sessions->CommitWrite(id);
        ASSERT_TRUE(committed) << committed.error().message;
        ASSERT_EQ(committed->disposition, CommitDisposition::Committed);
        const auto receiptPath = std::filesystem::path(root).parent_path() / "boundary.outcome";
        if (mode == "commit") {
            ASSERT_NO_FATAL_FAILURE(SaveOutcome(receiptPath, *committed));
        } else {
            auto encoded = detail::EncodeOutcome(*committed);
            ASSERT_TRUE(encoded);
            EXPECT_EQ(*encoded, ReadBytes(receiptPath));
            ASSERT_TRUE(sessions->Cleanup());
            auto replay = sessions->CommitWrite(id);
            ASSERT_TRUE(replay);
            EXPECT_EQ(*replay, *committed);
        }
        auto cursor = (*engine)->Scan({.collection_id = test::collection_id});
        ASSERT_TRUE(cursor);
        int64_t rows = 0;
        for (;;) {
            auto next = (*cursor)->Next();
            ASSERT_TRUE(next);
            if (!*next) {
                break;
            }
            const auto keys = std::static_pointer_cast<arrow::StringArray>((*next)->column(0));
            const auto values = std::static_pointer_cast<arrow::StringArray>((*next)->column(1));
            for (int64_t row = 0; row < (*next)->num_rows(); ++row) {
                EXPECT_EQ(keys->GetView(row), "k");
                EXPECT_EQ(values->GetView(row), value);
            }
            rows += (*next)->num_rows();
        }
        EXPECT_EQ(rows, 16);
    }
}
TEST(WriteSessionProcess, NearLimitAcknowledgedPartSealsCommitsAndReplaysInFreshProcesses)
{
    test::TempDir temp;
    const auto root = temp.path / "store";
    {
        auto engine = CreateStore({test::store_id, root.string()});
        ASSERT_TRUE(engine);
        const SchemaSpec schema{{{"key", FieldType::String, false}, {"value", FieldType::String, false}}};
        ASSERT_EQ((*engine)->CreateCollection({test::collection_id, "events", schema}).disposition,
            CommitDisposition::Committed);
    }
    for (const auto mode : {"put", "seal", "commit", "replay"}) {
        ASSERT_NO_FATAL_FAILURE(RunChild(root, {.Test = "WriteSessionBoundaryChild.Step", .State = mode}));
    }
}
TEST(WriteSessionProcess, FreshProcessesRecoverPartialUploadAndLostBeginPartSealCommitReplies)
{
    test::TempDir temp;
    const auto root = temp.path / "store";
    {
        auto engine = CreateStore({test::store_id, root.string()});
        ASSERT_TRUE(engine);
        ASSERT_EQ((*engine)->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }
    for (const auto mode :
        {"begin", "upload-crash", "part", "seal-crash", "seal", "commit", "resolve-committed", "resolve-committed"})
    {
        ASSERT_NO_FATAL_FAILURE(RunChild(root, {.Test = "WriteSessionChild.Step", .State = mode, .ExpectedExit = 73}));
    }
}
TEST(WriteSessionProcess, FreshProcessesResolveActualCatalogPublicationBeforeAndAfterUnlink)
{
    for (const auto mode : {"publish-before", "publish-after"}) {
        test::TempDir temp;
        const auto root = temp.path / "store";
        {
            auto engine = CreateStore({test::store_id, root.string()});
            ASSERT_TRUE(engine);
            ASSERT_EQ((*engine)->CreateCollection({test::collection_id, "events", Schema}).disposition,
                CommitDisposition::Committed);
        }
        for (const auto step : {"begin", "part", "seal", mode}) {
            ASSERT_NO_FATAL_FAILURE(
                RunChild(root, {.Test = "WriteSessionChild.Step", .State = step, .ExpectedExit = 73}));
        }
        ASSERT_NO_FATAL_FAILURE(RunChild(root,
            {.Test = "WriteSessionChild.Step",
                .State = std::string_view(mode) == "publish-after" ? "resolve-committed" : "resolve-failed",
                .ExpectedExit = 73}));
    }
}
}
}
