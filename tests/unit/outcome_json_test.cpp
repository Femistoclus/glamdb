#include "apps/transport/outcome_json.h"

#include "core/api/codec.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

namespace glamdb {
namespace {

struct JsonValue
{
    std::string type;
    std::string text;
};

JsonValue JsonAt(const std::string& json, const char* path)
{
    sqlite3* database = nullptr;
    EXPECT_EQ(sqlite3_open(":memory:", &database), SQLITE_OK);
    sqlite3_stmt* statement = nullptr;
    EXPECT_EQ(sqlite3_prepare_v2(database, "SELECT json_type(?1,?2),json_extract(?1,?2)", -1, &statement, nullptr),
        SQLITE_OK);
    EXPECT_EQ(sqlite3_bind_text(statement, 1, json.data(), static_cast<int>(json.size()), SQLITE_TRANSIENT), SQLITE_OK);
    EXPECT_EQ(sqlite3_bind_text(statement, 2, path, -1, SQLITE_TRANSIENT), SQLITE_OK);
    EXPECT_EQ(sqlite3_step(statement), SQLITE_ROW);
    const auto text = [&](int column) {
        const auto* value = sqlite3_column_text(statement, column);
        return value != nullptr ? std::string(reinterpret_cast<const char*>(value)) : std::string{};
    };
    const JsonValue value{text(0), text(1)};
    EXPECT_EQ(sqlite3_finalize(statement), SQLITE_OK);
    EXPECT_EQ(sqlite3_close(database), SQLITE_OK);
    return value;
}

AppendOutcome Committed()
{
    return {test::operation_id,
        CommitDisposition::Committed,
        CommitPhase::Complete,
        AppendResult{test::collection_id,
            test::operation_id,
            std::string(64, 'a'),
            1,
            9007199254740993LL,
            std::nullopt,
            3,
            1,
            123,
            1700000000000000LL,
            "local-delete-extra-v1",
            1},
        std::nullopt,
        std::nullopt};
}

TEST(OutcomeJson, CompleteReceiptPreservesIntegerPrecisionAndRecoveryEquality)
{
    const auto outcome = Committed();
    const auto encoded = json_adapter::ToJson(outcome);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->status, 200);
    EXPECT_EQ(JsonAt(encoded->body, "$.outcome").text, "committed");
    EXPECT_EQ(JsonAt(encoded->body, "$.phase").text, "complete");
    EXPECT_EQ(JsonAt(encoded->body, "$.result.snapshot_id").text, "9007199254740993");
    EXPECT_EQ(JsonAt(encoded->body, "$.result.snapshot_id").type, "text");
    EXPECT_EQ(JsonAt(encoded->body, "$.result.rows_appended").text, "3");
    EXPECT_EQ(JsonAt(encoded->body, "$.result.data_files_added").text, "1");
    EXPECT_EQ(JsonAt(encoded->body, "$.result.compressed_data_bytes_added").text, "123");
    EXPECT_EQ(JsonAt(encoded->body, "$.result.snapshot_created_at_us").text, "1700000000000000");
    EXPECT_EQ(JsonAt(encoded->body, "$.result.hash_version").type, "integer");
    EXPECT_EQ(JsonAt(encoded->body, "$.result.parent_snapshot_id").type, "null");
    const auto stored = detail::EncodeOutcome(outcome);
    ASSERT_TRUE(stored);
    const auto recovered = detail::DecodeOutcome(*stored);
    ASSERT_TRUE(recovered);
    EXPECT_EQ(json_adapter::ToJson(*recovered)->body, encoded->body);
}

TEST(OutcomeJson, UnknownIs503AndRetainsEntireRecoveryContext)
{
    const ErrorDetail first{"IO_ERROR", "directory sync \"failed\"\n", RetryHint::SameIdAfterReconcile};
    const BaseIdentity base{test::store_id,
        test::collection_id,
        test::table_uuid,
        "store://" + std::string(test::store_id) + "/warehouse/initial.json",
        std::nullopt,
        0,
        0};
    const RecoveryContext context{test::store_id,
        test::collection_id,
        test::operation_id,
        std::string(64, 'a'),
        1,
        base,
        test::attempt_id,
        9007199254740993LL,
        "store://" + std::string(test::store_id) + "/warehouse/next.json",
        first,
        true};
    const auto encoded = json_adapter::ToJson({test::operation_id,
        CommitDisposition::CommitUnknown,
        CommitPhase::CatalogCommit,
        std::nullopt,
        first,
        context});
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->status, 503);
    EXPECT_EQ(JsonAt(encoded->body, "$.outcome").text, "commit_unknown");
    EXPECT_EQ(JsonAt(encoded->body, "$.phase").text, "catalog_commit");
    EXPECT_EQ(JsonAt(encoded->body, "$.recovery_required").type, "true");
    EXPECT_EQ(JsonAt(encoded->body, "$.error.retry_hint").text, "SameIdAfterReconcile");
    EXPECT_EQ(JsonAt(encoded->body, "$.recovery.first_error.message").text, first.message);
    EXPECT_EQ(JsonAt(encoded->body, "$.recovery.candidate_snapshot_id").text, "9007199254740993");
    EXPECT_EQ(JsonAt(encoded->body, "$.recovery.publication_started").type, "true");
    EXPECT_EQ(JsonAt(encoded->body, "$.recovery.base.table_uuid").text, test::table_uuid);
    EXPECT_EQ(JsonAt(encoded->body, "$.recovery.base.current_snapshot_id").type, "null");
}

TEST(OutcomeJson, CollectionSuccessMatchesExistingUiShapeAndStableFieldIds)
{
    const CollectionInfo info{test::collection_id,
        "events",
        test::table_uuid,
        SchemaSpec{{{"n", FieldType::Int64, true}}},
        1,
        9007199254740993LL};
    const auto encoded = json_adapter::ToJson(
        CreateOutcome{CommitDisposition::Committed, CommitPhase::Complete, info, std::nullopt, std::nullopt});
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->status, 200);
    EXPECT_EQ(JsonAt(encoded->body, "$.collection_id").text, test::collection_id);
    EXPECT_EQ(JsonAt(encoded->body, "$.current_snapshot_id").text, "9007199254740993");
    EXPECT_EQ(JsonAt(encoded->body, "$.schema.fields[0].field_id").text, "1");
    EXPECT_EQ(JsonAt(encoded->body, "$.schema.fields[0].field_id").type, "integer");
    EXPECT_EQ(JsonAt(encoded->body, "$.mode").text, "append");
}

TEST(OutcomeJson, ExhaustedCapacityIs507WhileAdmissionStays429)
{
    const ErrorDetail capacity{"RESOURCE_EXHAUSTED",
        "storage preflight: No space left on device",
        RetryHint::OperatorAction};
    const ErrorDetail admission{"RESOURCE_EXHAUSTED", "pending budget is full", RetryHint::SameIdAfterReconcile};
    for (const auto& [error, status] : {std::pair{capacity, 507}, std::pair{admission, 429}}) {
        const auto created = json_adapter::ToJson(CreateOutcome{CommitDisposition::DefinitelyNotCommitted,
            CommitPhase::Validation,
            std::nullopt,
            error,
            std::nullopt});
        ASSERT_TRUE(created);
        EXPECT_EQ(created->status, status);
        EXPECT_EQ(JsonAt(created->body, "$.outcome").text, "definitely_not_committed");
        const auto appended = json_adapter::ToJson(AppendOutcome{test::operation_id,
            CommitDisposition::DefinitelyNotCommitted,
            CommitPhase::Validation,
            std::nullopt,
            error,
            std::nullopt});
        ASSERT_TRUE(appended);
        EXPECT_EQ(appended->status, status);
        EXPECT_EQ(JsonAt(appended->body, "$.error.code").text, "RESOURCE_EXHAUSTED");
        EXPECT_EQ(JsonAt(appended->body, "$.error.retry_hint").text,
            status == 507 ? "OperatorAction" : "SameIdAfterReconcile");
    }
}

TEST(OutcomeJson, InvalidRecoveryAndContradictoryCreateOutcomeAreRejected)
{
    const ErrorDetail error{"IO_ERROR", "uncertain", RetryHint::SameIdAfterReconcile};
    const AppendOutcome missing_identity{test::operation_id,
        CommitDisposition::CommitUnknown,
        CommitPhase::Binding,
        std::nullopt,
        error,
        RecoveryContext{}};
    EXPECT_FALSE(detail::ValidateOutcome(missing_identity));
    EXPECT_FALSE(json_adapter::ToJson(missing_identity));
    EXPECT_FALSE(json_adapter::ToJson(CreateOutcome{CommitDisposition::DefinitelyNotCommitted,
        CommitPhase::Complete,
        std::nullopt,
        error,
        CreateRecoveryContext{}}));
}

} // namespace
} // namespace glamdb
