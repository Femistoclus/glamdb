// A different-content retry of a bound operation ID is a plain 409 error on
// the wire. It must not carry a terminal outcome for the original operation,
// which may be committed or still unknown.

#include "apps/glamdb/outcome_json.h"

#include "core/api/codec.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <string>
#include <string_view>

namespace glamdb {
namespace {

// Evaluates a SQLite JSON expression over ?1 = json and ?2 = path.
std::string JsonQuery(const std::string& json, std::string_view expression, const char* path)
{
    sqlite3* database = nullptr;
    EXPECT_EQ(sqlite3_open(":memory:", &database), SQLITE_OK);
    const std::string sql = "SELECT " + std::string(expression);
    sqlite3_stmt* statement = nullptr;
    EXPECT_EQ(sqlite3_prepare_v2(database, sql.c_str(), -1, &statement, nullptr), SQLITE_OK);
    EXPECT_EQ(sqlite3_bind_text(statement, 1, json.data(), static_cast<int>(json.size()), SQLITE_TRANSIENT), SQLITE_OK);
    EXPECT_EQ(sqlite3_bind_text(statement, 2, path, -1, SQLITE_TRANSIENT), SQLITE_OK);
    EXPECT_EQ(sqlite3_step(statement), SQLITE_ROW);
    const auto* value = sqlite3_column_text(statement, 0);
    std::string result = value != nullptr ? reinterpret_cast<const char*>(value) : "";
    EXPECT_EQ(sqlite3_finalize(statement), SQLITE_OK);
    EXPECT_EQ(sqlite3_close(database), SQLITE_OK);
    return result;
}

TEST(OutcomeJsonConflict, DifferentPayloadConflictDoesNotClaimTheOriginalOperationOutcome)
{
    const AppendOutcome conflict{test::operation_id,
        CommitDisposition::DefinitelyNotCommitted,
        CommitPhase::Validation,
        std::nullopt,
        ErrorDetail{"OPERATION_CONFLICT",
            "operation ID is permanently bound to different content",
            RetryHint::OperatorAction},
        std::nullopt};
    ASSERT_TRUE(detail::ValidateOutcome(conflict));
    const auto encoded = json_adapter::ToJson(conflict);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->status, 409);
    EXPECT_EQ(JsonQuery(encoded->body, "json_extract(?1, ?2)", "$.error.code"), "OPERATION_CONFLICT");
    // The conflicting request changed nothing; the original operation may be committed or unknown.
    // The wire must not carry a terminal outcome keyed by the original operation ID.
    EXPECT_EQ(JsonQuery(encoded->body, "json_type(?1, ?2)", "$.outcome"), "") << encoded->body;
}

} // namespace
} // namespace glamdb
