#include "core/api/codec.h"
#include "storage/types.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

namespace glamdb {
namespace {

TEST(KVContracts, VersionThreeNoOpRoundTripsWithoutSnapshot)
{
    AppendResult result;
    result.collection_id = test::collection_id;
    result.operation_id = test::operation_id;
    result.payload_hash = std::string(64, 'a');
    result.durability_profile = "local-posix-v1";
    result.receipt_version = 3;
    result.mutation_kind = MutationKind::Delete;
    result.snapshot_created = false;
    result.input_rows = 3;
    const AppendOutcome outcome{test::operation_id,
        CommitDisposition::Committed,
        CommitPhase::Complete,
        result,
        std::nullopt,
        std::nullopt};
    ASSERT_TRUE(detail::ValidateOutcome(outcome));
    const auto encoded = detail::EncodeOutcome(outcome);
    ASSERT_TRUE(encoded);
    const auto decoded = detail::DecodeOutcome(*encoded);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(*decoded, outcome);
}

TEST(KVContracts, VersionThreeNoOpRejectsPhysicalChanges)
{
    AppendResult result;
    result.collection_id = test::collection_id;
    result.operation_id = test::operation_id;
    result.payload_hash = std::string(64, 'a');
    result.durability_profile = "local-posix-v1";
    result.receipt_version = 3;
    result.mutation_kind = MutationKind::Delete;
    result.snapshot_created = false;
    result.input_rows = 1;
    result.data_files_added = 1;
    const AppendOutcome outcome{test::operation_id,
        CommitDisposition::Committed,
        CommitPhase::Complete,
        result,
        std::nullopt,
        std::nullopt};
    EXPECT_FALSE(detail::ValidateOutcome(outcome));
}

TEST(KVContracts, UpsertCannotLoseRowsOrInventMoreInsertsThanAffectedKeys)
{
    AppendResult result;
    result.collection_id = test::collection_id;
    result.operation_id = test::operation_id;
    result.payload_hash = std::string(64, 'a');
    result.durability_profile = "local-posix-v1";
    result.receipt_version = 3;
    result.mutation_kind = MutationKind::Upsert;
    result.snapshot_id = 42;
    result.parent_snapshot_id = 41;
    result.snapshot_created_at_us = 1000;
    result.input_rows = 1;
    result.rows_affected = 1;
    result.rows_written = 1;
    result.rows_removed = 2;
    result.data_files_added = 1;
    result.data_files_removed = 1;
    result.compressed_data_bytes_added = 512;
    result.compressed_data_bytes_removed = 512;
    AppendOutcome outcome{test::operation_id,
        CommitDisposition::Committed,
        CommitPhase::Complete,
        result,
        std::nullopt,
        std::nullopt};
    EXPECT_FALSE(detail::ValidateOutcome(outcome));
    outcome.result.value().rows_written = 4;
    outcome.result.value().rows_removed = 1;
    EXPECT_FALSE(detail::ValidateOutcome(outcome));
}

TEST(KVContracts, CompactionNoOpCannotClaimInputRows)
{
    AppendResult result;
    result.collection_id = test::collection_id;
    result.operation_id = test::operation_id;
    result.payload_hash = std::string(64, 'a');
    result.durability_profile = "local-posix-v1";
    result.receipt_version = 3;
    result.mutation_kind = MutationKind::Compact;
    result.snapshot_created = false;
    AppendOutcome outcome{test::operation_id,
        CommitDisposition::Committed,
        CommitPhase::Complete,
        result,
        std::nullopt,
        std::nullopt};
    EXPECT_TRUE(detail::ValidateOutcome(outcome));
    outcome.result.value().input_rows = 1;
    EXPECT_FALSE(detail::ValidateOutcome(outcome));
}

TEST(KVContracts, AppendModeRemainsDefault)
{
    EXPECT_EQ(CreateCollectionRequest{}.mode, CollectionMode::Append);
    EXPECT_EQ(CollectionInfo{}.mode, CollectionMode::Append);
}

} // namespace
} // namespace glamdb
