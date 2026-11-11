#include "apps/transport/codec.h"
#include "apps/transport/outcome_json.h"

#include "core/api/canonical.h"
#include "core/api/codec.h"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

namespace glamdb::NTransport {
namespace {

TJson Fixture()
{
    std::ifstream input(
        std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() / "fixtures/http/outcomes-v0.json");
    return TJson::parse(input);
}

std::string Unhex(const std::string& hex)
{
    std::string bytes;
    for (size_t index = 0; index < hex.size(); index += 2) {
        bytes.push_back(static_cast<char>(std::stoul(hex.substr(index, 2), nullptr, 16)));
    }
    return bytes;
}

// Construct native values independently of the expected response envelopes.
AppendOutcome NativeAppend(const TJson& fixture, const std::string& name)
{
    const std::string collection = fixture.at("collection_id");
    const std::string store = fixture.at("store_id");
    const std::string table = fixture.at("table_uuid");
    const auto& request = fixture.at("appends").at(name == "committed_b" ? "b" : "a");
    const std::string operation = request.at("operation_id");
    const std::string hash = request.at("payload_hash");
    if (name == "pre_binding_rejection" || name == "registered_failure") {
        const bool validation = name == "pre_binding_rejection";
        return {operation,
            CommitDisposition::DefinitelyNotCommitted,
            validation ? CommitPhase::Validation : CommitPhase::Data,
            std::nullopt,
            ErrorDetail{validation ? "INVALID_ARGUMENT" : "IO_ERROR",
                validation ? "input rejected before binding" : "data file sync failed",
                validation ? RetryHint::FixInput : RetryHint::OperatorAction},
            std::nullopt};
    }
    if (name == "unknown_publication" || name == "blocked_lookup") {
        const bool blocked = name == "blocked_lookup";
        const ErrorDetail error{blocked ? "RECOVERY_REQUIRED" : "IO_ERROR",
            blocked ? "authoritative lookup is blocked" : "outer COMMIT result lost",
            RetryHint::SameIdAfterReconcile};
        const auto prefix = "store://" + store + "/warehouse/" + collection + "/metadata/";
        const BaseIdentity base{store,
            collection,
            table,
            prefix + (blocked ? "b.json" : "initial.json"),
            blocked ? std::optional<int64_t>(9007199254740995LL) : std::nullopt,
            0,
            0};
        return {operation,
            CommitDisposition::CommitUnknown,
            blocked ? CommitPhase::Reconciling : CommitPhase::CatalogCommit,
            std::nullopt,
            error,
            RecoveryContext{store,
                collection,
                operation,
                hash,
                1,
                base,
                fixture.at("attempt_id"),
                blocked ? std::nullopt : std::optional<int64_t>(9007199254740993LL),
                blocked ? std::nullopt : std::optional<std::string>(prefix + "a.json"),
                error,
                !blocked,
                blocked}};
    }
    const bool second = name == "committed_b";
    return {operation,
        CommitDisposition::Committed,
        CommitPhase::Complete,
        AppendResult{collection,
            operation,
            hash,
            1,
            second ? 9007199254740995LL : 9007199254740993LL,
            second ? std::optional<int64_t>(9007199254740993LL) : std::nullopt,
            second ? 1 : 3,
            1,
            second ? 124 : 123,
            1767225600123000LL,
            "local-delete-extra-v1",
            1},
        std::nullopt,
        std::nullopt};
}

TEST(OutcomeConformance, NativeSerializationMatchesSharedLiteralEnvelopes)
{
    const auto fixture = Fixture();
    for (const auto& example : fixture.at("cases")) {
        const std::string name = example.at("name");
        SCOPED_TRACE(name);
        if (name == "lost_create") {
            const CreateOutcome outcome{CommitDisposition::CommitUnknown,
                CommitPhase::CatalogCommit,
                std::nullopt,
                ErrorDetail{"IO_ERROR", "create COMMIT result lost", RetryHint::SameIdAfterReconcile},
                CreateRecoveryContext{fixture.at("store_id"),
                    fixture.at("collection_id"),
                    fixture.at("exact_name"),
                    fixture.at("schema_fingerprint"),
                    fixture.at("attempt_id"),
                    fixture.at("table_uuid").get<std::string>(),
                    std::nullopt,
                    true}};
            const auto response = json_adapter::ToJson(outcome);
            ASSERT_TRUE(response);
            EXPECT_EQ(response->status, example.at("status"));
            EXPECT_EQ(TJson::parse(response->body), example.at("response"));
            continue;
        }
        const auto outcome = NativeAppend(fixture, name);
        const auto response = json_adapter::ToJson(outcome);
        ASSERT_TRUE(response);
        EXPECT_EQ(response->status, example.at("status"));
        EXPECT_EQ(TJson::parse(response->body), example.at("response"));
        if (outcome.disposition != CommitDisposition::CommitUnknown) {
            const auto stored = detail::EncodeOutcome(outcome);
            ASSERT_TRUE(stored);
            const auto recovered = detail::DecodeOutcome(*stored);
            ASSERT_TRUE(recovered);
            EXPECT_EQ(*recovered, outcome);
            EXPECT_EQ(json_adapter::ToJson(*recovered)->body, response->body);
        }
    }
}

TEST(OutcomeConformance, CanonicalHashesAndTypedRowsMatchIndependentPreimages)
{
    const auto fixture = Fixture();
    const auto schema = ParseSchema(fixture.at("schema"));
    const auto encoded = detail::EncodeSchema(schema);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(*encoded, Unhex(fixture.at("schema_canonical_hex")));
    EXPECT_EQ(detail::SchemaFingerprint(schema), fixture.at("schema_fingerprint").get<std::string>());
    for (const auto& request : fixture.at("appends")) {
        const auto batch = ParseRows(schema, request.at("rows"));
        EXPECT_EQ(EncodeRows(*batch), request.at("rows"));
        const auto hash = detail::FingerprintBatches(schema, fixture.at("collection_id").get<std::string>(), {batch});
        ASSERT_TRUE(hash);
        EXPECT_EQ(*hash, request.at("payload_hash"));
        EXPECT_EQ(detail::Sha256(Unhex(request.at("canonical_hex"))), request.at("payload_hash"));
    }
}

TEST(OutcomeConformance, IncompleteNativeRecoveryCannotBeSerialized)
{
    const auto fixture = Fixture();
    auto append = NativeAppend(fixture, "unknown_publication");
    append.recovery = RecoveryContext{};
    EXPECT_FALSE(json_adapter::ToJson(append));
    const CreateOutcome create{CommitDisposition::CommitUnknown,
        CommitPhase::CatalogCommit,
        std::nullopt,
        ErrorDetail{"IO_ERROR", "create COMMIT result lost", RetryHint::SameIdAfterReconcile},
        CreateRecoveryContext{}};
    EXPECT_FALSE(json_adapter::ToJson(create));
}

} // namespace
} // namespace glamdb::NTransport
