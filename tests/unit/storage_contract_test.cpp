#include "storage/contract.h"
#include "storage/engine.h"

#include "core/engine/engine_factory.h"
#include "tests/support/fake_storage.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

namespace glamdb {
namespace {

EngineCapabilities LocalCapabilities()
{
    return {.backend = BackendKind::LocalStorage,
        .append = true,
        .scan = true,
        .metadata = true,
        .write_settings = true,
        .key_value = true,
        .storage_footprint = true};
}

TEST(StorageContract, SplitProfileRequiresNativeVersion12)
{
    TStorageContractRequest request;
    request.MaxVersion = {1, 2};
    request.RequiredFeatures = {EStorageFeature::ScanSplits};
    auto result = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_TRUE(result) << result.error().code;
    EXPECT_EQ(result->Version, (TStorageContractVersion{1, 2}));
    EXPECT_TRUE(result->HasOperation(EStorageOperation::PlanScan));
    EXPECT_TRUE(result->HasOperation(EStorageOperation::NextScanSplits));
    EXPECT_TRUE(result->HasOperation(EStorageOperation::ReadSplit));
    EXPECT_TRUE(result->HasOperation(EStorageOperation::CancelScanPlan));
    ASSERT_TRUE(result->EffectiveLimits.ScanSplits);
    request.Mode = CollectionMode::KeyValue;
    EXPECT_TRUE(NegotiateStorageContract(request, LocalCapabilities(), {}));
    request.Mode = CollectionMode::Append;

    request.MaxVersion = {1, 1};
    EXPECT_FALSE(NegotiateStorageContract(request, LocalCapabilities(), {}));
    request.MaxVersion = {1, 2};
    auto custom = LocalCapabilities();
    custom.backend = BackendKind::Custom;
    EXPECT_FALSE(NegotiateStorageContract(request, custom, {}));
}

TEST(StorageContract, SelectsSupportedVersionFromInclusiveRange)
{
    TStorageContractRequest request;
    request.MinVersion = {1, 0};
    request.MaxVersion = {3, 7};
    const auto result = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_TRUE(result);
    EXPECT_EQ(result->Version, (TStorageContractVersion{1, 2}));
    request.MaxVersion = {1, 0};
    ASSERT_TRUE(NegotiateStorageContract(request, LocalCapabilities(), {}));
}

TEST(StorageContract, ReadViewsAreAdditiveAndNativeOnly)
{
    TStorageContractRequest request;
    auto old = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_TRUE(old);
    EXPECT_EQ(old->Version, (TStorageContractVersion{1, 0}));
    EXPECT_FALSE(old->HasFeature(EStorageFeature::IndependentReadView));
    EXPECT_FALSE(old->HasOperation(EStorageOperation::AcquireReadView));
    EXPECT_FALSE(old->EffectiveLimits.ReadViews);
    request.RequiredFeatures = {EStorageFeature::IndependentReadView};
    ASSERT_FALSE(NegotiateStorageContract(request, LocalCapabilities(), {}));
    request.MaxVersion = {1, 1};
    auto latest = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_TRUE(latest) << latest.error().code;
    EXPECT_EQ(latest->Version, (TStorageContractVersion{1, 1}));
    EXPECT_TRUE(latest->HasOperation(EStorageOperation::AcquireReadView));
    EXPECT_TRUE(latest->HasOperation(EStorageOperation::ReadViewInfo));
    EXPECT_TRUE(latest->HasOperation(EStorageOperation::ScanReadView));
    EXPECT_TRUE(latest->HasOperation(EStorageOperation::RenewReadView));
    EXPECT_TRUE(latest->HasOperation(EStorageOperation::ReleaseReadView));
    EXPECT_TRUE(latest->HasOperation(EStorageOperation::CancelReadView));
    ASSERT_TRUE(latest->EffectiveLimits.ReadViews);
    EXPECT_EQ(latest->EffectiveLimits.ReadViews->MaxViews, 32U);
    EXPECT_EQ(latest->EffectiveLimits.ReadViews->MaxEntries, 256U);
    request.Mode = CollectionMode::KeyValue;
    EXPECT_TRUE(NegotiateStorageContract(request, LocalCapabilities(), {}));
    request.Mode = CollectionMode::Append;
    auto custom = LocalCapabilities();
    custom.backend = BackendKind::Custom;
    auto unavailable = NegotiateStorageContract(request, custom, {});
    ASSERT_FALSE(unavailable);
    EXPECT_EQ(unavailable.error().code, "UNSUPPORTED_FEATURE");
    request.RequiredFeatures.push_back(EStorageFeature::ScanSplits);
    unavailable = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_FALSE(unavailable);
    EXPECT_EQ(unavailable.error().code, "UNSUPPORTED_FEATURE");
}

TEST(StorageContract, RejectsDisjointVersionRangesAndMalformedRequests)
{
    for (const auto version :
        {TStorageContractVersion{0, 9}, TStorageContractVersion{1, 3}, TStorageContractVersion{2, 0}})
    {
        TStorageContractRequest request;
        request.MinVersion = version;
        request.MaxVersion = version;
        const auto result = NegotiateStorageContract(request, LocalCapabilities(), {});
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, "UNSUPPORTED_CONTRACT_VERSION");
        EXPECT_EQ(result.error().retry_hint, RetryHint::FixInput);
    }
    TStorageContractRequest request;
    request.MinVersion = {2, 0};
    request.MaxVersion = {1, 0};
    const auto reversed = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_FALSE(reversed);
    EXPECT_EQ(reversed.error().code, "INVALID_ARGUMENT");
    request = {};
    request.Mode = static_cast<CollectionMode>(99);
    const auto unknownMode = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_FALSE(unknownMode);
    EXPECT_EQ(unknownMode.error().code, "INVALID_ARGUMENT");
}

TEST(StorageContract, SeparatesAppendAndKeyValueOperations)
{
    TStorageContractRequest request;
    request.RequiredOperations = {EStorageOperation::Scan, EStorageOperation::Append, EStorageOperation::FindOutcome};
    request.RequiredFeatures = {EStorageFeature::TypedNulls, EStorageFeature::PreserveAppendDuplicates};
    const auto append = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_TRUE(append);
    EXPECT_EQ(append->Mode, CollectionMode::Append);
    EXPECT_TRUE(append->HasOperation(EStorageOperation::BulkAppend));
    EXPECT_TRUE(append->HasOperation(EStorageOperation::ExpireSnapshots));
    EXPECT_TRUE(append->HasOperation(EStorageOperation::MigrateAppendToKV));
    EXPECT_FALSE(append->HasOperation(EStorageOperation::Upsert));
    EXPECT_FALSE(append->HasOperation(EStorageOperation::Delete));
    EXPECT_FALSE(append->HasOperation(EStorageOperation::Lookup));
    EXPECT_FALSE(append->HasOperation(EStorageOperation::Compact));
    request.Mode = CollectionMode::KeyValue;
    request.RequiredOperations = {EStorageOperation::Scan, EStorageOperation::Upsert, EStorageOperation::FindOutcome};
    request.RequiredFeatures = {EStorageFeature::TypedNulls, EStorageFeature::DurableNoOp};
    const auto kv = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_TRUE(kv);
    EXPECT_TRUE(kv->HasOperation(EStorageOperation::BulkUpsert));
    EXPECT_TRUE(kv->HasOperation(EStorageOperation::Delete));
    EXPECT_TRUE(kv->HasOperation(EStorageOperation::Lookup));
    EXPECT_TRUE(kv->HasOperation(EStorageOperation::Compact));
    EXPECT_FALSE(kv->HasOperation(EStorageOperation::Append));
    EXPECT_FALSE(kv->HasOperation(EStorageOperation::BulkAppend));
    EXPECT_FALSE(kv->HasOperation(EStorageOperation::ExpireSnapshots));
    EXPECT_FALSE(kv->HasOperation(EStorageOperation::MigrateAppendToKV));
    EXPECT_FALSE(kv->HasFeature(EStorageFeature::PreserveAppendDuplicates));
    request.RequiredOperations = {EStorageOperation::Append};
    const auto wrongMode = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_FALSE(wrongMode);
    EXPECT_EQ(wrongMode.error().code, "UNSUPPORTED_OPERATION");
}

TEST(StorageContract, RejectsUnsupportedRequiredFeaturesAndUnknownEnumValues)
{
    for (const auto feature : {EStorageFeature::IndependentReadView,
             EStorageFeature::ScanSplits,
             EStorageFeature::StagedWrites,
             EStorageFeature::StableCAbi,
             EStorageFeature::ArrowFlight,
             EStorageFeature::RemoteDescriptors,
             EStorageFeature::Overwrite,
             EStorageFeature::Merge,
             EStorageFeature::SqlMutations,
             EStorageFeature::SchemaEvolution,
             EStorageFeature::CrossCollectionTransactions,
             static_cast<EStorageFeature>(65535)})
    {
        TStorageContractRequest request;
        request.RequiredFeatures = {feature};
        const auto result = NegotiateStorageContract(request, LocalCapabilities(), {});
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, "UNSUPPORTED_FEATURE");
        EXPECT_EQ(result.error().retry_hint, RetryHint::FixInput);
    }
    TStorageContractRequest request;
    request.RequiredOperations = {static_cast<EStorageOperation>(65535)};
    const auto unknown = NegotiateStorageContract(request, LocalCapabilities(), {});
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error().code, "UNSUPPORTED_OPERATION");
}

TEST(StorageContract, DerivesOperationsFromBackendSupport)
{
    const auto unavailable = NegotiateStorageContract({}, {}, {});
    ASSERT_TRUE(unavailable);
    EXPECT_TRUE(unavailable->HasOperation(EStorageOperation::CollectionMetadata));
    EXPECT_TRUE(unavailable->HasOperation(EStorageOperation::FindOutcome));
    EXPECT_FALSE(unavailable->HasOperation(EStorageOperation::CreateCollection));
    EXPECT_FALSE(unavailable->HasOperation(EStorageOperation::Scan));
    EXPECT_FALSE(unavailable->HasOperation(EStorageOperation::Append));
    EXPECT_FALSE(unavailable->HasFeature(EStorageFeature::TypedNulls));
    TStorageContractRequest request;
    request.RequiredOperations = {EStorageOperation::Scan};
    const auto rejected = NegotiateStorageContract(request, {}, {});
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, "UNSUPPORTED_OPERATION");
    auto capabilities = LocalCapabilities();
    capabilities.key_value = false;
    request = {};
    request.Mode = CollectionMode::KeyValue;
    const auto noKV = NegotiateStorageContract(request, capabilities, {});
    ASSERT_TRUE(noKV);
    EXPECT_FALSE(noKV->HasOperation(EStorageOperation::CreateCollection));
    EXPECT_FALSE(noKV->HasOperation(EStorageOperation::Upsert));
    EXPECT_FALSE(noKV->HasOperation(EStorageOperation::Scan));
    capabilities = LocalCapabilities();
    capabilities.append = false;
    capabilities.scan = false;
    const auto readDisabled = NegotiateStorageContract({}, capabilities, {});
    ASSERT_TRUE(readDisabled);
    EXPECT_FALSE(readDisabled->HasOperation(EStorageOperation::BulkAppend));
    EXPECT_FALSE(readDisabled->HasFeature(EStorageFeature::FixedSnapshotScan));
}

TEST(StorageContract, ReportsConfiguredEngineLimitsAndOperationCeilings)
{
    Limits limits;
    limits.max_collections = 16;
    limits.max_operations = 12345;
    limits.max_owned_input_bytes = 32LL << 20;
    limits.writer_wait = std::chrono::milliseconds(217);
    limits.history.max_snapshots = 321;
    const auto result = NegotiateStorageContract({}, LocalCapabilities(), limits);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->EffectiveLimits.Engine.max_collections, 16);
    EXPECT_EQ(result->EffectiveLimits.Engine.max_operations, 12345);
    EXPECT_EQ(result->EffectiveLimits.Engine.max_owned_input_bytes, 32LL << 20);
    EXPECT_EQ(result->EffectiveLimits.Engine.writer_wait, std::chrono::milliseconds(217));
    EXPECT_EQ(result->EffectiveLimits.Engine.history.max_snapshots, 321);
    EXPECT_EQ(result->EffectiveLimits.SchemaFields, 128);
    EXPECT_EQ(result->EffectiveLimits.AppendRows, 10000);
    EXPECT_EQ(result->EffectiveLimits.AppendDecodedBytes, 16LL << 20);
    EXPECT_EQ(result->EffectiveLimits.ScanBatchRows, 65536);
    EXPECT_EQ(result->EffectiveLimits.BulkRows, 25'600'000);
    EXPECT_EQ(result->EffectiveLimits.BulkDecodedBytes, 4LL << 30);
    EXPECT_EQ(result->EffectiveLimits.LookupKeys, 4096);
    EXPECT_EQ(result->EffectiveLimits.DeleteKeys, 10000);
}

TEST(StorageContract, CustomBackendDoesNotImplyNativeBulkOrKeyValueMutations)
{
    auto capabilities = LocalCapabilities();
    capabilities.backend = BackendKind::Custom;
    const auto append = NegotiateStorageContract({}, capabilities, {});
    ASSERT_TRUE(append);
    EXPECT_TRUE(append->HasOperation(EStorageOperation::Append));
    EXPECT_TRUE(append->HasFeature(EStorageFeature::SnapshotPreconditions));
    EXPECT_FALSE(append->HasOperation(EStorageOperation::BulkAppend));
    EXPECT_FALSE(append->HasOperation(EStorageOperation::ExpireSnapshots));
    TStorageContractRequest request;
    request.Mode = CollectionMode::KeyValue;
    const auto kv = NegotiateStorageContract(request, capabilities, {});
    ASSERT_TRUE(kv);
    EXPECT_FALSE(kv->HasOperation(EStorageOperation::Upsert));
    EXPECT_FALSE(kv->HasOperation(EStorageOperation::BulkUpsert));
    EXPECT_FALSE(kv->HasOperation(EStorageOperation::Compact));
    EXPECT_FALSE(kv->HasFeature(EStorageFeature::DurableNoOp));
    EXPECT_FALSE(kv->HasFeature(EStorageFeature::SnapshotPreconditions));
}

TEST(StorageContract, EngineConvenienceUsesEffectiveLimitsWithoutConsultingReadiness)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    const auto backend = std::make_shared<test::FakeStorage>(root);
    CreateStoreRequest request{test::store_id, root.string()};
    request.limits.max_operations = 5432;
    const auto created = detail::EngineFactory::Create(request, backend);
    ASSERT_TRUE(created) << created.error().message;
    const auto& engine = *created;
    const auto before = engine->NegotiateStorageContract();
    ASSERT_TRUE(before);
    EXPECT_EQ(before->EffectiveLimits.Engine.max_operations, 5432);
    ASSERT_TRUE(engine->Shutdown(std::chrono::seconds(1)));
    EXPECT_EQ(engine->GetHealth().state, StoreState::Closed);
    const auto after = engine->NegotiateStorageContract();
    ASSERT_TRUE(after);
    EXPECT_EQ(before->Operations, after->Operations);
    EXPECT_EQ(before->Features, after->Features);
    EXPECT_EQ(before->Version, after->Version);
    EXPECT_EQ(after->EffectiveLimits.Engine.max_operations, 5432);
}

} // namespace
} // namespace glamdb
