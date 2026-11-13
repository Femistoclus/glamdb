#include "tests/vertical/vertical_support.h"

#include "core/engine/storage_mapping.h"
#include "storage/resources.h"

#include <format>

#include <future>
#include <thread>

namespace glamdb {
namespace {
using namespace vertical;
using namespace std::chrono_literals;

class ReadView : public ::testing::Test
{
protected:
    test::TempDir Temporary;
    std::filesystem::path Root = Temporary.path / "store";
    std::shared_ptr<Engine> Store;

    void SetUp() override
    {
        auto created = CreateStore({test::store_id, Root.string()});
        ASSERT_TRUE(created) << created.error().message;
        Store = std::move(*created);
        ASSERT_EQ(Store->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }

    TReadViewRequest Request(SnapshotSelector selector = {})
    {
        return {{{test::collection_id, selector}}, "alice"};
    }

    std::vector<std::string> Rows(const TReadView& view, size_t entry = 0)
    {
        auto scan = Store->ScanReadView(view, entry, {}, "alice");
        EXPECT_TRUE(scan) << (scan ? "" : scan.error().message);
        std::vector<std::string> rows;
        if (!scan) {
            return rows;
        }
        for (;;) {
            auto next = (*scan)->Next();
            EXPECT_TRUE(next) << (next ? "" : next.error().message);
            if (!next || !*next) {
                break;
            }
            const auto part = Render(**next);
            rows.insert(rows.end(), part.begin(), part.end());
        }
        return Sorted(std::move(rows));
    }

    AppendResult Append(std::string operation,
        const std::vector<TInputRow>& rows = FirstRows,
        std::string collection = test::collection_id)
    {
        auto outcome = Store->AppendDurable({std::move(collection), std::move(operation), {}, {Batch(rows)}});
        EXPECT_EQ(outcome.disposition, CommitDisposition::Committed);
        EXPECT_TRUE(outcome.result);
        return outcome.result.value_or(AppendResult{});
    }
};

TEST_F(ReadView, RepeatedScansKeepTheCapturedVersionAcrossAppend)
{
    const auto first = Append(FirstOperation);
    auto view = Store->AcquireReadView(Request());
    ASSERT_TRUE(view) << view.error().code;
    EXPECT_EQ(Rows(**view), Expected({&FirstRows}));
    Append(SecondOperation, SecondRows);
    EXPECT_EQ(Rows(**view), Expected({&FirstRows}));
    auto info = Store->GetReadViewInfo(**view, "alice");
    ASSERT_TRUE(info);
    ASSERT_EQ((*info)->Entries.size(), 1U);
    EXPECT_EQ((*info)->Entries[0].SelectorResolved, SnapshotSelector::Id(first.snapshot_id));
    EXPECT_EQ((*info)->Entries[0].Collection.schema, Schema);
    EXPECT_EQ((*info)->Entries[0].Identity.collection_id, test::collection_id);
    auto latest = Store->AcquireReadView(Request());
    ASSERT_TRUE(latest);
    EXPECT_EQ(Rows(**latest), Expected({&FirstRows, &SecondRows}));
}

TEST_F(ReadView, CapturesHistoricalInitialAndNoDataEntriesWithZeroLimitSchemas)
{
    auto noData = Store->AcquireReadView(Request());
    ASSERT_TRUE(noData) << noData.error().code;
    const auto first = Append(FirstOperation);
    Append(SecondOperation, SecondRows);
    auto request = Request(SnapshotSelector::Id(first.snapshot_id));
    request.Entries.push_back({test::collection_id, SnapshotSelector::InitialEmpty()});
    request.Entries.push_back({test::collection_id});
    auto view = Store->AcquireReadView(request);
    ASSERT_TRUE(view);
    EXPECT_TRUE(Rows(**noData).empty());
    EXPECT_EQ(Rows(**view), Expected({&FirstRows}));
    EXPECT_TRUE(Rows(**view, 1).empty());
    EXPECT_EQ(Rows(**view, 2), Expected({&FirstRows, &SecondRows}));
    auto scan = Store->ScanReadView(**view, 0, {.Limit = 0}, "alice");
    ASSERT_TRUE(scan);
    EXPECT_EQ((*scan)->Info().selector_resolved, SnapshotSelector::Id(first.snapshot_id));
    EXPECT_EQ((*scan)->Info().output_schema->num_fields(), 5);
    auto next = (*scan)->Next();
    ASSERT_TRUE(next);
    EXPECT_EQ(*next, nullptr);
    scan = Store->ScanReadView(**view, 0, {.Projection = std::vector<std::string>{}}, "alice");
    ASSERT_TRUE(scan);
    next = (*scan)->Next();
    ASSERT_TRUE(next);
    ASSERT_NE(*next, nullptr);
    EXPECT_EQ((*next)->num_columns(), 0);
    EXPECT_EQ((*next)->num_rows(), 3);
}

TEST_F(ReadView, DelayedPlanningSurvivesRetentionAndReleasesRetiredRoot)
{
    const auto first = Append(FirstOperation);
    auto view = Store->AcquireReadView(Request());
    ASSERT_TRUE(view) << view.error().code;
    const auto info = Store->GetReadViewInfo(**view, "alice");
    ASSERT_TRUE(info);
    const auto location = (*info)->Entries[0].Identity.metadata_location;
    const auto path = Root / location.substr(std::string("store://").size() + std::string(test::store_id).size() + 1);
    ASSERT_TRUE(std::filesystem::exists(path));
    Append(SecondOperation, SecondRows);
    ASSERT_TRUE(Store->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}}));
    EXPECT_TRUE(std::filesystem::exists(path));
    auto fresh = Store->AcquireReadView(Request(SnapshotSelector::Id(first.snapshot_id)));
    ASSERT_FALSE(fresh);
    EXPECT_EQ(fresh.error().code, "SNAPSHOT_EXPIRED");
    // First scan is deliberately opened only after retention and root replacement.
    EXPECT_EQ(Rows(**view), Expected({&FirstRows}));
    ASSERT_TRUE(Store->ReleaseReadView(**view, "alice"));
    Append(ThirdOperation, SecondRows);
    EXPECT_FALSE(std::filesystem::exists(path));
    EXPECT_EQ((*info)->Entries[0].Collection.schema, Schema);
}

TEST_F(ReadView, DelayedKVScanKeepsRowsAcrossUpsertDeleteAndCompaction)
{
    const SchemaSpec schema{
        {{"key", FieldType::String, false}, {"value", FieldType::Int64, false}, {"payload", FieldType::String, false}}};
    const auto batch = [&](const std::vector<Row>& rows) {
        return detail::MakeRecordBatch(schema, rows).value();
    };
    const auto key = [](int n) {
        return std::format("key:{:05}", n);
    };
    const auto row = [&](int n) -> Row {
        return {key(n), int64_t{n}, std::string(4096, 'x')};
    };
    CreateCollectionRequest create{SecondCollection, "kv", schema};
    create.mode = CollectionMode::KeyValue;
    create.write_settings.compression = ParquetCompression{ParquetCodec::Uncompressed, std::nullopt};
    create.write_settings.dictionary = DictionaryPolicy::Disabled;
    ASSERT_EQ(Store->CreateCollection(create).disposition, CommitDisposition::Committed);
    std::vector<Row> before;
    DeleteRequest thin{SecondCollection, "00000000-0000-4000-8000-0000000000b4", {}, {}};
    // Four real ~8 MiB ranges; thinning leaves four small files for a real 4:1 compaction.
    for (int part = 0; part < 4; ++part) {
        std::vector<Row> rows;
        before.push_back(row(part * 2048));
        for (int offset = 0; offset < 2048; ++offset) {
            rows.push_back(row(part * 2048 + offset));
            if (offset != 0) {
                thin.keys.push_back(key(part * 2048 + offset));
            }
        }
        const auto inserted = Store->UpsertDurable(
            {SecondCollection, "00000000-0000-4000-8000-0000000000b" + std::to_string(part), {}, {batch(rows)}});
        ASSERT_EQ(inserted.disposition, CommitDisposition::Committed)
            << (inserted.error ? inserted.error->message : "");
    }
    ASSERT_EQ(Store->DeleteDurable(thin).disposition, CommitDisposition::Committed);
    auto request = Request();
    request.Entries[0].CollectionID = SecondCollection;
    auto view = Store->AcquireReadView(request);
    ASSERT_TRUE(view) << view.error().code;
    auto oldFiles = Store->ListFiles(SecondCollection);
    ASSERT_TRUE(oldFiles);
    ASSERT_EQ(oldFiles->Files.size(), 4U);
    ASSERT_EQ(Store->CompactDurable({SecondCollection, FirstOperation}).disposition, CommitDisposition::Committed);
    auto compacted = Store->ListFiles(SecondCollection);
    ASSERT_TRUE(compacted);
    ASSERT_EQ(compacted->Files.size(), 1U);
    auto changed = row(0);
    changed[1] = int64_t{999};
    ASSERT_EQ(Store->UpsertDurable({SecondCollection, SecondOperation, {}, {batch({changed})}}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Store->DeleteDurable({SecondCollection, ThirdOperation, {}, {key(2048)}}).disposition,
        CommitDisposition::Committed);
    EXPECT_EQ(Rows(**view), Sorted(Render(*batch(before))));
    EXPECT_EQ(Rows(**view), Sorted(Render(*batch(before))));
    auto latest = Store->AcquireReadView(request);
    ASSERT_TRUE(latest);
    EXPECT_EQ(Rows(**latest), Sorted(Render(*batch({changed, row(4096), row(6144)}))));
}

TEST_F(ReadView, CapturesOneCoherentStoreCutUnderSerializedPublications)
{
    ASSERT_EQ(Store->CreateCollection({SecondCollection, "other", Schema}).disposition, CommitDisposition::Committed);
    auto request = Request();
    request.Entries.push_back({SecondCollection});
    request.Entries.push_back({test::collection_id});
    auto initial = Store->AcquireReadView(request);
    ASSERT_TRUE(initial);
    std::promise<void> aPublished;
    std::promise<void> allowB;
    auto aReady = aPublished.get_future();
    auto bAllowed = allowB.get_future();
    auto writer = std::async(std::launch::async, [&] {
        Append(FirstOperation, FirstRows);
        aPublished.set_value();
        bAllowed.wait();
        Append(FirstOperation, FirstRows, SecondCollection);
    });
    aReady.wait();
    // The writer cannot publish B until this intermediate A-after/B-before cut
    // is acquired. Unblock/join before assertions so failure cannot strand it.
    auto intermediate = Store->AcquireReadView(request);
    allowB.set_value();
    writer.get();
    ASSERT_TRUE(intermediate) << intermediate.error().code;
    auto final = Store->AcquireReadView(request);
    ASSERT_TRUE(final);
    EXPECT_TRUE(Rows(**initial).empty());
    EXPECT_TRUE(Rows(**initial, 1).empty());
    EXPECT_EQ(Rows(**intermediate), Expected({&FirstRows}));
    EXPECT_TRUE(Rows(**intermediate, 1).empty());
    EXPECT_EQ(Rows(**final), Expected({&FirstRows}));
    EXPECT_EQ(Rows(**final, 1), Expected({&FirstRows}));
    // All three vectors are checked after B advanced. Reject impossible cuts
    // and verify duplicate entries still share the same captured table version.
    for (const auto& view : {*initial, *intermediate, *final}) {
        const auto a = Rows(*view).size();
        const auto b = Rows(*view, 1).size();
        EXPECT_TRUE(a == b || a == b + 3) << a << ", " << b;
        EXPECT_EQ(Rows(*view, 2).size(), a);
    }
}

TEST_F(ReadView, EnforcesScopesBoundsAndAllOrNothingAcquisition)
{
    auto invalid = Request();
    invalid.AuthorizationScope.clear();
    EXPECT_FALSE(Store->AcquireReadView(invalid));
    invalid = Request();
    invalid.AuthorizationScope.assign(257, 'x');
    EXPECT_FALSE(Store->AcquireReadView(invalid));
    invalid = Request(SnapshotSelector::Id(-1));
    EXPECT_FALSE(Store->AcquireReadView(invalid));
    invalid = Request();
    invalid.Entries.clear();
    EXPECT_FALSE(Store->AcquireReadView(invalid));
    invalid = Request();
    invalid.Entries.resize(65, invalid.Entries.front());
    EXPECT_FALSE(Store->AcquireReadView(invalid));
    invalid = Request();
    invalid.Lifetime = 0ms;
    EXPECT_FALSE(Store->AcquireReadView(invalid));
    invalid.Lifetime = 6min;
    EXPECT_FALSE(Store->AcquireReadView(invalid));
    invalid = Request();
    invalid.Entries.push_back({SecondCollection});
    for (int n = 0; n < 10; ++n) {
        EXPECT_FALSE(Store->AcquireReadView(invalid));
    }
    std::vector<std::shared_ptr<TReadView>> held;
    for (int n = 0; n < 8; ++n) {
        auto view = Store->AcquireReadView(Request());
        ASSERT_TRUE(view) << view.error().code;
        held.push_back(*view);
    }
    auto refused = Store->AcquireReadView(Request());
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    auto wrong = Store->ScanReadView(*held[0], 0, {}, "bob");
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error().code, "READ_VIEW_SCOPE_MISMATCH");
    EXPECT_FALSE(Store->GetReadViewInfo(*held[0], "bob"));
    EXPECT_FALSE(Store->RenewReadView(*held[0], "bob", 1s));
    EXPECT_FALSE(Store->ReleaseReadView(*held[0], "bob"));
    EXPECT_FALSE(Store->CancelReadView(*held[0], "bob"));
    EXPECT_FALSE(Store->ScanReadView(*held[0], 1, {}, "alice"));
    EXPECT_FALSE(Store->ScanReadView(*held[0], 0, {.BatchRows = 0}, "alice"));
    held.pop_back();
    auto admitted = Store->AcquireReadView(Request());
    ASSERT_TRUE(admitted);
    ASSERT_TRUE(Store->CancelReadView(**admitted, "alice"));
    ASSERT_TRUE(Store->CancelReadView(**admitted, "alice"));
    auto cancelled = Store->GetReadViewInfo(**admitted, "alice");
    ASSERT_FALSE(cancelled);
    EXPECT_EQ(cancelled.error().code, "READ_VIEW_CANCELLED");
    EXPECT_TRUE(Store->AcquireReadView(Request()));
}

TEST_F(ReadView, GlobalViewAndEntryQuotasReleaseOnEveryTerminalPath)
{
    const auto resources = Store->GetResourceOwner();
    const auto baseline = resources->GetUsage().memory_bytes;
    std::vector<std::shared_ptr<TReadView>> held;
    for (int n = 0; n < 32; ++n) {
        auto request = Request();
        request.AuthorizationScope = "scope" + std::to_string(n);
        auto view = Store->AcquireReadView(request);
        ASSERT_TRUE(view) << view.error().code;
        held.push_back(*view);
    }
    auto refused = Store->AcquireReadView(Request());
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    held.clear();
    EXPECT_EQ(resources->GetUsage().memory_bytes, baseline);
    for (int n = 0; n < 4; ++n) {
        auto request = Request();
        request.Entries.resize(64, request.Entries.front());
        auto view = Store->AcquireReadView(request);
        ASSERT_TRUE(view) << view.error().code;
        held.push_back(*view);
    }
    refused = Store->AcquireReadView(Request());
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    ASSERT_TRUE(Store->CancelReadView(*held.back(), "alice"));
    auto resumed = Store->AcquireReadView(Request());
    ASSERT_TRUE(resumed);
    held.clear();
    resumed->reset();
    EXPECT_EQ(resources->GetUsage().memory_bytes, baseline);
}

TEST_F(ReadView, AcquisitionReclaimsExpiredMetadataBeforeSharedAdmission)
{
    const auto resources = Store->GetResourceOwner();
    const auto baseline = resources->GetUsage().memory_bytes;
    auto expiring = Store->AcquireReadView(Request());
    ASSERT_TRUE(expiring);
    auto info = Store->GetReadViewInfo(**expiring, "alice");
    ASSERT_TRUE(info);
    const std::weak_ptr<const TReadViewInfo> expiredInfo = *info;
    info->reset();
    const auto pinned = resources->GetUsage().memory_bytes;
    const auto metadata = static_cast<size_t>(ResourceClass::Metadata);
    const auto control = static_cast<size_t>(ResourceClass::Control);
    ASSERT_GT(pinned[metadata], baseline[metadata]);
    ASSERT_GT(pinned[control], baseline[control]);
    auto pressure =
        resources->Reserve(ResourceClass::Metadata, resources->GetLimits().memory_bytes[metadata] - pinned[metadata]);
    ASSERT_TRUE(pressure);
    ASSERT_TRUE(Store->RenewReadView(**expiring, "alice", 1ms));
    std::this_thread::sleep_for(5ms);
    // No view API or writer may sweep expiry before this acquisition. The expired
    // registry's reservation is the only memory available for its replacement.
    auto replacement = Store->AcquireReadView(Request());
    ASSERT_TRUE(replacement) << replacement.error().code << ": " << replacement.error().message;
    EXPECT_TRUE(expiredInfo.expired());
    auto expired = Store->GetReadViewInfo(**expiring, "alice");
    ASSERT_FALSE(expired);
    EXPECT_EQ(expired.error().code, "READ_VIEW_EXPIRED");
    replacement->reset();
    // Expiry reclaims the pin but the caller still owns its charged terminal handle.
    EXPECT_EQ(resources->GetUsage().memory_bytes[control], pinned[control]);
    expiring->reset();
    EXPECT_EQ(resources->GetUsage().memory_bytes[control], baseline[control]);
    pressure->reset();
    EXPECT_EQ(resources->GetUsage().memory_bytes, baseline);
}

TEST_F(ReadView, ExpiryCannotReclaimExternallyRetainedInfoToAdmitAnotherView)
{
    const auto resources = Store->GetResourceOwner();
    const auto baseline = resources->GetUsage().memory_bytes;
    auto expiring = Store->AcquireReadView(Request());
    ASSERT_TRUE(expiring);
    auto retained = Store->GetReadViewInfo(**expiring, "alice");
    ASSERT_TRUE(retained);
    const auto pinned = resources->GetUsage().memory_bytes;
    const auto metadata = static_cast<size_t>(ResourceClass::Metadata);
    auto pressure =
        resources->Reserve(ResourceClass::Metadata, resources->GetLimits().memory_bytes[metadata] - pinned[metadata]);
    ASSERT_TRUE(pressure);
    ASSERT_TRUE(Store->RenewReadView(**expiring, "alice", 1ms));
    std::this_thread::sleep_for(5ms);
    auto refused = Store->AcquireReadView(Request());
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ((*retained)->Entries[0].Collection.schema, Schema);
    auto expired = Store->GetReadViewInfo(**expiring, "alice");
    ASSERT_FALSE(expired);
    EXPECT_EQ(expired.error().code, "READ_VIEW_EXPIRED");
    expiring->reset();
    EXPECT_GT(resources->GetUsage().memory_bytes[metadata], baseline[metadata]);
    retained->reset();
    auto replacement = Store->AcquireReadView(Request());
    ASSERT_TRUE(replacement);
    replacement->reset();
    pressure->reset();
    EXPECT_EQ(resources->GetUsage().memory_bytes, baseline);
}

TEST_F(ReadView, SharedAdmissionFailureReleasesPartialReservations)
{
    const auto resources = Store->GetResourceOwner();
    const auto baseline = resources->GetUsage().memory_bytes;
    const auto metadata = static_cast<size_t>(ResourceClass::Metadata);
    auto pressure =
        resources->Reserve(ResourceClass::Metadata, resources->GetLimits().memory_bytes[metadata] - baseline[metadata]);
    ASSERT_TRUE(pressure);
    const auto full = resources->GetUsage().memory_bytes;
    auto refused = Store->AcquireReadView(Request());
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(resources->GetUsage().memory_bytes, full);
    pressure->reset();
    EXPECT_EQ(resources->GetUsage().memory_bytes, baseline);
    auto view = Store->AcquireReadView(Request());
    ASSERT_TRUE(view);
}

TEST_F(ReadView, CancellationAndExpiryPreserveAlreadyAdmittedScans)
{
    Append(FirstOperation);
    for (bool cancel : {true, false}) {
        auto request = Request();
        request.Lifetime = 50ms;
        auto view = Store->AcquireReadView(request);
        ASSERT_TRUE(view);
        auto cursor = Store->ScanReadView(**view, 0, {}, "alice");
        ASSERT_TRUE(cursor);
        if (cancel) {
            ASSERT_TRUE(Store->CancelReadView(**view, "alice"));
        } else {
            std::this_thread::sleep_for(60ms);
            ASSERT_FALSE(Store->GetReadViewInfo(**view, "alice"));
        }
        auto next = (*cursor)->Next();
        ASSERT_TRUE(next);
        ASSERT_NE(*next, nullptr);
        EXPECT_EQ(Render(**next), Render(*Batch(FirstRows)));
        EXPECT_FALSE(Store->ScanReadView(**view, 0, {}, "alice"));
    }
}

TEST_F(ReadView, RenewalCannotReviveExpiredPinsAndExpiryReclaimsQuota)
{
    auto request = Request();
    request.Lifetime = 20ms;
    auto view = Store->AcquireReadView(request);
    ASSERT_TRUE(view) << view.error().code;
    ASSERT_TRUE(Store->RenewReadView(**view, "alice", 1s));
    std::this_thread::sleep_for(25ms);
    EXPECT_TRUE(Store->GetReadViewInfo(**view, "alice"));
    request.Lifetime = 1ms;
    auto expiring = Store->AcquireReadView(request);
    ASSERT_TRUE(expiring);
    std::this_thread::sleep_for(5ms);
    auto expired = Store->GetReadViewInfo(**expiring, "alice");
    ASSERT_FALSE(expired);
    EXPECT_EQ(expired.error().code, "READ_VIEW_EXPIRED");
    auto renew = Store->RenewReadView(**expiring, "alice", 1s);
    ASSERT_FALSE(renew);
    EXPECT_EQ(renew.error().code, "READ_VIEW_EXPIRED");
    auto scan = Store->ScanReadView(**expiring, 0, {}, "alice");
    ASSERT_FALSE(scan);
    EXPECT_EQ(scan.error().code, "READ_VIEW_EXPIRED");
    for (int n = 0; n < 8; ++n) {
        auto next = Store->AcquireReadView(request);
        ASSERT_TRUE(next);
        std::this_thread::sleep_for(2ms);
    }
}

TEST_F(ReadView, ReleaseDoesNotInvalidateAdmittedCursorsOrRetainedBuffers)
{
    Append(FirstOperation);
    auto view = Store->AcquireReadView(Request());
    ASSERT_TRUE(view) << view.error().code;
    auto cursor = Store->ScanReadView(**view, 0, {}, "alice");
    ASSERT_TRUE(cursor);
    ASSERT_TRUE(Store->ReleaseReadView(**view, "alice"));
    ASSERT_TRUE(Store->ReleaseReadView(**view, "alice"));
    auto rejected = Store->ScanReadView(**view, 0, {}, "alice");
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, "READ_VIEW_RELEASED");
    auto next = (*cursor)->Next();
    ASSERT_TRUE(next);
    auto batch = std::move(*next);
    ASSERT_NE(batch, nullptr);
    auto slice = batch->column(1)->Slice(0, 1);
    auto buffer = slice->data()->buffers[1];
    cursor->reset();
    view->reset();
    ASSERT_TRUE(Store->Shutdown(1s));
    Store.reset();
    EXPECT_EQ(Render(*batch), Render(*Batch(FirstRows)));
    batch.reset();
    EXPECT_EQ(RenderCell(*slice, 0), "1");
    slice.reset();
    auto locked = OpenExisting({Root.string(), test::store_id});
    ASSERT_FALSE(locked);
    EXPECT_EQ(locked.error().code, "STATE_IN_USE");
    buffer.reset();
    auto reopened = OpenExisting({Root.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
}

TEST_F(ReadView, FacadeDestructionReleasesPinsWhileItsCursorRemainsReadable)
{
    Append(FirstOperation);
    auto view = Store->AcquireReadView(Request());
    ASSERT_TRUE(view);
    auto info = Store->GetReadViewInfo(**view, "alice");
    ASSERT_TRUE(info);
    const std::weak_ptr<const TReadViewInfo> pinnedInfo = *info;
    info->reset();
    auto cursor = Store->ScanReadView(**view, 0, {}, "alice");
    ASSERT_TRUE(cursor);
    Store.reset();
    EXPECT_TRUE(pinnedInfo.expired());
    auto next = (*cursor)->Next();
    ASSERT_TRUE(next);
    ASSERT_NE(*next, nullptr);
    EXPECT_EQ(Render(**next), Render(*Batch(FirstRows)));
    next->reset();
    cursor->reset();
    auto reopened = OpenExisting({Root.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    EXPECT_FALSE((*reopened)->GetReadViewInfo(**view, "alice"));
}

TEST_F(ReadView, WrongEngineShutdownAndReopenAreExplicitAndDoNotRetainStore)
{
    auto view = Store->AcquireReadView(Request());
    ASSERT_TRUE(view) << view.error().code;
    auto other = CreateStore({"00000000-0000-4000-8000-000000000099", (Temporary.path / "other").string()});
    ASSERT_TRUE(other);
    auto wrong = (*other)->GetReadViewInfo(**view, "alice");
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error().code, "READ_VIEW_WRONG_ENGINE");
    auto cursor = Store->ScanReadView(**view, 0, {}, "alice");
    ASSERT_TRUE(cursor);
    ASSERT_TRUE(Store->Shutdown(1s));
    auto closed = Store->ScanReadView(**view, 0, {}, "alice");
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, "READ_VIEW_CLOSED");
    auto next = (*cursor)->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CANCELLED");
    cursor->reset();
    Store.reset();
    auto reopened = OpenExisting({Root.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    auto stale = (*reopened)->ScanReadView(**view, 0, {}, "alice");
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error().code, "READ_VIEW_RESTARTED");
}

} // namespace
} // namespace glamdb
