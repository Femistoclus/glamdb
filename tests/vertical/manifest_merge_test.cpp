#include "core/storage/base_state.h"
#include "tests/support/checked.h"
#include "tests/vertical/storage_fault_support.h"

#include <format>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

std::shared_ptr<arrow::RecordBatch> MakeMergeBatch()
{
    // Arrow buffers must be released before its global memory pools shut down.
    return Batch({{"duplicate", 7, {}, {}, {}}, {"duplicate", 7, {}, {}, {}}});
}

std::string MergeOperation(int index)
{
    return std::format("16900000-0000-4000-8000-{:012x}", index);
}

void CheckMergeViews(const std::shared_ptr<Engine>& engine, int appends)
{
    const auto mergeBatch = MakeMergeBatch();
    const auto history = engine->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history) << history.error().message;
    ASSERT_EQ(history->snapshots.size(), static_cast<size_t>(appends));
    for (const auto selector : {SnapshotSelector::Latest(), SnapshotSelector::InitialEmpty()}) {
        auto cursor = engine->Scan({.collection_id = test::collection_id, .selector = selector});
        ASSERT_TRUE(cursor) << cursor.error().message;
        int64_t count = 0;
        while (true) {
            const auto batch = (*cursor)->Next();
            ASSERT_TRUE(batch) << batch.error().message;
            if (!*batch) {
                break;
            }
            for (const auto& row : Render(**batch)) {
                EXPECT_EQ(row, "duplicate|7|null|null|null");
                ++count;
            }
        }
        EXPECT_EQ(count, selector.kind == SnapshotSelectorKind::Latest ? appends * 2 : 0);
    }
    for (const int index : {1, 99, 100, 198, 199}) {
        if (index > appends) {
            continue;
        }
        const auto found = engine->FindAppendOutcome(test::collection_id, MergeOperation(index));
        ASSERT_TRUE(found);
        ASSERT_TRUE(*found);
        const auto& outcome = test::Present(*found);
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed);
        ASSERT_TRUE(outcome.result);
        EXPECT_EQ(engine->AppendDurable({test::collection_id, MergeOperation(index), {}, {mergeBatch}}), outcome);
        const auto cursor = engine->Scan({.collection_id = test::collection_id,
            .selector = SnapshotSelector::Id(test::Present(outcome.result).snapshot_id)});
        ASSERT_TRUE(cursor);
        int64_t count = 0;
        while (true) {
            const auto batch = (*cursor)->Next();
            ASSERT_TRUE(batch);
            if (!*batch) {
                break;
            }
            count += (*batch)->num_rows();
        }
        EXPECT_EQ(count, index * 2);
    }
    if (appends == 99) {
        const auto failed = engine->FindAppendOutcome(test::collection_id, MergeOperation(100));
        ASSERT_TRUE(failed);
        ASSERT_TRUE(*failed);
        ASSERT_EQ(test::Present(*failed).disposition, CommitDisposition::DefinitelyNotCommitted);
        EXPECT_EQ(engine->AppendDurable({test::collection_id, MergeOperation(100), {}, {mergeBatch}}),
            test::Present(*failed));
    }
}

TEST(ManifestMergeChild, ReopensHistoryAndExactRetries)
{
    const auto* root = std::getenv("GLAMDB_STORAGE_FAULT_ROOT");
    const auto* count = std::getenv("GLAMDB_STORAGE_FAULT_COMMITTED");
    if (!root || !count) {
        GTEST_SKIP() << "Invoked by a merge test with a disposable store.";
    }
    auto opened = OpenExisting({root, test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    CheckMergeViews(*opened, std::stoi(count));
    EXPECT_TRUE((*opened)->Shutdown(1000ms));
}

class ManifestMergeTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    const std::filesystem::path Root_ = Temporary_.path / "store";
    test::JournalFaultVfs Vfs_;
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<TFaultFS> Faults_ = std::make_shared<TFaultFS>(Posix_);
    std::shared_ptr<detail::TLocalStorageBackend> Backend_;
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        auto created = detail::EngineFactory::CreateWith(
            {test::store_id, Root_.string()},
            [this](const std::filesystem::path& root,
                const StoreInfo& store) -> Result<std::shared_ptr<detail::StorageBackend>> {
                detail::TLocalStorageOptions options;
                options.Fs = Faults_;
                auto opened = detail::TLocalStorageBackend::Open(root, store, options);
                if (!opened) {
                    return std::unexpected(opened.error());
                }
                Backend_ = *opened;
                return Backend_;
            },
            Vfs_.Name());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    void Populate(int count)
    {
        const auto mergeBatch = MakeMergeBatch();
        for (int index = 1; index <= count; ++index) {
            const auto result = Engine_->AppendDurable({test::collection_id, MergeOperation(index), {}, {mergeBatch}});
            ASSERT_EQ(result.disposition, CommitDisposition::Committed)
                << index << ": " << (result.error ? result.error->message : "");
        }
    }

    size_t LatestManifests()
    {
        const auto cursor = Engine_->Scan({.collection_id = test::collection_id, .limit = 0});
        EXPECT_TRUE(cursor);
        if (!cursor) {
            return 0;
        }
        const auto state = Backend_->VerifiedScanState((*cursor)->Info().identity);
        EXPECT_TRUE(state);
        return state ? (*state)->CurrentManifests.size() : 0;
    }

    void FreshProcess(int count)
    {
        Engine_.reset();
        Backend_.reset();
        RunChild(Root_, {.Test = "ManifestMergeChild.ReopensHistoryAndExactRetries", .State = std::to_string(count)});
    }
};

TEST_F(ManifestMergeTest, BoundsLatestManifestsAcrossRepeatedMergesAndReopens)
{
    const auto mergeBatch = MakeMergeBatch();
    Populate(99);
    ASSERT_FALSE(HasFatalFailure());
    EXPECT_EQ(LatestManifests(), 99U);
    const auto first = Engine_->FindAppendOutcome(test::collection_id, MergeOperation(1));
    ASSERT_TRUE(first && *first);
    for (int index = 100; index <= 205; ++index) {
        const auto result = Engine_->AppendDurable({test::collection_id, MergeOperation(index), {}, {mergeBatch}});
        ASSERT_EQ(result.disposition, CommitDisposition::Committed)
            << index << ": " << (result.error ? result.error->message : "");
        ASSERT_LT(LatestManifests(), 100U);
    }
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, MergeOperation(1), {}, {mergeBatch}}),
        test::Present(*first));
    CheckMergeViews(Engine_, 205);
    FreshProcess(205);
}

class ManifestMergeFaultTest
    : public ManifestMergeTest
    , public ::testing::WithParamInterface<int>
{ };

TEST_P(ManifestMergeFaultTest, FailureAtMergeBoundaryNeverPublishesPartialMembership)
{
    Populate(99);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_EQ(LatestManifests(), 99U);
    const int mode = GetParam();
    if (mode < 3) {
        Faults_->AddRule({EIOOp::SyncFile,
            mode == 0       ? "*-m0.avro"
                : mode == 1 ? "*-m1.avro"
                            : "*snap-*.avro",
            1,
            1,
            std::make_error_code(std::errc::io_error),
            {}});
    } else {
        Vfs_.Arm(2, mode == 4);
    }
    const auto result = Engine_->AppendDurable({test::collection_id, MergeOperation(100), {}, {MakeMergeBatch()}});
    EXPECT_EQ(result.disposition,
        mode < 3 ? CommitDisposition::DefinitelyNotCommitted : CommitDisposition::CommitUnknown);
    Vfs_.Disarm();
    FreshProcess(mode == 4 ? 100 : 99);
}

INSTANTIATE_TEST_SUITE_P(Boundaries, ManifestMergeFaultTest, ::testing::Range(0, 5));

// The verifier caps every manifest's decoded Avro bytes at ManifestBytes.
// Merging targets encoded bytes, so one merged manifest used to outgrow the
// cap (at ~29,500 one-row appends with the 8 MiB default): the collection's
// live data files were bounded by a single manifest. A 128 KiB cap reaches
// the same boundary after a few hundred appends.
class ManifestDecodedBoundTest : public ::testing::Test
{
protected:
    static constexpr uint64_t DecodedCap = 128ULL << 10;
    static constexpr int Appends = 900;

    test::TempDir Temporary_;
    const std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<detail::TLocalStorageBackend> Backend_;

    Result<std::shared_ptr<detail::StorageBackend>> OpenBackend(const std::filesystem::path& root,
        const StoreInfo& store)
    {
        detail::TLocalStorageOptions options;
        options.Storage.ManifestBytes = DecodedCap;
        auto opened = detail::TLocalStorageBackend::Open(root, store, options);
        if (!opened) {
            return std::unexpected(opened.error());
        }
        Backend_ = *opened;
        return Backend_;
    }

    std::shared_ptr<const NGlamDB::NStorage::TBaseState> Current(Engine& engine)
    {
        const auto cursor = engine.Scan({.collection_id = test::collection_id, .limit = 0});
        EXPECT_TRUE(cursor);
        if (!cursor) {
            return nullptr;
        }
        const auto state = Backend_->VerifiedScanState((*cursor)->Info().identity);
        EXPECT_TRUE(state);
        return state ? *state : nullptr;
    }

    void CheckManifests(Engine& engine)
    {
        const auto state = Current(engine);
        ASSERT_TRUE(state);
        // Several merged manifests, each within the decoded cap: the list grew.
        size_t merged = 0;
        int64_t files = 0;
        for (const auto& manifest : state->CurrentManifests) {
            merged += manifest.existing_files_count.value_or(0) > 0 ? 1 : 0;
            files += manifest.added_files_count.value_or(0) + manifest.existing_files_count.value_or(0);
        }
        EXPECT_GE(merged, 2U);
        EXPECT_EQ(files, Appends);
        EXPECT_LT(state->CurrentManifests.size(), 120U);
        ASSERT_EQ(state->CurrentManifestDecodedBytes.size(), state->CurrentManifests.size());
        for (const auto bytes : state->CurrentManifestDecodedBytes) {
            EXPECT_GT(bytes, 0U);
            EXPECT_LE(bytes, DecodedCap);
        }
        const auto statistics = engine.GetStatistics(test::collection_id, SnapshotSelector::Latest());
        ASSERT_TRUE(statistics) << statistics.error().message;
        EXPECT_EQ(statistics->rows.value, 2 * Appends);
    }
};

TEST_F(ManifestDecodedBoundTest, MergedManifestsStayWithinTheDecodedCapAndTheListGrows)
{
    std::shared_ptr<Engine> engine;
    {
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()},
            [this](const auto& root, const auto& store) {
                return OpenBackend(root, store);
            });
        ASSERT_TRUE(created) << created.error().message;
        engine = std::move(*created);
    }
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    const auto batch = MakeMergeBatch();
    for (int index = 1; index <= Appends; ++index) {
        const auto result = engine->AppendDurable({test::collection_id, MergeOperation(index), {}, {batch}});
        ASSERT_EQ(result.disposition, CommitDisposition::Committed)
            << index << ": " << (result.error ? result.error->message : "");
    }
    CheckManifests(*engine);
    ASSERT_FALSE(HasFatalFailure());
    const auto before = Current(*engine);
    ASSERT_TRUE(before);
    ASSERT_TRUE(engine->Shutdown(1000ms));
    engine.reset();
    // Both open paths measure the same decoded sizes.
    for (const bool full : {false, true}) {
        auto opened = detail::EngineFactory::OpenWith(
            {.absolute_root = Root_.string(), .expected_store_id = test::store_id, .verify_full = full},
            [this](const auto& root, const auto& store) {
                return OpenBackend(root, store);
            });
        ASSERT_TRUE(opened) << opened.error().message;
        CheckManifests(**opened);
        const auto after = Current(**opened);
        ASSERT_TRUE(after);
        EXPECT_EQ(after->CurrentManifests, before->CurrentManifests);
        EXPECT_EQ(after->CurrentManifestDecodedBytes, before->CurrentManifestDecodedBytes);
        ASSERT_TRUE((*opened)->Shutdown(1000ms));
    }
}

} // namespace
} // namespace glamdb
