#include "core/storage/base_state.h"
#include "tests/support/checked.h"
#include "tests/vertical/storage_fault_support.h"

#include <format>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

const auto MergeBatch = Batch({{"duplicate", 7, {}, {}, {}}, {"duplicate", 7, {}, {}, {}}});

std::string MergeOperation(int index)
{
    return std::format("16900000-0000-4000-8000-{:012x}", index);
}

void CheckMergeViews(const std::shared_ptr<Engine>& engine, int appends)
{
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
        EXPECT_EQ(engine->AppendDurable({test::collection_id, MergeOperation(index), {}, {MergeBatch}}), outcome);
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
        EXPECT_EQ(engine->AppendDurable({test::collection_id, MergeOperation(100), {}, {MergeBatch}}),
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
        for (int index = 1; index <= count; ++index) {
            const auto result = Engine_->AppendDurable({test::collection_id, MergeOperation(index), {}, {MergeBatch}});
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
    Populate(99);
    ASSERT_FALSE(HasFatalFailure());
    EXPECT_EQ(LatestManifests(), 99U);
    const auto first = Engine_->FindAppendOutcome(test::collection_id, MergeOperation(1));
    ASSERT_TRUE(first && *first);
    for (int index = 100; index <= 205; ++index) {
        const auto result = Engine_->AppendDurable({test::collection_id, MergeOperation(index), {}, {MergeBatch}});
        ASSERT_EQ(result.disposition, CommitDisposition::Committed)
            << index << ": " << (result.error ? result.error->message : "");
        ASSERT_LT(LatestManifests(), 100U);
    }
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, MergeOperation(1), {}, {MergeBatch}}),
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
    const auto result = Engine_->AppendDurable({test::collection_id, MergeOperation(100), {}, {MergeBatch}});
    EXPECT_EQ(result.disposition,
        mode < 3 ? CommitDisposition::DefinitelyNotCommitted : CommitDisposition::CommitUnknown);
    Vfs_.Disarm();
    FreshProcess(mode == 4 ? 100 : 99);
}

INSTANTIATE_TEST_SUITE_P(Boundaries, ManifestMergeFaultTest, ::testing::Range(0, 5));

} // namespace
} // namespace glamdb
