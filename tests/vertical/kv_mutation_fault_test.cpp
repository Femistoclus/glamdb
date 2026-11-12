#include "core/api/codec.h"
#include "core/engine/storage_mapping.h"
#include "tests/support/checked.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <format>
#include <map>
#include <spawn.h>
#include <stdexcept>
#include <sys/wait.h>

extern char** environ;

namespace glamdb {
namespace {
using namespace std::chrono_literals;
using namespace vertical;

constexpr char MutationOperation[] = "00000000-0000-4000-8000-0000000000af";
constexpr int64_t RowsPerPart = 2048;
constexpr int64_t PayloadBytes = 4096;
const SchemaSpec MutationSchema{
    {{"key", FieldType::String, false}, {"value", FieldType::Int64, false}, {"payload", FieldType::String, true}}};

std::string CompactKey(int64_t ordinal)
{
    return std::format("key:{:05}", ordinal);
}

Row CompactRow(int64_t ordinal)
{
    return {CompactKey(ordinal), ordinal, std::string(PayloadBytes, static_cast<char>('a' + ordinal % 26))};
}

std::vector<Row> BaseRows(MutationKind kind)
{
    if (kind == MutationKind::Compact) {
        std::vector<Row> rows;
        for (int64_t part = 0; part < 4; ++part) {
            rows.push_back(CompactRow(part * RowsPerPart));
        }
        return rows;
    }
    return {{std::string("a"), int64_t{1}, std::string("old-a")}, {std::string("b"), int64_t{2}, std::string("old-b")}};
}

std::vector<Row> LatestRows(MutationKind kind, bool committed)
{
    if (!committed || kind == MutationKind::Compact) {
        return BaseRows(kind);
    }
    if (kind == MutationKind::Delete) {
        return {BaseRows(kind).front()};
    }
    return {{std::string("a"), int64_t{10}, std::monostate{}},
        {std::string("b"), int64_t{2}, std::string("old-b")},
        {std::string("c"), int64_t{3}, std::string("new-c")}};
}

std::shared_ptr<arrow::RecordBatch> MutationBatch(const std::vector<Row>& rows)
{
    auto batch = detail::MakeRecordBatch(MutationSchema, rows);
    if (!batch) {
        throw std::runtime_error(batch.error().message);
    }
    return *batch;
}

AppendOutcome ExactMutation(Engine& engine, MutationKind kind, int64_t baseSnapshot)
{
    const SnapshotPrecondition expected{SnapshotPreconditionKind::Equals, baseSnapshot};
    if (kind == MutationKind::Upsert) {
        return engine.UpsertDurable({test::collection_id,
            MutationOperation,
            expected,
            {MutationBatch({{std::string("a"), int64_t{10}, std::monostate{}},
                {std::string("c"), int64_t{3}, std::string("new-c")}})}});
    }
    if (kind == MutationKind::Delete) {
        return engine.DeleteDurable({test::collection_id, MutationOperation, expected, {"b", "absent", "b"}});
    }
    return engine.CompactDurable({test::collection_id, MutationOperation, expected});
}

void ExpectView(Engine& engine,
    SnapshotSelector selector,
    const std::vector<Row>& expected,
    MutationKind kind,
    SnapshotSelector resolved)
{
    std::vector<std::string> keys = kind == MutationKind::Compact
        ? std::vector<std::string>{CompactKey(0),
              CompactKey(RowsPerPart),
              CompactKey(2 * RowsPerPart),
              CompactKey(3 * RowsPerPart),
              CompactKey(1),
              "absent",
              CompactKey(0)}
        : std::vector<std::string>{"a", "b", "c", "absent", "a"};
    const auto lookup = engine.Lookup({test::collection_id, keys, selector});
    ASSERT_TRUE(lookup) << lookup.error().message;
    EXPECT_EQ(lookup->snapshot, resolved);
    ASSERT_EQ(lookup->rows.size(), keys.size());
    for (size_t index = 0; index < keys.size(); ++index) {
        const auto row = std::ranges::find_if(expected, [&](const Row& value) {
            return std::get<std::string>(value[0]) == keys[index];
        });
        EXPECT_EQ(lookup->rows[index], row == expected.end() ? std::optional<Row>{} : *row) << keys[index];
    }
    const auto cursor = engine.Scan({.collection_id = test::collection_id, .selector = selector});
    ASSERT_TRUE(cursor) << cursor.error().message;
    EXPECT_EQ((*cursor)->Info().selector_resolved, resolved);
    std::map<std::string, Row> actual;
    for (;;) {
        const auto next = (*cursor)->Next();
        ASSERT_TRUE(next) << next.error().message;
        if (!*next) {
            break;
        }
        const auto& batch = **next;
        const auto& key = static_cast<const arrow::StringArray&>(*batch.column(0));
        const auto& value = static_cast<const arrow::Int64Array&>(*batch.column(1));
        const auto& payload = static_cast<const arrow::StringArray&>(*batch.column(2));
        for (int64_t row = 0; row < batch.num_rows(); ++row) {
            Row cells{key.GetString(row),
                value.Value(row),
                payload.IsNull(row) ? Cell(std::monostate{}) : Cell(payload.GetString(row))};
            EXPECT_TRUE(actual.emplace(key.GetString(row), std::move(cells)).second);
        }
    }
    std::map<std::string, Row> expectedRows;
    for (const auto& row : expected) {
        expectedRows.emplace(std::get<std::string>(row[0]), row);
    }
    EXPECT_EQ(actual, expectedRows);
}

void ExpectResolvedState(Engine& engine,
    MutationKind kind,
    bool committed,
    const AppendOutcome& baseline,
    const AppendOutcome& terminal)
{
    ASSERT_TRUE(baseline.result);
    const auto& base = test::Present(baseline.result);
    ASSERT_EQ(terminal.disposition,
        committed ? CommitDisposition::Committed : CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(terminal.recovery);
    ASSERT_TRUE(detail::ValidateOutcome(terminal));
    const auto recorded = engine.FindAppendOutcome(test::collection_id, MutationOperation);
    ASSERT_TRUE(recorded) << recorded.error().message;
    ASSERT_TRUE(*recorded);
    EXPECT_EQ(test::Present(*recorded), terminal);
    if (committed) {
        ASSERT_TRUE(terminal.result);
        const auto& receipt = test::Present(terminal.result);
        EXPECT_EQ(receipt.receipt_version, 3);
        EXPECT_EQ(receipt.mutation_kind, kind);
        EXPECT_TRUE(receipt.snapshot_created);
        EXPECT_EQ(receipt.parent_snapshot_id, base.snapshot_id);
        EXPECT_NE(receipt.snapshot_id, base.snapshot_id);
        EXPECT_GT(receipt.snapshot_created_at_us, 0);
        EXPECT_EQ(receipt.rows_appended, 0);
        EXPECT_EQ(receipt.input_rows, kind == MutationKind::Upsert ? 2 : kind == MutationKind::Delete ? 3 : 0);
        EXPECT_EQ(receipt.rows_affected, kind == MutationKind::Upsert ? 2 : kind == MutationKind::Delete ? 1 : 0);
        if (kind == MutationKind::Compact) {
            EXPECT_EQ(receipt.data_files_removed, 4);
            EXPECT_EQ(receipt.data_files_added, 1);
            EXPECT_EQ(receipt.rows_written, 4);
            EXPECT_EQ(receipt.rows_removed, 4);
        }
    } else {
        EXPECT_FALSE(terminal.result);
        EXPECT_TRUE(terminal.error);
    }
    const size_t baseCount = kind == MutationKind::Compact ? 5 : 1;
    const auto history = engine.ListSnapshots(test::collection_id);
    ASSERT_TRUE(history) << history.error().message;
    ASSERT_EQ(history->snapshots.size(), baseCount + static_cast<size_t>(committed));
    EXPECT_EQ(history->snapshots[baseCount - 1].receipt, base);
    std::optional<int64_t> previous;
    for (const auto& snapshot : history->snapshots) {
        EXPECT_EQ(snapshot.parent_snapshot_id, previous);
        EXPECT_EQ(snapshot.receipt.snapshot_id, snapshot.snapshot_id);
        const auto original = engine.FindAppendOutcome(test::collection_id, snapshot.receipt.operation_id);
        ASSERT_TRUE(original);
        ASSERT_TRUE(*original);
        EXPECT_EQ((test::Present(*original)).result, snapshot.receipt);
        previous = snapshot.snapshot_id;
    }
    const auto latest = committed ? test::Present(terminal.result).snapshot_id : base.snapshot_id;
    EXPECT_EQ(history->identity.current_snapshot_id, latest);
    EXPECT_EQ(history->snapshots.back().receipt, committed ? test::Present(terminal.result) : base);
    const auto collection = engine.GetCollection(test::collection_id);
    ASSERT_TRUE(collection);
    EXPECT_EQ(collection->mode, CollectionMode::KeyValue);
    EXPECT_EQ(collection->current_snapshot_id, latest);
    ASSERT_NO_FATAL_FAILURE(ExpectView(engine,
        SnapshotSelector::Latest(),
        LatestRows(kind, committed),
        kind,
        SnapshotSelector::Id(latest)));
    ASSERT_NO_FATAL_FAILURE(ExpectView(engine,
        SnapshotSelector::Id(base.snapshot_id),
        BaseRows(kind),
        kind,
        SnapshotSelector::Id(base.snapshot_id)));
    ASSERT_NO_FATAL_FAILURE(
        ExpectView(engine, SnapshotSelector::InitialEmpty(), {}, kind, SnapshotSelector::InitialEmpty()));
    const auto statistics = engine.GetStatistics(test::collection_id);
    ASSERT_TRUE(statistics);
    EXPECT_EQ(statistics->rows.value, static_cast<int64_t>(LatestRows(kind, committed).size()));
    const auto oldFiles = engine.ListFiles(test::collection_id, SnapshotSelector::Id(base.snapshot_id));
    const auto files = engine.ListFiles(test::collection_id);
    ASSERT_TRUE(oldFiles);
    ASSERT_TRUE(files);
    EXPECT_EQ(oldFiles->Files.size(), kind == MutationKind::Compact ? 4U : 1U);
    EXPECT_EQ(files->Files.size(), kind == MutationKind::Compact && !committed ? 4U : 1U);
}

void SaveReceipt(const std::filesystem::path& path, const AppendOutcome& outcome)
{
    const auto encoded = detail::EncodeOutcome(outcome);
    ASSERT_TRUE(encoded) << encoded.error().message;
    WriteBytes(path, *encoded);
    EXPECT_EQ(ReadBytes(path), *encoded);
}

AppendOutcome LoadReceipt(const std::filesystem::path& path)
{
    const auto decoded = detail::DecodeOutcome(ReadBytes(path));
    if (!decoded) {
        throw std::runtime_error(decoded.error().message);
    }
    return *decoded;
}

TEST(KVMutationFaultChild, ReopensWithProductionDefaults)
{
    const auto* root = std::getenv("GLAMDB_KV_MUTATION_ROOT");
    const auto* kindText = std::getenv("GLAMDB_KV_MUTATION_KIND");
    const auto* committedText = std::getenv("GLAMDB_KV_MUTATION_COMMITTED");
    if (!root || !kindText || !committedText) {
        GTEST_SKIP() << "Invoked by the mutation fault test in a fresh process.";
    }
    const auto kind = static_cast<MutationKind>(std::stoi(kindText));
    const bool committed = std::string_view(committedText) == "1";
    const auto directory = std::filesystem::path(root).parent_path();
    const auto baseline = LoadReceipt(directory / "baseline.outcome");
    const auto terminal = LoadReceipt(directory / "mutation.outcome");
    ASSERT_TRUE(baseline.result);
    const auto opened = OpenExisting({root, test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    const auto& engine = *opened;
    const auto before = CountRegularFiles(std::filesystem::path(root) / "warehouse");
    EXPECT_EQ(ExactMutation(*engine, kind, test::Present(baseline.result).snapshot_id), terminal);
    EXPECT_EQ(CountRegularFiles(std::filesystem::path(root) / "warehouse"), before);
    ASSERT_NO_FATAL_FAILURE(ExpectResolvedState(*engine, kind, committed, baseline, terminal));
    EXPECT_TRUE(engine->Shutdown(1000ms));
}

void ExpectFreshReopen(const std::filesystem::path& root, MutationKind kind, bool committed)
{
#ifdef __linux__
    std::vector<std::string> environment;
    for (auto** entry = environ; *entry; ++entry) {
        if (!std::string_view(*entry).starts_with("GLAMDB_KV_MUTATION_")) {
            environment.emplace_back(*entry);
        }
    }
    environment.push_back("GLAMDB_KV_MUTATION_ROOT=" + root.string());
    environment.push_back("GLAMDB_KV_MUTATION_KIND=" + std::to_string(static_cast<int>(kind)));
    environment.push_back(committed ? "GLAMDB_KV_MUTATION_COMMITTED=1" : "GLAMDB_KV_MUTATION_COMMITTED=0");
    std::vector<char*> variables;
    variables.reserve(environment.size() + 1);
    for (auto& entry : environment) {
        variables.push_back(entry.data());
    }
    variables.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=KVMutationFaultChild.ReopensWithProductionDefaults";
    char* arguments[]{executable.data(), filter.data(), nullptr};
    pid_t child = 0;
    ASSERT_EQ(::posix_spawn(&child, executable.c_str(), nullptr, nullptr, arguments, variables.data()), 0);
    int status = 0;
    pid_t waited;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(waited, child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
#else
    static_cast<void>(root);
    static_cast<void>(kind);
    static_cast<void>(committed);
    GTEST_SKIP() << "Fresh-process recovery is checked on Linux.";
#endif
}

struct TMutationFault
{
    MutationKind Kind;
    bool AfterUnlink;
    const char* Name;
};

class KVMutationFault : public ::testing::TestWithParam<TMutationFault>
{
protected:
    test::JournalFaultVfs Vfs_;
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<Engine> Engine_;
    AppendOutcome Baseline_;

    void SetUp() override
    {
        const detail::BackendFactory factory =
            [](const auto& path, const StoreInfo& store) -> Result<std::shared_ptr<detail::StorageBackend>> {
            return detail::TLocalStorageBackend::Open(path, store);
        };
        const auto opened = detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, factory, Vfs_.Name());
        ASSERT_TRUE(opened) << opened.error().message;
        Engine_ = *opened;
        CreateCollectionRequest collection{test::collection_id, "mutation-fault", MutationSchema};
        collection.mode = CollectionMode::KeyValue;
        collection.write_settings.compression = ParquetCompression{ParquetCodec::Uncompressed, std::nullopt};
        collection.write_settings.dictionary = DictionaryPolicy::Disabled;
        ASSERT_EQ(Engine_->CreateCollection(collection).disposition, CommitDisposition::Committed);
        if (GetParam().Kind != MutationKind::Compact) {
            Baseline_ = Engine_->UpsertDurable(
                {test::collection_id, FirstOperation, {}, {MutationBatch(BaseRows(GetParam().Kind))}});
        } else {
            // Four bounded ~8 MiB ranges, then four sparse surviving files.
            // This exercises a real 4:1 compaction rather than a no-op receipt.
            for (int64_t part = 0; part < 4; ++part) {
                std::vector<Row> rows;
                rows.reserve(RowsPerPart);
                for (int64_t offset = 0; offset < RowsPerPart; ++offset) {
                    rows.push_back(CompactRow(part * RowsPerPart + offset));
                }
                const auto operation = std::format("00000000-0000-4000-8000-{:012x}", 0xc000 + part);
                const auto inserted =
                    Engine_->UpsertDurable({test::collection_id, operation, {}, {MutationBatch(rows)}});
                ASSERT_EQ(inserted.disposition, CommitDisposition::Committed)
                    << (inserted.error ? inserted.error->message : "");
            }
            DeleteRequest thin{test::collection_id, "00000000-0000-4000-8000-00000000c010", {}, {}};
            for (int64_t row = 0; row < 4 * RowsPerPart; ++row) {
                if (row % RowsPerPart != 0) {
                    thin.keys.push_back(CompactKey(row));
                }
            }
            Baseline_ = Engine_->DeleteDurable(thin);
            ASSERT_EQ(Baseline_.disposition, CommitDisposition::Committed)
                << (Baseline_.error ? Baseline_.error->message : "");
            const auto files = Engine_->ListFiles(test::collection_id);
            ASSERT_TRUE(files);
            ASSERT_EQ(files->Files.size(), 4U);
        }
        ASSERT_EQ(Baseline_.disposition, CommitDisposition::Committed);
        ASSERT_TRUE(Baseline_.result);
        ASSERT_TRUE(test::Present(Baseline_.result).snapshot_created);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }
};

TEST_P(KVMutationFault, ReconcilesPublicationAndReopensTheExactTerminalReceipt)
{
    const auto [kind, afterUnlink, name] = GetParam();
    SCOPED_TRACE(name);
    const auto base = test::Present(Baseline_.result).snapshot_id;
    Vfs_.Arm(2, afterUnlink); // Binding commits; publication loses its COMMIT result.
    const auto unknown = ExactMutation(*Engine_, kind, base);
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown) << (unknown.error ? unknown.error->message : "");
    EXPECT_EQ(unknown.phase, CommitPhase::CatalogCommit);
    EXPECT_EQ(Vfs_.FailureCount(), 1);
    ASSERT_TRUE(unknown.recovery);
    ASSERT_TRUE(test::Present(unknown.recovery).candidate_snapshot_id);
    ASSERT_TRUE(test::Present(unknown.recovery).candidate_metadata_location);
    EXPECT_TRUE(test::Present(unknown.recovery).publication_started);
    EXPECT_EQ(test::Present(unknown.recovery).base.current_snapshot_id, base);
    EXPECT_EQ(Engine_->GetHealth().state, StoreState::RecoveryRequired);
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    const auto before = CountRegularFiles(Root_ / "warehouse");
    const auto blocked = ExactMutation(*Engine_, kind, base);
    EXPECT_EQ(blocked.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(blocked.recovery);
    EXPECT_EQ(test::Present(blocked.recovery).attempt_id, test::Present(unknown.recovery).attempt_id);
    EXPECT_EQ(test::Present(blocked.recovery).candidate_snapshot_id,
        test::Present(unknown.recovery).candidate_snapshot_id);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), before);
    Vfs_.Disarm();
    ASSERT_TRUE(Engine_->Reconcile());
    const auto recorded = Engine_->FindAppendOutcome(test::collection_id, MutationOperation);
    ASSERT_TRUE(recorded) << recorded.error().message;
    ASSERT_TRUE(*recorded);
    const auto terminal = test::Present(*recorded);
    if (afterUnlink) {
        ASSERT_EQ(terminal.disposition, CommitDisposition::Committed);
        ASSERT_TRUE(terminal.result);
        EXPECT_EQ(test::Present(terminal.result).snapshot_id, test::Present(unknown.recovery).candidate_snapshot_id);
    } else {
        ASSERT_EQ(terminal.disposition, CommitDisposition::DefinitelyNotCommitted);
    }
    const auto beforeRetry = CountRegularFiles(Root_ / "warehouse");
    EXPECT_EQ(ExactMutation(*Engine_, kind, base), terminal);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), beforeRetry);
    ASSERT_NO_FATAL_FAILURE(ExpectResolvedState(*Engine_, kind, afterUnlink, Baseline_, terminal));
    ASSERT_NO_FATAL_FAILURE(SaveReceipt(Temporary_.path / "baseline.outcome", Baseline_));
    ASSERT_NO_FATAL_FAILURE(SaveReceipt(Temporary_.path / "mutation.outcome", terminal));
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    ASSERT_NO_FATAL_FAILURE(ExpectFreshReopen(Root_, kind, afterUnlink));
}

INSTANTIATE_TEST_SUITE_P(Publication,
    KVMutationFault,
    ::testing::Values(TMutationFault{MutationKind::Upsert, true, "UpsertLostCommitReply"},
        TMutationFault{MutationKind::Upsert, false, "UpsertCommitRollback"},
        TMutationFault{MutationKind::Delete, true, "DeleteLostCommitReply"},
        TMutationFault{MutationKind::Delete, false, "DeleteCommitRollback"},
        TMutationFault{MutationKind::Compact, true, "CompactLostCommitReply"},
        TMutationFault{MutationKind::Compact, false, "CompactCommitRollback"}),
    [](const ::testing::TestParamInfo<TMutationFault>& value) {
        return value.param.Name;
    });

} // namespace
} // namespace glamdb
