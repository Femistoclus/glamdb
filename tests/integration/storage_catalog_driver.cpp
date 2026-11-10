#include "core/api/canonical.h"
#include "core/api/codec.h"
#include "core/catalog/durable_sqlite_catalog.h"
#include "core/io/store_uri.h"
#include "core/storage/append_stager.h"
#include "core/storage/data_file_writer.h"
#include "core/storage/durable_local_file_io.h"
#include "core/storage/publication_guard.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/builder_binary.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/table_requirement.h>
#include <iceberg/transaction.h>
#include <parquet/arrow/reader.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <set>
#include <stdexcept>
#include <type_traits>

namespace {
using namespace NGlamDB;
using namespace NGlamDB::NIO;
using namespace NGlamDB::NStorage;
using glamdb::detail::BoundOperation;
using glamdb::detail::Database;
using glamdb::detail::DurableSqliteCatalogStore;
using glamdb::detail::Failure;
using glamdb::detail::OperationState;

constexpr auto StoreID = "11111111-1111-4111-8111-111111111111";
constexpr auto CollectionID = "22222222-2222-4222-8222-222222222222";
constexpr auto ProfileID = "local-delete-extra-v1";
const glamdb::SchemaSpec Schema{{{"key", glamdb::FieldType::String, false}}};

void Check(bool condition, std::string_view message)
{
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

template <typename T, typename E>
T Take(std::expected<T, E> result)
{
    if (!result) {
        throw std::runtime_error(result.error().message);
    }
    if constexpr (!std::is_void_v<T>) {
        return std::move(*result);
    }
}

template <typename T>
T TakeIO(TResult<T> result)
{
    if (!result) {
        throw std::system_error(result.Error());
    }
    return std::move(result).Value();
}

std::string OperationID(int index)
{
    return "44444444-4444-4444-8444-44444444444" + std::to_string(index);
}

std::string AttemptID(int index)
{
    return "33333333-3333-7333-8333-33333333333" + std::to_string(index);
}

std::string Location()
{
    return std::string("store://") + StoreID + "/warehouse/" + CollectionID;
}

iceberg::TableIdentifier Identifier()
{
    return {.ns = iceberg::Namespace{{"columnar"}}, .name = "events"};
}

std::shared_ptr<arrow::RecordBatch> Batch(int rows)
{
    arrow::StringBuilder builder;
    for (int row = 0; row < rows; ++row) {
        Check(builder.Append("duplicate").ok(), "cannot build input rows");
    }
    const auto values = builder.Finish();
    Check(values.ok(), "cannot finish input batch");
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false)}), rows, {*values});
}

enum class EPublishMode : std::uint8_t { Success, Stale, RejectGuard, Rollback };

struct TCommitObservation
{
    std::vector<std::string> UUIDs;
    std::vector<std::pair<std::string, std::optional<std::int64_t>>> References;
};

class TObservedCatalog final : public iceberg::Catalog
{
    std::shared_ptr<iceberg::Catalog> Inner_;
    std::vector<TCommitObservation> Commits_;

public:
    explicit TObservedCatalog(std::shared_ptr<iceberg::Catalog> inner)
        : Inner_(std::move(inner))
    { }

    const auto& GetCommits() const
    {
        return Commits_;
    }
    void Reset()
    {
        Commits_.clear();
    }

    std::string_view name() const override
    {
        return Inner_->name();
    }

    iceberg::Status CreateNamespace(const iceberg::Namespace& ns,
        const std::unordered_map<std::string, std::string>& properties) override
    {
        return Inner_->CreateNamespace(ns, properties);
    }

    iceberg::Result<std::vector<iceberg::Namespace>> ListNamespaces(const iceberg::Namespace& ns) const override
    {
        return Inner_->ListNamespaces(ns);
    }

    iceberg::Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
        const iceberg::Namespace& ns) const override
    {
        return Inner_->GetNamespaceProperties(ns);
    }

    iceberg::Status DropNamespace(const iceberg::Namespace& ns) override
    {
        return Inner_->DropNamespace(ns);
    }
    iceberg::Result<bool> NamespaceExists(const iceberg::Namespace& ns) const override
    {
        return Inner_->NamespaceExists(ns);
    }

    iceberg::Status UpdateNamespaceProperties(const iceberg::Namespace& ns,
        const std::unordered_map<std::string, std::string>& updates,
        const std::unordered_set<std::string>& removals) override
    {
        return Inner_->UpdateNamespaceProperties(ns, updates, removals);
    }

    iceberg::Result<std::vector<iceberg::TableIdentifier>> ListTables(const iceberg::Namespace& ns) const override
    {
        return Inner_->ListTables(ns);
    }

    iceberg::Result<std::shared_ptr<iceberg::Table>> CreateTable(const iceberg::TableIdentifier& identifier,
        const std::shared_ptr<iceberg::Schema>& schema,
        const std::shared_ptr<iceberg::PartitionSpec>& spec,
        const std::shared_ptr<iceberg::SortOrder>& order,
        const std::string& location,
        const std::unordered_map<std::string, std::string>& properties) override
    {
        return Inner_->CreateTable(identifier, schema, spec, order, location, properties);
    }

    iceberg::Result<std::shared_ptr<iceberg::Table>> UpdateTable(const iceberg::TableIdentifier& identifier,
        const std::vector<std::unique_ptr<iceberg::TableRequirement>>& requirements,
        const std::vector<std::unique_ptr<iceberg::TableUpdate>>& updates) override
    {
        auto& observed = Commits_.emplace_back();
        for (const auto& requirement : requirements) {
            if (const auto* uuid = dynamic_cast<const iceberg::table::AssertUUID*>(requirement.get())) {
                observed.UUIDs.push_back(uuid->uuid());
            }
            if (const auto* reference = dynamic_cast<const iceberg::table::AssertRefSnapshotID*>(requirement.get())) {
                observed.References.emplace_back(reference->ref_name(), reference->snapshot_id());
            }
        }
        return Inner_->UpdateTable(identifier, requirements, updates);
    }

    iceberg::Result<std::shared_ptr<iceberg::Transaction>> StageCreateTable(const iceberg::TableIdentifier& identifier,
        const std::shared_ptr<iceberg::Schema>& schema,
        const std::shared_ptr<iceberg::PartitionSpec>& spec,
        const std::shared_ptr<iceberg::SortOrder>& order,
        const std::string& location,
        const std::unordered_map<std::string, std::string>& properties) override
    {
        return Inner_->StageCreateTable(identifier, schema, spec, order, location, properties);
    }

    iceberg::Result<bool> TableExists(const iceberg::TableIdentifier& identifier) const override
    {
        return Inner_->TableExists(identifier);
    }

    iceberg::Status DropTable(const iceberg::TableIdentifier& identifier, bool purge) override
    {
        return Inner_->DropTable(identifier, purge);
    }

    iceberg::Status RenameTable(const iceberg::TableIdentifier& from, const iceberg::TableIdentifier& to) override
    {
        return Inner_->RenameTable(from, to);
    }

    iceberg::Result<std::shared_ptr<iceberg::Table>> LoadTable(const iceberg::TableIdentifier& identifier) override
    {
        return Inner_->LoadTable(identifier);
    }

    iceberg::Result<std::shared_ptr<iceberg::Table>> RegisterTable(const iceberg::TableIdentifier& identifier,
        const std::string& metadataFileLocation) override
    {
        return Inner_->RegisterTable(identifier, metadataFileLocation);
    }
};

struct TPrepared
{
    int Index;
    BoundOperation Binding;
    glamdb::AppendResult Receipt;
    std::string ExpectedBytes;
    std::shared_ptr<const TBaseState> Base;
    std::shared_ptr<TIOAttempt> Attempt;
    TAttemptScope Scope;
    TStagedAppend Stage;
    std::unordered_map<std::string, std::string> Summary;
};

class TFixture
{
    std::filesystem::path Root_;
    std::shared_ptr<TStoreDirs> Dirs_;
    std::shared_ptr<TDurableLocalFileIO> IO_;
    std::shared_ptr<DurableSqliteCatalogStore> Store_;
    std::shared_ptr<iceberg::sql::SqlCatalog> Catalog_;
    std::shared_ptr<TObservedCatalog> ObservedCatalog_;
    TTableProfile Profile_;
    std::shared_ptr<const TBaseState> Base_;

    std::filesystem::path Physical(std::string_view location) const
    {
        return Root_ / TakeIO(TStoreUri::Parse(location, StoreID)).RelativePath();
    }

    std::filesystem::path OraclePath(int index) const
    {
        return Root_.parent_path() / ("caller-outcome-" + std::to_string(index) + ".bin");
    }

    std::optional<std::string> ObserverPointer() const
    {
        Database observer;
        observer.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READONLY, {});
        const auto rows = observer.Query(
            "SELECT metadata_location FROM iceberg_tables WHERE namespace='columnar' AND table_name='events'");
        return rows.empty() ? std::nullopt : rows.front().front();
    }

    TPublicationContext Context(const std::shared_ptr<TIOAttempt>& attempt,
        const std::shared_ptr<const TBaseState>& base = {}) const
    {
        return {.Directories = Dirs_,
            .FileIO = IO_,
            .Attempt = attempt,
            .Namespace = "columnar",
            .Name = "events",
            .Profile = Profile_,
            .Base = base,
            .StoreUsage = base ? base->Usage : TStorageUsage{}};
    }

    glamdb::Status Guard(TStoragePublicationGuard* guard, const TPointerChange& change) const
    {
        const auto checked = guard->BeforePointerChange(change);
        if (!checked) {
            return std::unexpected(
                glamdb::ErrorDetail{"GUARD_REJECTED", checked.error().message, glamdb::RetryHint::OperatorAction});
        }
        return {};
    }

    void CheckBound(int index) const
    {
        const auto record = Store_->FindOperation(CollectionID, OperationID(index));
        Check(record && record->state == OperationState::Bound && !record->outcome && record->outcome_bytes.empty(),
            "rejected publication became a terminal outcome");
    }

    void CheckSnapshot(const iceberg::TableMetadata& metadata,
        const glamdb::AppendResult& receipt,
        std::int64_t expectedRows) const
    {
        const auto snapshot = Take(metadata.SnapshotById(receipt.snapshot_id));
        Check(snapshot->parent_snapshot_id == receipt.parent_snapshot_id &&
                std::chrono::duration_cast<std::chrono::microseconds>(snapshot->timestamp_ms.time_since_epoch())
                        .count() == receipt.snapshot_created_at_us,
            "receipt snapshot identity or timestamp mismatch");
        Check(snapshot->summary.at("glamdb.operation-id") == receipt.operation_id &&
                snapshot->summary.at("glamdb.content-hash") == receipt.payload_hash &&
                snapshot->summary.at("engine.rows-appended") == std::to_string(receipt.rows_appended),
            "receipt differs from persisted snapshot summary");
        const iceberg::SnapshotReader snapshotReader(snapshot.get());
        const auto manifests = Take(snapshotReader.Manifests(IO_));
        std::set<std::string> paths;
        std::int64_t rows = 0;
        std::int64_t addedFiles = 0;
        std::int64_t addedBytes = 0;
        std::int64_t addedRows = 0;
        for (const auto& manifest : manifests) {
            const auto reader = Take(
                iceberg::ManifestReader::Make(manifest, IO_, Take(metadata.Schema()), Take(metadata.PartitionSpec())));
            for (const auto& entry : Take(reader->Entries())) {
                Check(entry.data_file && entry.status != iceberg::ManifestStatus::kDeleted,
                    "unexpected manifest entry");
                const auto& file = *entry.data_file;
                Check(paths.insert(file.file_path).second, "repeated data file in snapshot");
                const auto bytes = Take(IO_->ReadFile(file.file_path, std::nullopt));
                Check(static_cast<std::int64_t>(bytes.size()) == file.file_size_in_bytes,
                    "data file length differs from manifest");
                const auto parquet =
                    parquet::arrow::OpenFile(arrow::io::BufferReader::FromString(bytes), arrow::default_memory_pool());
                Check(parquet.ok(), "cannot decode Parquet");
                const auto table = (*parquet)->ReadTable();
                Check(table.ok() && (*table)->num_columns() == 1 && (*table)->num_rows() == file.record_count,
                    "decoded rows differ from manifest");
                for (const auto& chunk : (*table)->column(0)->chunks()) {
                    const auto strings = std::dynamic_pointer_cast<arrow::StringArray>(chunk);
                    Check(strings && strings->null_count() == 0, "invalid decoded keys");
                    for (std::int64_t row = 0; row < strings->length(); ++row) {
                        Check(strings->GetView(row) == "duplicate", "input row was lost or changed");
                    }
                }
                rows += file.record_count;
                if (entry.snapshot_id == receipt.snapshot_id) {
                    ++addedFiles;
                    addedBytes += file.file_size_in_bytes;
                    addedRows += file.record_count;
                }
            }
        }
        Check(rows == expectedRows && addedFiles == receipt.data_files_added &&
                addedBytes == receipt.compressed_data_bytes_added && addedRows == receipt.rows_appended,
            "snapshot rows or new files differ from caller receipt");
    }

public:
    TFixture(std::filesystem::path root, bool create)
        : Root_(std::move(root))
    {
        Check(Root_.is_absolute() && (create ? !std::filesystem::exists(Root_) : std::filesystem::is_directory(Root_)),
            "invalid test root");
        auto fs = std::make_shared<TPosixLocalFS>();
        auto latch = std::make_shared<TWriteLatch>();
        Dirs_ = TakeIO(create ? TStoreDirs::Create(fs, Root_, latch) : TStoreDirs::Open(fs, Root_, latch));
        IO_ = Take(TDurableLocalFileIO::Make(Dirs_, StoreID));
        const glamdb::StoreInfo info{StoreID, ProfileID, 1};
        Store_ = create ? DurableSqliteCatalogStore::Create(Root_ / "catalog.sqlite", info, {})
                        : DurableSqliteCatalogStore::Open(Root_ / "catalog.sqlite", info, {});
        Profile_ = Take(MakeTableProfile({{{"key", EFieldType::String}}}));
        Catalog_ = Take(
            iceberg::sql::SqlCatalog::Make({.name = "storage-catalog-test",
                                               .uri = "",
                                               .warehouse_location = std::string("store://") + StoreID + "/warehouse",
                                               .max_connections = 1,
                                               .props = {}},
                IO_,
                Store_));
        ObservedCatalog_ = std::make_shared<TObservedCatalog>(Catalog_);
        if (!create) {
            const auto base = Store_->Base(CollectionID);
            Base_ =
                std::make_shared<TBaseState>(Take(VerifyReachable(IO_, base.metadata_location, StoreID, CollectionID)));
            Check(Base_->Metadata->table_uuid == base.table_uuid &&
                    Base_->Metadata->current_snapshot_id == base.current_snapshot_id,
                "authoritative catalog base disagrees with reachable root");
        }
    }

    void Create()
    {
        TakeIO(Dirs_->CreateCollection(CollectionID));
        const auto attempt =
            TakeIO(TIOAttempt::Make({StoreID, CollectionID, AttemptID(0), EAttemptKind::CreateCollection, std::nullopt},
                Dirs_->Latch()));
        auto scope = Take(IO_->BindAttempt(attempt));
        const auto transaction = Take(Catalog_->StageCreateTable(Identifier(),
            Profile_.Schema,
            iceberg::PartitionSpec::Unpartitioned(),
            iceberg::SortOrder::Unsorted(),
            Location(),
            Profile_.Properties));
        constexpr auto conflictingID = "22222222-2222-4222-8222-222222222223";
        const auto competing = Take(Catalog_->StageCreateTable(Identifier(),
            Profile_.Schema,
            iceberg::PartitionSpec::Unpartitioned(),
            iceberg::SortOrder::Unsorted(),
            std::string("store://") + StoreID + "/warehouse/" + conflictingID,
            Profile_.Properties));
        Check(competing->table()->uuid() != transaction->table()->uuid(), "competing create reused its table UUID");
        TStoragePublicationGuard guard(Context(attempt));
        const glamdb::CollectionInfo collection{CollectionID,
            "events",
            transaction->table()->uuid(),
            Schema,
            0,
            std::nullopt};
        Store_->PublishCreate(
            collection,
            [&](std::string_view location) {
                Check(!ObserverPointer(), "create pointer visible before outer commit");
                return Guard(&guard,
                    {EPointerOp::InsertTable, "columnar", "events", std::nullopt, std::string(location)});
            },
            [&]() -> iceberg::Status {
                const auto result = transaction->Commit();
                if (!result) {
                    return std::unexpected(result.error());
                }
                Check(!ObserverPointer(), "inner create escaped outer transaction");
                return {};
            });
        Base_ = guard.GetCandidate();
        Check(Base_ && Base_->Metadata->snapshots.empty() && ObserverPointer() == Base_->MetadataLocation,
            "create did not publish guarded InitialEmpty root");
        auto conflicting = collection;
        conflicting.collection_id = conflictingID;
        conflicting.table_uuid = competing->table()->uuid();
        int bodies = 0;
        int guards = 0;
        bool rejected = false;
        const auto records = attempt->Records().size();
        try {
            Store_->PublishCreate(
                conflicting,
                [&](std::string_view) -> glamdb::Status {
                    ++guards;
                    return {};
                },
                [&]() -> iceberg::Status {
                    ++bodies;
                    const auto result = competing->Commit();
                    return result ? iceberg::Status{} : std::unexpected(result.error());
                });
        } catch (const Failure& error) {
            Check(error.detail.code == "ALREADY_EXISTS" && !error.uncertain, "wrong stale-create failure");
            rejected = true;
        }
        Check(rejected && bodies == 0 && guards == 0 && attempt->Records().size() == records &&
                Store_->Collections().size() == 1 && Store_->Resolve("events").collection_id == CollectionID &&
                Store_->Base(CollectionID).table_uuid == collection.table_uuid &&
                ObserverPointer() == Base_->MetadataLocation,
            "stale create changed the winning name, UUID or root");
        std::cout << "PASS competing staged create rejected before body/guard; winning name and UUID preserved\n";
    }

    TPrepared Prepare(int index, const std::shared_ptr<arrow::RecordBatch>& batch)
    {
        const auto rows = batch->num_rows();
        const auto hash = Take(glamdb::detail::FingerprintBatches(Schema, CollectionID, {batch}));
        BoundOperation binding{CollectionID, OperationID(index), hash, 1, Store_->Base(CollectionID), AttemptID(index)};
        Store_->BindOperation(binding);
        const auto attempt = TakeIO(
            TIOAttempt::Make({StoreID, CollectionID, binding.attempt_id, EAttemptKind::Append, binding.operation_id},
                Dirs_->Latch()));
        auto scope = Take(IO_->BindAttempt(attempt));
        const auto loaded = Take(Catalog_->LoadTable(Identifier()));
        const auto table = Take(iceberg::Table::Make(loaded->name(),
            loaded->metadata(),
            std::string(loaded->metadata_file_location()),
            IO_,
            ObservedCatalog_));
        Check(table->metadata_file_location() == binding.base.metadata_location &&
                Base_->MetadataLocation == binding.base.metadata_location,
            "prepare changed its base");
        const auto path = Location() + "/data/" + binding.operation_id + "-" + binding.attempt_id + "-00000.parquet";
        const auto data = Take(WriteDataFile(IO_, attempt, *table->metadata(), path, {batch}));
        std::unordered_map<std::string, std::string> summary{{"glamdb.operation-id", binding.operation_id},
            {"glamdb.content-hash", hash},
            {"engine.rows-appended", std::to_string(rows)},
            {"engine.data-files-added", "1"},
            {"engine.data-bytes-added", std::to_string(data.Receipt.Length)}};
        auto stage = Take(StageAppend(table, data.DataFile, summary, attempt));
        const auto snapshot = Take(stage.Transaction->current().Snapshot());
        glamdb::AppendResult receipt{CollectionID,
            binding.operation_id,
            hash,
            1,
            stage.SnapshotID,
            stage.ParentID,
            rows,
            1,
            static_cast<std::int64_t>(data.Receipt.Length),
            std::chrono::duration_cast<std::chrono::microseconds>(snapshot->timestamp_ms.time_since_epoch()).count(),
            ProfileID,
            1};
        const auto bytes = Take(glamdb::detail::EncodeOutcome({binding.operation_id,
            glamdb::CommitDisposition::Committed,
            glamdb::CommitPhase::Complete,
            receipt,
            std::nullopt,
            std::nullopt}));
        return {index,
            std::move(binding),
            std::move(receipt),
            bytes,
            Base_,
            attempt,
            std::move(scope),
            std::move(stage),
            std::move(summary)};
    }

    void Publish(TPrepared& prepared, EPublishMode mode)
    {
        auto context = Context(prepared.Attempt, prepared.Base);
        context.ExpectedSummary = prepared.Summary;
        if (mode == EPublishMode::RejectGuard) {
            context.ExpectedSummary["glamdb.content-hash"] = "invalid-test-expectation";
        }
        TStoragePublicationGuard guard(std::move(context));
        const auto previous = Store_->Base(CollectionID);
        const auto accepted = Base_;
        const auto records = prepared.Attempt->Records().size();
        int bodies = 0;
        int guards = 0;
        std::string candidate;
        bool rejected = false;
        bool guardRejected = false;
        bool rollbackInjected = false;
        ObservedCatalog_->Reset();
        try {
            Store_->PublishAppend(
                prepared.Binding,
                prepared.Receipt,
                [&](std::string_view location) {
                    ++guards;
                    candidate = location;
                    Check(Store_->Base(CollectionID) == previous && ObserverPointer() == previous.metadata_location,
                        "pointer changed before closure guard");
                    const auto record = TakeIO(prepared.Attempt->Record(location));
                    Check(record.Receipt && record.State == EFileState::DirSynced,
                        "guard observed a non-durable candidate root");
                    auto result = Guard(&guard,
                        {EPointerOp::UpdateLocation,
                            "columnar",
                            "events",
                            prepared.Binding.base.metadata_location,
                            candidate});
                    guardRejected = !result;
                    return result;
                },
                [&]() -> iceberg::Status {
                    ++bodies;
                    const auto result = prepared.Stage.Transaction->Commit();
                    if (!result) {
                        return std::unexpected(result.error());
                    }
                    Check(ObserverPointer() == previous.metadata_location && Base_ == accepted,
                        "inner commit exposed tentative state");
                    if (mode == EPublishMode::Rollback) {
                        rollbackInjected = true;
                        return iceberg::IOError("test-only failure after inner commit");
                    }
                    return {};
                });
        } catch (const Failure& error) {
            Check(mode != EPublishMode::Success && !error.uncertain, "unexpected or uncertain publication failure");
            if (mode == EPublishMode::Stale) {
                Check(error.detail.code == "STALE_BASE", "stale base failed for another reason");
            }
            rejected = true;
        }
        prepared.Scope.Reset();
        const auto& commits = ObservedCatalog_->GetCommits();
        Check(commits.size() == (mode == EPublishMode::Stale ? 0U : 1U),
            "transaction retried or bypassed the observed catalog");
        if (!commits.empty()) {
            const auto& observed = commits.front();
            Check(observed.UUIDs == std::vector<std::string>{prepared.Binding.base.table_uuid} &&
                    observed.References ==
                        std::vector<std::pair<std::string, std::optional<std::int64_t>>>{
                            {"main", prepared.Binding.base.current_snapshot_id}},
                "emitted requirements differ from the prepared UUID or main snapshot");
            const auto& snapshotID = observed.References.front().second;
            std::cout << "TRACE AssertUUID(" << observed.UUIDs.front() << ") AssertRefSnapshotID(main,"
                      << (snapshotID ? std::to_string(*snapshotID) : "nullopt")
                      << ") prepared_location=" << prepared.Binding.base.metadata_location << '\n';
        }
        if (mode == EPublishMode::Success) {
            Check(!rejected && bodies == 1 && guards == 1, "publication did not commit exactly once");
            Base_ = guard.GetCandidate();
            Check(Base_ && ObserverPointer() == Base_->MetadataLocation, "outer commit lost guarded pointer");
            std::ofstream oracle(OraclePath(prepared.Index), std::ios::binary);
            oracle.write(prepared.ExpectedBytes.data(), static_cast<std::streamsize>(prepared.ExpectedBytes.size()));
            oracle.close();
            Check(oracle.good(), "cannot save caller-result oracle");
        } else {
            Check(rejected && Store_->Base(CollectionID) == previous && Base_ == accepted &&
                    ObserverPointer() == previous.metadata_location,
                "failed publication advanced accepted state");
            CheckBound(prepared.Index);
            if (mode == EPublishMode::Stale) {
                Check(bodies == 0 && guards == 0 && prepared.Attempt->Records().size() == records,
                    "stale publication ran or rebased its prepared transaction");
            } else {
                Check(mode == EPublishMode::Rollback ? rollbackInjected : guardRejected,
                    "publication failed before the intended rejection boundary");
                Check(bodies == 1 && guards == 1 && !candidate.empty() &&
                        std::filesystem::is_regular_file(Physical(candidate)),
                    "rollback did not leave the real candidate as an orphan");
                Check(static_cast<bool>(guard.GetCandidate()) == (mode == EPublishMode::Rollback),
                    "unexpected tentative guard state");
            }
        }
    }

    void ReplaceRootByTestActor()
    {
        const auto previous = Store_->Base(CollectionID);
        const auto actorIO = Take(TDurableLocalFileIO::Make(Dirs_, StoreID));
        const auto attempt =
            TakeIO(TIOAttempt::Make({StoreID, CollectionID, AttemptID(8), EAttemptKind::Append, OperationID(8)},
                Dirs_->Latch()));
        auto scope = Take(actorIO->BindAttempt(attempt));
        const auto location = Location() + "/metadata/00008-" + AttemptID(8) + ".metadata.json";
        Take(iceberg::TableMetadataUtil::Write(*actorIO, location, *Base_->Metadata));
        scope.Reset();
        auto replacement = Take(VerifyReachable(actorIO, location, StoreID, CollectionID));
        Database actor;
        actor.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
        actor.Execute("PRAGMA synchronous=EXTRA");
        actor.Execute("BEGIN IMMEDIATE");
        actor.Execute("UPDATE iceberg_tables SET metadata_location=?,previous_metadata_location=? "
                      "WHERE namespace='columnar' AND table_name='events' AND metadata_location=?",
            {location, previous.metadata_location, previous.metadata_location});
        Check(actor.Changes() == 1, "test-only SQL actor missed the prepared base");
        actor.Execute("COMMIT");
        Check(Store_->Base(CollectionID).current_snapshot_id == previous.current_snapshot_id,
            "metadata-only actor changed the snapshot");
        Base_ = std::make_shared<TBaseState>(std::move(replacement));
    }

    void Run()
    {
        Create();
        auto staleEmpty = Prepare(3, Batch(1));
        staleEmpty.Scope.Reset();
        auto first = Prepare(1, Batch(1000));
        Publish(first, EPublishMode::Success);
        Publish(staleEmpty, EPublishMode::Stale);
        auto staleSnapshot = Prepare(4, Batch(1));
        staleSnapshot.Scope.Reset();
        auto second = Prepare(2, Batch(10));
        Publish(second, EPublishMode::Success);
        Publish(staleSnapshot, EPublishMode::Stale);
        auto staleLocation = Prepare(5, Batch(1));
        staleLocation.Scope.Reset();
        ReplaceRootByTestActor();
        Publish(staleLocation, EPublishMode::Stale);
        auto rejected = Prepare(6, Batch(1));
        Publish(rejected, EPublishMode::RejectGuard);
        auto rolledBack = Prepare(7, Batch(1));
        Publish(rolledBack, EPublishMode::Rollback);
        CheckSnapshot(*Base_->Metadata, first.Receipt, 1000);
        CheckSnapshot(*Base_->Metadata, second.Receipt, 1010);
        std::cout << "PASS durable create/append, first-snapshot/snapshot/location stale bases, guard rejection, outer "
                     "rollback\n";
    }

    void Verify()
    {
        Store_->ValidateInventory();
        const auto table = Take(Catalog_->LoadTable(Identifier()));
        Check(table->metadata_file_location() == Base_->MetadataLocation && *table->metadata() == *Base_->Metadata &&
                Base_->Usage.Snapshots == 2 && Base_->Usage.DataFiles == 2,
            "fresh process did not select the complete authoritative closure");
        for (int index = 1; index <= 2; ++index) {
            std::ifstream oracle(OraclePath(index), std::ios::binary);
            const std::string expected{std::istreambuf_iterator<char>(oracle), std::istreambuf_iterator<char>()};
            Check(!expected.empty() && !oracle.bad(), "missing caller-result oracle");
            const auto outcome = Take(glamdb::detail::DecodeOutcome(expected));
            const auto& receipt = outcome.result;
            if (!receipt) {
                throw std::runtime_error("caller oracle lacks a receipt");
            }
            const auto record = Store_->FindOperation(CollectionID, OperationID(index));
            Check(record && record->state == OperationState::Committed && record->outcome_bytes == expected &&
                    record->outcome == outcome,
                "fresh process receipt differs from caller result");
            CheckSnapshot(*Base_->Metadata, *receipt, index == 1 ? 1000 : 1010);
        }
        for (int index = 3; index <= 7; ++index) {
            CheckBound(index);
        }
        Check(Store_->Operations().size() == 7, "unexpected operation inventory");
        std::cout << "PASS fresh catalog-selected closure and exact caller receipt bytes; A=1000 latest=1010 duplicate "
                     "rows\n";
    }
};

}

int main(int argc, char** argv)
{
    try {
        if (argc == 3 && std::string_view(argv[1]) == "--verify") {
            TFixture(argv[2], false).Verify();
        } else {
            Check(argc == 2, "usage: storage_catalog_driver [--verify] ABSOLUTE_ROOT");
            TFixture(argv[1], true).Run();
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}
