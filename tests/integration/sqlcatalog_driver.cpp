// Real Iceberg integration. The test-only local FileIO establishes ordering;
// file synchronization and power-loss durability require separate validation.
#include "core/catalog/durable_sqlite_catalog.h"
#include "tests/support/checked.h"

#include <arrow/util/config.h>
#include <iceberg/arrow/arrow_io_util.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/file_io.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <iceberg/update/update_properties.h>
#include <iceberg/version.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using glamdb::detail::BoundOperation;
using glamdb::detail::Database;
using glamdb::detail::DurableSqliteCatalogStore;
using glamdb::detail::Failure;
using glamdb::detail::OperationState;
const std::string store_id = "00000000-0000-0000-0000-000000000001";
const std::string collection_id = "00112233-4455-6677-8899-aabbccddeeff";
const std::string operation_id = "00000000-0000-0000-0000-000000000003";
const std::string attempt_id = "00000000-0000-0000-0000-000000000004";
const std::string profile_id = "local-delete-extra-v1";

void Check(bool condition, const std::string& message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <class T>
T Take(iceberg::Result<T> result, const char* where)
{
    if (!result) {
        throw std::runtime_error(std::string(where) + ": " + result.error().message);
    }
    return std::move(*result);
}
void Need(iceberg::Status result, const char* where)
{
    if (!result) {
        throw std::runtime_error(std::string(where) + ": " + result.error().message);
    }
}

struct Trace
{
    std::vector<std::string> events;
    void Add(std::string event)
    {
        events.push_back(std::move(event));
    }
    bool Contains(const std::string& event) const
    {
        return std::find(events.begin(), events.end(), event) != events.end();
    }
};

class TracedStream final : public iceberg::PositionOutputStream
{
public:
    TracedStream(std::unique_ptr<iceberg::PositionOutputStream> stream,
        std::shared_ptr<Trace> trace,
        std::string logical)
        : stream_(std::move(stream))
        , trace_(std::move(trace))
        , logical_(std::move(logical))
    { }
    iceberg::Result<int64_t> Position() const override
    {
        return stream_->Position();
    }
    iceberg::Result<int64_t> StoredLength() const override
    {
        return stream_->StoredLength();
    }
    iceberg::Status Write(std::span<const std::byte> data) override
    {
        return stream_->Write(data);
    }
    iceberg::Status Flush() override
    {
        return stream_->Flush();
    }
    iceberg::Status Close() override
    {
        auto status = stream_->Close();
        if (status && !closed_) {
            trace_->Add("close-ok " + logical_);
            closed_ = true;
        }
        return status;
    }

private:
    std::unique_ptr<iceberg::PositionOutputStream> stream_;
    std::shared_ptr<Trace> trace_;
    std::string logical_;
    bool closed_ = false;
};

class TracedOutput final : public iceberg::OutputFile
{
public:
    TracedOutput(std::unique_ptr<iceberg::OutputFile> output, std::shared_ptr<Trace> trace, std::string logical)
        : output_(std::move(output))
        , trace_(std::move(trace))
        , logical_(std::move(logical))
    { }
    std::string_view location() const override
    {
        return logical_;
    }
    iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> Create() override
    {
        return Wrap(output_->Create());
    }
    iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> CreateOrOverwrite() override
    {
        return Wrap(output_->CreateOrOverwrite());
    }

private:
    iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> Wrap(
        iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> stream)
    {
        if (!stream) {
            return std::unexpected(stream.error());
        }
        return std::make_unique<TracedStream>(std::move(*stream), trace_, logical_);
    }
    std::unique_ptr<iceberg::OutputFile> output_;
    std::shared_ptr<Trace> trace_;
    std::string logical_;
};

class MappedFileIO final : public iceberg::FileIO
{
public:
    MappedFileIO(std::filesystem::path root, std::shared_ptr<Trace> trace)
        : root_(std::move(root))
        , trace_(std::move(trace))
        , local_(iceberg::arrow::MakeLocalFileIO())
    { }
    std::filesystem::path Physical(std::string_view logical) const
    {
        const std::string prefix = "store://" + store_id + "/warehouse/";
        Check(logical.starts_with(prefix), "unexpected logical URI");
        const auto suffix = logical.substr(prefix.size());
        Check(suffix.find("..") == std::string_view::npos, "unexpected relative traversal");
        return root_ / "warehouse" / suffix;
    }
    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string logical) override
    {
        return local_->NewInputFile(Physical(logical).string());
    }
    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string logical, size_t length) override
    {
        return local_->NewInputFile(Physical(logical).string(), length);
    }
    iceberg::Result<std::unique_ptr<iceberg::OutputFile>> NewOutputFile(std::string logical) override
    {
        const auto physical = Physical(logical);
        std::filesystem::create_directories(physical.parent_path());
        auto output = local_->NewOutputFile(physical.string());
        if (!output) {
            return std::unexpected(output.error());
        }
        trace_->Add("open-output " + logical);
        return std::make_unique<TracedOutput>(std::move(*output), trace_, std::move(logical));
    }
    iceberg::Status DeleteFile(const std::string&) override
    {
        return iceberg::NotSupported("test probe retains orphan metadata");
    }

private:
    std::filesystem::path root_;
    std::shared_ptr<Trace> trace_;
    std::unique_ptr<iceberg::FileIO> local_;
};

std::optional<std::string> ObserverPointer(const std::filesystem::path& db, const std::string& name)
{
    Database observer;
    observer.Open(db, SQLITE_OPEN_READONLY, {});
    const auto rows = observer.Query("SELECT metadata_location FROM iceberg_tables WHERE "
                                     "namespace='columnar' AND table_name=?",
        {name});
    observer.Close();
    return rows.empty() ? std::nullopt : rows.at(0).at(0);
}

iceberg::TableIdentifier Identifier(const std::string& name)
{
    return {.ns = iceberg::Namespace{{"columnar"}}, .name = name};
}

std::shared_ptr<iceberg::Transaction> StageCreate(const std::shared_ptr<iceberg::sql::SqlCatalog>& catalog,
    const std::string& name)
{
    auto schema = std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{iceberg::SchemaField::MakeOptional(1, "n", iceberg::int64())});
    return Take(catalog->StageCreateTable(Identifier(name),
                    schema,
                    iceberg::PartitionSpec::Unpartitioned(),
                    iceberg::SortOrder::Unsorted(),
                    "store://" + store_id + "/warehouse/" + name,
                    {{"commit.retry.num-retries", "0"}, {"write.metadata.delete-after-commit.enabled", "false"}}),
        "StageCreateTable");
}

glamdb::CollectionInfo
Collection(const std::shared_ptr<iceberg::Transaction>& transaction, const std::string& name, const std::string& id)
{
    return {id,
        name,
        transaction->table()->uuid(),
        glamdb::SchemaSpec{{{"n", glamdb::FieldType::Int64, true}}},
        0,
        std::nullopt};
}
} // namespace

int main(int argc, char** argv)
{
    try {
        if (argc == 3 && std::string_view(argv[1]) == "--verify") {
            const std::filesystem::path root = argv[2];
            Check(root.is_absolute() && std::filesystem::is_directory(root),
                "verify requires an existing absolute directory");
            auto io = std::make_shared<MappedFileIO>(root, std::make_shared<Trace>());
            auto store = DurableSqliteCatalogStore::Open(root / "catalog.sqlite", {store_id, profile_id, 1}, {});
            iceberg::sql::SqlCatalogConfig config;
            config.name = "probe";
            config.warehouse_location = "store://" + store_id + "/warehouse";
            config.max_connections = 1;
            auto catalog = Take(iceberg::sql::SqlCatalog::Make(config, io, store), "fresh-process SqlCatalog::Make");
            auto table = Take(catalog->LoadTable(Identifier("events")), "fresh-process LoadTable");
            Check(table->properties().configs().at("probe.property") == "changed" && table->snapshots().empty(),
                "fresh process did not load expected real property-only root");
            Check(table->uuid() == store->Collection(collection_id).table_uuid, "fresh-process UUID mismatch");
            Check(table->metadata_file_location() == store->Base(collection_id).metadata_location,
                "fresh-process catalog/root identity mismatch");
            Check(!Take(catalog->TableExists(Identifier("rolled_back")), "fresh-process rolled-back TableExists"),
                "fresh process found rolled-back create");
            const auto operation = store->FindOperation(collection_id, operation_id);
            Check(operation && operation->state == OperationState::Bound && !operation->outcome,
                "fresh process found a receipt for rolled-back publication");
            Check(store->FenceValue() == 1, "fresh process lost fence increment");
            const auto profile = store->Profile();
            Check(profile.journal_mode == "delete" && profile.synchronous == 3, "fresh-process settings mismatch");
            std::cout << "PASS fresh-process Open(no CREATE) -> real "
                         "SqlCatalog::Make/LoadTable; pointer="
                      << table->metadata_file_location()
                      << " snapshot=InitialEmpty operation=Bound "
                         "receipt=absent fence="
                      << store->FenceValue() << " sqlite=" << profile.sqlite_version
                      << " journal=" << profile.journal_mode << " synchronous=" << profile.synchronous << '\n';
            std::cout << "FRESH_PROCESS_VERIFY_PASS\n";
            return 0;
        }
        Check(argc == 2, "usage: real-sqlcatalog-probe ABSOLUTE_NEW_OUTPUT_DIRECTORY");
        const std::filesystem::path root = argv[1];
        Check(root.is_absolute() && !std::filesystem::exists(root), "output directory must be new and absolute");
        std::filesystem::create_directory(root);
        const auto db = root / "catalog.sqlite";
        auto trace = std::make_shared<Trace>();
        auto io = std::make_shared<MappedFileIO>(root, trace);
        glamdb::StoreInfo info{store_id, profile_id, 1};
        auto store = DurableSqliteCatalogStore::Create(db, info, {});
        auto profile = store->Profile();
        std::cout << "iceberg=" << ICEBERG_VERSION_STRING << " commit=" << ICEBERG_GIT_COMMIT_ID
                  << " arrow=" << ARROW_VERSION_STRING << '\n';
        std::cout << "sqlite=" << profile.sqlite_version << " source_id=" << profile.source_id << " vfs=" << profile.vfs
                  << " journal=" << profile.journal_mode << " synchronous=" << profile.synchronous << '\n';
        for (const auto& option : profile.compile_options) {
            std::cout << "sqlite_compile_option=" << option << '\n';
        }
        iceberg::sql::SqlCatalogConfig config;
        config.name = "probe";
        config.warehouse_location = "store://" + store_id + "/warehouse";
        config.max_connections = 1;
        auto catalog = Take(iceberg::sql::SqlCatalog::Make(config, io, store), "SqlCatalog::Make");
        Check(Take(catalog->NamespaceExists(iceberg::Namespace{{"columnar"}}), "NamespaceExists"),
            "missing internal namespace");
        std::cout << "PASS real SqlCatalog::Make + Initialize + namespace lookup\n";

        auto create = StageCreate(catalog, "events");
        auto expected_collection = Collection(create, "events", collection_id);
        std::string initial_location;
        auto created = store->PublishCreate(
            expected_collection,
            [&](std::string_view location) -> glamdb::Status {
                initial_location = location;
                Check(trace->Contains("close-ok " + initial_location), "create guard ran before successful root Close");
                Check(std::filesystem::is_regular_file(io->Physical(location)), "create root missing at guard");
                auto metadata =
                    Take(iceberg::TableMetadataUtil::Read(*io, initial_location), "read create root at guard");
                Check(metadata->table_uuid == expected_collection.table_uuid && metadata->snapshots.empty(),
                    "initial root identity/snapshot mismatch");
                Check(!ObserverPointer(db, "events"), "external observer saw pointer before guarded INSERT");
                trace->Add("guard-create " + initial_location);
                return {};
            },
            [&]() -> iceberg::Status {
                auto committed = create->Commit();
                if (!committed) {
                    return std::unexpected(committed.error());
                }
                Check(!ObserverPointer(db, "events"), "inner create escaped outer transaction");
                return {};
            });
        Check(store->Base(collection_id).metadata_location == initial_location,
            "create pointer missing after outer commit");
        Check(ObserverPointer(db, "events") == initial_location, "external observer missing committed pointer");
        Check(created.table_uuid == Take(catalog->LoadTable(Identifier("events")), "LoadTable")->uuid(),
            "registry/table UUID mismatch");
        std::cout << "PASS real initial-empty create: absent -> " << initial_location << "\n";

        auto rollback_create = StageCreate(catalog, "rolled_back");
        auto rollback_info = Collection(rollback_create, "rolled_back", "00112233-4455-6677-8899-aabbccddee00");
        std::string create_orphan;
        bool create_failed = false;
        try {
            (void)store->PublishCreate(
                rollback_info,
                [&](std::string_view location) -> glamdb::Status {
                    create_orphan = location;
                    Check(trace->Contains("close-ok " + create_orphan),
                        "rolled-back create root not closed before guard");
                    return {};
                },
                [&]() -> iceberg::Status {
                    auto committed = rollback_create->Commit();
                    if (!committed) {
                        return std::unexpected(committed.error());
                    }
                    Check(Take(store->TableExists("columnar", "rolled_back"), "tentative create existence"),
                        "missing tentative table");
                    Check(!ObserverPointer(db, "rolled_back"), "tentative create visible externally");
                    return iceberg::IOError("probe: fail after inner Iceberg create");
                });
        } catch (const Failure& error) {
            Check(!error.uncertain, "successful create rollback was marked unknown");
            create_failed = true;
        }
        Check(create_failed && !ObserverPointer(db, "rolled_back") && store->Collections().size() == 1,
            "outer rollback retained created table or registry");
        Check(!create_orphan.empty() && std::filesystem::exists(io->Physical(create_orphan)),
            "expected orphan create root missing");
        std::cout << "PASS outer create rollback removes table+registry; root "
                     "retained as orphan\n";

        const auto initial_base = store->Base(collection_id);
        BoundOperation binding{collection_id, operation_id, std::string(64, 'a'), 1, initial_base, attempt_id};
        store->BindOperation(binding);
        auto table = Take(catalog->LoadTable(Identifier("events")), "LoadTable before metadata-only update");
        auto update = Take(iceberg::Transaction::Make(table, iceberg::TransactionKind::kUpdate), "Transaction::Make");
        auto properties = Take(update->NewUpdateProperties(), "NewUpdateProperties");
        properties->Set("probe.property", "changed");
        Need(properties->Commit(), "stage property update");
        Check(ObserverPointer(db, "events") == initial_location, "staging changed pointer");
        // The receipt is intentionally never committed: property-only updates
        // have no append snapshot.
        glamdb::AppendResult synthetic_receipt{collection_id,
            operation_id,
            binding.payload_hash,
            1,
            42,
            std::nullopt,
            1,
            1,
            1,
            1700000000000000LL,
            profile_id,
            1};
        std::string metadata_orphan;
        int update_guards = 0;
        bool outer_failed = false;
        try {
            store->PublishAppend(
                binding,
                synthetic_receipt,
                [&](std::string_view location) -> glamdb::Status {
                    metadata_orphan = location;
                    ++update_guards;
                    Check(trace->Contains("close-ok " + metadata_orphan), "metadata guard ran before root Close");
                    auto metadata =
                        Take(iceberg::TableMetadataUtil::Read(*io, metadata_orphan), "read updated root at guard");
                    Check(metadata->properties.configs().at("probe.property") == "changed" &&
                            metadata->snapshots.empty(),
                        "candidate was not a real InitialEmpty property-only "
                        "update");
                    Check(store->Base(collection_id) == initial_base, "pointer changed before guard");
                    Check(ObserverPointer(db, "events") == initial_location,
                        "observer saw candidate before guarded CAS");
                    trace->Add("guard-update " + metadata_orphan);
                    return {};
                },
                [&]() -> iceberg::Status {
                    return store->RunInTransaction([&]() -> iceberg::Status {
                        auto committed = update->Commit();
                        if (!committed) {
                            return std::unexpected(committed.error());
                        }
                        Check(std::string((*committed)->metadata_file_location()) == metadata_orphan,
                            "inner result root mismatch");
                        Check(store->Base(collection_id).metadata_location == metadata_orphan, "tentative CAS missing");
                        Check(ObserverPointer(db, "events") == initial_location, "inner Commit escaped outer scope");
                        trace->Add("tentative-pointer " + metadata_orphan);
                        return iceberg::IOError("probe: fail after inner property-only commit");
                    });
                });
        } catch (const Failure& error) {
            Check(!error.uncertain, "successful update rollback was marked unknown");
            outer_failed = true;
        }
        Check(outer_failed && update_guards == 1 && store->Base(collection_id) == initial_base,
            "outer metadata rollback did not restore base");
        const auto operation = store->FindOperation(collection_id, operation_id);
        Check(operation && operation->state == OperationState::Bound && !operation->outcome,
            "rollback retained terminal receipt or lost binding");
        Check(ObserverPointer(db, "events") == initial_location, "external pointer not restored after rollback");
        Check(!metadata_orphan.empty() && std::filesystem::exists(io->Physical(metadata_orphan)),
            "candidate root should remain orphan");
        store->Reopen();
        catalog = Take(iceberg::sql::SqlCatalog::Make(config, io, store), "SqlCatalog::Make after Reopen");
        Check(!Take(catalog->LoadTable(Identifier("events")), "LoadTable after rollback")
                   ->properties()
                   .configs()
                   .contains("probe.property"),
            "rolled-back property visible after reopen");
        std::cout << "PASS real property-only update: " << initial_location << " -> tentative " << metadata_orphan
                  << " -> outer rollback " << initial_location << "; Bound survives, no receipt\n";

        // A test-only fault actor publishes the already-written real
        // metadata-only root. This does not use or claim a production
        // metadata-update API.
        Database actor;
        actor.Open(db, SQLITE_OPEN_READWRITE, {});
        actor.Execute("PRAGMA synchronous=EXTRA");
        actor.Execute("BEGIN IMMEDIATE");
        actor.Execute("UPDATE iceberg_tables SET "
                      "metadata_location=?,previous_metadata_location=? WHERE "
                      "table_name='events'",
            {metadata_orphan, initial_location});
        Check(actor.Changes() == 1, "fault actor failed to replace base");
        actor.Execute("COMMIT");
        actor.Close();
        auto changed_table =
            Take(catalog->LoadTable(Identifier("events")), "LoadTable after fault-actor metadata-only replacement");
        Check(changed_table->properties().configs().at("probe.property") == "changed" &&
                changed_table->snapshots().empty(),
            "fault actor did not publish a real same-null-snapshot metadata "
            "change");
        Check(!store->Base(collection_id).current_snapshot_id &&
                store->Base(collection_id).metadata_location != initial_location,
            "fault actor unexpectedly changed snapshot identity");
        bool stale_rejected = false;
        bool stale_body = false;
        bool stale_guard = false;
        try {
            store->PublishAppend(
                binding,
                synthetic_receipt,
                [&](std::string_view) -> glamdb::Status {
                    stale_guard = true;
                    return {};
                },
                [&]() -> iceberg::Status {
                    stale_body = true;
                    return {};
                });
        } catch (const Failure& error) {
            Check(error.detail.code == "STALE_BASE" && !error.uncertain, "wrong stale-base failure");
            stale_rejected = true;
        }
        Check(stale_rejected && !stale_body && !stale_guard && ObserverPointer(db, "events") == metadata_orphan,
            "stale prepared operation ran or replaced newer root");
        Check(glamdb::test::Present(store->FindOperation(collection_id, operation_id)).state == OperationState::Bound,
            "stale rejection unexpectedly completed operation");
        std::cout << "PASS same-null-snapshot metadata replacement rejects "
                     "stale prepared base before body/guard\n";
        const auto fence_before = store->FenceValue();
        store->DurabilityFence();
        Check(store->FenceValue() == fence_before + 1, "durability fence did not increment");
        store->Reopen();
        catalog = Take(iceberg::sql::SqlCatalog::Make(config, io, store), "SqlCatalog::Make final reopen");
        Check(
            std::string(Take(catalog->LoadTable(Identifier("events")), "final LoadTable")->metadata_file_location()) ==
                metadata_orphan,
            "metadata-only fault-actor pointer lost on reopen");
        std::cout << "PASS final adapter reopen + real LoadTable; fence=" << store->FenceValue() << '\n';
        for (const auto& event : trace->events) {
            std::cout << "trace=" << event << '\n';
        }
        std::cout << "NOT_VALIDATED durable FileIO, Parquet/Avro closure, full "
                     "append, recovery cross-check, power loss\n";
        std::cout << "ALL_PROBE_CASES_PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "PROBE_FAILED " << error.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "PROBE_FAILED unknown exception\n";
        return 1;
    }
}
