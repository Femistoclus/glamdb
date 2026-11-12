#include "core/telemetry/structured_log.h"
#include "storage/engine.h"

#include "apps/transport/codec.h"

#include <CLI/CLI.hpp>

#include <chrono>
#include <iostream>
#include <string>
#include <string_view>

int RunCommand(int argc, char** argv)
try {
    CLI::App app{"GlamDB offline store administration"};
    app.require_subcommand(1);
    std::string root;
    std::string id;
    auto* create = app.add_subcommand("create-store", "Create an empty store");
    auto* inspect = app.add_subcommand("inspect-store", "Inspect an existing store");
    auto* read = app.add_subcommand("read", "Read a bounded snapshot from an offline store");
    auto* quiesce = app.add_subcommand("quiesce-store", "Close SQLite and retain store ownership until stdin closes");
    for (auto* command : {create, inspect, read, quiesce}) {
        command->add_option("--root", root, "Absolute store path")->required();
        command->add_option("--store-id", id, "Store UUID")->required();
    }
    std::string collectionID, snapshot = "latest";
    int64_t limit = 10000;
    read->add_option("--collection-id", collectionID, "Collection UUID")->required();
    read->add_option("--snapshot", snapshot, "latest, initial, or an exact snapshot ID");
    read->add_option("--limit", limit, "Maximum returned rows")->check(CLI::Range(0, 10000));
    if (argc == 1) {
        std::cout << app.help(argv[0]);
        return 0;
    }
    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        const auto code = app.exit(error);
        return code == 0 ? 0 : 2;
    }
    const auto opened = create->parsed() ? glamdb::CreateStore({id, root}) : glamdb::OpenExisting({root, id});
    if (!opened) {
        std::cerr << opened.error().code << ": " << opened.error().message << '\n';
        return 1;
    }
    const auto& info = (*opened)->GetStoreInfo();
    if (read->parsed()) {
        using namespace glamdb::NTransport;
        auto selector = glamdb::SnapshotSelector::Latest();
        if (snapshot == "initial") {
            selector = glamdb::SnapshotSelector::InitialEmpty();
        } else if (snapshot != "latest") {
            selector = glamdb::SnapshotSelector::Id(ParseInt64(snapshot));
        }
        const auto cursor = Unwrap(
            (*opened)->Scan({.collection_id = collectionID, .selector = selector, .limit = limit, .batch_rows = 128}));
        TJson rows = TJson::array();
        size_t rowBytes = 0;
        for (;;) {
            const auto batch = Unwrap(cursor->Next());
            if (!batch) {
                break;
            }
            for (auto& row : EncodeRows(*batch)) {
                rowBytes += row.dump().size() + 1;
                Require(rowBytes <= MaxResponseBytes - 4096, "offline rows exceed 16 MiB", "RESOURCE_EXHAUSTED");
                rows.push_back(std::move(row));
            }
        }
        const auto& scan = cursor->Info();
        const auto view = scan.selector_resolved.kind == glamdb::SnapshotSelectorKind::InitialEmpty
            ? "initial"
            : std::to_string(scan.selector_resolved.snapshot_id);
        const TJson result{{"schema", EncodeSchema(*scan.output_schema)},
            {"identity", EncodeIdentity(scan.identity)},
            {"snapshot_id", view},
            {"rows", std::move(rows)}};
        const auto bytes = result.dump();
        Require(bytes.size() <= MaxResponseBytes, "offline response exceeds 16 MiB", "RESOURCE_EXHAUSTED");
        std::cout << bytes << '\n';
    }
    if (quiesce->parsed()) {
        const auto closed = (*opened)->Shutdown(std::chrono::milliseconds(5000));
        if (!closed) {
            std::cerr << closed.error().code << ": " << closed.error().message << '\n';
            return 1;
        }
        // Shutdown closes SQLite. Retaining this Engine retains its independent
        // directory StoreLease, so another owner cannot enter during the copy.
        std::cout << "{\"state\":\"quiesced\",\"catalog_closed\":true,\"store_id\":\"" << info.store_id << "\"}\n";
        std::cout.flush();
        if (!std::cout) {
            return 1;
        }
        char release = 0;
        std::cin.get(release);
        return 0;
    }
    // StoreInfo contains validated UUID/profile tokens, never arbitrary
    // strings.
    if (!read->parsed()) {
        std::cout << "{\"store_id\":\"" << info.store_id << "\",\"profile_id\":\"" << info.profile_id
                  << "\",\"format_version\":" << info.format_version << ",\"durability_validation\":\"designed\"}\n";
    }
    const auto closed = (*opened)->Shutdown(std::chrono::milliseconds(5000));
    if (!closed) {
        std::cerr << closed.error().code << ": " << closed.error().message << '\n';
        return 1;
    }
    std::cout.flush();
    if (!std::cout) {
        std::cerr << "output failed; store operation may have completed\n";
        return 1;
    }
    return 0;
} catch (const glamdb::NTransport::TFailure& error) {
    std::cerr << error.Detail.code << ": " << error.Detail.message << '\n';
    return 1;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
} catch (...) {
    return 100;
}

int main(int argc, char** argv)
try {
    using namespace NGlamDB::NTelemetry;
    TLogRuntime logging;
    auto* logger = logging.Get();
    const auto requestID = logger ? logger->NewRequestID() : 0;
    const auto operation = argc > 1 && std::string_view(argv[1]) == "create-store" ? ELogOperation::CreateStore
        : argc > 1 && std::string_view(argv[1]) == "inspect-store"                 ? ELogOperation::InspectStore
        : argc > 1 && std::string_view(argv[1]) == "read"                          ? ELogOperation::Scan
        : argc > 1 && std::string_view(argv[1]) == "quiesce-store"                 ? ELogOperation::QuiesceStore
                                                                                   : ELogOperation::Other;
    const auto started = std::chrono::steady_clock::now();
    TLogScope scope(logger, operation, requestID);
    const auto result = RunCommand(argc, argv);
    const auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started).count();
    if (logger) {
        logger->TryLog({.Event = ELogEvent::RequestCompleted,
            .Operation = operation,
            .Outcome = result == 0 ? ELogOutcome::Ok : ELogOutcome::Error,
            .RequestID = requestID,
            .DurationUs = static_cast<std::uint64_t>(duration),
            .StatusCode = static_cast<std::uint32_t>(result)});
    }
    logging.SetResult(result == 0);
    return result;
} catch (...) {
    return 100;
}
