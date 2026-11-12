#include "apps/server/databases.h"
#include "apps/server/metrics.h"
#include "apps/server/server.h"
#include "storage/resources.h"

#include "core/telemetry/structured_log.h"

#include <CLI/CLI.hpp>

#include <iostream>

int main(int argc, char** argv)
try {
    using namespace NGlamDB::NTelemetry;
    TLogOptions logOptions;
    CLI::App app{"GlamDB native HTTP server"};
    app.add_subcommand("serve", "Serve an existing store or a managed database directory")->fallthrough();
    std::string root, storeID, uiRoot, dataDir, metricsAddress, logDirectory;
    std::string address = "127.0.0.1";
    uint16_t port = 8080;
    int timeoutMs = 10000;
    glamdb::NServer::TScanLimits scanLimits;
    glamdb::ResourceLimits resourceLimits;
    glamdb::NServer::TReaderLimits readerLimits;
    glamdb::Limits engineLimits;
    int64_t scanIdle = 60, scanLifetime = 0, completedTtl = 60;
    app.add_option("--data-dir", dataDir, "Server directory for managed databases");
    app.add_option("--root", root, "Legacy single-store path");
    app.add_option("--store-id", storeID, "Expected legacy store UUID");
    app.add_option("--max-collections", engineLimits.max_collections, "Legacy store collection limit (default 8)")
        ->check(CLI::Range(int64_t{1}, glamdb::Limits::MaxSupportedCollections));
    app.add_option("--listen", address, "HTTP listen address; use TLS proxy outside loopback");
    app.add_option("--port", port, "HTTP port; zero selects an available port");
    app.add_option("--metrics-listen-address", metricsAddress, "Private Prometheus IP:port; disabled by default");
    app.add_option("--log-directory", logDirectory, "Private bounded JSONL log directory; disabled by default");
    app.add_option("--ui-root", uiRoot, "Workbench directory (defaults to installed share/glamdb/ui)");
    app.add_option("--request-timeout-ms", timeoutMs, "Total header/body read timeout")->check(CLI::Range(100, 60000));
    app.add_option("--http-readers", readerLimits.Active, "Active HTTP readers (default 4)")->check(CLI::Range(1, 4));
    app.add_option("--http-readers-per-database",
           readerLimits.PerDatabase,
           "Active HTTP readers per database (default 2)")
        ->check(CLI::Range(1, 4));
    app.add_option("--scan-live-limit", scanLimits.Live, "Global live scan cursor limit")->check(CLI::Range(1, 4096));
    app.add_option("--scan-live-per-database", scanLimits.LivePerDatabase, "Live scan limit per database")
        ->check(CLI::Range(1, 4096));
    app.add_option("--scan-completed-limit", scanLimits.Completed, "Global completed replay retention limit")
        ->check(CLI::Range(0, 4096));
    app.add_option("--scan-completed-per-database", scanLimits.CompletedPerDatabase, "Completed replays per database")
        ->check(CLI::Range(0, 4096));
    app.add_option("--scan-replay-bytes", scanLimits.ReplayBytes, "Global serialized replay reservation limit");
    app.add_option("--scan-replay-bytes-per-database",
        scanLimits.ReplayBytesPerDatabase,
        "Replay reservation per database");
    app.add_option("--scan-idle-seconds", scanIdle, "Live cursor idle timeout")->check(CLI::Range(1, 86400));
    app.add_option("--scan-lifetime-seconds", scanLifetime, "Absolute cursor lifetime; zero disables")
        ->check(CLI::Range(0, 31536000));
    app.add_option("--scan-completed-ttl-seconds", completedTtl, "Completed replay TTL; replay does not refresh it")
        ->check(CLI::Range(1, 86400));
    app.add_option("--resource-data-bytes", resourceLimits.memory_bytes[0], "Process data reservation cap");
    app.add_option("--resource-metadata-bytes", resourceLimits.memory_bytes[1], "Process metadata reservation cap");
    app.add_option("--resource-control-bytes",
        resourceLimits.memory_bytes[2],
        "Process ordinary control reservation cap");
    app.add_option("--resource-completion-bytes",
        resourceLimits.memory_bytes[3],
        "Protected completion reservation cap");
    app.add_option("--resource-recovery-bytes", resourceLimits.memory_bytes[4], "Protected recovery reservation cap");
    app.add_option("--log-success-sample-every", logOptions.SuccessSampleEvery, "Log one in N successful requests")
        ->check(CLI::PositiveNumber);
    app.add_option("--log-max-success-per-second",
           logOptions.MaxSuccessPerSecond,
           "Maximum sampled successes per second")
        ->check(CLI::Range(1, 1000000));
    CLI11_PARSE(app, argc, argv);
    glamdb::NServer::ValidateReaderLimits(readerLimits);
    scanLimits.Idle = std::chrono::seconds(scanIdle);
    scanLimits.Lifetime = std::chrono::seconds(scanLifetime);
    scanLimits.CompletedTtl = std::chrono::seconds(completedTtl);
    glamdb::NServer::ValidateScanLimits(scanLimits, &resourceLimits);
    auto resources = std::make_shared<glamdb::ResourceOwner>(resourceLimits);
    logOptions.Directory = logDirectory;
    if (!logDirectory.empty() && app.get_option("--log-success-sample-every")->count() == 0) {
        logOptions.SuccessSampleEvery = 1;
    }
    TLogRuntime logging(logOptions);
    TLogScope scope(logging.Get(), ELogOperation::Other, logging.Get() ? logging.Get()->NewRequestID() : 0);
    glamdb::NTransport::Require((!dataDir.empty() && root.empty() && storeID.empty()) ||
            (dataDir.empty() && !root.empty() && !storeID.empty()),
        "use --data-dir, or both --root and --store-id for a legacy store");
    glamdb::NTransport::Require(dataDir.empty() || app.count("--max-collections") == 0,
        "configure managed collection limits through the database lifecycle API");
    const auto token = glamdb::NTransport::ReadToken();
    if (uiRoot.empty()) {
        std::error_code error;
        auto executable = std::filesystem::canonical("/proc/self/exe", error);
        if (error) {
            executable = std::filesystem::canonical(argv[0], error);
        }
        const auto installed = executable.parent_path().parent_path() / "share/glamdb/ui";
        if (!error && std::filesystem::is_directory(installed, error)) {
            uiRoot = installed.string();
        }
    }
    if (!dataDir.empty()) {
        auto databases = std::make_shared<glamdb::NServer::TDatabases>(dataDir, "", resources);
        glamdb::NServer::TMetrics metrics(metricsAddress, [databases] {
            return databases->IsReady();
        });
        glamdb::NServer::TRoutes routes(databases, uiRoot, token, scanLimits);
        glamdb::NServer::RunServer(routes,
            address,
            port,
            std::chrono::milliseconds(timeoutMs),
            96,
            logging.Get(),
            readerLimits);
        routes.CancelScans();
        databases->Shutdown();
        logging.SetResult(true);
        return 0;
    }
    auto engine = glamdb::NTransport::Unwrap(glamdb::OpenExisting(
        {.absolute_root = root, .expected_store_id = storeID, .limits = engineLimits, .resources = resources}));
    glamdb::NServer::TMetrics metrics(metricsAddress, [engine] {
        return engine->GetHealth().write_ready;
    });
    glamdb::NServer::TRoutes routes(engine, uiRoot, token, scanLimits);
    glamdb::NServer::RunServer(routes,
        address,
        port,
        std::chrono::milliseconds(timeoutMs),
        96,
        logging.Get(),
        readerLimits);
    routes.CancelScans();
    const auto closed = engine->Shutdown(std::chrono::milliseconds(60000));
    if (!closed) {
        std::cerr << closed.error().code << ": " << closed.error().message << '\n';
        return 1;
    }
    logging.SetResult(true);
    return 0;
} catch (const glamdb::NTransport::TFailure& error) {
    std::cerr << error.Detail.code << ": " << error.Detail.message << '\n';
    return 1;
} catch (const std::exception& error) {
    std::cerr << "server failed: " << error.what() << '\n';
    return 1;
}
