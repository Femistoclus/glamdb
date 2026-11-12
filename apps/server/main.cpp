#include "apps/server/databases.h"
#include "apps/server/server.h"

#include <CLI/CLI.hpp>

#include <iostream>

int main(int argc, char** argv)
try {
    using namespace NGlamDB::NTelemetry;
    TLogOptions logOptions;
    CLI::App app{"GlamDB native HTTP server"};
    std::string root, storeID, uiRoot, dataDir;
    std::string address = "127.0.0.1";
    uint16_t port = 8080;
    int timeoutMs = 10000;
    app.add_option("--data-dir", dataDir, "Server directory for managed databases");
    app.add_option("--root", root, "Legacy single-store path");
    app.add_option("--store-id", storeID, "Expected legacy store UUID");
    app.add_option("--listen", address, "HTTP listen address; use TLS proxy outside loopback");
    app.add_option("--port", port, "HTTP port; zero selects an available port");
    app.add_option("--ui-root", uiRoot, "Workbench directory (defaults to installed share/glamdb/ui)");
    app.add_option("--request-timeout-ms", timeoutMs, "Total header/body read timeout")->check(CLI::Range(100, 60000));
    app.add_option("--log-success-sample-every", logOptions.SuccessSampleEvery, "Log one in N successful requests")
        ->check(CLI::PositiveNumber);
    app.add_option("--log-max-success-per-second",
           logOptions.MaxSuccessPerSecond,
           "Maximum sampled successes per second")
        ->check(CLI::Range(1, 1000000));
    CLI11_PARSE(app, argc, argv);
    TLogRuntime logging(logOptions);
    TLogScope scope(logging.Get(), ELogOperation::Other, logging.Get() ? logging.Get()->NewRequestID() : 0);
    glamdb::NTransport::Require((!dataDir.empty() && root.empty() && storeID.empty()) ||
            (dataDir.empty() && !root.empty() && !storeID.empty()),
        "use --data-dir, or both --root and --store-id for a legacy store");
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
        auto databases = std::make_shared<glamdb::NServer::TDatabases>(dataDir);
        glamdb::NServer::TRoutes routes(databases, uiRoot, token);
        glamdb::NServer::RunServer(routes, address, port, std::chrono::milliseconds(timeoutMs), 96, logging.Get());
        routes.CancelScans();
        databases->Shutdown();
        logging.SetResult(true);
        return 0;
    }
    auto engine = glamdb::NTransport::Unwrap(glamdb::OpenExisting({root, storeID}));
    glamdb::NServer::TRoutes routes(engine, uiRoot, token);
    glamdb::NServer::RunServer(routes, address, port, std::chrono::milliseconds(timeoutMs), 96, logging.Get());
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
