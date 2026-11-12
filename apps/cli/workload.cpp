#include "workload.h"

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <reproc++/reproc.hpp>
#include <stdexcept>
#include <string>
#include <vector>

namespace glamdb::NClient {
namespace {
volatile std::sig_atomic_t StopSignal = 0;

void OnSignal(int signal)
{
    StopSignal = signal;
}

int RunPython(const std::vector<std::string>& arguments)
{
    const auto* configured = std::getenv("GLAMDB_PYTHON");
    const std::string python = configured && *configured ? configured : "python3";
    // A bootstrap supplies an actionable error even if the package is absent.
    const std::string bootstrap =
        "import runpy,sys\n"
        "try:\n runpy.run_module('glamdb_workload',run_name='__main__')\n"
        "except ModuleNotFoundError:\n"
        " print(\"Install workload dependencies: python -m pip install -e sdk/python "
        "-e 'tools/workload[generate,bench,arrow]'; select that Python with GLAMDB_PYTHON\",file=sys.stderr)\n"
        " sys.exit(4)\n";
    std::vector<std::string> argv{python, "-c", bootstrap};
    argv.insert(argv.end(), arguments.begin(), arguments.end());
    reproc::process child;
    reproc::options options;
    options.redirect.parent = true;
    options.stop = {{reproc::stop::terminate, reproc::milliseconds(5000)},
        {reproc::stop::kill, reproc::milliseconds(2000)},
        {reproc::stop::noop, reproc::milliseconds(0)}};
    StopSignal = 0;
    const auto previousInt = std::signal(SIGINT, OnSignal);
    const auto previousTerm = std::signal(SIGTERM, OnSignal);
    struct TSignalGuard
    {
        decltype(previousInt) Int;
        decltype(previousTerm) Term;
        ~TSignalGuard()
        {
            std::signal(SIGINT, Int);
            std::signal(SIGTERM, Term);
        }
    } guard{previousInt, previousTerm};
    if (const auto error = child.start(argv, options)) {
        std::cerr << "Cannot start Python. Set GLAMDB_PYTHON to the workload virtual environment's interpreter. "
                  << "Install with: python -m pip install -e sdk/python -e 'tools/workload[generate,bench,arrow]'\n";
        return 4;
    }
    while (true) {
        const auto [status, error] = child.wait(reproc::milliseconds(100));
        if (!error) {
            return StopSignal ? 128 + StopSignal : status;
        }
        if (StopSignal) {
            const auto [stopped, stopError] = child.stop(options.stop);
            (void)stopped;
            if (stopError) {
                throw std::runtime_error("Python did not stop within the termination timeout");
            }
            return 128 + StopSignal;
        }
        if (error != std::errc::timed_out) {
            throw std::runtime_error("cannot wait for Python process");
        }
    }
}

struct TArguments
{
    std::vector<std::string> Prefix;
    std::map<std::string, std::string> Values;
    std::map<std::string, bool> Flags;
    std::vector<CLI::Option*> Options;
};

std::shared_ptr<TArguments> Command(CLI::App& app, std::vector<std::string> prefix, std::optional<int>& exitCode)
{
    auto args = std::make_shared<TArguments>();
    args->Prefix = std::move(prefix);
    app.callback([args, &exitCode] {
        auto argv = args->Prefix;
        for (const auto* option : args->Options) {
            if (option->count() == 0) {
                continue;
            }
            const auto name = option->get_name();
            argv.push_back(name);
            if (args->Values.contains(name)) {
                argv.push_back(args->Values.at(name));
            }
        }
        exitCode = RunPython(argv);
    });
    return args;
}

void Value(CLI::App& app, TArguments& args, const std::string& name, const char* help, bool required = false)
{
    auto* option = app.add_option(name, args.Values[name], help);
    if (required) {
        option->required();
    }
    args.Options.push_back(option);
}

void Flag(CLI::App& app, TArguments& args, const std::string& name, const char* help)
{
    args.Options.push_back(app.add_flag(name, args.Flags[name], help));
}
} // namespace

void AddWorkloadCommands(CLI::App& app, std::optional<int>& exitCode)
{
    auto* generate = app.add_subcommand("generate", "Generate a bounded, reproducible dataset with Mimesis");
    auto generation = Command(*generate, {"generate"}, exitCode);
    for (const auto* name : {"--schema", "--rows", "--seed", "--output"}) {
        Value(*generate, *generation, name, "Dataset configuration", true);
    }
    Value(*generate, *generation, "--locale", "Mimesis locale (default en)");
    Value(*generate, *generation, "--part-rows", "Rows per part, up to 10000 (default 1000)");
    Value(*generate, *generation, "--part-bytes", "Maximum bytes per part (default 4194304)");
    auto* bench = app.add_subcommand("bench", "Run Locust SDK workloads and resolve/verify their journals");
    bench->require_subcommand(1);
    auto* tpch = bench->add_subcommand("tpch", "Manage a dedicated TPC-H benchmark database");
    tpch->require_subcommand(1);
    auto* clean = tpch->add_subcommand("clean", "Drop the exact database identity saved in a run state file");
    auto cleanup = Command(*clean, {"bench", "tpch", "clean"}, exitCode);
    Value(*clean, *cleanup, "--state", "Saved version-1 run state JSON file; never a database name", true);
    Value(*clean, *cleanup, "--ca-file", "Additional PEM certificate authority");
    Value(*clean, *cleanup, "--timeout", "SDK deadline in seconds (default 30)");
    auto* clickbench =
        bench->add_subcommand("clickbench", "Prepare, ingest, verify and measure a ClickBench hits dataset");
    clickbench->require_subcommand(1);
    auto* fetch = clickbench->add_subcommand("fetch-queries", "Fetch pinned query material and its original license");
    auto fetching = Command(*fetch, {"bench", "clickbench", "fetch-queries"}, exitCode);
    Value(*fetch, *fetching, "--output", "New cache directory; checksums are pinned in the adapter", true);
    auto* query =
        clickbench->add_subcommand("query", "Run all scan/reduce scenarios with independent source validation");
    auto querying = Command(*query, {"bench", "clickbench", "query"}, exitCode);
    for (const auto* name : {"--state", "--catalog", "--report"}) {
        Value(*query, *querying, name, "Original state, pinned query cache, or output JSON report", true);
    }
    for (const auto* name : {"--source",
             "--ids",
             "--mode",
             "--warmup",
             "--repeats",
             "--timeout",
             "--ca-file",
             "--memory-bytes",
             "--temp-bytes",
             "--max-candidate-bytes",
             "--max-result-rows",
             "--max-result-bytes"})
    {
        Value(*query, *querying, name, "Query campaign setting; see tools/workload/CLICKBENCH.md");
    }
    auto* prepare = clickbench->add_subcommand("prepare", "Convert all 105 columns to immutable bounded parts");
    auto preparation = Command(*prepare, {"bench", "clickbench", "prepare"}, exitCode);
    for (const auto* name : {"--source", "--output", "--type-profile"}) {
        Value(*prepare,
            *preparation,
            name,
            "Source Parquet, output directory, or explicit legacy-date/native profile",
            true);
    }
    Value(*prepare, *preparation, "--rows", "Deterministic prefix rows; omit for the complete source");
    Value(*prepare, *preparation, "--part-rows", "Maximum rows per append part (default 10000)");
    Value(*prepare, *preparation, "--part-bytes", "Maximum part bytes (default 4194304)");
    Value(*prepare, *preparation, "--max-source-bytes", "Source download/read budget (default 34359738368)");
    Value(*prepare, *preparation, "--max-converted-bytes", "Converted dataset budget (default 8589934592)");
    for (const auto* mode : {"init", "load", "status", "resolve", "verify", "size", "clean"}) {
        auto* command = clickbench->add_subcommand(mode);
        auto args = Command(*command, {"bench", "clickbench", mode}, exitCode);
        Value(*command,
            *args,
            "--state",
            "Original ClickBench run state; contains the fixed endpoint and identities",
            true);
        Value(*command, *args, "--ca-file", "Additional PEM certificate authority");
        Value(*command, *args, "--timeout", "SDK deadline in seconds (verify default 120, other commands 30)");
        const std::string_view action(mode);
        if (action == "init") {
            for (const auto* name : {"--dataset", "--url", "--name"}) {
                Value(*command,
                    *args,
                    name,
                    "Immutable dataset, native server endpoint, or dedicated database name",
                    true);
            }
            Value(*command, *args, "--limits", "Explicit finite database limits JSON");
            Value(*command,
                *args,
                "--max-journal-bytes",
                "Journal request-body budget, excluding SQLite overhead (default 1073741824)");
            Value(*command,
                *args,
                "--test-backend",
                "Explicit mock test fixture only; excluded from performance results");
        } else if (action == "load") {
            Flag(*command,
                *args,
                "--resume",
                "Resume original immutable requests after resolving ambiguous operations");
        } else if (action == "resolve") {
            Flag(*command,
                *args,
                "--retry",
                "Explicitly retry authoritative OPERATION_NOT_FOUND using original ID and bytes");
        } else if (action == "verify") {
            Value(*command, *args, "--format", "arrow or json (default arrow); compare complete row multiplicities");
        } else if (action == "size") {
            Value(*command,
                *args,
                "--report",
                "JSON report path; also writes CSV and Markdown using the same stem",
                true);
            Value(*command,
                *args,
                "--owned-store-dir",
                "Optional caller-owned local store UUID directory; never a remote path");
            Flag(*command, *args, "--include-footers", "Read column-chunk sizes for each committed Parquet file");
        }
    }
    for (const auto* mode : {"write", "read", "verify", "resolve"}) {
        auto* command = bench->add_subcommand(mode);
        auto args = Command(*command, {"bench", mode}, exitCode);
        Value(*command, *args, "--url", "Server URL; HTTPS required outside loopback", true);
        Value(*command, *args, "--ca-file", "Additional PEM certificate authority");
        Value(*command, *args, "--timeout", "SDK deadline in seconds (default 30)");
        if (std::string_view(mode) == "verify" || std::string_view(mode) == "resolve") {
            Value(*command, *args, "--run", "Existing workload report directory", true);
            if (std::string_view(mode) == "resolve") {
                Flag(*command,
                    *args,
                    "--retry",
                    "Explicitly resend OPERATION_NOT_FOUND operations with their original ID and bytes");
            } else {
                Value(*command, *args, "--format", "json or arrow (default json)");
            }
            continue;
        }
        for (const auto* name : {"--database", "--collection", "--report"}) {
            Value(*command, *args, name, "Workload target or report directory", true);
        }
        Value(*command, *args, "--users", "Concurrent users, not target RPS (default 1)");
        Value(*command, *args, "--spawn-rate", "New users per second (default 1)");
        Value(*command, *args, "--duration", "Maximum run time, e.g. 60s (default 60s)");
        Value(*command, *args, "--stop-timeout", "Graceful request drain seconds (default 3)");
        Value(*command, *args, "--max-journal-bytes", "Per-worker journal payload budget (default 1073741824)");
        Flag(*command, *args, "--master", "Locust master; workers must share the report directory");
        Flag(*command, *args, "--worker", "Locust worker using an existing shared run");
        Value(*command, *args, "--master-host", "Locust master hostname (default 127.0.0.1)");
        Value(*command, *args, "--master-port", "Locust control port (default 5557)");
        Value(*command, *args, "--expect-workers", "Workers required before starting (default 1)");
        Value(*command, *args, "--processes", "Local worker processes behind one master and merged report");
        if (std::string_view(mode) == "write") {
            Value(*command, *args, "--dataset", "Generated dataset directory", true);
        } else {
            Value(*command, *args, "--query", "SDK scan parameters as JSON", true);
            Value(*command, *args, "--format", "json or arrow (default json)");
            Value(*command, *args, "--consume", "rows (decoded Python rows) or batches (Arrow only; default rows)");
        }
    }
}
} // namespace glamdb::NClient
