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
        if (std::string_view(mode) == "write") {
            Value(*command, *args, "--dataset", "Generated dataset directory", true);
        } else {
            Value(*command, *args, "--query", "SDK scan parameters as JSON", true);
            Value(*command, *args, "--format", "json or arrow (default json)");
        }
    }
}
} // namespace glamdb::NClient
