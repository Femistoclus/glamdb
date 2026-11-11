#include "apps/client/http.h"
#include "workload.h"
#include <CLI/CLI.hpp>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <optional>
#include <stdexcept>
#include <unistd.h>
#include <utility>

namespace {
class TTemporaryOutput
{
public:
    explicit TTemporaryOutput(const std::filesystem::path& destination)
        : Path_(destination.string() + ".tmp.XXXXXX")
        , Descriptor_(mkstemp(Path_.data()))
    {
        if (Descriptor_ < 0) {
            throw std::runtime_error("cannot create temporary output file");
        }
    }
    TTemporaryOutput(const TTemporaryOutput&) = delete;
    TTemporaryOutput& operator=(const TTemporaryOutput&) = delete;
    ~TTemporaryOutput()
    {
        if (Descriptor_ >= 0) {
            close(Descriptor_);
        }
        if (!Path_.empty()) {
            std::error_code ignored;
            std::filesystem::remove(Path_, ignored);
        }
    }
    void Publish(const std::filesystem::path& destination, std::string_view bytes)
    {
        while (!bytes.empty()) {
            const auto written = write(Descriptor_, bytes.data(), bytes.size());
            if (written < 0 && errno == EINTR) {
                continue;
            }
            if (written <= 0) {
                throw std::runtime_error("output write failed");
            }
            bytes.remove_prefix(static_cast<size_t>(written));
        }
        if (fsync(Descriptor_) != 0) {
            throw std::runtime_error("output flush failed");
        }
        if (close(std::exchange(Descriptor_, -1)) != 0) {
            throw std::runtime_error("output close failed");
        }
        std::filesystem::rename(Path_, destination);
        Path_.clear();
    }

private:
    std::string Path_;
    int Descriptor_;
};

// HTTP field values are bytes, not UTF-8. Preserve obs-text as the matching
// Latin-1 code points when exporting JSON, without altering response body bytes.
std::string HeaderText(std::string_view bytes)
{
    std::string result;
    for (const unsigned char byte : bytes) {
        if (byte < 128) {
            result += static_cast<char>(byte);
        } else {
            result += static_cast<char>(0xc0 | (byte >> 6));
            result += static_cast<char>(0x80 | (byte & 0x3f));
        }
    }
    return result;
}
nlohmann::json HeaderJson(const glamdb::NClient::TResponse& response)
{
    nlohmann::json headers = nlohmann::json::object();
    for (const auto& [name, values] : response.Headers) {
        if (values.size() == 1) {
            headers[name] = HeaderText(values.front());
        } else {
            auto array = nlohmann::json::array();
            for (const auto& value : values) {
                array.push_back(HeaderText(value));
            }
            headers[name] = std::move(array);
        }
    }
    return {{"status", response.Status}, {"headers", std::move(headers)}};
}
void Write(const std::filesystem::path& path, std::string_view bytes)
{
    if (path.empty() || path == "-") {
        std::cout.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        std::cout.flush();
        if (!std::cout) {
            throw std::runtime_error("output write failed");
        }
    } else {
        std::error_code error;
        const auto status = std::filesystem::symlink_status(path, error);
        if ((error && error != std::errc::no_such_file_or_directory) ||
            (std::filesystem::exists(status) && !std::filesystem::is_regular_file(status)))
        {
            throw std::runtime_error("output destination must be a regular file, not a symlink or special file");
        }
        TTemporaryOutput temporary(path);
        temporary.Publish(path, bytes);
    }
}
}
int main(int argc, char** argv)
try {
#ifdef SIGPIPE
    std::signal(SIGPIPE, SIG_IGN);
#endif
    CLI::App app{"GlamDB network client"};
    app.require_subcommand(1);
    std::optional<int> workloadExitCode;
    glamdb::NClient::AddWorkloadCommands(app, workloadExitCode);
    auto* request = app.add_subcommand("request", "Call any server API route; write exact response bytes");
    auto* create = app.add_subcommand("create-store", "Create a database through the running server");
    glamdb::NClient::TRequest input;
    std::string url, host = "127.0.0.1", path, file, output, headers, name;
    uint16_t port = 8080;
    for (auto* command : {request, create}) {
        command->add_option("--url", url, "Full http(s) URL (create-store: server base URL)");
        command->add_option("--host", host, "Loopback HTTP hostname; use --url for HTTPS");
        command->add_option("--port", port, "HTTP port");
        command->add_option("--ca-file", input.CaFile, "Additional PEM certificate authority");
        command->add_option("--timeout", input.TimeoutSeconds, "Whole request deadline in seconds");
        command->add_option("--output,-o", output, "Response file; default stdout; binary-safe");
        command->add_option("--headers", headers, "Write status and response headers as JSON to this file");
    }
    request->add_option("--path", path, "API route (required unless --url includes the route)");
    request->add_option("--method", input.Method, "GET or POST");
    request->add_option("--body", file, "Request bytes file, or - for stdin");
    request->add_option("--accept", input.Accept, "Response media type, including application/vnd.apache.arrow.stream");
    request->add_option("--content-type", input.ContentType, "Request media type");
    create->add_option("--name", name, "Database name")->required();
    if (argc == 1) {
        std::cout << app.help();
        return 0;
    }
    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        return app.exit(error) == 0 ? 0 : 2;
    }
    if (workloadExitCode) {
        return *workloadExitCode;
    }
    if (!url.empty() &&
        (request->count("--host") || request->count("--port") || create->count("--host") || create->count("--port")))
    {
        throw std::runtime_error("--url cannot be combined with --host/--port");
    }
    if (headers == "-" || (!headers.empty() && headers == output)) {
        throw std::runtime_error("headers require a separate output file");
    }
    if (create->parsed()) {
        path = "/databases";
        input.Method = "POST";
        input.Body = nlohmann::json{{"name", name}}.dump();
    } else if (!file.empty()) {
        std::ifstream source;
        if (file != "-") {
            source.open(file, std::ios::binary);
            if (!source) {
                throw std::runtime_error("cannot open input file");
            }
        }
        auto& stream = file == "-" ? std::cin : source;
        char block[4096];
        while (stream.read(block, sizeof(block)) || stream.gcount() > 0) {
            input.Body.append(block, static_cast<size_t>(stream.gcount()));
            if (input.Body.size() > size_t{16} * 1024 * 1024) {
                throw std::runtime_error("input body too large");
            }
        }
        if (stream.bad()) {
            throw std::runtime_error("input read failed");
        }
        if (!request->count("--method")) {
            input.Method = "POST";
        }
    }
    if (!path.empty() && (!path.starts_with('/') || path.starts_with("//"))) {
        throw std::runtime_error("path must begin with one slash");
    }
    if (url.empty()) {
        if (path.empty()) {
            throw std::runtime_error("--path or --url is required");
        }
        if (host.find(':') != std::string::npos && !host.starts_with('[')) {
            host = '[' + host + ']';
        }
        input.Url = "http://" + host + ':' + std::to_string(port) + path;
    } else {
        if (!path.empty()) {
            if (url.find_first_of("?#") != std::string::npos) {
                throw std::runtime_error("base URL cannot contain a query or fragment when using --path");
            }
            while (url.ends_with('/')) {
                url.pop_back();
            }
            url += path;
        }
        input.Url = url;
    }
    const auto response = glamdb::NClient::SendRequest(input);
    try {
        if (!headers.empty()) {
            Write(headers, HeaderJson(response).dump(2) + '\n');
        }
        Write(output, response.Body);
    } catch (const std::exception& error) {
        std::cerr << error.what() << "; output failed after complete HTTP " << response.Status;
        if (input.Method == "POST") {
            std::cerr << "; server operation may already have committed";
        }
        std::cerr << '\n';
        return 4;
    }
    return response.Status >= 200 && response.Status < 300 ? 0 : 1;
} catch (const glamdb::NClient::TOutcomeUnknown& error) {
    std::cerr << error.what() << '\n';
    return 3;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
} catch (...) {
    return 100;
}
