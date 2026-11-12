#include "apps/client/http.h"
#include <algorithm>
#include <asio.hpp>
#include <asio/ssl.hpp>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <openssl/sha.h>
#include <sstream>
#include <stdexcept>

namespace glamdb::NClient {
namespace {
void Check(bool condition, const char* message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}
bool Clean(std::string_view value)
{
    return std::ranges::all_of(value, [](unsigned char c) {
        return c >= 33 && c <= 126;
    });
}
size_t Number(std::string_view value, const char* message = "invalid response length")
{
    size_t result = 0;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
    Check(parsed.ec == std::errc{} && parsed.ptr == value.data() + value.size(), message);
    return result;
}
struct TUrl
{
    bool Tls;
    std::string Host, Port, Target, Authority;
};
TUrl ParseUrl(std::string_view url)
{
    TUrl result;
    result.Tls = url.starts_with("https://");
    Check(result.Tls || url.starts_with("http://"), "URL must use http:// or https://");
    url.remove_prefix(result.Tls ? 8 : 7);
    const auto slash = url.find_first_of("/?#");
    result.Authority = url.substr(0, slash);
    result.Target = slash == std::string_view::npos ? "/" : std::string(url.substr(slash));
    if (result.Target.starts_with('?')) {
        result.Target.insert(0, "/");
    }
    Check(Clean(result.Authority) && !result.Authority.empty() && result.Authority.find('@') == std::string::npos &&
            Clean(result.Target) && result.Target.find('#') == std::string::npos,
        "invalid URL");
    std::string_view authority = result.Authority;
    result.Port = result.Tls ? "443" : "80";
    if (authority.starts_with('[')) {
        const auto end = authority.find(']');
        Check(end != std::string_view::npos, "invalid IPv6 URL");
        result.Host = authority.substr(1, end - 1);
        authority.remove_prefix(end + 1);
        Check(authority.empty() || authority.starts_with(':'), "invalid URL authority");
        if (!authority.empty()) {
            result.Port = authority.substr(1);
        }
    } else {
        const auto colon = authority.find(':');
        result.Host = authority.substr(0, colon);
        if (colon != std::string_view::npos) {
            result.Port = authority.substr(colon + 1);
        }
    }
    const auto port = Number(result.Port, "invalid URL port");
    Check(!result.Host.empty() && port > 0 && port <= 65535, "invalid URL port");
    return result;
}
TResponse ParseResponse(const std::string& wire, bool complete = true)
{
    constexpr size_t MaxHeader = 16384, MaxBody = size_t{64} * 1024 * 1024;
    Check(wire.find("\r\n\r\n") != std::string::npos, "missing response header terminator");
    const auto headerEnd = wire.find("\r\n\r\n") + 4;
    Check(headerEnd <= MaxHeader && wire.starts_with("HTTP/1.1 "), "invalid HTTP response");
    const auto lineEnd = wire.find("\r\n");
    Check(lineEnd >= 13 && wire[12] == ' ', "invalid HTTP status line");
    TResponse response;
    response.Status = static_cast<int>(Number(std::string_view(wire).substr(9, 3), "invalid HTTP status"));
    Check(response.Status >= 200 && response.Status <= 599, "unsupported HTTP status");
    size_t position = lineEnd + 2;
    while (position < headerEnd - 2) {
        const auto end = wire.find("\r\n", position);
        const auto colon = wire.find(':', position);
        Check(colon < end && colon > position, "invalid response header");
        auto name = wire.substr(position, colon - position);
        Check(std::ranges::all_of(name,
                  [](unsigned char c) {
                      return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                          std::string_view("!#$%&'*+-.^_`|~").find(static_cast<char>(c)) != std::string_view::npos;
                  }),
            "invalid response header name");
        std::ranges::transform(name, name.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        auto value = wire.substr(colon + 1, end - colon - 1);
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
            value.erase(0, 1);
        }
        while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) {
            value.pop_back();
        }
        Check(std::ranges::all_of(value,
                  [](unsigned char c) {
                      return c == '\t' || (c >= 32 && c != 127);
                  }),
            "invalid response header value");
        auto& values = response.Headers[name];
        const bool singular = name == "content-length" || name == "transfer-encoding" || name == "x-content-sha256" ||
            name == "x-glamdb-next-cursor";
        Check(!singular || values.empty(), "duplicate response header");
        values.push_back(std::move(value));
        position = end + 2;
    }
    Check(!response.Headers.contains("transfer-encoding"), "transfer encoding is unsupported");
    Check(!response.Headers.contains("content-encoding") ||
            response.Headers.at("content-encoding") == std::vector<std::string>{"identity"},
        "content encoding is unsupported");
    Check(response.Headers.contains("content-length"), "missing response content-length");
    const auto length = Number(response.Headers.at("content-length").front());
    Check(length <= MaxBody && wire.size() - headerEnd <= length, "response exceeds declared or configured length");
    if (!complete) {
        return response;
    }
    Check(wire.size() - headerEnd == length, "interrupted response");
    response.Body = wire.substr(headerEnd);
    if (response.Headers.contains("x-content-sha256")) {
        unsigned char digest[SHA256_DIGEST_LENGTH];
        SHA256(reinterpret_cast<const unsigned char*>(response.Body.data()), response.Body.size(), digest);
        constexpr char Hex[] = "0123456789abcdef";
        std::string expected;
        for (const auto byte : digest) {
            expected += Hex[byte >> 4];
            expected += Hex[byte & 15];
        }
        Check(response.Headers.at("x-content-sha256").front() == expected, "response checksum mismatch");
    }
    return response;
}
}
TResponse SendRequest(const TRequest& request)
{
    const auto url = ParseUrl(request.Url);
    Check(request.Method == "GET" || request.Method == "POST", "unsupported method; use GET or POST");
    Check(Clean(request.Accept) && Clean(request.ContentType), "invalid media type");
    Check(request.Body.size() <= size_t{16} * 1024 * 1024, "request body too large");
    std::ifstream bodyFile;
    size_t bodyLength = request.Body.size();
    if (!request.BodyFile.empty()) {
        Check(request.Method == "POST" && request.Body.empty() && request.ContentType == "application/x-ndjson",
            "file uploads require a bulk POST");
        Check(std::filesystem::is_regular_file(request.BodyFile), "bulk input must be a regular file");
        bodyFile.open(request.BodyFile, std::ios::binary | std::ios::ate);
        Check(static_cast<bool>(bodyFile), "cannot open bulk input file");
        const auto length = bodyFile.tellg();
        Check(length > 0 && static_cast<uint64_t>(length) <= (4ULL << 30), "bulk input must be 1 byte..4 GiB");
        bodyLength = static_cast<size_t>(length);
        bodyFile.seekg(0);
        Check(static_cast<bool>(bodyFile), "cannot rewind bulk input file");
    }
    Check(request.TimeoutSeconds > 0 && request.TimeoutSeconds <= 3600, "timeout must be 1..3600 seconds");
    const char* token = std::getenv("GLAMDB_TOKEN");
    Check(token && *token && std::string_view(token).size() <= size_t{8} * 1024 && Clean(token),
        "set GLAMDB_TOKEN to a printable bearer token (at most 8192 bytes)");
    std::ostringstream output;
    output << request.Method << ' ' << url.Target << " HTTP/1.1\r\nHost: " << url.Authority
           << "\r\nAuthorization: Bearer " << token << "\r\nAccept: " << request.Accept
           << "\r\nContent-Type: " << request.ContentType << "\r\nContent-Length: " << bodyLength
           << "\r\nConnection: close\r\n\r\n"
           << request.Body;
    const auto bytes = output.str();
    asio::io_context io;
    asio::ip::tcp::resolver resolver(io);
    asio::ssl::context context(asio::ssl::context::tls_client);
    Check(SSL_CTX_set_min_proto_version(context.native_handle(), TLS1_2_VERSION) == 1,
        "cannot set TLS minimum version");
    context.set_default_verify_paths();
    if (!request.CaFile.empty()) {
        context.load_verify_file(request.CaFile);
    }
    asio::ssl::stream<asio::ip::tcp::socket> stream(io, context);
    stream.set_verify_mode(asio::ssl::verify_peer);
    stream.set_verify_callback(asio::ssl::host_name_verification(url.Host));
    std::error_code addressError;
    asio::ip::make_address(url.Host, addressError);
    Check(!url.Tls || !addressError || SSL_set_tlsext_host_name(stream.native_handle(), url.Host.c_str()) == 1,
        "cannot set TLS server name");
    asio::steady_timer timer(io, std::chrono::seconds(request.TimeoutSeconds));
    bool timedOut = false;
    bool requestStarted = false;
    timer.async_wait([&](std::error_code error) {
        if (!error) {
            timedOut = true;
            resolver.cancel();
            std::error_code ignored;
            stream.next_layer().close(ignored);
        }
    });
    TResponse response;
    std::exception_ptr failure;
    std::string wire;
    std::array<char, 65536> uploadBuffer{};
    size_t uploaded = 0;
    std::function<void()> uploadNext;
    auto finish = [&](const std::error_code& error) {
        try {
            Check(!error || error == asio::error::eof || error == asio::ssl::error::stream_truncated,
                "HTTP exchange failed");
            response = ParseResponse(wire);
        } catch (...) {
            failure = std::current_exception();
        }
        timer.cancel();
        std::error_code ignored;
        stream.next_layer().close(ignored);
    };
    auto readResponse = [&](auto& connection) {
        // An early refusal may already be readable after a request write fails.
        // Only a complete, valid response resolves the outcome in that case.
        auto* connectionPtr = &connection;
        asio::async_read_until(*connectionPtr,
            asio::dynamic_buffer(wire, 16384),
            "\r\n\r\n",
            [&, connectionPtr](const std::error_code& headerError, size_t) {
                if (headerError) {
                    finish(headerError);
                    return;
                }
                try {
                    const auto frame = ParseResponse(wire, false);
                    const auto length = Number(frame.Headers.at("content-length").front());
                    const auto remaining = length - (wire.size() - wire.find("\r\n\r\n") - 4);
                    if (remaining == 0) {
                        finish({});
                        return;
                    }
                    asio::async_read(*connectionPtr,
                        asio::dynamic_buffer(wire, size_t{64} * 1024 * 1024 + 16384),
                        asio::transfer_exactly(remaining),
                        [&](const std::error_code& bodyError, size_t) {
                            finish(bodyError);
                        });
                } catch (...) {
                    failure = std::current_exception();
                    timer.cancel();
                    std::error_code ignored;
                    stream.next_layer().close(ignored);
                }
            });
    };
    auto exchange = [&](auto& connection) {
        requestStarted = true;
        uploadNext = [&, connectionPtr = &connection] {
            if (!bodyFile.is_open() || uploaded == bodyLength) {
                readResponse(*connectionPtr);
                return;
            }
            try {
                const auto count = std::min(uploadBuffer.size(), bodyLength - uploaded);
                bodyFile.read(uploadBuffer.data(), static_cast<std::streamsize>(count));
                Check(bodyFile.gcount() == static_cast<std::streamsize>(count), "bulk input changed or read failed");
                uploaded += count;
                asio::async_write(*connectionPtr,
                    asio::buffer(uploadBuffer.data(), count),
                    [&](const std::error_code& error, size_t) {
                        if (error) {
                            readResponse(*connectionPtr);
                        } else {
                            uploadNext();
                        }
                    });
            } catch (...) {
                failure = std::current_exception();
                timer.cancel();
                std::error_code ignored;
                stream.next_layer().close(ignored);
            }
        };
        // Once transmission starts, partial sends have an unresolved outcome.
        asio::async_write(connection, asio::buffer(bytes), [&](const std::error_code& error, size_t) {
            if (error) {
                readResponse(connection);
            } else {
                uploadNext();
            }
        });
    };
    resolver.async_resolve(url.Host,
        url.Port,
        [&](const std::error_code& error, const asio::ip::tcp::resolver::results_type& endpoints) {
            if (error) {
                finish(error);
                return;
            }
            if (!url.Tls) {
                for (const auto& endpoint : endpoints) {
                    if (!endpoint.endpoint().address().is_loopback()) {
                        failure = std::make_exception_ptr(std::runtime_error("remote endpoints require HTTPS"));
                        timer.cancel();
                        return;
                    }
                }
            }
            asio::async_connect(stream.next_layer(),
                endpoints,
                [&](const std::error_code& connectError, const asio::ip::tcp::endpoint&) {
                    if (connectError) {
                        finish(connectError);
                        return;
                    }
                    if (url.Tls) {
                        stream.async_handshake(asio::ssl::stream_base::client, [&](const std::error_code& tlsError) {
                            if (tlsError) {
                                finish(tlsError);
                                return;
                            }
                            exchange(stream);
                        });
                    } else {
                        exchange(stream.next_layer());
                    }
                });
        });
    io.run();
    if (timedOut) {
        failure = std::make_exception_ptr(std::runtime_error("request timed out"));
    }
    if (failure) {
        try {
            std::rethrow_exception(failure);
        } catch (const std::exception& error) {
            if (requestStarted && request.Method == "POST") {
                throw TOutcomeUnknown(
                    std::string(error.what()) + "; write outcome may be unknown; reconcile using operation ID");
            }
            throw;
        }
    }
    return response;
}
}
