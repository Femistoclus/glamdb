#include "apps/transport/http.h"
#include "apps/transport/bulk_upload.h"

#include <charconv>
#include <cstdlib>
#include <sstream>

namespace glamdb::NTransport {
namespace {
std::string Lower(std::string value)
{
    for (auto& c : value) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c + ('a' - 'A'));
        }
    }
    return value;
}
std::string Trim(std::string_view text)
{
    const auto first = text.find_first_not_of(" \t");
    if (first == std::string_view::npos) {
        return {};
    }
    return std::string(text.substr(first, text.find_last_not_of(" \t") - first + 1));
}
size_t ParseSize(std::string_view text)
{
    size_t size = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), size);
    Require(!text.empty() && parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size(),
        "invalid Content-Length");
    return size;
}
bool IsToken(std::string_view text)
{
    return !text.empty() && std::ranges::all_of(text, [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
            std::string_view("!#$%&'*+-.^_`|~").find(static_cast<char>(c)) != std::string_view::npos;
    });
}
} // namespace

bool IsBulkUpload(const TRequest& request)
{
    const auto [path, query] = ParseTarget(request.Target);
    const auto split = path.starts_with("/databases/") ? path.find('/', 11) : std::string::npos;
    const auto suffix = split == std::string::npos ? path : path.substr(split);
    constexpr std::string_view prefix = "/collections/", ending = "/bulk-append";
    return request.Method == "POST" && query.empty() && suffix.starts_with(prefix) && suffix.ends_with(ending) &&
        suffix.size() > prefix.size() + ending.size() &&
        suffix.find('/', prefix.size()) == suffix.size() - ending.size();
}

bool HasBulkContentType(const TRequest& request)
{
    const auto type = request.Headers.find("content-type");
    return type != request.Headers.end() && Lower(Trim(type->second)) == "application/x-ndjson";
}

bool UsesProtectedAdmission(const TRequest& request)
{
    const auto [path, query] = ParseTarget(request.Target);
    (void)query;
    const auto split = path.starts_with("/databases/") ? path.find('/', 11) : std::string::npos;
    const auto suffix = split == std::string::npos ? path : path.substr(split);
    return path == "/healthz" || path == "/databases" ||
        (path.starts_with("/databases/") && split == std::string::npos) || suffix == "/readyz" ||
        suffix == "/capabilities" || suffix == "/open" || suffix == "/close" || suffix == "/reconcile" ||
        suffix == "/discard" || suffix == "/drop" || suffix.find("/operations/") != std::string::npos ||
        suffix.ends_with("/scan/cancel") || (request.Method == "GET" && suffix == "/collections:resolve");
}

TRequest ParseHeaders(std::string_view bytes)
{
    Require(bytes.size() <= 16384 && bytes.ends_with("\r\n\r\n"), "invalid HTTP headers");
    TRequest result;
    const auto lineEnd = bytes.find("\r\n");
    const auto line = bytes.substr(0, lineEnd);
    const auto first = line.find(' '), last = line.rfind(' ');
    Require(first != std::string_view::npos && first != last && line.substr(last + 1) == "HTTP/1.1",
        "expected HTTP/1.1 request");
    result.Method = line.substr(0, first);
    result.Target = line.substr(first + 1, last - first - 1);
    Require(IsToken(result.Method) && result.Target.starts_with('/') && result.Target.size() <= 4096 &&
            std::ranges::all_of(result.Target,
                [](unsigned char c) {
                    return c > 32 && c < 127;
                }),
        "invalid request target");
    size_t position = lineEnd + 2;
    while (position < bytes.size() - 2) {
        const auto end = bytes.find("\r\n", position);
        const auto header = bytes.substr(position, end - position);
        const auto colon = header.find(':');
        Require(colon != std::string_view::npos && IsToken(header.substr(0, colon)), "invalid header name");
        auto name = Lower(std::string(header.substr(0, colon)));
        auto value = Trim(header.substr(colon + 1));
        Require(std::ranges::all_of(value,
                    [](unsigned char c) {
                        return c == 9 || (c >= 32 && c != 127);
                    }),
            "invalid header value");
        Require(result.Headers.emplace(std::move(name), std::move(value)).second, "duplicate header");
        position = end + 2;
    }
    Require(result.Headers.contains("host") && !result.Headers.at("host").empty(), "Host is required");
    Require(!result.Headers.contains("transfer-encoding"), "Transfer-Encoding is unsupported");
    Require(!result.Headers.contains("expect") || Lower(result.Headers.at("expect")) == "100-continue",
        "unsupported expectation",
        "EXPECTATION_FAILED");
    Require(!result.Headers.contains("content-encoding") || result.Headers.at("content-encoding") == "identity",
        "compressed requests are unsupported");
    if (result.Headers.contains("content-length")) {
        result.ContentLength = ParseSize(result.Headers.at("content-length"));
    }
    Require(result.ContentLength <= (IsBulkUpload(result) ? MaxBulkRequestBytes : MaxRequestBytes),
        "request body is too large",
        "PAYLOAD_TOO_LARGE");
    Require(result.Method != "POST" || result.Headers.contains("content-length"), "POST requires Content-Length");
    Require(result.Method == "POST" || result.ContentLength == 0, "body only allowed for POST");
    return result;
}

std::string SerializeResponseHead(const TResponse& response)
{
    std::string output = "HTTP/1.1 " + std::to_string(response.Status) +
        " Response\r\nContent-Type: " + response.ContentType +
        "\r\nContent-Length: " + std::to_string(response.Body.size()) +
        "\r\nConnection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: "
        "no-referrer\r\n";
    for (const auto& [name, value] : response.Headers) {
        output.append(name).append(": ").append(value).append("\r\n");
    }
    return output + "\r\n";
}

TResponse ErrorResponse(int status, std::string_view code, std::string_view message, std::optional<RetryHint> hint)
{
    return {status,
        EncodeError({std::string(code), std::string(message), hint.value_or(ErrorPolicy(code).Hint)}).dump()};
}

namespace {
std::string DecodeUrlPart(std::string_view text, bool queryValue)
{
    std::string result;
    for (size_t i = 0; i < text.size(); ++i) {
        auto c = text[i];
        const bool escaped = c == '%';
        if (escaped) {
            Require(i + 2 < text.size(), "invalid URL escape");
            unsigned value = 0;
            const auto parsed = std::from_chars(text.data() + i + 1, text.data() + i + 3, value, 16);
            Require(parsed.ec == std::errc{} && parsed.ptr == text.data() + i + 3, "invalid URL escape");
            c = static_cast<char>(value);
            i += 2;
        } else if (c == '+') {
            c = ' ';
        }
        // Exact UTF-8 collection names may contain controls. Only percent-encoded
        // query values admit them; paths, query keys, raw framing and NUL do not.
        Require(c != '\0' && ((queryValue && escaped) || (static_cast<unsigned char>(c) >= 32 && c != 127)),
            "invalid URL character");
        result += c;
    }
    return result;
}
} // namespace

std::string DecodeUrl(std::string_view text)
{
    return DecodeUrlPart(text, false);
}

std::pair<std::string, std::map<std::string, std::string>> ParseTarget(std::string_view target)
{
    const auto question = target.find('?');
    const auto rawPath = target.substr(0, question);
    Require(rawPath.find("%2f") == std::string_view::npos && rawPath.find("%2F") == std::string_view::npos,
        "escaped path separators are unsupported");
    std::pair<std::string, std::map<std::string, std::string>> result{DecodeUrl(target.substr(0, question)), {}};
    if (question == std::string_view::npos) {
        return result;
    }
    size_t position = question + 1;
    while (position < target.size()) {
        const auto end = target.find('&', position);
        const auto pair = target.substr(position, end == std::string_view::npos ? end : end - position);
        const auto equals = pair.find('=');
        Require(equals != std::string_view::npos &&
                result.second.emplace(DecodeUrl(pair.substr(0, equals)), DecodeUrlPart(pair.substr(equals + 1), true))
                    .second,
            "invalid or duplicate query parameter");
        if (end == std::string_view::npos) {
            break;
        }
        position = end + 1;
    }
    return result;
}

std::string ReadToken()
{
    const char* value = std::getenv("GLAMDB_TOKEN");
    const std::string token = value ? value : "";
    Require(!token.empty() && token.size() <= 256 &&
            std::ranges::all_of(token,
                [](unsigned char c) {
                    return c >= 33 && c <= 126;
                }),
        "set GLAMDB_TOKEN to a nonempty printable ASCII bearer token");
    return token;
}

TResponse ReadinessResponse(const StoreHealth& health)
{
    return {health.write_ready ? 200 : 503, EncodeHealth(health).dump()};
}

} // namespace glamdb::NTransport
