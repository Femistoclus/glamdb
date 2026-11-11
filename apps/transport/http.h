#pragma once

#include "apps/transport/codec.h"

#include <map>
#include <utility>

namespace glamdb::NTransport {
struct TRequest
{
    std::string Method;
    std::string Target;
    std::map<std::string, std::string> Headers;
    std::string Body;
    size_t ContentLength = 0;
};
// Base ownership is destroyed after response payloads.
struct TResponseResources
{
    std::shared_ptr<const void> ResourceLease{};
};
struct TResponse : TResponseResources
{
    TResponse(int status = 200,
        std::string body = {},
        std::string contentType = "application/json",
        std::map<std::string, std::string> headers = {},
        std::shared_ptr<const void> resourceLease = {})
        : TResponseResources{std::move(resourceLease)}
        , Status(status)
        , Body(std::move(body))
        , ContentType(std::move(contentType))
        , Headers(std::move(headers))
    { }
    TResponse(const TResponse&) = default;
    TResponse(TResponse&&) noexcept = default;
    TResponse& operator=(TResponse other) noexcept
    {
        std::swap(Status, other.Status);
        Body.swap(other.Body);
        ContentType.swap(other.ContentType);
        Headers.swap(other.Headers);
        ResourceLease.swap(other.ResourceLease);
        return *this;
    }
    int Status = 200;
    std::string Body;
    std::string ContentType = "application/json";
    std::map<std::string, std::string> Headers{};
};

bool UsesProtectedAdmission(const TRequest& request);
TResponse ReadinessResponse(const StoreHealth& health);
TRequest ParseHeaders(std::string_view bytes);
std::string SerializeResponseHead(const TResponse& response);
TResponse
ErrorResponse(int status, std::string_view code, std::string_view message, std::optional<RetryHint> hint = {});
std::string DecodeUrl(std::string_view text);
std::pair<std::string, std::map<std::string, std::string>> ParseTarget(std::string_view target);
std::string ReadToken();
} // namespace glamdb::NTransport
