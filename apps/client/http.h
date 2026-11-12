#pragma once
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace glamdb::NClient {
struct TRequest
{
    std::string Url;
    std::string Method = "GET";
    std::string Body;
    std::string Accept = "application/json";
    std::string ContentType = "application/json";
    std::string CaFile;
    unsigned TimeoutSeconds = 65;
    // Stream a regular NDJSON file without retaining the whole bulk body.
    std::string BodyFile;
};
struct TResponse
{
    int Status = 0;
    std::map<std::string, std::vector<std::string>> Headers;
    std::string Body;
};
class TOutcomeUnknown : public std::runtime_error
{
public:
    using std::runtime_error::runtime_error;
};
TResponse SendRequest(const TRequest& request);
}
