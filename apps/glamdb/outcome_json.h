#pragma once

#include "storage/types.h"

namespace glamdb::json_adapter {

struct Response
{
    int status = 500;
    std::string body;
};

// Transport serialization only. Neither this adapter nor its callers publish
// data or reinterpret an uncertain core outcome as a definite failure.
Result<Response> ToJson(const AppendOutcome& outcome);
Result<Response> ToJson(const CreateOutcome& outcome);
Result<std::string> CollectionJson(const CollectionInfo& collection);

} // namespace glamdb::json_adapter
