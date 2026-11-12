#pragma once

#include "core/api/canonical.h"

namespace glamdb::detail {

Result<std::string> FingerprintBulkSource(const SchemaSpec& schema,
    const BulkAppendRequest& request,
    std::chrono::steady_clock::time_point deadline);
Result<std::shared_ptr<TBulkInput>> PrepareBulkInput(const SchemaSpec& schema,
    const BulkAppendRequest& request,
    std::string expectedHash,
    std::chrono::steady_clock::time_point deadline,
    std::function<Status()> checkHealth);

} // namespace glamdb::detail
