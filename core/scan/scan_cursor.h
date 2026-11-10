#pragma once

#include "storage/scan.h"

#include <atomic>
#include <functional>
#include <memory>

namespace iceberg {
class FileIO;
} // namespace iceberg

namespace glamdb::detail {

struct TScanContext
{
    CollectionInfo Collection;
    // The committed view resolved once by the engine.
    BaseIdentity Base;
    std::shared_ptr<iceberg::FileIO> FileIO;
    // Aliases the engine's closing flag and retains the engine state, its
    // catalog and store lease until the cursor is released.
    std::shared_ptr<const std::atomic<bool>> Closing;
    // Retains the store lease in returned Arrow buffers so the store stays
    // locked until the last returned batch, slice or C Data export is released.
    std::shared_ptr<const void> Lease;
    std::function<void(const ErrorDetail&)> OnError;
};

Result<std::shared_ptr<ScanCursor>> OpenScan(TScanContext context, const ScanRequest& request);

} // namespace glamdb::detail
