#pragma once

#include "apps/transport/bulk_upload.h"

#include <atomic>

namespace glamdb::NServer {
// The I/O thread owns Pending; the worker owns job fields until Done publishes
// them. Notification is an optimization: a watchdog can always observe Done.
struct TUploadJob
{
    std::shared_ptr<NTransport::TBulkUpload> Upload;
    std::shared_ptr<ResourceOwner> Resources;
    std::string Bytes;
    uint64_t Length = 0;
    bool Complete = false, Cleanup = false;
    std::exception_ptr Failure;
    std::atomic<bool> Done{false};
    std::atomic<bool> NotificationFailed{false};

    template <class F>
    void Run(F&& notify) noexcept
    {
        Execute();
        Done.store(true);
        try {
            notify();
        } catch (...) {
            // Posting may allocate. The listener watchdog/reaper owns fallback.
            NotificationFailed.store(true);
        }
    }

private:
    void Execute() noexcept;
};

template <class F>
void QueueUpload(std::shared_ptr<TUploadJob>& pending,
    std::shared_ptr<NTransport::TBulkUpload>& owner,
    const std::shared_ptr<TUploadJob>& job,
    F&& submit)
{
    job->Upload = std::move(owner);
    pending = job;
    try {
        submit();
    } catch (...) {
        owner = std::move(job->Upload);
        pending.reset();
        throw;
    }
}
} // namespace glamdb::NServer
