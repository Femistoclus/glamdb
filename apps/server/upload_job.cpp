#include "apps/server/upload_job.h"

namespace glamdb::NServer {
void TUploadJob::Execute() noexcept
{
    try {
        if (Cleanup) {
            // Destruction may flush/reclaim blocks. Keep it on the spill pool.
            Upload.reset();
            return;
        }
        if (!Upload) {
            Upload = std::make_shared<NTransport::TBulkUpload>(Resources, Length);
        }
        Upload->Append(Bytes);
        if (Complete) {
            Upload->Seal();
        }
    } catch (...) {
        Failure = std::current_exception();
    }
}
} // namespace glamdb::NServer
