#pragma once
#include "core/catalog/sqlite.h"
#include "storage/resources.h"
#include "storage/write_session.h"

#include <functional>
#include <mutex>

namespace glamdb::detail {
template <class T>
T SessionTake(Result<T> value)
{
    if (!value) {
        throw Failure(value.error());
    }
    return std::move(*value);
}
inline void SessionTake(Status value)
{
    if (!value) {
        throw Failure(value.error());
    }
}

// One SQLite owner and one bounded emergency file. Not an Engine catalog.
class TWriteJournal
{
public:
    TWriteJournal(std::filesystem::path directory,
        std::shared_ptr<ResourceOwner> resources,
        const std::string& store,
        const std::string& scope,
        TWriteSessionLimits limits);
    ~TWriteJournal();
    void Transaction(const std::function<void()>& action, bool control = true);
    void RequireUploadCapacity();
    Database DB;
    const TWriteSessionLimits Limits;

private:
    std::filesystem::path Directory_;
    std::shared_ptr<ResourceOwner> Resources_;
    int Lock_ = -1;
    bool ReserveAvailable_ = false;
    std::vector<std::shared_ptr<ResourceReservation>> Reservations_;
    void ReleaseEmergency();
};

std::string PartKey(TWritePart part);
std::string WriteKey(const TWriteIdentity& id);
int64_t SessionNow();
std::string EncodeSessionBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
std::shared_ptr<arrow::RecordBatch> DecodeSessionBatch(std::string bytes);
} // namespace glamdb::detail
