#pragma once

#include "storage/read_view.h"
#include "storage/resources.h"

#include <map>

namespace NGlamDB::NStorage {
struct TBaseState;
}

namespace glamdb::detail {

struct ArchivedSnapshotRecord;

enum class EReadViewState { Active, Released, Cancelled, Expired, Closed };

struct TReadViewState
{
    std::shared_ptr<ResourceReservation> ControlMemory;
    std::weak_ptr<const void> Owner;
    std::string StoreID;
    std::string Scope;
    EReadViewState State = EReadViewState::Active;
};

// All calls (including destruction callbacks) hold Engine::Impl::state_mutex.
// There is no registry lock: root readers and registry membership change together.
class TReadViews
{
public:
    using TClock = std::chrono::steady_clock;
    using TRootReaders = std::map<std::string, size_t, std::less<>>;
    using TVerifiedState = std::shared_ptr<const NGlamDB::NStorage::TBaseState>;
    static constexpr uint64_t EntryMemoryBytes = 256ULL << 10;
    static constexpr uint64_t ControlMemoryBytes = 4096;

    struct TCapture
    {
        std::shared_ptr<const TReadViewInfo> Info;
        // Preserve backend aliasing ownership, not just ScanIndex's shared pointer.
        std::vector<TVerifiedState> States;
        // Per entry: the archive row of a selected snapshot outside the pinned
        // root (bound to its receipt at acquisition), else empty.
        std::vector<std::shared_ptr<const ArchivedSnapshotRecord>> Archived;
    };

    explicit TReadViews(TRootReaders& roots);
    ~TReadViews();
    static void Validate(const TReadViewRequest& request);
    void CheckCapacity(const TReadViewRequest& request) const;
    void Add(const TReadView& handle, std::string_view storeID, const TReadViewRequest& request, TCapture capture);
    // Private planning bridge. Caller copies the selected entry/state and takes a
    // separate root reader before unlocking; no Latest or catalog lookup on use.
    const TCapture& Resolve(const TReadView& handle, std::string_view storeID, std::string_view scope);
    void Renew(const TReadView& handle,
        std::string_view storeID,
        std::string_view scope,
        std::chrono::milliseconds lifetime);
    void End(const TReadView& handle, std::string_view storeID, std::string_view scope, EReadViewState state);
    void Drop(const std::shared_ptr<TReadViewState>& state);
    void Expire();
    void Clear();

private:
    struct TRecord
    {
        std::shared_ptr<TReadViewState> Handle;
        TCapture Capture;
        TClock::time_point Acquired;
        TClock::time_point Expires;
    };
    using TRecords = std::map<TReadViewState*, TRecord>;
    void CheckIdentity(const TReadView& handle, std::string_view storeID, std::string_view scope) const;
    void CheckActive(const TReadView& handle) const;
    TRecords::iterator Finish(TRecords::iterator it, EReadViewState state);

    TRootReaders& Roots_;
    std::shared_ptr<const void> Identity_ = std::make_shared<int>(0);
    TRecords Records_;
    size_t Entries_ = 0;
};

} // namespace glamdb::detail
