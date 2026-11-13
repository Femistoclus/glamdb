#include "core/engine/read_views.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"

#include <algorithm>

namespace glamdb {

TReadView::TReadView()
    : State_(std::make_shared<detail::TReadViewState>())
{ }

TReadView::~TReadView()
{
    if (Release_) {
        Release_();
    }
}

namespace detail {
namespace {

void ValidateLifetime(std::chrono::milliseconds lifetime)
{
    Require(lifetime.count() > 0 && lifetime <= TReadViewLimits{}.MaxLifetime,
        "INVALID_ARGUMENT",
        "read-view lifetime must be positive and at most five minutes");
}

} // namespace

TReadViews::TReadViews(TRootReaders& roots)
    : Roots_(roots)
{ }

TReadViews::~TReadViews()
{
    Clear();
}

void TReadViews::Validate(const TReadViewRequest& request)
{
    const TReadViewLimits limits;
    Require(!request.AuthorizationScope.empty() && request.AuthorizationScope.size() <= limits.MaxScopeBytes,
        "INVALID_ARGUMENT",
        "read-view authorization scope must contain 1..256 bytes");
    Require(!request.Entries.empty() && request.Entries.size() <= limits.MaxEntriesPerView,
        "INVALID_ARGUMENT",
        "read view requires 1..64 entries");
    ValidateLifetime(request.Lifetime);
    for (const auto& entry : request.Entries) {
        Require(ValidUuid(entry.CollectionID), "INVALID_ARGUMENT", "read view requires canonical collection UUIDs");
        Require(entry.Selector.kind == SnapshotSelectorKind::Latest ||
                entry.Selector.kind == SnapshotSelectorKind::InitialEmpty ||
                (entry.Selector.kind == SnapshotSelectorKind::Id && entry.Selector.snapshot_id > 0),
            "INVALID_ARGUMENT",
            "invalid read-view snapshot selector");
    }
}

void TReadViews::CheckCapacity(const TReadViewRequest& request) const
{
    const TReadViewLimits limits;
    const auto scopeCount = std::ranges::count_if(Records_, [&](const auto& item) {
        return item.second.Handle->Scope == request.AuthorizationScope;
    });
    Require(Records_.size() < limits.MaxViews && static_cast<size_t>(scopeCount) < limits.MaxViewsPerScope &&
            request.Entries.size() <= limits.MaxEntries - Entries_,
        "RESOURCE_EXHAUSTED",
        "read-view count or entry quota exhausted");
}

void TReadViews::Add(const TReadView& handle,
    std::string_view storeID,
    const TReadViewRequest& request,
    TCapture capture)
{
    auto& state = *handle.State_;
    state.Owner = Identity_;
    state.StoreID = storeID;
    state.Scope = request.AuthorizationScope;
    const auto now = TClock::now();
    auto [it, inserted] =
        Records_.emplace(handle.State_.get(), TRecord{handle.State_, std::move(capture), now, now + request.Lifetime});
    Require(inserted, "INTERNAL", "read-view handle already registered");
    const auto& entries = it->second.Capture.Info->Entries;
    size_t leased = 0;
    try {
        for (const auto& entry : entries) {
            ++Roots_[entry.Identity.metadata_location];
            ++leased;
        }
    } catch (...) {
        for (size_t n = 0; n < leased; ++n) {
            auto root = Roots_.find(entries[n].Identity.metadata_location);
            if (--root->second == 0) {
                Roots_.erase(root);
            }
        }
        Records_.erase(it);
        throw;
    }
    Entries_ += entries.size();
}

void TReadViews::CheckIdentity(const TReadView& handle, std::string_view storeID, std::string_view scope) const
{
    const auto& state = *handle.State_;
    Require(state.Owner.lock() == Identity_,
        state.StoreID == storeID ? "READ_VIEW_RESTARTED" : "READ_VIEW_WRONG_ENGINE",
        "read view belongs to a different engine incarnation");
    Require(state.Scope == scope, "READ_VIEW_SCOPE_MISMATCH", "read-view authorization scope differs");
}

void TReadViews::CheckActive(const TReadView& handle) const
{
    switch (handle.State_->State) {
        case EReadViewState::Active:
            return;
        case EReadViewState::Released:
            throw Failure({"READ_VIEW_RELEASED", "read view was released"});
        case EReadViewState::Cancelled:
            throw Failure({"READ_VIEW_CANCELLED", "read view was cancelled"});
        case EReadViewState::Expired:
            throw Failure({"READ_VIEW_EXPIRED", "read view expired"});
        case EReadViewState::Closed:
            throw Failure({"READ_VIEW_CLOSED", "owning engine is closing or closed"});
    }
}

const TReadViews::TCapture&
TReadViews::Resolve(const TReadView& handle, std::string_view storeID, std::string_view scope)
{
    CheckIdentity(handle, storeID, scope);
    Expire();
    CheckActive(handle);
    const auto it = Records_.find(handle.State_.get());
    Require(it != Records_.end(), "INTERNAL", "active read view is not registered");
    return it->second.Capture;
}

void TReadViews::Renew(const TReadView& handle,
    std::string_view storeID,
    std::string_view scope,
    std::chrono::milliseconds lifetime)
{
    (void)Resolve(handle, storeID, scope);
    ValidateLifetime(lifetime);
    auto& record = Records_.at(handle.State_.get());
    const auto now = TClock::now();
    // Resolve and clock sampling are separate; never renew a deadline crossed here.
    if (now >= record.Expires) {
        Expire();
        CheckActive(handle);
    }
    record.Expires = std::min(now + lifetime, record.Acquired + TReadViewLimits{}.MaxLifetime);
}

TReadViews::TRecords::iterator TReadViews::Finish(TRecords::iterator it, EReadViewState state)
{
    const auto& entries = it->second.Capture.Info->Entries;
    for (const auto& entry : entries) {
        auto root = Roots_.find(entry.Identity.metadata_location);
        if (--root->second == 0) {
            Roots_.erase(root);
        }
    }
    Entries_ -= entries.size();
    it->second.Handle->State = state;
    return Records_.erase(it);
}

void TReadViews::End(const TReadView& handle, std::string_view storeID, std::string_view scope, EReadViewState state)
{
    CheckIdentity(handle, storeID, scope);
    Expire();
    if (handle.State_->State == state) {
        return;
    }
    CheckActive(handle);
    Finish(Records_.find(handle.State_.get()), state);
}

void TReadViews::Drop(const std::shared_ptr<TReadViewState>& state)
{
    const auto it = Records_.find(state.get());
    if (it != Records_.end()) {
        Finish(it, EReadViewState::Released);
    }
}

void TReadViews::Expire()
{
    const auto now = TClock::now();
    for (auto it = Records_.begin(); it != Records_.end();) {
        if (it->second.Expires <= now) {
            it = Finish(it, EReadViewState::Expired);
        } else {
            ++it;
        }
    }
}

void TReadViews::Clear()
{
    while (!Records_.empty()) {
        Finish(Records_.begin(), EReadViewState::Closed);
    }
}

} // namespace detail
} // namespace glamdb
