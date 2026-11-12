#pragma once

#include "storage/scan.h"

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace glamdb {
namespace detail {
class TReadViews;
struct TReadViewState;
}

struct TReadViewLimits
{
    size_t MaxViews = 32;
    size_t MaxViewsPerScope = 8;
    size_t MaxEntriesPerView = 64;
    size_t MaxEntries = 256;
    size_t MaxScopeBytes = 256;
    std::chrono::milliseconds MaxLifetime = std::chrono::minutes(5);
};

struct TReadViewSelection
{
    std::string CollectionID;
    SnapshotSelector Selector{};
};

struct TReadViewRequest
{
    // Ordered vector; duplicate collections and different selectors are allowed.
    std::vector<TReadViewSelection> Entries;
    // Trusted caller's authorization identity. Required, exact-match on every use.
    std::string AuthorizationScope;
    std::chrono::milliseconds Lifetime = std::chrono::seconds(30);
};

struct TReadViewEntryInfo
{
    CollectionInfo Collection;
    // Root identity at capture; selected historical snapshot is SelectorResolved.
    BaseIdentity Identity;
    SnapshotSelector SelectorResolved;
};

struct TReadViewInfo
{
    // Returned immutable information owns its accounting independently of the pin.
    std::shared_ptr<const void> ResourceLease;
    std::vector<TReadViewEntryInfo> Entries;
};

// Scan parameters cannot override a captured entry's collection or selector.
struct TReadViewScanRequest
{
    std::optional<std::vector<std::string>> Projection{};
    Predicate Filter = Predicate::True();
    std::optional<int64_t> Limit{};
    int64_t BatchRows = 65536;
};

// Opaque process-local handle. Last shared owner releases its pin. No serialization
// or restart survival. Release/cancel/expiry stop new scans; admitted cursors retain
// their own fixed state and cancellation/lifetime. Engine shutdown cancels cursors.
class TReadView final
{
public:
    ~TReadView();
    TReadView(const TReadView&) = delete;
    TReadView& operator=(const TReadView&) = delete;

private:
    TReadView();
    std::shared_ptr<detail::TReadViewState> State_;
    std::function<void()> Release_;
    friend class Engine;
    friend class detail::TReadViews;
};

} // namespace glamdb
