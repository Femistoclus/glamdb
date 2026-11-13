#pragma once

#include "storage/resources.h"
#include "storage/scan_split.h"

namespace glamdb::detail {

// Position and cancellation are protected by the owning engine's state_mutex.
// Identity must be checked through View before consulting either mutable field.
// No verified generation is retained here: release/expiry can reclaim the pin.
struct TScanPlanState
{
    static constexpr uint64_t ControlBytes = 4096;
    std::shared_ptr<ResourceReservation> ControlMemory;
    std::shared_ptr<ResourceReservation> RequestMemory;
    std::shared_ptr<const TReadView> View;
    TReadViewScanRequest Request;
    TScanPlanOptions Options;
    ScanInfo Info;
    size_t Entry = 0;
    size_t Next = 0;
    bool Cancelled = false;
};

} // namespace glamdb::detail
