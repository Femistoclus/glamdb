#pragma once

#include <cstdint>
#include <optional>

namespace glamdb {

// Provenance only: this enum does not certify that a raw bound is an exact scalar.
enum class MetricKind : uint8_t { Unavailable, Metadata, Estimate, Measured };

// An unavailable value is null, never a fabricated zero. Metadata values are
// derived from committed file descriptors, not from reading every data page.
struct Metric
{
    std::optional<int64_t> value{};
    MetricKind kind = MetricKind::Unavailable;
};

} // namespace glamdb
