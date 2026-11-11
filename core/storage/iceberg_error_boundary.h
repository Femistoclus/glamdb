#pragma once

#include <iceberg/result.h>

#include <string_view>
#include <utility>

namespace NGlamDB::NStorage {

// Stable message prefixes that classify storage failures for the engine.
// CORRUPTION: committed or retained state violates a storage invariant.
// CAPACITY: a configured storage or file limit would be exceeded.
// RESOURCE: the process ran out of memory or workspace; nothing persisted is wrong.
inline constexpr std::string_view CorruptionPrefix = "CORRUPTION: ";
inline constexpr std::string_view CapacityPrefix = "CAPACITY: ";
inline constexpr std::string_view ResourcePrefix = "RESOURCE: ";

template <typename Function>
auto CatchIcebergExceptions(Function&& function) -> decltype(std::forward<Function>(function)());

} // namespace NGlamDB::NStorage

#define GLAMDB_CORE_STORAGE_ICEBERG_ERROR_BOUNDARY_INL_H_
#include "iceberg_error_boundary-inl.h"
#undef GLAMDB_CORE_STORAGE_ICEBERG_ERROR_BOUNDARY_INL_H_
