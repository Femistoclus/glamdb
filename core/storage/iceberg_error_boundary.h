#pragma once

#include <iceberg/result.h>

#include <utility>

namespace NGlamDB::NStorage {

template <typename Function>
auto CatchIcebergExceptions(Function&& function) -> decltype(std::forward<Function>(function)());

} // namespace NGlamDB::NStorage

#define GLAMDB_CORE_STORAGE_ICEBERG_ERROR_BOUNDARY_INL_H_
#include "iceberg_error_boundary-inl.h"
#undef GLAMDB_CORE_STORAGE_ICEBERG_ERROR_BOUNDARY_INL_H_
