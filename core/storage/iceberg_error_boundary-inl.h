#ifndef GLAMDB_CORE_STORAGE_ICEBERG_ERROR_BOUNDARY_INL_H_
#error "Include iceberg_error_boundary.h instead of iceberg_error_boundary-inl.h"
#endif

#include <exception>
#include <new>

namespace NGlamDB::NStorage {

template <typename Function>
auto CatchIcebergExceptions(Function&& function) -> decltype(std::forward<Function>(function)())
{
    try {
        return std::forward<Function>(function)();
    } catch (const std::bad_alloc& error) {
        return iceberg::IOError("{}{}", ResourcePrefix, error.what());
    } catch (const std::exception& error) {
        return iceberg::UnknownError(error.what());
    } catch (...) {
        return iceberg::UnknownError("Unknown upstream exception");
    }
}

} // namespace NGlamDB::NStorage
