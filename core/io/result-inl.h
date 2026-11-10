#ifndef GLAMDB_CORE_IO_RESULT_INL_H_
#error "Include result.h instead of result-inl.h"
#endif

#include <utility>

namespace NGlamDB::NIO {

template <typename T>
TResult<T>::TResult(T value)
    : Value_(std::move(value))
{ }

template <typename T>
TResult<T>::TResult(std::error_code error)
    : Value_(error)
{ }

template <typename T>
TResult<T>::TResult(std::errc error)
    : Value_(std::make_error_code(error))
{ }

template <typename T>
TResult<T>::operator bool() const noexcept
{
    return std::holds_alternative<T>(Value_);
}

template <typename T>
T& TResult<T>::Value() &
{
    return std::get<T>(Value_);
}

template <typename T>
const T& TResult<T>::Value() const&
{
    return std::get<T>(Value_);
}

template <typename T>
T&& TResult<T>::Value() &&
{
    return std::get<T>(std::move(Value_));
}

template <typename T>
std::error_code TResult<T>::Error() const noexcept
{
    const auto* error = std::get_if<std::error_code>(&Value_);
    return error ? *error : std::error_code{};
}

}
