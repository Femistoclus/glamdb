#pragma once

#include <system_error>
#include <variant>

namespace NGlamDB::NIO {

template <typename T>
class TResult
{
public:
    TResult(T value);
    TResult(std::error_code error);
    TResult(std::errc error);

    explicit operator bool() const noexcept;
    T& Value() &;
    const T& Value() const&;
    T&& Value() &&;
    std::error_code Error() const noexcept;

private:
    std::variant<T, std::error_code> Value_;
};

}

#define GLAMDB_CORE_IO_RESULT_INL_H_
#include "result-inl.h"
#undef GLAMDB_CORE_IO_RESULT_INL_H_
