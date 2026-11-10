#pragma once

#include <optional>
#include <stdexcept>

namespace glamdb::test {

// Throwing makes missing test evidence a test failure, including inside
// callbacks where a fatal GoogleTest assertion cannot return its value.
template <class T>
const T& Present(const std::optional<T>& value)
{
    if (!value.has_value()) {
        throw std::runtime_error("expected optional test evidence is absent");
    }
    return *value;
}

} // namespace glamdb::test
