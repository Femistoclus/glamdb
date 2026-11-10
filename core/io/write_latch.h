#pragma once

#include <atomic>
#include <system_error>

namespace NGlamDB::NIO {

class TWriteLatch
{
public:
    void Observe(std::error_code error) noexcept;
    void Block() noexcept;
    bool IsSet() const noexcept;
    std::error_code Error() const noexcept;

private:
    std::atomic<bool> Blocked_{false};
};

}
