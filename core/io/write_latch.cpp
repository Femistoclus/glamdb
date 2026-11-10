#include "core/io/write_latch.h"

namespace NGlamDB::NIO {

void TWriteLatch::Observe(std::error_code error) noexcept
{
    if (error == std::errc::io_error || error == std::errc::read_only_file_system) {
        Block();
    }
}

void TWriteLatch::Block() noexcept
{
    Blocked_.store(true, std::memory_order_relaxed);
}

bool TWriteLatch::IsSet() const noexcept
{
    return Blocked_.load(std::memory_order_relaxed);
}

std::error_code TWriteLatch::Error() const noexcept
{
    return IsSet() ? std::make_error_code(std::errc::io_error) : std::error_code{};
}

}
