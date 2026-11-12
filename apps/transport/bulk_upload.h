#pragma once

#include "apps/transport/codec.h"
#include "storage/resources.h"

#include <array>
#include <cstdio>

namespace glamdb::NTransport {
inline constexpr uint64_t MaxBulkRequestBytes = 4ULL << 30;
inline constexpr size_t MaxBulkHeaderBytes = 64ULL << 10;
// Anonymous, nondurable transport scratch. All methods run on workers, never
// the listener. The owner retains both disk admission and bounded source memory.
class TBulkUpload
{
public:
    TBulkUpload(const std::shared_ptr<ResourceOwner>& resources, uint64_t length);
    ~TBulkUpload();
    TBulkUpload(const TBulkUpload&) = delete;
    TBulkUpload& operator=(const TBulkUpload&) = delete;
    void Append(std::string_view bytes);
    void Seal();
    void Rewind();
    std::optional<std::string> ReadLine(size_t limit = MaxRequestBytes);

private:
    std::shared_ptr<ResourceReservation> Disk_, Memory_;
    std::FILE* File_ = nullptr;
    uint64_t Length_, Written_ = 0;
    bool Sealed_ = false;
    std::array<char, 65536> Buffer_{};
    size_t Position_ = 0, Available_ = 0;
};
} // namespace glamdb::NTransport
