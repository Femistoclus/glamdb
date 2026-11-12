#include "apps/transport/bulk_upload.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <unistd.h>
#if defined(__linux__)
#include <linux/magic.h>
#include <sys/vfs.h>
#endif

namespace glamdb::NTransport {
TBulkUpload::TBulkUpload(const std::shared_ptr<ResourceOwner>& resources, uint64_t length)
    : Length_(length)
{
    Require(length > 0 && length <= MaxBulkRequestBytes, "bulk body must be 1 byte..4 GiB", "PAYLOAD_TOO_LARGE");
    const auto directory = std::filesystem::canonical(std::filesystem::temp_directory_path());
#if defined(__linux__)
    struct statfs filesystem = {};
    Require(statfs(directory.c_str(), &filesystem) == 0, "cannot inspect bulk scratch filesystem", "IO_ERROR");
    Require(filesystem.f_type != TMPFS_MAGIC && filesystem.f_type != RAMFS_MAGIC,
        "bulk scratch requires a disk-backed temporary filesystem",
        "CAPACITY_EXCEEDED");
#endif
    Disk_ = Unwrap(resources->ReserveDevice(directory.string(), length, 1));
    Memory_ = Unwrap(resources->Reserve(ResourceClass::Data, 128ULL << 20));
    auto path = (directory / "glamdb-upload-XXXXXX").string();
    const int descriptor = mkstemp(path.data());
    Require(descriptor >= 0, "cannot create bulk upload scratch", "IO_ERROR");
    // Unlink before accepting data: disconnect, process exit and restart leave
    // no named payload journal or cleanup obligation.
    const bool removed = unlink(path.c_str()) == 0;
    if (removed) {
        File_ = fdopen(descriptor, "w+b");
    }
    if (!File_) {
        close(descriptor);
        Fail("cannot open anonymous bulk upload scratch", "IO_ERROR");
    }
}

TBulkUpload::~TBulkUpload()
{
    if (File_) {
        std::fclose(File_);
    }
}

void TBulkUpload::Append(std::string_view bytes)
{
    Require(!Sealed_ && bytes.size() <= Length_ - Written_, "invalid bulk upload length");
    Require(std::fwrite(bytes.data(), 1, bytes.size(), File_) == bytes.size(), "bulk scratch write failed", "IO_ERROR");
    Written_ += bytes.size();
}

void TBulkUpload::Seal()
{
    Require(!Sealed_ && Written_ == Length_, "incomplete bulk upload");
    Require(std::fflush(File_) == 0, "bulk scratch flush failed", "IO_ERROR");
    Sealed_ = true;
    Rewind();
}

void TBulkUpload::Rewind()
{
    Require(Sealed_, "incomplete bulk upload");
    Require(std::fseek(File_, 0, SEEK_SET) == 0, "bulk scratch rewind failed", "IO_ERROR");
    Position_ = Available_ = 0;
}

std::optional<std::string> TBulkUpload::ReadLine(size_t limit)
{
    Require(Sealed_, "incomplete bulk upload");
    std::string line;
    for (;;) {
        if (Position_ == Available_) {
            Available_ = std::fread(Buffer_.data(), 1, Buffer_.size(), File_);
            Position_ = 0;
            Require(std::ferror(File_) == 0, "bulk scratch read failed", "IO_ERROR");
            if (Available_ == 0) {
                Require(line.empty(), "bulk records must end with LF");
                return {};
            }
        }
        const auto* begin = Buffer_.data() + Position_;
        const auto* end = static_cast<const char*>(std::memchr(begin, '\n', Available_ - Position_));
        const auto count = end ? static_cast<size_t>(end - begin) + 1 : Available_ - Position_;
        Require(count <= limit - line.size(), "bulk record exceeds its byte limit", "PAYLOAD_TOO_LARGE");
        line.append(begin, count);
        Position_ += count;
        if (end) {
            Require(line.size() > 1, "empty bulk record");
            return line;
        }
    }
}
} // namespace glamdb::NTransport
