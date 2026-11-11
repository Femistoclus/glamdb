#pragma once

#include <arrow/io/interfaces.h>
#include <iceberg/file_io.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace glamdb::detail {

// Reads of one data file, counted at the FileIO boundary.
struct TDataFileReads
{
    int64_t Calls = 0;
    int64_t Bytes = 0;
    // The largest single read and the most bytes returned by reads that were
    // still referenced at the same time (by the decoder or its pages).
    int64_t MaxReadBytes = 0;
    int64_t PeakHeldBytes = 0;
};

// Read-only random access to one committed data file through one stream of
// the store FileIO. Every read must lie inside the file length checked against
// the manifest; a read outside it is reported as corruption. A FileIO failure
// is kept so the caller can classify it by its storage error instead of as a
// decoder failure. `retain` (for example a memory reservation for the read
// bytes) is kept until the source and every buffer it returned are released.
class TDataFileSource final : public arrow::io::RandomAccessFile
{
public:
    TDataFileSource(std::string location,
        std::unique_ptr<iceberg::InputFile> file,
        std::unique_ptr<iceberg::SeekableInputStream> stream,
        int64_t size,
        std::shared_ptr<const void> retain = {});
    ~TDataFileSource() override;

    // An iceberg::InputFile over the same open stream, for the footer verifier.
    std::unique_ptr<iceberg::InputFile> AsInputFile();

    // Reads into `out`, which must hold `size` bytes, through the FileIO stream.
    iceberg::Status ReadInto(int64_t position, int64_t size, void* out);

    // The first FileIO failure of any read, if one occurred.
    std::optional<iceberg::Error> ReadError() const;
    TDataFileReads Reads() const;
    // Closes the stream; the result of the FileIO close.
    iceberg::Status CloseStream();

    arrow::Status Close() override;
    bool closed() const override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Seek(int64_t position) override;
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;
    arrow::Result<int64_t> GetSize() override;

    struct TState;

private:
    const std::string Location_;
    const std::unique_ptr<iceberg::InputFile> File_;
    const int64_t Size_;
    // Shared with returned buffers so held bytes are released after the source.
    const std::shared_ptr<TState> State_;
    mutable std::mutex Mutex_;
    std::unique_ptr<iceberg::SeekableInputStream> Stream_;
    std::optional<iceberg::Error> ReadError_;
    std::optional<iceberg::Status> CloseResult_;
    int64_t Position_ = 0;

    arrow::Status CheckRange(int64_t position, int64_t nbytes) const;
    arrow::Status ReadChecked(int64_t position, int64_t nbytes, void* out);
};

} // namespace glamdb::detail
