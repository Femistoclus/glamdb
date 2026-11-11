#include "core/scan/data_file_source.h"

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>

#include <algorithm>
#include <span>
#include <utility>

namespace glamdb::detail {

struct TDataFileSource::TState
{
    std::shared_ptr<const void> Retain;
    std::atomic<int64_t> Calls{0};
    std::atomic<int64_t> Bytes{0};
    std::atomic<int64_t> MaxReadBytes{0};
    std::atomic<int64_t> HeldBytes{0};
    std::atomic<int64_t> PeakHeldBytes{0};

    void Hold(int64_t bytes)
    {
        const auto held = HeldBytes.fetch_add(bytes) + bytes;
        auto peak = PeakHeldBytes.load();
        while (held > peak && !PeakHeldBytes.compare_exchange_weak(peak, held)) {
        }
    }
};

namespace {

void Raise(std::atomic<int64_t>& value, int64_t candidate)
{
    auto current = value.load();
    while (candidate > current && !value.compare_exchange_weak(current, candidate)) {
    }
}

// Bytes returned by one read; counted as held until the last reference
// (including slices) is released.
class THeldBuffer final : public arrow::Buffer
{
public:
    THeldBuffer(std::unique_ptr<arrow::Buffer> inner, std::shared_ptr<TDataFileSource::TState> state)
        : arrow::Buffer(inner->data(), inner->size())
        , Inner_(std::move(inner))
        , State_(std::move(state))
    {
        State_->Hold(size_);
    }

    ~THeldBuffer() override
    {
        State_->HeldBytes.fetch_sub(size_);
    }

private:
    std::unique_ptr<arrow::Buffer> Inner_;
    std::shared_ptr<TDataFileSource::TState> State_;
};

// The footer verifier's view of the source: reads go through the same stream
// and are counted; closing the view leaves the stream open for the decoder.
class TSourceStream final : public iceberg::SeekableInputStream
{
public:
    TSourceStream(TDataFileSource& source, int64_t size)
        : Source_(source)
        , Size_(size)
    { }

    iceberg::Result<int64_t> Position() const override
    {
        return Position_;
    }

    iceberg::Status Seek(int64_t position) override
    {
        if (position < 0 || position > Size_) {
            return iceberg::Invalid("CORRUPTION: seek outside the data file");
        }
        Position_ = position;
        return {};
    }

    iceberg::Result<int64_t> Read(std::span<std::byte> out) override
    {
        const auto count = std::min(static_cast<int64_t>(out.size()), Size_ - Position_);
        if (auto status = Source_.ReadInto(Position_, count, out.data()); !status) {
            return std::unexpected(status.error());
        }
        Position_ += count;
        return count;
    }

    iceberg::Status ReadFully(int64_t position, std::span<std::byte> out) override
    {
        return Source_.ReadInto(position, static_cast<int64_t>(out.size()), out.data());
    }

    iceberg::Status Close() override
    {
        return {};
    }

private:
    TDataFileSource& Source_;
    const int64_t Size_;
    int64_t Position_ = 0;
};

class TSourceInputFile final : public iceberg::InputFile
{
public:
    TSourceInputFile(TDataFileSource& source, std::string_view location, iceberg::InputFile& file, int64_t size)
        : Source_(source)
        , Location_(location)
        , File_(file)
        , Size_(size)
    { }

    std::string_view location() const override
    {
        return Location_;
    }

    // The FileIO's own size check (identity and current length).
    iceberg::Result<int64_t> Size() const override
    {
        return File_.Size();
    }

    iceberg::Result<std::unique_ptr<iceberg::SeekableInputStream>> Open() override
    {
        return std::make_unique<TSourceStream>(Source_, Size_);
    }

private:
    TDataFileSource& Source_;
    const std::string_view Location_;
    iceberg::InputFile& File_;
    const int64_t Size_;
};

} // namespace

TDataFileSource::TDataFileSource(std::string location,
    std::unique_ptr<iceberg::InputFile> file,
    std::unique_ptr<iceberg::SeekableInputStream> stream,
    int64_t size,
    std::shared_ptr<const void> retain)
    : Location_(std::move(location))
    , File_(std::move(file))
    , Size_(size)
    , State_(std::make_shared<TState>())
    , Stream_(std::move(stream))
{
    State_->Retain = std::move(retain);
}

TDataFileSource::~TDataFileSource()
{
    (void)CloseStream();
}

std::unique_ptr<iceberg::InputFile> TDataFileSource::AsInputFile()
{
    return std::make_unique<TSourceInputFile>(*this, Location_, *File_, Size_);
}

iceberg::Status TDataFileSource::ReadInto(int64_t position, int64_t size, void* out)
{
    if (position < 0 || size < 0 || position > Size_ || size > Size_ - position) {
        return iceberg::Invalid("CORRUPTION: read outside the data file");
    }
    std::lock_guard lock(Mutex_);
    if (!Stream_) {
        return iceberg::Invalid("data file stream is closed");
    }
    if (size == 0) {
        return {};
    }
    auto status = Stream_->ReadFully(position, std::span(static_cast<std::byte*>(out), static_cast<size_t>(size)));
    if (!status) {
        if (!ReadError_) {
            ReadError_ = status.error();
        }
        return status;
    }
    State_->Calls.fetch_add(1);
    State_->Bytes.fetch_add(size);
    Raise(State_->MaxReadBytes, size);
    return {};
}

std::optional<iceberg::Error> TDataFileSource::ReadError() const
{
    std::lock_guard lock(Mutex_);
    return ReadError_;
}

TDataFileReads TDataFileSource::Reads() const
{
    return {.Calls = State_->Calls.load(),
        .Bytes = State_->Bytes.load(),
        .MaxReadBytes = State_->MaxReadBytes.load(),
        .PeakHeldBytes = State_->PeakHeldBytes.load()};
}

iceberg::Status TDataFileSource::CloseStream()
{
    std::lock_guard lock(Mutex_);
    if (!CloseResult_) {
        CloseResult_ = Stream_ ? Stream_->Close() : iceberg::Status{};
        Stream_.reset();
    }
    return *CloseResult_;
}

arrow::Status TDataFileSource::Close()
{
    const auto status = CloseStream();
    return status ? arrow::Status::OK() : arrow::Status::IOError(status.error().message);
}

bool TDataFileSource::closed() const
{
    std::lock_guard lock(Mutex_);
    return CloseResult_.has_value();
}

arrow::Result<int64_t> TDataFileSource::Tell() const
{
    std::lock_guard lock(Mutex_);
    return Position_;
}

arrow::Status TDataFileSource::Seek(int64_t position)
{
    if (position < 0 || position > Size_) {
        return arrow::Status::Invalid("seek outside the data file");
    }
    std::lock_guard lock(Mutex_);
    Position_ = position;
    return arrow::Status::OK();
}

arrow::Status TDataFileSource::CheckRange(int64_t position, int64_t nbytes) const
{
    if (position < 0 || nbytes < 0 || position > Size_ || nbytes > Size_ - position) {
        return arrow::Status::Invalid("read outside the data file");
    }
    return arrow::Status::OK();
}

arrow::Status TDataFileSource::ReadChecked(int64_t position, int64_t nbytes, void* out)
{
    ARROW_RETURN_NOT_OK(CheckRange(position, nbytes));
    const auto status = ReadInto(position, nbytes, out);
    return status ? arrow::Status::OK() : arrow::Status::IOError(status.error().message);
}

arrow::Result<int64_t> TDataFileSource::Read(int64_t nbytes, void* out)
{
    int64_t position = 0;
    {
        std::lock_guard lock(Mutex_);
        position = Position_;
    }
    const auto count = std::min(std::max<int64_t>(nbytes, 0), Size_ - position);
    ARROW_RETURN_NOT_OK(ReadChecked(position, count, out));
    std::lock_guard lock(Mutex_);
    Position_ = position + count;
    return count;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> TDataFileSource::Read(int64_t nbytes)
{
    int64_t position = 0;
    {
        std::lock_guard lock(Mutex_);
        position = Position_;
    }
    const auto count = std::min(std::max<int64_t>(nbytes, 0), Size_ - position);
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(position, count));
    std::lock_guard lock(Mutex_);
    Position_ = position + count;
    return buffer;
}

arrow::Result<int64_t> TDataFileSource::ReadAt(int64_t position, int64_t nbytes, void* out)
{
    ARROW_RETURN_NOT_OK(ReadChecked(position, nbytes, out));
    return nbytes;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> TDataFileSource::ReadAt(int64_t position, int64_t nbytes)
{
    ARROW_RETURN_NOT_OK(CheckRange(position, nbytes));
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(nbytes, arrow::default_memory_pool()));
    ARROW_RETURN_NOT_OK(ReadChecked(position, nbytes, buffer->mutable_data()));
    return std::make_shared<THeldBuffer>(std::move(buffer), State_);
}

arrow::Result<int64_t> TDataFileSource::GetSize()
{
    return Size_;
}

} // namespace glamdb::detail
