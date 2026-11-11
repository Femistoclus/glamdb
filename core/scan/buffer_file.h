#pragma once

#include <iceberg/file_io.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace glamdb::detail {

// Read-only iceberg::InputFile over bytes already read once, so verification
// and decoding observe exactly the same file content and a read failure is
// reported by the FileIO read, never as a truncated decoder input.
class TBufferInputStream final : public iceberg::SeekableInputStream
{
public:
    explicit TBufferInputStream(std::string_view bytes)
        : Bytes_(bytes)
    { }

    iceberg::Result<int64_t> Position() const override
    {
        return Position_;
    }

    iceberg::Status Seek(int64_t position) override
    {
        if (position < 0 || position > static_cast<int64_t>(Bytes_.size())) {
            return iceberg::InvalidArgument("seek outside the buffered file");
        }
        Position_ = position;
        return {};
    }

    iceberg::Result<int64_t> Read(std::span<std::byte> out) override
    {
        const auto count = std::min(static_cast<int64_t>(out.size()), static_cast<int64_t>(Bytes_.size()) - Position_);
        std::memcpy(out.data(), Bytes_.data() + Position_, static_cast<size_t>(count));
        Position_ += count;
        return count;
    }

    iceberg::Status ReadFully(int64_t position, std::span<std::byte> out) override
    {
        if (position < 0 || static_cast<uint64_t>(position) > Bytes_.size() ||
            out.size() > Bytes_.size() - static_cast<size_t>(position))
        {
            return iceberg::Invalid("CORRUPTION: read outside the buffered file");
        }
        std::memcpy(out.data(), Bytes_.data() + position, out.size());
        return {};
    }

    iceberg::Status Close() override
    {
        return {};
    }

private:
    const std::string_view Bytes_;
    int64_t Position_ = 0;
};

// A file location and the bytes read from it.
struct TBufferedFile
{
    std::string_view Location;
    std::string_view Bytes;
};

class TBufferInputFile final : public iceberg::InputFile
{
public:
    explicit TBufferInputFile(TBufferedFile file)
        : Location_(file.Location)
        , Bytes_(file.Bytes)
    { }

    std::string_view location() const override
    {
        return Location_;
    }

    iceberg::Result<int64_t> Size() const override
    {
        return static_cast<int64_t>(Bytes_.size());
    }

    iceberg::Result<std::unique_ptr<iceberg::SeekableInputStream>> Open() override
    {
        return std::make_unique<TBufferInputStream>(Bytes_);
    }

private:
    const std::string_view Location_;
    const std::string_view Bytes_;
};

// Read-only FileIO that serves one already-read file to an upstream decoder.
class TBufferFileIO final : public iceberg::FileIO
{
public:
    TBufferFileIO(std::string location, std::string bytes)
        : Location_(std::move(location))
        , Bytes_(std::move(bytes))
    { }

    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string location) override
    {
        if (location != Location_) {
            return iceberg::NotFound("buffered FileIO serves only {}", Location_);
        }
        return std::make_unique<TBufferInputFile>(TBufferedFile{.Location = Location_, .Bytes = Bytes_});
    }

    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string location, size_t length) override
    {
        if (length != Bytes_.size()) {
            return iceberg::Invalid("CORRUPTION: {} length differs from its metadata", Location_);
        }
        return NewInputFile(std::move(location));
    }

    iceberg::Result<std::unique_ptr<iceberg::OutputFile>> NewOutputFile(std::string) override
    {
        return iceberg::NotAllowed("buffered FileIO is read-only");
    }

    iceberg::Result<std::string> ReadFile(const std::string& location, std::optional<size_t> length) override
    {
        if (location != Location_) {
            return iceberg::NotFound("buffered FileIO serves only {}", Location_);
        }
        if (length && *length != Bytes_.size()) {
            return iceberg::Invalid("CORRUPTION: {} length differs from its metadata", Location_);
        }
        return Bytes_;
    }

    iceberg::Status WriteFile(const std::string&, std::string_view) override
    {
        return iceberg::NotAllowed("buffered FileIO is read-only");
    }

    iceberg::Status DeleteFile(const std::string&) override
    {
        return iceberg::NotAllowed("buffered FileIO is read-only");
    }

private:
    const std::string Location_;
    const std::string Bytes_;
};

} // namespace glamdb::detail
