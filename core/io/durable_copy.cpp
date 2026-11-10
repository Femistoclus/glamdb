#include "core/io/durable_copy.h"

#include "core/io/durable_file.h"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <utility>

namespace NGlamDB::NIO {
namespace {

bool IsLeaf(std::string_view name)
{
    return !name.empty() && name != "." && name != ".." && name.find_first_of("/\0", 0, 2) == std::string_view::npos;
}

class TSourceFile
{
public:
    TSourceFile(std::shared_ptr<ILocalFS> fs, THandle file)
        : Fs_(std::move(fs))
        , File_(std::move(file))
    { }

    ~TSourceFile()
    {
        [[maybe_unused]] const auto error = Close();
    }

    const THandle& GetHandle() const
    {
        return File_;
    }

    std::error_code Close() noexcept
    {
        const auto file = std::exchange(File_, {});
        try {
            return file ? Fs_->Close(file) : std::error_code{};
        } catch (const std::bad_alloc&) {
            return std::make_error_code(std::errc::not_enough_memory);
        } catch (const std::system_error& error) {
            return error.code();
        } catch (...) {
            return std::make_error_code(std::errc::io_error);
        }
    }

private:
    const std::shared_ptr<ILocalFS> Fs_;
    THandle File_;
};

std::error_code CopyBytes(ILocalFS& fs, const THandle& source, TDurableFile& target, std::uint64_t length)
{
    std::array<std::byte, 65'536> buffer;
    std::uint64_t offset = 0;
    while (offset < length) {
        const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(length - offset, buffer.size()));
        auto read = fs.PRead(source, offset, std::span(buffer).first(count));
        if (!read) {
            if (read.Error() == std::errc::interrupted) {
                continue;
            }
            return read.Error();
        }
        if (read.Value() == 0 || read.Value() > count) {
            return std::make_error_code(std::errc::state_not_recoverable);
        }
        if (auto error = target.Write(std::span(buffer).first(read.Value()))) {
            return error;
        }
        if (auto error = target.Error()) {
            return error;
        }
        offset += read.Value();
    }
    return {};
}

}

TResult<TDurableFileReceipt>
CopyDurableFile(const TCopySource& source, const TCopyTarget& target, std::uint64_t expectedLength)
{
    if (!source.Fs || !source.Directory || !target.Fs || !target.Directory || !target.Latch || !IsLeaf(source.Name) ||
        !IsLeaf(target.Name))
    {
        return std::errc::invalid_argument;
    }
    if (expectedLength > std::numeric_limits<std::int64_t>::max()) {
        return std::errc::value_too_large;
    }
    if (auto error = target.Latch->Error()) {
        return error;
    }
    try {
        auto opened = source.Fs->OpenRead(source.Directory, source.Name);
        if (!opened) {
            return opened.Error();
        }
        TSourceFile input(source.Fs, std::move(opened).Value());
        const auto before = source.Fs->Stat(input.GetHandle());
        if (!before) {
            return before.Error();
        }
        if (before.Value().Type != EFileType::Regular || before.Value().Size != expectedLength) {
            return std::errc::state_not_recoverable;
        }
        auto output = TDurableFile::Create(target.Fs, target.Directory, target.Name, target.Latch);
        if (!output) {
            return output.Error();
        }
        if (auto error = CopyBytes(*source.Fs, input.GetHandle(), *output.Value(), expectedLength)) {
            return error;
        }
        const auto after = source.Fs->Stat(input.GetHandle());
        if (!after) {
            return after.Error();
        }
        if (after.Value().Type != EFileType::Regular || after.Value().Size != before.Value().Size ||
            after.Value().Device != before.Value().Device || after.Value().Inode != before.Value().Inode)
        {
            return std::errc::state_not_recoverable;
        }
        if (auto error = input.Close()) {
            return error;
        }
        if (auto error = output.Value()->Close()) {
            return error;
        }
        auto receipt = output.Value()->Receipt();
        return receipt ? TResult<TDurableFileReceipt>(std::move(*receipt))
                       : TResult<TDurableFileReceipt>(std::errc::state_not_recoverable);
    } catch (const std::bad_alloc&) {
        return std::errc::not_enough_memory;
    } catch (const std::system_error& error) {
        return error.code();
    } catch (...) {
        return std::errc::io_error;
    }
}

}
