#include "tests/support/fault_fs.h"

#include <algorithm>
#include <fnmatch.h>
#include <map>
#include <mutex>
#include <stdexcept>
#include <type_traits>

namespace NGlamDB::NTesting {
namespace {

using namespace NIO;

struct TTrackedHandle final : TFSHandle
{
    THandle Inner;
    std::shared_ptr<int> Owner;
    std::string Path;
    TIOTag Tag;
    bool Closed = false;
};

std::string Quote(std::string_view value)
{
    constexpr std::string_view hex = "0123456789abcdef";
    std::string result = "\"";
    for (const unsigned char byte : value) {
        if (byte == '"' || byte == '\\') {
            result += '\\';
            result += static_cast<char>(byte);
        } else if (byte < 0x20 || byte >= 0x7f) {
            result += "\\u00";
            result += hex[byte >> 4];
            result += hex[byte & 15];
        } else {
            result += static_cast<char>(byte);
        }
    }
    return result + '"';
}

std::string_view Name(EIOOp op)
{
    switch (op) {
        case EIOOp::OpenDir:
            return "OpenDir";
        case EIOOp::MakeDir:
            return "MakeDir";
        case EIOOp::CreateExclusive:
            return "CreateExclusive";
        case EIOOp::OpenRead:
            return "OpenRead";
        case EIOOp::Write:
            return "Write";
        case EIOOp::PRead:
            return "PRead";
        case EIOOp::Stat:
            return "Stat";
        case EIOOp::StatFs:
            return "StatFs";
        case EIOOp::MeasureTree:
            return "MeasureTree";
        case EIOOp::SyncFile:
            return "SyncFile";
        case EIOOp::SyncDir:
            return "SyncDir";
        case EIOOp::Close:
            return "Close";
        case EIOOp::Publish:
            return "Publish";
    }
    throw std::invalid_argument("Unknown IO operation");
}

std::string_view Name(EFileKind kind)
{
    switch (kind) {
        case EFileKind::Other:
            return "Other";
        case EFileKind::Data:
            return "Data";
        case EFileKind::Manifest:
            return "Manifest";
        case EFileKind::ManifestList:
            return "ManifestList";
        case EFileKind::Metadata:
            return "Metadata";
    }
    throw std::invalid_argument("Unknown file kind");
}

}

struct TFaultFS::TState
{
    struct TRule
    {
        TFaultRule Fault;
        std::uint64_t Matches = 0;
    };

    ILocalFS& Underlying;
    std::shared_ptr<int> Identity = std::make_shared<int>(0);
    mutable std::mutex Mutex;
    std::vector<TRule> Rules;
    std::vector<TIOEvent> Events;
    std::map<std::string, TIOTag> Tags;

    explicit TState(ILocalFS& fs)
        : Underlying(fs)
    { }

    std::shared_ptr<TTrackedHandle> Get(const THandle& handle) const
    {
        auto tracked = std::dynamic_pointer_cast<TTrackedHandle>(handle);
        return tracked && tracked->Owner == Identity && !tracked->Closed ? tracked : nullptr;
    }

    std::optional<TFaultRule> Match(EIOOp op, const std::string& path)
    {
        std::optional<TFaultRule> selected;
        for (auto& rule : Rules) {
            if (rule.Fault.Op != op || fnmatch(rule.Fault.PathGlob.c_str(), path.c_str(), 0) != 0) {
                continue;
            }
            ++rule.Matches;
            if (!selected && rule.Matches >= rule.Fault.Occurrence &&
                rule.Matches - rule.Fault.Occurrence < rule.Fault.Repeat)
            {
                selected = rule.Fault;
            }
        }
        return selected;
    }

    void Record(EIOOp op, const std::string& path, const TIOTag& tag, std::uint64_t bytes, std::error_code error)
    {
        Events.push_back({Events.size() + 1, op, path, tag, bytes, error, {}});
    }

    std::error_code InvalidHandle(EIOOp op, const TIOTag& tag = {})
    {
        const auto error = std::make_error_code(std::errc::bad_file_descriptor);
        Record(op, {}, tag, 0, error);
        return error;
    }

    template <typename T, class Action>
    TResult<T> Call(EIOOp op, const std::string& path, const TIOTag& tag, Action action)
    {
        auto fault = Match(op, path);
        TResult<T> result = fault && fault->Error && !fault->AfterSuccess
            ? TResult<T>(fault->Error)
            : action(fault ? fault->MaxBytes : std::nullopt);
        if (result && fault && fault->AfterSuccess) {
            result = TResult<T>(fault->Error);
        }
        std::uint64_t bytes = 0;
        if constexpr (std::is_same_v<T, std::size_t> || std::is_same_v<T, std::uint64_t>) {
            if (result) {
                bytes = result.Value();
            }
        }
        Record(op, path, tag, bytes, result.Error());
        return result;
    }

    template <typename Action>
    TResult<THandle> Open(EIOOp op, const std::string& path, const TIOTag& tag, Action action)
    {
        auto result = Call<THandle>(op, path, tag, [&](auto) {
            return action();
        });
        if (!result) {
            return result.Error();
        }
        auto handle = std::make_shared<TTrackedHandle>();
        handle->Inner = std::move(result).Value();
        handle->Owner = Identity;
        handle->Path = path;
        handle->Tag = tag;
        Tags[path] = tag;
        return THandle(std::move(handle));
    }

    template <typename T, class Action>
    TResult<T> WithHandle(const THandle& handle, EIOOp op, Action action)
    {
        std::lock_guard lock(Mutex);
        auto tracked = Get(handle);
        if (!tracked) {
            return InvalidHandle(op);
        }
        return Call<T>(op, tracked->Path, tracked->Tag, [&](auto limit) {
            return action(*tracked, limit);
        });
    }
};

TFaultFS::TFaultFS(ILocalFS& underlying)
    : State_(std::make_unique<TState>(underlying))
{ }
TFaultFS::~TFaultFS() = default;

void TFaultFS::AddRule(TFaultRule rule)
{
    if (rule.Occurrence == 0 || rule.Repeat == 0 || static_cast<bool>(rule.Error) == rule.MaxBytes.has_value() ||
        (rule.MaxBytes && rule.Op != EIOOp::Write && rule.Op != EIOOp::PRead) || rule.Op == EIOOp::Publish ||
        (rule.AfterSuccess && (rule.Op != EIOOp::SyncFile && rule.Op != EIOOp::SyncDir)) ||
        rule.PathGlob.find('\0') != std::string::npos)
    {
        throw std::invalid_argument("Invalid fault rule");
    }
    std::lock_guard lock(State_->Mutex);
    State_->Rules.push_back({std::move(rule), 0});
}

std::vector<TIOEvent> TFaultFS::Events() const
{
    std::lock_guard lock(State_->Mutex);
    return State_->Events;
}

std::string TFaultFS::JsonLines() const
{
    std::string result;
    for (const auto& event : Events()) {
        result += "{\"string_encoding\":\"latin1-bytes\",\"seq\":" + std::to_string(event.Seq) +
            ",\"op\":" + Quote(Name(event.Op)) + ",\"path\":" + Quote(event.Path) +
            ",\"kind\":" + Quote(Name(event.Tag.Kind)) + ",\"attempt_id\":" + Quote(event.Tag.AttemptId) +
            ",\"bytes\":" + std::to_string(event.Bytes) + ",\"result\":" + std::to_string(event.Error.value()) +
            ",\"references\":[";
        for (std::size_t i = 0; i < event.References.size(); ++i) {
            if (i != 0) {
                result += ',';
            }
            result += Quote(event.References[i]);
        }
        result += "]}\n";
    }
    return result;
}

void TFaultFS::Publish(std::vector<std::string> references)
{
    std::lock_guard lock(State_->Mutex);
    State_->Events.push_back({State_->Events.size() + 1, EIOOp::Publish, {}, {}, 0, {}, std::move(references)});
}

TResult<THandle> TFaultFS::OpenDir(const std::filesystem::path& path)
{
    std::lock_guard lock(State_->Mutex);
    auto normalized = path.lexically_normal();
    if (normalized.has_relative_path() && normalized.filename().empty()) {
        normalized = normalized.parent_path();
    }
    return State_->Open(EIOOp::OpenDir, normalized.generic_string(), {}, [&] {
        return State_->Underlying.OpenDir(path);
    });
}

TResult<THandle> TFaultFS::OpenDir(const THandle& parent, std::string_view name)
{
    std::lock_guard lock(State_->Mutex);
    auto tracked = State_->Get(parent);
    if (!tracked) {
        return State_->InvalidHandle(EIOOp::OpenDir);
    }
    const auto path = (std::filesystem::path(tracked->Path) / name).generic_string();
    return State_->Open(EIOOp::OpenDir, path, {}, [&] {
        return State_->Underlying.OpenDir(tracked->Inner, name);
    });
}

std::error_code TFaultFS::MakeDir(const THandle& parent, std::string_view name)
{
    std::lock_guard lock(State_->Mutex);
    auto tracked = State_->Get(parent);
    if (!tracked) {
        return State_->InvalidHandle(EIOOp::MakeDir);
    }
    const auto path = (std::filesystem::path(tracked->Path) / name).generic_string();
    return State_
        ->Call<bool>(EIOOp::MakeDir,
            path,
            {},
            [&](auto) -> TResult<bool> {
                auto error = State_->Underlying.MakeDir(tracked->Inner, name);
                return error ? TResult<bool>(error) : TResult<bool>(true);
            })
        .Error();
}

TResult<THandle> TFaultFS::CreateExclusive(const THandle& parent, std::string_view name, const TIOTag& tag)
{
    std::lock_guard lock(State_->Mutex);
    auto tracked = State_->Get(parent);
    if (!tracked) {
        return State_->InvalidHandle(EIOOp::CreateExclusive, tag);
    }
    const auto path = (std::filesystem::path(tracked->Path) / name).generic_string();
    return State_->Open(EIOOp::CreateExclusive, path, tag, [&] {
        return State_->Underlying.CreateExclusive(tracked->Inner, name, tag);
    });
}

TResult<THandle> TFaultFS::OpenRead(const THandle& parent, std::string_view name)
{
    std::lock_guard lock(State_->Mutex);
    auto tracked = State_->Get(parent);
    if (!tracked) {
        return State_->InvalidHandle(EIOOp::OpenRead);
    }
    const auto path = (std::filesystem::path(tracked->Path) / name).generic_string();
    const auto found = State_->Tags.find(path);
    const auto tag = found == State_->Tags.end() ? TIOTag{} : found->second;
    return State_->Open(EIOOp::OpenRead, path, tag, [&] {
        return State_->Underlying.OpenRead(tracked->Inner, name);
    });
}

TResult<std::size_t> TFaultFS::Write(const THandle& file, std::span<const std::byte> bytes)
{
    return State_->WithHandle<std::size_t>(file, EIOOp::Write, [&](auto& tracked, auto limit) {
        return State_->Underlying.Write(tracked.Inner,
            bytes.first(std::min(bytes.size(), limit.value_or(bytes.size()))));
    });
}

TResult<std::size_t> TFaultFS::PRead(const THandle& file, std::uint64_t offset, std::span<std::byte> bytes)
{
    return State_->WithHandle<std::size_t>(file, EIOOp::PRead, [&](auto& tracked, auto limit) {
        return State_->Underlying.PRead(tracked.Inner,
            offset,
            bytes.first(std::min(bytes.size(), limit.value_or(bytes.size()))));
    });
}

TResult<TFileStat> TFaultFS::Stat(const THandle& handle)
{
    return State_->WithHandle<TFileStat>(handle, EIOOp::Stat, [&](auto& tracked, auto) {
        return State_->Underlying.Stat(tracked.Inner);
    });
}

TResult<TSpaceInfo> TFaultFS::StatFs(const THandle& directory)
{
    return State_->WithHandle<TSpaceInfo>(directory, EIOOp::StatFs, [&](auto& tracked, auto) {
        return State_->Underlying.StatFs(tracked.Inner);
    });
}

TResult<std::uint64_t> TFaultFS::MeasureTree(const THandle& directory, std::uint64_t limit)
{
    return State_->WithHandle<std::uint64_t>(directory, EIOOp::MeasureTree, [&](auto& tracked, auto) {
        return State_->Underlying.MeasureTree(tracked.Inner, limit);
    });
}

std::error_code TFaultFS::SyncFile(const THandle& file)
{
    return State_
        ->WithHandle<bool>(file,
            EIOOp::SyncFile,
            [&](auto& tracked, auto) -> TResult<bool> {
                auto error = State_->Underlying.SyncFile(tracked.Inner);
                return error ? TResult<bool>(error) : TResult<bool>(true);
            })
        .Error();
}

std::error_code TFaultFS::SyncDir(const THandle& directory)
{
    return State_
        ->WithHandle<bool>(directory,
            EIOOp::SyncDir,
            [&](auto& tracked, auto) -> TResult<bool> {
                auto error = State_->Underlying.SyncDir(tracked.Inner);
                return error ? TResult<bool>(error) : TResult<bool>(true);
            })
        .Error();
}

std::error_code TFaultFS::Close(const THandle& handle)
{
    std::lock_guard lock(State_->Mutex);
    auto tracked = State_->Get(handle);
    if (!tracked) {
        return State_->InvalidHandle(EIOOp::Close);
    }
    tracked->Closed = true;
    auto fault = State_->Match(EIOOp::Close, tracked->Path);
    auto error = State_->Underlying.Close(tracked->Inner);
    if (!error && fault) {
        error = fault->Error;
    }
    State_->Record(EIOOp::Close, tracked->Path, tracked->Tag, 0, error);
    return error;
}

}
