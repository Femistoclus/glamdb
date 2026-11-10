#include "tests/support/durability_trace.h"

#include <stdexcept>

namespace NGlamDB::NTesting {
namespace {

std::string Parent(const std::string& path)
{
    return std::filesystem::path(path).parent_path().generic_string();
}

bool NormalPath(const std::string& path)
{
    const std::filesystem::path parsed(path);
    return path.find('\0') == std::string::npos && parsed.is_absolute() &&
        (parsed == parsed.root_path() || !parsed.filename().empty()) &&
        parsed.lexically_normal().generic_string() == path;
}

}

TDurabilityTrace::TDurabilityTrace(std::vector<std::string> durableDirectories)
{
    for (auto& path : durableDirectories) {
        if (!NormalPath(path)) {
            throw std::invalid_argument("Baseline directory must be an absolute normalized path");
        }
        Directories_.emplace(std::move(path), TDirectory{true, true, true, false});
    }
}

bool TDurabilityTrace::DirectoryDurable(const std::string& path) const
{
    auto found = Directories_.find(path);
    if (found == Directories_.end()) {
        return false;
    }
    const auto& directory = found->second;
    if (directory.Uncertain) {
        return false;
    }
    if (directory.Baseline) {
        return directory.Synced;
    }
    const auto parent = Parent(path);
    return directory.Synced && directory.Named && parent != path && DirectoryDurable(parent);
}

std::set<std::string> TDurabilityTrace::DurableFiles() const
{
    std::set<std::string> result;
    for (const auto& [path, file] : Files_) {
        if (file.Synced && file.Named && !file.Uncertain && DirectoryDurable(Parent(path))) {
            result.insert(path);
        }
    }
    return result;
}

std::optional<std::string> TDurabilityTrace::Apply(const TIOEvent& event)
{
    if (event.Seq == 0 || event.Seq - LastSeq_ != 1) {
        return "Trace sequence is not contiguous";
    }
    LastSeq_ = event.Seq;
    if (event.Op == EIOOp::Publish) {
        const auto durable = DurableFiles();
        for (const auto& reference : event.References) {
            if (!durable.contains(reference)) {
                return "Published file is not durable: " + reference;
            }
        }
        return std::nullopt;
    }
    if (!NormalPath(event.Path)) {
        return event.Error ? std::nullopt : std::optional<std::string>("Invalid trace path");
    }
    if (event.Op == EIOOp::MakeDir && (!event.Error || event.Error == std::errc::file_exists)) {
        Directories_.try_emplace(event.Path);
    }
    if (event.Error) {
        const bool uncertain = event.Error == std::errc::io_error || event.Error == std::errc::read_only_file_system;
        auto file = Files_.find(event.Path);
        if (file != Files_.end() && (event.Op == EIOOp::Write || event.Op == EIOOp::SyncFile)) {
            file->second.Synced = false;
            file->second.Uncertain |= uncertain;
        }
        if (event.Op == EIOOp::SyncDir && Directories_.contains(event.Path)) {
            Directories_.at(event.Path).Synced = false;
            Directories_.at(event.Path).Uncertain |= uncertain;
        }
        return std::nullopt;
    }
    switch (event.Op) {
        case EIOOp::OpenDir:
            Directories_.try_emplace(event.Path);
            break;
        case EIOOp::CreateExclusive:
            if (!Files_.try_emplace(event.Path).second) {
                return "Duplicate exclusive create: " + event.Path;
            }
            break;
        case EIOOp::Write:
        case EIOOp::SyncFile: {
            auto file = Files_.find(event.Path);
            if (file == Files_.end()) {
                return "Unknown file: " + event.Path;
            }
            if (event.Op == EIOOp::Write && event.Bytes != 0) {
                file->second.Synced = false;
            } else if (event.Op == EIOOp::SyncFile) {
                file->second.Synced = true;
            }
            break;
        }
        case EIOOp::SyncDir:
            if (!Directories_.contains(event.Path)) {
                return "Unknown directory: " + event.Path;
            }
            Directories_.at(event.Path).Synced = true;
            for (auto& [path, directory] : Directories_) {
                if (Parent(path) == event.Path) {
                    directory.Named = true;
                }
            }
            for (auto& [path, file] : Files_) {
                if (Parent(path) == event.Path) {
                    file.Named = true;
                }
            }
            break;
        default:
            break;
    }
    return std::nullopt;
}

}
