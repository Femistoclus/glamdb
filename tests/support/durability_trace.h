#pragma once

#include "tests/support/fault_fs.h"

#include <map>
#include <set>

namespace NGlamDB::NTesting {

class TDurabilityTrace
{
public:
    explicit TDurabilityTrace(std::vector<std::string> durableDirectories);
    std::optional<std::string> Apply(const TIOEvent& event);
    std::set<std::string> DurableFiles() const;

private:
    struct TDirectory
    {
        bool Baseline = false;
        bool Synced = false;
        bool Named = false;
        bool Uncertain = false;
    };
    struct TFile
    {
        bool Synced = false;
        bool Named = false;
        bool Uncertain = false;
    };
    std::map<std::string, TDirectory> Directories_;
    std::map<std::string, TFile> Files_;
    std::uint64_t LastSeq_ = 0;

    bool DirectoryDurable(const std::string& path) const;
};

}
