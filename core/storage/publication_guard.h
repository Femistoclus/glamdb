#pragma once

#include "core/storage/base_state.h"
#include "core/storage/table_profile.h"

#include <optional>
#include <unordered_map>

namespace NGlamDB::NIO {
class TStoreDirs;
}

namespace NGlamDB::NStorage {

enum class EPointerOp : std::uint8_t { InsertTable, UpdateLocation };

struct TPointerChange
{
    EPointerOp Op;
    std::string Namespace;
    std::string Name;
    std::optional<std::string> ExpectedCurrentLocation;
    std::string NewLocation;
};

struct IPublicationGuard
{
    virtual ~IPublicationGuard() = default;
    virtual iceberg::Status BeforePointerChange(const TPointerChange& change) = 0;
};

struct TPublicationContext
{
    std::shared_ptr<NIO::TStoreDirs> Directories;
    std::shared_ptr<iceberg::FileIO> FileIO;
    std::shared_ptr<NIO::TIOAttempt> Attempt;
    std::string Namespace;
    std::string Name;
    TTableProfile Profile;
    std::shared_ptr<const TBaseState> Base{};
    std::unordered_map<std::string, std::string> ExpectedSummary{};
    TStorageUsage StoreUsage{};
    TStorageLimits Limits{};
};

class TStoragePublicationGuard final : public IPublicationGuard
{
public:
    explicit TStoragePublicationGuard(TPublicationContext context);
    iceberg::Status BeforePointerChange(const TPointerChange& change) override;
    // Install this tentative state only after the caller confirms its outer COMMIT.
    std::shared_ptr<const TBaseState> GetCandidate() const;

private:
    const TPublicationContext Context_;
    std::shared_ptr<const TBaseState> Candidate_;
    bool Invoked_ = false;

    iceberg::Status CheckContext(const TPointerChange& change) const;
    iceberg::Status CheckCandidate(const TPointerChange& change);
};

}
