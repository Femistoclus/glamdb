#pragma once

#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace glamdb::NServer {

struct TDropIdentity
{
    std::string Device;
    std::string Inode;
    std::string Token;
};

class TDropStore
{
public:
    TDropStore(const std::filesystem::path& serviceRoot,
        const std::string& id,
        const std::optional<TDropIdentity>& expected = std::nullopt,
        int serviceRootDescriptor = -1,
        int databasesDescriptor = -1);
    ~TDropStore();
    TDropStore(const TDropStore&) = delete;
    TDropStore& operator=(const TDropStore&) = delete;

    TDropIdentity GetIdentity() const;
    // Persist the returned identity in the registry before calling Remove.
    TDropIdentity Prepare();
    void Remove(const std::function<void(std::string_view)>& checkpoint = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace glamdb::NServer
