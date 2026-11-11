#include "apps/server/databases.h"
#include "apps/server/server.h"
#include "storage/resources.h"

#include <arrow/memory_pool.h>

#include <iostream>
#include <sys/resource.h>

int main(int argc, char** argv)
try {
    glamdb::NTransport::Require(argc == 2 || argc == 3, "expected service directory and optional saturation mode");
    glamdb::ResourceLimits limits;
    limits.memory_bytes[1] = 8ULL << 30;
    if (argc == 3 && std::string_view(argv[2]) == "many-readers") {
        limits.memory_bytes[0] = 4ULL << 30;
        limits.memory_bytes[1] = 16ULL << 30;
        limits.memory_bytes[4] = 8ULL << 30;
    }
    auto owner = std::make_shared<glamdb::ResourceOwner>(limits);
    {
        auto databases = std::make_shared<glamdb::NServer::TDatabases>(argv[1], "", owner);
        std::vector<std::shared_ptr<glamdb::ResourceReservation>> saturated;
        if (argc == 3 && std::string_view(argv[2]) == "full") {
            for (const auto resource :
                {glamdb::ResourceClass::Data, glamdb::ResourceClass::Metadata, glamdb::ResourceClass::Control})
            {
                const auto index = static_cast<size_t>(resource);
                saturated.push_back(glamdb::NTransport::Unwrap(owner->Reserve(resource,
                    owner->GetLimits().memory_bytes[index] - owner->GetUsage().memory_bytes[index])));
            }
        }
        glamdb::NServer::TRoutes routes(databases, "", glamdb::NTransport::ReadToken());
        glamdb::NServer::RunServer(routes, "127.0.0.1", 0, std::chrono::seconds(10));
        routes.CancelScans();
        databases->Shutdown();
    }
    const auto usage = owner->GetUsage();
    rusage process{};
    glamdb::NTransport::Require(getrusage(RUSAGE_SELF, &process) == 0, "cannot measure peak RSS");
    std::cout << glamdb::NTransport::TJson({{"memory_reservation_peak_bytes", usage.memory_peak},
                                               {"memory_remaining_bytes", usage.memory_bytes},
                                               {"device_reservation_peak_bytes", usage.device_peak_bytes},
                                               {"inode_reservation_peak", usage.device_peak_inodes},
                                               {"device_remaining_bytes", usage.device_reserved_bytes},
                                               {"inode_remaining", usage.device_reserved_inodes},
                                               {"arrow_peak_bytes", arrow::default_memory_pool()->max_memory()},
                                               {"rss_peak_bytes", static_cast<uint64_t>(process.ru_maxrss) * 1024}})
                     .dump()
              << '\n';
    return 0;
} catch (const glamdb::NTransport::TFailure& error) {
    std::cerr << error.Detail.code << ": " << error.Detail.message << '\n';
    return 1;
} catch (const std::exception& error) {
    std::cerr << "profile failed: " << error.what() << '\n';
    return 1;
}
