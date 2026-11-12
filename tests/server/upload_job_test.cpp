#include "apps/server/routes.h"
#include "apps/server/upload_job.h"
#include "core/engine/engine_factory.h"
#include "tests/support/fake_storage.h"
#include "tests/support/temp_dir.h"

#include <asio.hpp>
#include <future>
#include <gtest/gtest.h>
#include <thread>

namespace glamdb::NServer {
TEST(UploadJob, FailedWorkerEnqueueRestoresScratchAndClearsPendingState)
{
    auto resources = std::make_shared<ResourceOwner>();
    auto owner = std::make_shared<NTransport::TBulkUpload>(resources, 3);
    const auto original = owner.get();
    std::shared_ptr<TUploadJob> pending;
    auto job = std::make_shared<TUploadJob>();
    EXPECT_THROW(QueueUpload(pending,
                     owner,
                     job,
                     [] {
                         throw std::bad_alloc();
                     }),
        std::bad_alloc);
    EXPECT_FALSE(pending);
    EXPECT_EQ(owner.get(), original);
    EXPECT_EQ(resources->GetUsage().device_reserved_bytes, 3);
}

TEST(UploadJob, FailedCompletionNotificationLeavesAnObservableFinishedJob)
{
    auto job = std::make_shared<TUploadJob>();
    job->Resources = std::make_shared<ResourceOwner>();
    job->Length = 3;
    job->Bytes = "{}\n";
    job->Complete = true;
    EXPECT_NO_THROW(job->Run([] {
        throw std::bad_alloc();
    }));
    EXPECT_TRUE(job->Done.load());
    EXPECT_TRUE(job->NotificationFailed.load());
    EXPECT_FALSE(job->Failure);
    ASSERT_TRUE(job->Upload);
    EXPECT_EQ(job->Upload->ReadLine().value_or(""), "{}\n");
}

TEST(UploadJob, BlockedScratchCloseRetainsResourcesWhileListenerServesHealth)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto backend = std::make_shared<test::FakeStorage>(root);
    auto engine = NTransport::Unwrap(detail::EngineFactory::Create({test::store_id, root.string()}, backend));
    TRoutes routes(engine, {}, "secret");
    auto resources = std::make_shared<ResourceOwner>();
    std::promise<void> entered, release;
    auto enteredFuture = entered.get_future();
    auto releaseFuture = release.get_future();
    std::thread::id closingThread;
    auto job = std::make_shared<TUploadJob>();
    job->Cleanup = true;
    job->Upload = std::shared_ptr<NTransport::TBulkUpload>(new NTransport::TBulkUpload(resources, 3),
        [&](NTransport::TBulkUpload* upload) {
            closingThread = std::this_thread::get_id();
            entered.set_value();
            releaseFuture.wait();
            delete upload;
        });
    asio::thread_pool workers(1);
    asio::post(workers, [job] {
        job->Run([] {
        });
    });
    const bool closeStarted = enteredFuture.wait_for(std::chrono::seconds(3)) == std::future_status::ready;
    asio::io_context io;
    int status = 0;
    asio::post(io, [&] {
        status = routes.Handle({"GET", "/healthz", {}, {}}).Status;
    });
    io.run();
    const auto reserved = resources->GetUsage().device_reserved_bytes;
    const bool doneWhileBlocked = job->Done.load();
    release.set_value();
    workers.join();
    EXPECT_TRUE(closeStarted);
    EXPECT_NE(closingThread, std::this_thread::get_id());
    EXPECT_EQ(status, 200);
    EXPECT_EQ(reserved, 3);
    EXPECT_FALSE(doneWhileBlocked);
    EXPECT_TRUE(job->Done.load());
    EXPECT_EQ(resources->GetUsage().device_reserved_bytes, 0);
}
} // namespace glamdb::NServer
