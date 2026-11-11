#include "tests/support/journal_fault_vfs.h"
#include "tests/support/temp_dir.h"

#include <atomic>
#include <fstream>
#include <future>
#include <gtest/gtest.h>
#include <thread>

namespace glamdb::test {
namespace {

TEST(JournalFaultVfs, ControlsCanChangeTheFaultWhileDeleteIsPaused)
{
    JournalFaultVfs faults;
    TempDir temporary;
    const auto path = (temporary.path / "data-journal").string();
    auto* vfs = sqlite3_vfs_find(faults.Name().c_str());
    ASSERT_NE(vfs, nullptr);
    for (const auto expected : {SQLITE_IOERR_DELETE, SQLITE_IOERR_DIR_FSYNC, SQLITE_OK}) {
        std::ofstream(path) << "journal";
        faults.Arm(1, false);
        faults.PauseBeforeDelete(1);
        auto deletion = std::async(std::launch::async, [&] {
            return vfs->xDelete(vfs, path.c_str(), 0);
        });
        const bool paused = faults.WaitUntilPaused(std::chrono::seconds(5));
        EXPECT_TRUE(paused);
        if (expected == SQLITE_OK) {
            faults.Disarm();
        } else {
            faults.Arm(1, expected == SQLITE_IOERR_DIR_FSYNC);
        }
        faults.Release();
        EXPECT_EQ(deletion.get(), expected);
        EXPECT_EQ(std::filesystem::exists(path), expected == SQLITE_IOERR_DELETE);
    }
}

TEST(JournalFaultVfs, ConcurrentRearmingAndDeletingPreservesFaultSemantics)
{
    JournalFaultVfs faults;
    TempDir temporary;
    const auto path = (temporary.path / "data-journal").string();
    auto* vfs = sqlite3_vfs_find(faults.Name().c_str());
    ASSERT_NE(vfs, nullptr);
    std::atomic<bool> started{false}, done{false};
    std::thread controls([&] {
        started = true;
        while (!done.load()) {
            faults.Arm(1, false);
            faults.Arm(1, true);
            faults.Disarm();
        }
    });
    while (!started.load()) {
        std::this_thread::yield();
    }
    for (int iteration = 0; iteration < 256; ++iteration) {
        std::ofstream(path) << "journal";
        const auto result = vfs->xDelete(vfs, path.c_str(), 0);
        EXPECT_TRUE(result == SQLITE_OK || result == SQLITE_IOERR_DELETE || result == SQLITE_IOERR_DIR_FSYNC);
        EXPECT_EQ(std::filesystem::exists(path), result == SQLITE_IOERR_DELETE);
    }
    done = true;
    controls.join();
}

} // namespace
} // namespace glamdb::test
