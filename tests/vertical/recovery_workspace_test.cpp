#include "tests/vertical/vertical_support.h"

#include "storage/resources.h"

#include <sqlite3.h>

#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <spawn.h>
#include <sys/wait.h>

extern char** environ;

namespace glamdb {
namespace {
using namespace vertical;
using namespace std::chrono_literals;

class TWorkspaceCrashVfs
{
public:
    explicit TWorkspaceCrashVfs(const std::filesystem::path& root)
        : Prefix_(root.string() + "/.recovery-")
    {
        Parent_ = sqlite3_vfs_find(nullptr);
        if (!Parent_) {
            throw std::runtime_error("SQLite default VFS is missing");
        }
        Vfs_ = *Parent_;
        Vfs_.pNext = nullptr;
        Vfs_.zName = "glamdb-workspace-crash";
        Vfs_.xOpen = Open;
        Active_ = this;
        if (sqlite3_vfs_register(&Vfs_, 1) != SQLITE_OK) {
            throw std::runtime_error("cannot register workspace crash VFS");
        }
    }
    ~TWorkspaceCrashVfs()
    {
        sqlite3_vfs_register(Parent_, 1);
        sqlite3_vfs_unregister(&Vfs_);
        Active_ = nullptr;
    }
    TWorkspaceCrashVfs(const TWorkspaceCrashVfs&) = delete;
    TWorkspaceCrashVfs& operator=(const TWorkspaceCrashVfs&) = delete;

private:
    inline static TWorkspaceCrashVfs* Active_ = nullptr;
    const std::string Prefix_;
    sqlite3_vfs* Parent_ = nullptr;
    sqlite3_vfs Vfs_{};
    const sqlite3_io_methods* Original_ = nullptr;
    sqlite3_io_methods Methods_{};

    static int Open(sqlite3_vfs*, const char* name, sqlite3_file* file, int flags, int* outFlags)
    {
        auto& owner = *Active_;
        const auto code = owner.Parent_->xOpen(owner.Parent_, name, file, flags, outFlags);
        if (code == SQLITE_OK && name && std::string_view(name).starts_with(owner.Prefix_)) {
            owner.Original_ = file->pMethods;
            owner.Methods_ = *file->pMethods;
            owner.Methods_.xWrite = Write;
            file->pMethods = &owner.Methods_;
        }
        return code;
    }
    static int Write(sqlite3_file* file, const void* data, int count, sqlite3_int64 offset)
    {
        const auto code = Active_->Original_->xWrite(file, data, count, offset);
        if (code == SQLITE_OK) {
            std::raise(SIGKILL);
        }
        return code;
    }
};

TEST(RecoveryWorkspaceChild, CrashesDuringVerification)
{
    const auto* root = std::getenv("GLAMDB_WORKSPACE_CRASH_ROOT");
    if (!root) {
        GTEST_SKIP() << "Invoked by the workspace interruption test in a fresh process.";
    }
    ASSERT_EQ(sqlite3_initialize(), SQLITE_OK);
    TWorkspaceCrashVfs crash(root);
    const auto opened = OpenExisting({.absolute_root = root, .expected_store_id = test::store_id});
    FAIL() << "recovery did not reach workspace I/O: " << (opened ? "opened" : opened.error().message);
}

void CrashRecovery(const std::filesystem::path& root)
{
    std::vector<std::string> environment;
    for (auto** entry = environ; *entry; ++entry) {
        if (!std::string_view(*entry).starts_with("GLAMDB_WORKSPACE_CRASH_ROOT=")) {
            environment.emplace_back(*entry);
        }
    }
    environment.push_back("GLAMDB_WORKSPACE_CRASH_ROOT=" + root.string());
    std::vector<char*> variables;
    variables.reserve(environment.size() + 1);
    for (auto& value : environment) {
        variables.push_back(value.data());
    }
    variables.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=RecoveryWorkspaceChild.CrashesDuringVerification";
    char* arguments[]{executable.data(), filter.data(), nullptr};
    pid_t child = 0;
    ASSERT_EQ(posix_spawn(&child, executable.c_str(), nullptr, nullptr, arguments, variables.data()), 0);
    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(waited, child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
}

TEST(RecoveryWorkspaces, RepeatedInterruptedRecoveryReclaimsFilesAndPreservesReceipts)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto created = CreateStore({.store_id = test::store_id, .absolute_root = root.string()});
    ASSERT_TRUE(created) << created.error().message;
    ASSERT_EQ((*created)->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    AppendRequest request{test::collection_id, FirstOperation, {}, {Batch(FirstRows)}};
    const auto receipt = (*created)->AppendDurable(request);
    ASSERT_EQ(receipt.disposition, CommitDisposition::Committed);
    ASSERT_TRUE((*created)->Shutdown(1000ms));
    created->reset();
    for (int attempt = 0; attempt < 3; ++attempt) {
        ASSERT_NO_FATAL_FAILURE(CrashRecovery(root));
        std::vector<std::filesystem::path> abandoned;
        for (const auto& entry : std::filesystem::directory_iterator(root)) {
            if (entry.path().filename().string().starts_with(".recovery-")) {
                abandoned.push_back(entry.path());
            }
        }
        ASSERT_EQ(abandoned.size(), 1);
        EXPECT_GT(std::filesystem::file_size(abandoned.front()), 0);
        auto opened = OpenExisting({.absolute_root = root.string(), .expected_store_id = test::store_id});
        ASSERT_TRUE(opened) << opened.error().message;
        ASSERT_FALSE(std::filesystem::exists(abandoned.front()));
        EXPECT_EQ((*opened)->AppendDurable(request), receipt);
        {
            auto scan = (*opened)->Scan({.collection_id = test::collection_id});
            ASSERT_TRUE(scan) << scan.error().message;
            auto batch = (*scan)->Next();
            ASSERT_TRUE(batch);
            ASSERT_NE(*batch, nullptr);
            EXPECT_EQ(Sorted(Render(**batch)), Expected({&FirstRows}));
        }
        ASSERT_TRUE((*opened)->Shutdown(1000ms));
    }
}

TEST(RecoveryWorkspaces, CleanupRequiresOwnershipAndIdentityAndPreservesOtherPaths)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto created = CreateStore({.store_id = test::store_id, .absolute_root = root.string()});
    ASSERT_TRUE(created) << created.error().message;
    const auto stale = root / ".recovery-STALE1";
    const auto other = root / ".recovery-keep";
    const auto directory = root / ".recovery-DIR001";
    const auto link = root / ".recovery-LINK01";
    const auto target = temporary.path / "user-file";
    std::ofstream(stale) << "temporary";
    std::ofstream(other) << "keep";
    std::ofstream(target) << "user data";
    std::filesystem::create_directory(directory);
    std::filesystem::create_symlink(target, link);
    const auto busy = OpenExisting({.absolute_root = root.string(), .expected_store_id = test::store_id});
    ASSERT_FALSE(busy);
    EXPECT_EQ(busy.error().code, "STATE_IN_USE");
    EXPECT_TRUE(std::filesystem::exists(stale));
    ASSERT_TRUE((*created)->Shutdown(1000ms));
    created->reset();
    const auto wrong = OpenExisting({.absolute_root = root.string(), .expected_store_id = test::table_uuid});
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error().code, "STORE_IDENTITY_MISMATCH");
    EXPECT_TRUE(std::filesystem::exists(stale));
    auto reopened = OpenExisting({.absolute_root = root.string(), .expected_store_id = test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    EXPECT_FALSE(std::filesystem::exists(stale));
    EXPECT_TRUE(std::filesystem::exists(other));
    EXPECT_TRUE(std::filesystem::is_directory(directory));
    EXPECT_TRUE(std::filesystem::is_symlink(link));
    EXPECT_EQ(std::filesystem::file_size(target), 9);
}

TEST(RecoveryWorkspaces, CleanupPrecedesDeviceAdmission)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto created = CreateStore({.store_id = test::store_id, .absolute_root = root.string()});
    ASSERT_TRUE(created) << created.error().message;
    ASSERT_TRUE((*created)->Shutdown(1000ms));
    created->reset();
    const auto stale = root / ".recovery-STALE1";
    std::ofstream(stale) << "temporary";
    ResourceLimits limits;
    limits.device_reserved_bytes = 0;
    auto owner = std::make_shared<ResourceOwner>(limits);
    const auto refused =
        OpenExisting({.absolute_root = root.string(), .expected_store_id = test::store_id, .resources = owner});
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    EXPECT_FALSE(std::filesystem::exists(stale));
    EXPECT_EQ(owner->GetUsage().memory_bytes, (std::array<uint64_t, 5>{}));
    EXPECT_EQ(owner->GetUsage().device_reserved_bytes, 0);
}
} // namespace
} // namespace glamdb
