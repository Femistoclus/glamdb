#include "apps/server/drop_store.h"

#include "apps/transport/codec.h"

#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <fcntl.h>
#include <fstream>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <stdexcept>
#include <sys/file.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace glamdb::NServer {
namespace {

constexpr auto DataName = "00000000-0000-0000-0000-000000000003-00000000-0000-0000-0000-000000000004-00000.parquet";
constexpr auto MetadataName = "00000-00000000-0000-0000-0000-000000000004.metadata.json";

bool InstallSyscallFailure(int syscallNumber)
{
    std::array<sock_filter, 4> filter{{
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, nr)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, static_cast<unsigned>(syscallNumber), 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EIO),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    }};
    const sock_fprog program{.len = static_cast<unsigned short>(filter.size()), .filter = filter.data()};
    return prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) == 0 &&
        syscall(SYS_seccomp, SECCOMP_SET_MODE_FILTER, 0, &program) == 0;
}

class TDropStoreTest : public testing::Test
{
protected:
    test::TempDir Temporary_;
    const std::filesystem::path ServiceRoot_ = Temporary_.path / "service";
    const std::filesystem::path Root_ = ServiceRoot_ / "databases" / test::store_id;
    const std::filesystem::path Collection_ = Root_ / "warehouse" / test::collection_id;
    const std::filesystem::path Data_ = Collection_ / "data" / DataName;

    void SetUp() override
    {
        CreateStore();
    }

    void CreateStore()
    {
        std::filesystem::create_directories(Collection_ / "data");
        std::filesystem::create_directory(Collection_ / "metadata");
        for (const auto* marker : {"store.identity", "store.ready"}) {
            std::ofstream(Root_ / marker) << "GLAMDB\n1\n" << test::store_id << "\nlocal-delete-extra-v1\n";
        }
        std::ofstream(Root_ / "catalog.sqlite") << "catalog";
        std::ofstream(Root_ / "catalog.sqlite-journal") << "journal";
        std::ofstream(Root_ / ".recovery-aB019z") << "recovery";
        std::ofstream(Data_) << "parquet";
        std::ofstream(Collection_ / "metadata" / MetadataName) << "metadata";
        std::ofstream(Collection_ / "metadata/00000000-0000-0000-0000-000000000004-m0.avro") << "manifest";
        std::ofstream(Collection_ / "metadata/snap-123-0-00000000-0000-0000-0000-000000000004.avro") << "list";
        std::ofstream(Collection_ / "metadata/snap--123-1-00000000-0000-0000-0000-000000000004.avro") << "list";
    }

    void ResetStore()
    {
        std::filesystem::remove_all(ServiceRoot_);
        CreateStore();
    }

    size_t CountEntries() const
    {
        return static_cast<size_t>(std::distance(std::filesystem::recursive_directory_iterator(Root_),
            std::filesystem::recursive_directory_iterator{}));
    }
};

TEST_F(TDropStoreTest, RemovesOwnedLayoutAndKeepsSibling)
{
    const auto sibling = ServiceRoot_ / "databases/retained";
    std::filesystem::create_directory(sibling);
    std::ofstream(sibling / "keep") << "retained";
    struct stat status
    { };
    ASSERT_EQ(lstat(Root_.c_str(), &status), 0);
    TDropStore store(ServiceRoot_, test::store_id);
    EXPECT_EQ(store.GetIdentity().Device, std::to_string(status.st_dev));
    EXPECT_EQ(store.GetIdentity().Inode, std::to_string(status.st_ino));
    EXPECT_TRUE(std::filesystem::exists(Data_));
    store.Prepare();
    store.Remove();
    EXPECT_FALSE(std::filesystem::exists(Root_));
    EXPECT_TRUE(std::filesystem::exists(sibling / "keep"));
}

TEST_F(TDropStoreTest, ResumesAfterEveryMutationAndSyncBoundary)
{
    for (const std::string_view boundary :
        {"file-unlinked", "directory-unlinked", "directory-synced", "root-synced", "root-unlinked", "parent-synced"})
    {
        SCOPED_TRACE(boundary);
        ResetStore();
        TDropIdentity identity;
        bool interrupted = false;
        {
            TDropStore store(ServiceRoot_, test::store_id);
            identity = store.Prepare();
            EXPECT_THROW(store.Remove([&](std::string_view point) {
                if (point == boundary) {
                    interrupted = true;
                    throw std::runtime_error("simulated interruption");
                }
            }),
                std::runtime_error);
        }
        ASSERT_TRUE(interrupted);
        {
            TDropStore resumed(ServiceRoot_, test::store_id, identity);
            resumed.Remove();
        }
        EXPECT_FALSE(std::filesystem::exists(Root_));
        TDropStore repeated(ServiceRoot_, test::store_id, identity);
        EXPECT_NO_THROW(repeated.Remove());
    }
}

TEST_F(TDropStoreTest, RetainsIdentityMarkersDuringWarehouseRemoval)
{
    TDropIdentity identity;
    {
        TDropStore store(ServiceRoot_, test::store_id);
        identity = store.Prepare();
        EXPECT_THROW(store.Remove([&](std::string_view point) {
            if (point == "file-unlinked" && !std::filesystem::exists(Data_)) {
                EXPECT_TRUE(std::filesystem::exists(Root_ / "store.identity"));
                EXPECT_TRUE(std::filesystem::exists(Root_ / "store.ready"));
                throw std::runtime_error("simulated interruption");
            }
        }),
            std::runtime_error);
    }
    ASSERT_TRUE(std::filesystem::exists(Root_));
    TDropStore resumed(ServiceRoot_, test::store_id, identity);
    resumed.Remove();
    EXPECT_FALSE(std::filesystem::exists(Root_));
}

TEST_F(TDropStoreTest, RejectsUnknownContentsBeforeAnyRemoval)
{
    for (const std::filesystem::path unexpected : {"operator-notes",
             "warehouse/not-a-collection",
             "warehouse/00112233-4455-6677-8899-aabbccddeeff/data/unowned.parquet",
             "warehouse/00112233-4455-6677-8899-aabbccddeeff/metadata/private.json"})
    {
        SCOPED_TRACE(unexpected);
        ResetStore();
        std::ofstream(Root_ / unexpected) << "retain";
        const auto count = CountEntries();
        EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id), NTransport::TFailure);
        EXPECT_EQ(CountEntries(), count);
        EXPECT_TRUE(std::filesystem::exists(Data_));
    }
}

TEST_F(TDropStoreTest, RejectsForeignOrMalformedMarkers)
{
    for (const auto* invalid : {"GLAMDB\n1\n00000000-0000-0000-0000-000000000099\nlocal-delete-extra-v1\n",
             "GLAMDB\n2\n00000000-0000-0000-0000-000000000001\nlocal-delete-extra-v1\n",
             "GLAMDB\n1\n00000000-0000-0000-0000-000000000001\nforeign\n",
             "GLAMDB\n1\n00000000-0000-0000-0000-000000000001\nlocal-delete-extra-v1\nextra"})
    {
        SCOPED_TRACE(invalid);
        ResetStore();
        std::ofstream(Root_ / "store.ready") << invalid;
        EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id), NTransport::TFailure);
        EXPECT_TRUE(std::filesystem::exists(Data_));
    }
}

TEST_F(TDropStoreTest, MissingMarkersRequireRecordedDirectoryIdentity)
{
    for (const int missingCount : {1, 2}) {
        ResetStore();
        TDropIdentity identity;
        {
            TDropStore store(ServiceRoot_, test::store_id);
            identity = store.Prepare();
        }
        std::filesystem::remove(Root_ / "store.ready");
        if (missingCount == 2) {
            std::filesystem::remove(Root_ / "store.identity");
        }
        EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id), NTransport::TFailure);
        TDropStore resumed(ServiceRoot_, test::store_id, identity);
        resumed.Remove();
        EXPECT_FALSE(std::filesystem::exists(Root_));
    }
}

TEST_F(TDropStoreTest, ExactMarkersPermitCleanupOfMissingCatalogAndWarehouse)
{
    std::filesystem::remove(Root_ / "catalog.sqlite");
    std::filesystem::remove_all(Root_ / "warehouse");
    TDropStore store(ServiceRoot_, test::store_id);
    store.Prepare();
    EXPECT_NO_THROW(store.Remove());
    EXPECT_FALSE(std::filesystem::exists(Root_));
}

TEST_F(TDropStoreTest, RejectsSymlinksHardLinksAndSpecialFiles)
{
    for (const std::string_view kind : {"marker-symlink", "data-symlink", "directory-symlink", "hard-link", "fifo"}) {
        SCOPED_TRACE(kind);
        ResetStore();
        const auto outside = Temporary_.path / "outside";
        std::ofstream(outside) << "keep";
        if (kind == "marker-symlink") {
            std::filesystem::remove(Root_ / "store.ready");
            std::filesystem::create_symlink(outside, Root_ / "store.ready");
        } else if (kind == "data-symlink") {
            std::filesystem::remove(Data_);
            std::filesystem::create_symlink(outside, Data_);
        } else if (kind == "directory-symlink") {
            std::filesystem::remove_all(Collection_ / "data");
            std::filesystem::create_directory_symlink(Temporary_.path, Collection_ / "data");
        } else if (kind == "hard-link") {
            std::filesystem::remove(Data_);
            std::filesystem::create_hard_link(outside, Data_);
        } else {
            std::filesystem::remove(Data_);
            ASSERT_EQ(mkfifo(Data_.c_str(), 0600), 0);
        }
        const auto count = CountEntries();
        EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id), NTransport::TFailure);
        EXPECT_EQ(CountEntries(), count);
        EXPECT_TRUE(std::filesystem::exists(outside));
        EXPECT_TRUE(std::filesystem::exists(Root_ / "catalog.sqlite"));
    }
}

TEST_F(TDropStoreTest, RejectsRootAndParentSymlinks)
{
    for (const bool parent : {false, true}) {
        ResetStore();
        const auto selected = parent ? Root_.parent_path() : Root_;
        const auto moved = Temporary_.path / (parent ? "parent" : "root");
        std::filesystem::rename(selected, moved);
        std::filesystem::create_directory_symlink(moved, selected);
        EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id), NTransport::TFailure);
        EXPECT_TRUE(
            std::filesystem::exists(parent ? moved / test::store_id / "catalog.sqlite" : moved / "catalog.sqlite"));
    }
}

TEST_F(TDropStoreTest, RejectsReplacementBeforeRemoval)
{
    TDropStore store(ServiceRoot_, test::store_id);
    store.Prepare();
    const auto moved = Temporary_.path / "original";
    std::filesystem::rename(Root_, moved);
    CreateStore();
    EXPECT_THROW(store.Remove(), NTransport::TFailure);
    EXPECT_TRUE(std::filesystem::exists(Data_));
    EXPECT_TRUE(std::filesystem::exists(moved / "catalog.sqlite"));
}

TEST_F(TDropStoreTest, RejectsRecreatedRootDuringRecovery)
{
    TDropIdentity identity;
    {
        TDropStore store(ServiceRoot_, test::store_id);
        identity = store.Prepare();
    }
    std::filesystem::rename(Root_, Temporary_.path / "original");
    CreateStore();
    EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id, identity), NTransport::TFailure);
    EXPECT_TRUE(std::filesystem::exists(Data_));
}

TEST_F(TDropStoreTest, HoldsExclusiveStoreLease)
{
    {
        TDropStore store(ServiceRoot_, test::store_id);
        EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id), NTransport::TFailure);
        const int descriptor = open(Root_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        ASSERT_GE(descriptor, 0);
        EXPECT_EQ(flock(descriptor, LOCK_EX | LOCK_NB), -1);
        EXPECT_EQ(close(descriptor), 0);
    }
    EXPECT_NO_THROW(TDropStore(ServiceRoot_, test::store_id));
}

TEST_F(TDropStoreTest, RechecksFullLayoutBeforeFirstRemoval)
{
    TDropStore store(ServiceRoot_, test::store_id);
    store.Prepare();
    const auto unexpected = Collection_ / "metadata/operator-notes";
    std::ofstream(unexpected) << "retain";
    const auto count = CountEntries();
    EXPECT_THROW(store.Remove(), NTransport::TFailure);
    EXPECT_EQ(CountEntries(), count);
    EXPECT_TRUE(std::filesystem::exists(Data_));
}

TEST_F(TDropStoreTest, RejectsRootReplacementBetweenMutations)
{
    TDropStore store(ServiceRoot_, test::store_id);
    store.Prepare();
    const auto moved = Temporary_.path / "original";
    bool replaced = false;
    EXPECT_THROW(store.Remove([&](std::string_view point) {
        if (point == "file-unlinked" && !replaced) {
            replaced = true;
            std::filesystem::rename(Root_, moved);
            CreateStore();
        }
    }),
        NTransport::TFailure);
    ASSERT_TRUE(replaced);
    EXPECT_TRUE(std::filesystem::exists(Data_));
    EXPECT_TRUE(std::filesystem::exists(moved / "store.identity"));
}

TEST_F(TDropStoreTest, RecoveryRejectsUnsafeRemainingEntries)
{
    TDropIdentity identity;
    {
        TDropStore store(ServiceRoot_, test::store_id);
        identity = store.Prepare();
    }
    std::filesystem::remove(Root_ / "store.ready");
    std::filesystem::remove(Data_);
    std::filesystem::create_symlink(Temporary_.path, Data_);
    EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id, identity), NTransport::TFailure);
    EXPECT_TRUE(std::filesystem::is_symlink(Data_));
    EXPECT_TRUE(std::filesystem::exists(Root_ / "catalog.sqlite"));
}

TEST_F(TDropStoreTest, RejectsInvalidExpectedIdentityAndDatabaseID)
{
    for (const auto& invalid :
        {TDropIdentity{"", "1", ""}, TDropIdentity{"1", "-1", ""}, TDropIdentity{"1", "18446744073709551616", ""}})
    {
        EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id, invalid), NTransport::TFailure);
    }
    EXPECT_THROW(TDropStore(ServiceRoot_, "../escape"), NTransport::TFailure);
    EXPECT_TRUE(std::filesystem::exists(Data_));
}

TEST_F(TDropStoreTest, SyncAndCloseErrorsRemainRecoverableFailures)
{
    for (const std::string_view failure : {"directory-sync", "parent-sync", "close"}) {
        SCOPED_TRACE(failure);
        ResetStore();
        TDropIdentity identity;
        {
            TDropStore store(ServiceRoot_, test::store_id);
            identity = store.Prepare();
        }
        ASSERT_EXIT(
            {
                TDropStore store(ServiceRoot_, test::store_id, identity);
                if (failure != "parent-sync" && !InstallSyscallFailure(failure == "close" ? SYS_close : SYS_fsync)) {
                    _exit(20);
                }
                try {
                    store.Remove([&](std::string_view point) {
                        if (failure == "parent-sync" && point == "root-unlinked" && !InstallSyscallFailure(SYS_fsync)) {
                            _exit(21);
                        }
                    });
                } catch (const NTransport::TFailure& error) {
                    _exit(error.Detail.code == "IO_ERROR" &&
                                error.Detail.message.find(test::store_id) == std::string::npos &&
                                error.Detail.message.find(Temporary_.path.string()) == std::string::npos
                            ? 0
                            : 22);
                }
                _exit(23);
            },
            testing::ExitedWithCode(0),
            "");
        EXPECT_EQ(std::filesystem::exists(Root_), failure != "parent-sync");
        TDropStore resumed(ServiceRoot_, test::store_id, identity);
        EXPECT_NO_THROW(resumed.Remove());
        EXPECT_FALSE(std::filesystem::exists(Root_));
    }
}

TEST_F(TDropStoreTest, PreparationCreatesDurableReusableDeletionSeal)
{
    TDropIdentity identity;
    const auto count = CountEntries();
    {
        TDropStore store(ServiceRoot_, test::store_id);
        EXPECT_FALSE(std::filesystem::exists(Root_ / "store.deleting"));
        EXPECT_TRUE(store.GetIdentity().Token.empty());
        identity = store.Prepare();
        EXPECT_EQ(identity.Token.size(), 36U);
        EXPECT_TRUE(std::filesystem::is_regular_file(Root_ / "store.deleting"));
        EXPECT_EQ(CountEntries(), count + 1);
        EXPECT_EQ(store.Prepare().Token, identity.Token);
        EXPECT_TRUE(std::filesystem::exists(Data_));
    }
    TDropStore retried(ServiceRoot_, test::store_id);
    EXPECT_EQ(retried.Prepare().Token, identity.Token);
    retried.Remove();
    EXPECT_FALSE(std::filesystem::exists(Root_));
}

TEST_F(TDropStoreTest, RemovalRequiresPreparedDeletionSeal)
{
    TDropStore store(ServiceRoot_, test::store_id);
    const auto count = CountEntries();
    EXPECT_THROW(store.Remove(), NTransport::TFailure);
    EXPECT_EQ(CountEntries(), count);
    EXPECT_TRUE(std::filesystem::exists(Data_));
}

TEST_F(TDropStoreTest, RecoveryRequiresOriginalDeletionTokenEvenWhenInodeMatches)
{
    TDropIdentity oldIdentity;
    {
        TDropStore store(ServiceRoot_, test::store_id);
        oldIdentity = store.Prepare();
    }
    ResetStore();
    TDropIdentity replacementIdentity;
    {
        TDropStore replacement(ServiceRoot_, test::store_id);
        replacementIdentity = replacement.Prepare();
    }
    ASSERT_NE(replacementIdentity.Token, oldIdentity.Token);
    replacementIdentity.Token = oldIdentity.Token;
    EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id, replacementIdentity), NTransport::TFailure);
    EXPECT_TRUE(std::filesystem::exists(Data_));
    EXPECT_TRUE(std::filesystem::exists(Root_ / "store.deleting"));
}

TEST_F(TDropStoreTest, RecoveryRefusesExistingDirectoryAfterSealRemoval)
{
    TDropIdentity identity;
    {
        TDropStore store(ServiceRoot_, test::store_id);
        identity = store.Prepare();
        EXPECT_THROW(store.Remove([](std::string_view point) {
            if (point == "seal-unlinked") {
                throw std::runtime_error("simulated interruption");
            }
        }),
            std::runtime_error);
    }
    ASSERT_TRUE(std::filesystem::exists(Root_));
    EXPECT_TRUE(std::filesystem::is_empty(Root_));
    EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id, identity), NTransport::TFailure);
    EXPECT_TRUE(std::filesystem::exists(Root_));
}

TEST_F(TDropStoreTest, EmptyTokenRequiresOriginalInitializedLayout)
{
    TDropIdentity identity;
    {
        TDropStore store(ServiceRoot_, test::store_id);
        identity = store.GetIdentity();
    }
    ASSERT_TRUE(identity.Token.empty());
    std::filesystem::remove(Root_ / "store.ready");
    EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id, identity), NTransport::TFailure);
    EXPECT_TRUE(std::filesystem::exists(Data_));
}

TEST_F(TDropStoreTest, RejectsServiceAndParentReplacementsAgainstLifetimeDescriptors)
{
    for (const bool parent : {false, true}) {
        ResetStore();
        const int serviceDescriptor = open(ServiceRoot_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        ASSERT_GE(serviceDescriptor, 0);
        const int databasesDescriptor = openat(serviceDescriptor, "databases", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        ASSERT_GE(databasesDescriptor, 0);
        const auto moved = Temporary_.path / (parent ? "original-parent" : "original-service");
        std::filesystem::rename(parent ? Root_.parent_path() : ServiceRoot_, moved);
        CreateStore();
        EXPECT_THROW(TDropStore(ServiceRoot_, test::store_id, std::nullopt, serviceDescriptor, databasesDescriptor),
            NTransport::TFailure);
        EXPECT_TRUE(std::filesystem::exists(Data_));
        EXPECT_TRUE(std::filesystem::exists(parent ? moved / test::store_id / "catalog.sqlite"
                                                   : moved / "databases" / test::store_id / "catalog.sqlite"));
        EXPECT_EQ(close(databasesDescriptor), 0);
        EXPECT_EQ(close(serviceDescriptor), 0);
    }
}

} // namespace
} // namespace glamdb::NServer
