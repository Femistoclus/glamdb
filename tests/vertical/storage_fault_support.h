#pragma once

#include "tests/support/fault_fs.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

#include <cstdint>

namespace glamdb::vertical {

using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;

enum class ESecondOutcome : std::uint8_t { Absent, Failed, Committed };

struct TChildInvocation
{
    std::string_view Test;
    std::string_view State;
    int ExpectedExit = 0;
};

AppendRequest FirstRequest();
AppendRequest SecondRequest();
void ExpectRows(const std::shared_ptr<Engine>& engine, bool secondCommitted);
void ExpectHistory(const std::shared_ptr<Engine>& engine, const AppendOutcome& first, bool secondCommitted);
void SaveOutcome(const std::filesystem::path& path, const AppendOutcome& outcome);
void RunChild(const std::filesystem::path& root, TChildInvocation invocation);
void ExpectRecoveryInFreshProcess(const std::filesystem::path& root,
    ESecondOutcome state,
    const AppendOutcome& first,
    const AppendOutcome& second);

class TStorageFaultTest : public ::testing::Test
{
protected:
    test::JournalFaultVfs Vfs_;
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<TFaultFS> Faults_ = std::make_shared<TFaultFS>(Posix_);
    std::shared_ptr<Engine> Engine_;
    std::size_t BaselineStart_ = 0;
    AppendOutcome First_;

    void SetUp() override
    {
        const detail::BackendFactory factory =
            [fs = Faults_](const std::filesystem::path& root,
                const StoreInfo& store) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = fs;
            return detail::TLocalStorageBackend::Open(root, store, options);
        };
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, factory, Vfs_.Name());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        BaselineStart_ = Faults_->Events().size();
        First_ = Engine_->AppendDurable(FirstRequest());
        ASSERT_EQ(First_.disposition, CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(std::chrono::milliseconds(1000)));
            Engine_.reset();
        }
    }

    void Close()
    {
        ASSERT_TRUE(Engine_->Shutdown(std::chrono::milliseconds(1000)));
        Engine_.reset();
    }

    void ExpectRealFilesSince(std::size_t start)
    {
        const auto events = Faults_->Events();
        for (const auto kind : {NGlamDB::NIO::EFileKind::Data,
                 NGlamDB::NIO::EFileKind::Manifest,
                 NGlamDB::NIO::EFileKind::ManifestList,
                 NGlamDB::NIO::EFileKind::Metadata})
        {
            const auto found = std::find_if(events.begin() + static_cast<std::ptrdiff_t>(start),
                events.end(),
                [kind](const auto& event) {
                    return event.Op == EIOOp::SyncFile && event.Tag.Kind == kind;
                });
            ASSERT_NE(found, events.end()) << static_cast<int>(kind);
            const auto bytes = ReadBytes(found->Path);
            if (kind == NGlamDB::NIO::EFileKind::Data) {
                EXPECT_TRUE(bytes.starts_with("PAR1"));
                EXPECT_TRUE(bytes.ends_with("PAR1"));
            } else if (kind == NGlamDB::NIO::EFileKind::Metadata) {
                EXPECT_NE(bytes.find("\"format-version\""), std::string::npos);
            } else {
                EXPECT_TRUE(bytes.starts_with("Obj\x01"));
            }
        }
    }
};

} // namespace glamdb::vertical
