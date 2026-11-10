#include "core/io/io_attempt.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <limits>
#include <stdexcept>
#include <thread>

namespace NGlamDB::NIO {
namespace {

constexpr std::string_view KStore = "01234567-89ab-cdef-0123-456789abcdef";
constexpr std::string_view KCollection = "89abcdef-0123-4567-89ab-cdef01234567";
constexpr std::string_view KAttempt = "01234567-89ab-7def-8123-456789abcdef";
constexpr std::string_view KOperation = "fedcba98-7654-3210-fedc-ba9876543210";

TAttemptSpec Spec(EAttemptKind kind = EAttemptKind::Append)
{
    return {std::string(KStore),
        std::string(KCollection),
        std::string(KAttempt),
        kind,
        kind == EAttemptKind::Append ? std::optional(std::string(KOperation)) : std::nullopt};
}

std::string Location(std::string_view suffix)
{
    return "store://" + std::string(KStore) + "/warehouse/" + std::string(KCollection) + "/" + std::string(suffix);
}

std::string Data(std::string_view sequence = "00000")
{
    return Location(
        "data/" + std::string(KOperation) + "-" + std::string(KAttempt) + "-" + std::string(sequence) + ".parquet");
}

std::string Metadata(std::string_view version = "00000")
{
    return Location("metadata/" + std::string(version) + "-" + std::string(KOperation) + ".metadata.json");
}

std::string Manifest(unsigned index)
{
    return Location("metadata/" + std::string(KOperation) + "-m" + std::to_string(index) + ".avro");
}

std::string ManifestList(unsigned index)
{
    return Location("metadata/snap-1-" + std::to_string(index) + "-" + std::string(KOperation) + ".avro");
}

std::shared_ptr<TIOAttempt> Attempt(TAttemptLimits limits = {})
{
    return TIOAttempt::Make(Spec(), std::make_shared<TWriteLatch>(), limits).Value();
}

TSha256Digest Digest(std::string_view value)
{
    TSha256 hash;
    EXPECT_FALSE(hash.Update(std::as_bytes(std::span(value))));
    return hash.Finalize();
}

void RegisterData(const std::shared_ptr<TIOAttempt>& attempt)
{
    ASSERT_FALSE(attempt->DeclareDataFile(Data()));
    const auto result = attempt->RegisterFile(Data());
    ASSERT_TRUE(result);
    EXPECT_EQ(result.Value(), EFileKind::Data);
}

void ReachDirSynced(const std::shared_ptr<TIOAttempt>& attempt, const std::string& path)
{
    ASSERT_FALSE(attempt->Advance(path, EFileState::Synced));
    ASSERT_FALSE(attempt->Advance(path, EFileState::Closed));
    ASSERT_FALSE(attempt->Advance(path, EFileState::DirSynced));
}

TEST(IoAttempt, FactoryValidatesIdentityAndAttemptKind)
{
    auto latch = std::make_shared<TWriteLatch>();
    ASSERT_TRUE(TIOAttempt::Make(Spec(), latch));
    ASSERT_TRUE(TIOAttempt::Make(Spec(EAttemptKind::CreateCollection), latch));
    EXPECT_EQ(TIOAttempt::Make(Spec(), nullptr).Error(), std::errc::invalid_argument);
    for (int field = 0; field < 7; ++field) {
        auto spec = Spec();
        switch (field) {
            case 0:
                spec.StoreId = "invalid";
                break;
            case 1:
                spec.CollectionId.clear();
                break;
            case 2:
                spec.AttemptId[14] = '4';
                break;
            case 3:
                spec.AttemptId[19] = '7';
                break;
            case 4:
                spec.OperationId.reset();
                break;
            case 5:
                spec.OperationId = "not-a-uuid";
                break;
            case 6:
                spec.Kind = static_cast<EAttemptKind>(255);
                break;
            default:
                FAIL() << field;
        }
        EXPECT_EQ(TIOAttempt::Make(spec, latch).Error(), std::errc::invalid_argument) << field;
    }
    auto create = Spec();
    create.Kind = EAttemptKind::CreateCollection;
    EXPECT_EQ(TIOAttempt::Make(create, latch).Error(), std::errc::invalid_argument);
}

TEST(IoAttempt, RequiresExactDeclaredDataPath)
{
    EXPECT_EQ(Attempt()->RegisterFile(Data()).Error(), std::errc::invalid_argument);
    for (const auto& path : {Data("0000"),
             Data("000000"),
             Data("00a00"),
             Location("data/file.parquet"),
             Metadata(),
             "store://" + std::string(KOperation) + Data().substr(44),
             "store://" + std::string(KStore) + "/warehouse/" + std::string(KOperation) + Data().substr(91)})
    {
        auto attempt = Attempt();
        EXPECT_TRUE(attempt->DeclareDataFile(path)) << path;
        EXPECT_TRUE(attempt->FirstError());
    }
    auto otherAttemptPath = Data();
    otherAttemptPath[otherAttemptPath.find(std::string(KAttempt), 91)] = '1';
    EXPECT_EQ(Attempt()->DeclareDataFile(otherAttemptPath), std::errc::invalid_argument);
    auto attempt = Attempt();
    RegisterData(attempt);
    EXPECT_EQ(attempt->Id(), KAttempt);
    EXPECT_EQ(attempt->DeclareDataFile(Data("00001")), std::errc::file_exists);
}

TEST(IoAttempt, RestrictsCollectionAndFileInventory)
{
    auto duplicate = Attempt();
    RegisterData(duplicate);
    EXPECT_EQ(duplicate->RegisterFile(Data()).Error(), std::errc::file_exists);
    EXPECT_EQ(duplicate->Record(Data()).Value().State, EFileState::Failed);

    auto roots = Attempt();
    ASSERT_TRUE(roots->RegisterFile(Metadata()));
    EXPECT_EQ(roots->RegisterFile(Metadata("00001")).Error(), std::errc::no_buffer_space);
    auto manifests = Attempt();
    for (unsigned i = 0; i < 8; ++i) {
        ASSERT_TRUE(manifests->RegisterFile(Manifest(i)));
    }
    EXPECT_EQ(manifests->RegisterFile(Manifest(8)).Error(), std::errc::no_buffer_space);
    auto lists = Attempt();
    ASSERT_TRUE(lists->RegisterFile(ManifestList(0)));
    EXPECT_EQ(lists->RegisterFile(ManifestList(1)).Error(), std::errc::no_buffer_space);

    const auto foreign =
        "store://" + std::string(KStore) + "/warehouse/" + std::string(KOperation) + Metadata().substr(91);
    EXPECT_EQ(Attempt()->RegisterFile(foreign).Error(), std::errc::invalid_argument);
    EXPECT_EQ(Attempt()->RegisterFile(Location("metadata/unknown.json")).Error(), std::errc::invalid_argument);
}

TEST(IoAttempt, CreateCollectionOnlyRegistersMetadata)
{
    auto create = TIOAttempt::Make(Spec(EAttemptKind::CreateCollection), std::make_shared<TWriteLatch>()).Value();
    EXPECT_EQ(create->RegisterFile(Metadata()).Value(), EFileKind::Metadata);
    EXPECT_EQ(create->DeclareDataFile(Data()), std::errc::invalid_argument);
    for (const auto& path : {Data(), Manifest(0), ManifestList(0)}) {
        auto fresh = TIOAttempt::Make(Spec(EAttemptKind::CreateCollection), std::make_shared<TWriteLatch>()).Value();
        EXPECT_EQ(fresh->RegisterFile(path).Error(), std::errc::invalid_argument);
    }
}

TEST(IoAttempt, ReceiptRequiresFullDurableStateAndIndependentFormatVerification)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    auto permit = attempt->ReserveWrite(Data(), 5);
    ASSERT_TRUE(permit);
    ASSERT_FALSE(permit.Value().Finish(3));
    EXPECT_EQ(attempt->CanRead(Data()), std::errc::operation_in_progress);
    for (auto state : {EFileState::Synced, EFileState::Closed, EFileState::DirSynced}) {
        ASSERT_FALSE(attempt->Advance(Data(), state));
        EXPECT_EQ(attempt->Record(Data()).Value().State, state);
        EXPECT_TRUE(attempt->Receipts().empty());
        EXPECT_EQ(attempt->CanRead(Data()), std::errc::operation_in_progress);
    }
    TSha256Digest digest{};
    digest[0] = std::byte{0x42};
    const auto receipt = attempt->CompleteFile(Data(), 3, digest);
    ASSERT_TRUE(receipt);
    EXPECT_EQ(receipt.Value().Path, Data());
    EXPECT_EQ(receipt.Value().Kind, EFileKind::Data);
    EXPECT_EQ(receipt.Value().Length, 3);
    EXPECT_EQ(receipt.Value().Sha256, digest);
    EXPECT_EQ(attempt->Record(Data()).Value().Sha256, digest);
    EXPECT_EQ(receipt.Value().AttemptId, KAttempt);
    EXPECT_FALSE(attempt->CanRead(Data()));
    EXPECT_FALSE(attempt->CheckWrite(Data()));
    EXPECT_FALSE(attempt->Record(Data()).Value().FormatVerified);
    ASSERT_FALSE(attempt->Seal());
    EXPECT_FALSE(attempt->Seal());
    EXPECT_FALSE(attempt->ChargeRead(3));
    ASSERT_FALSE(attempt->MarkFormatVerified(Data()));
    EXPECT_FALSE(attempt->MarkFormatVerified(Data()));
    auto records = attempt->Records();
    ASSERT_EQ(records.size(), 1);
    EXPECT_TRUE(records.front().FormatVerified);
    records.front().FormatVerified = false;
    EXPECT_TRUE(attempt->Record(Data()).Value().FormatVerified);
    EXPECT_EQ(attempt->Receipts().size(), 1);
    EXPECT_FALSE(attempt->FirstError());
}

TEST(IoAttempt, RejectsSkippedStatesAndPrematureReceipts)
{
    for (auto state : {EFileState::Open, EFileState::Closed, EFileState::DirSynced, EFileState::Failed}) {
        auto attempt = Attempt();
        RegisterData(attempt);
        EXPECT_EQ(attempt->Advance(Data(), state), std::errc::operation_not_permitted);
        EXPECT_EQ(attempt->Record(Data()).Value().State, EFileState::Failed);
    }
    auto premature = Attempt();
    RegisterData(premature);
    EXPECT_EQ(premature->CompleteFile(Data(), 0, {}).Error(), std::errc::invalid_argument);
    auto format = Attempt();
    RegisterData(format);
    EXPECT_EQ(format->MarkFormatVerified(Data()), std::errc::operation_not_permitted);
    auto mismatch = Attempt();
    RegisterData(mismatch);
    ReachDirSynced(mismatch, Data());
    EXPECT_EQ(mismatch->CompleteFile(Data(), 1, {}).Error(), std::errc::invalid_argument);
    auto repeated = Attempt();
    RegisterData(repeated);
    ReachDirSynced(repeated, Data());
    ASSERT_TRUE(repeated->CompleteFile(Data(), 0, {}));
    EXPECT_EQ(repeated->CompleteFile(Data(), 0, {}).Error(), std::errc::invalid_argument);
    EXPECT_TRUE(repeated->Receipts().empty());
}

TEST(IoAttempt, FirstErrorsAreStickyPerAttemptAndFile)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    ASSERT_TRUE(attempt->RegisterFile(Manifest(0)));
    const auto original = std::make_error_code(std::errc::no_space_on_device);
    attempt->FailFile(Data(), original);
    attempt->FailFile(Data(), std::make_error_code(std::errc::io_error));
    attempt->FailFile(Manifest(0), std::make_error_code(std::errc::invalid_argument));
    attempt->FailFile(Data(), {});
    EXPECT_EQ(attempt->FirstError(), original);
    EXPECT_EQ(attempt->Record(Data()).Value().FirstError, original);
    EXPECT_EQ(attempt->Record(Manifest(0)).Value().FirstError, std::errc::invalid_argument);
    EXPECT_EQ(attempt->CheckWrite(Data()), original);
    EXPECT_EQ(attempt->RegisterFile(Metadata()).Error(), original);
    EXPECT_EQ(attempt->Advance(Data(), EFileState::Synced), original);
    EXPECT_EQ(attempt->CompleteFile(Data(), 0, {}).Error(), original);
    EXPECT_EQ(attempt->MarkFormatVerified(Data()), original);
    EXPECT_EQ(attempt->ChargeRead(1), original);
    EXPECT_EQ(attempt->Seal(), original);
    EXPECT_TRUE(attempt->Receipts().empty());
}

TEST(IoAttempt, LatchBlocksAdmissionButLogicalFailureDoesNotSetIt)
{
    auto latch = std::make_shared<TWriteLatch>();
    auto attempt = TIOAttempt::Make(Spec(), latch).Value();
    EXPECT_EQ(attempt->Latch(), latch);
    RegisterData(attempt);
    attempt->FailFile(Data(), std::make_error_code(std::errc::io_error));
    EXPECT_FALSE(latch->IsSet());
    auto next = TIOAttempt::Make(Spec(), latch).Value();
    RegisterData(next);
    latch->Observe(std::make_error_code(std::errc::read_only_file_system));
    EXPECT_EQ(next->CheckWrite(Data()), std::errc::io_error);
    EXPECT_EQ(TIOAttempt::Make(Spec(), latch).Error(), std::errc::io_error);
    EXPECT_EQ(next->Seal(), std::errc::io_error);
}

TEST(IoAttempt, ReservationsRefundUnusedNewBytesAndChargeRequestedIo)
{
    auto attempt = Attempt({4, 100});
    RegisterData(attempt);
    auto first = attempt->ReserveWrite(Data(), 4);
    ASSERT_TRUE(first);
    EXPECT_EQ(attempt->Budget().ReservedNewFileBytes, 4);
    EXPECT_EQ(attempt->Budget().NewFileBytes, 0);
    EXPECT_EQ(attempt->Budget().IoBytes, 4);
    ASSERT_FALSE(first.Value().Finish(2));
    auto second = attempt->ReserveWrite(Data(), 2);
    ASSERT_TRUE(second);
    ASSERT_FALSE(second.Value().Finish(2));
    const auto budget = attempt->Budget();
    EXPECT_EQ(budget.NewFileBytes, 4);
    EXPECT_EQ(budget.ReservedNewFileBytes, 0);
    EXPECT_EQ(budget.IoBytes, 6);
    EXPECT_EQ(attempt->Record(Data()).Value().Length, 4);
    EXPECT_EQ(attempt->ReserveWrite(Data(), 1).Error(), std::errc::no_buffer_space);
    EXPECT_EQ(attempt->Budget().IoBytes, 6);
}

TEST(IoAttempt, ReadAndWriteRequestsShareIoBudgetIncludingRetries)
{
    auto attempt = Attempt({100, 5});
    RegisterData(attempt);
    ASSERT_FALSE(attempt->ChargeRead(2));
    auto retry = attempt->ReserveWrite(Data(), 3);
    ASSERT_TRUE(retry);
    ASSERT_FALSE(retry.Value().Finish(0));
    EXPECT_EQ(attempt->Budget().NewFileBytes, 0);
    EXPECT_EQ(attempt->Budget().IoBytes, 5);
    EXPECT_EQ(attempt->ReserveWrite(Data(), 1).Error(), std::errc::no_buffer_space);
    auto reads = Attempt({0, 5});
    ASSERT_FALSE(reads->ChargeRead(5));
    EXPECT_EQ(reads->ChargeRead(1), std::errc::no_buffer_space);
}

TEST(IoAttempt, BudgetBoundariesDoNotOverflow)
{
    auto zero = Attempt({0, 0});
    RegisterData(zero);
    auto empty = zero->ReserveWrite(Data(), 0);
    ASSERT_TRUE(empty);
    ASSERT_FALSE(empty.Value().Finish(0));
    EXPECT_FALSE(zero->ChargeRead(0));
    EXPECT_EQ(zero->ReserveWrite(Data(), 1).Error(), std::errc::no_buffer_space);

    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    auto large = Attempt({maximum, maximum});
    RegisterData(large);
    auto permit = large->ReserveWrite(Data(), maximum);
    ASSERT_TRUE(permit);
    EXPECT_EQ(large->Budget().ReservedNewFileBytes, maximum);
    ASSERT_FALSE(permit.Value().Finish(maximum));
    EXPECT_EQ(large->Budget().NewFileBytes, maximum);
    EXPECT_EQ(large->Budget().IoBytes, maximum);
    EXPECT_EQ(large->Record(Data()).Value().Length, maximum);
    EXPECT_EQ(large->ReserveWrite(Data(), 1).Error(), std::errc::no_buffer_space);
    auto reads = Attempt({0, maximum});
    ASSERT_FALSE(reads->ChargeRead(maximum));
    EXPECT_FALSE(reads->ChargeRead(0));
    EXPECT_EQ(reads->ChargeRead(1), std::errc::no_buffer_space);
}

TEST(IoAttempt, PendingReservationsCannotBeClosedOrSealed)
{
    for (bool seal : {false, true}) {
        auto attempt = Attempt();
        RegisterData(attempt);
        auto permit = attempt->ReserveWrite(Data(), 4);
        ASSERT_TRUE(permit);
        EXPECT_TRUE(seal ? attempt->Seal() : attempt->Advance(Data(), EFileState::Synced));
        const auto firstError = attempt->FirstError();
        EXPECT_EQ(permit.Value().Finish(3), firstError);
        EXPECT_EQ(attempt->Record(Data()).Value().Length, 3);
        EXPECT_EQ(attempt->Budget().NewFileBytes, 3);
        EXPECT_EQ(attempt->Budget().ReservedNewFileBytes, 0);
        EXPECT_EQ(attempt->Record(Data()).Value().State, EFileState::Failed);
        EXPECT_TRUE(attempt->Receipts().empty());
    }
}

TEST(IoAttempt, ConcurrentReservationsNeverOversubscribeCapacity)
{
    auto attempt = Attempt({10, 100});
    RegisterData(attempt);
    std::barrier gate(2);
    std::atomic<unsigned> accepted = 0;
    std::array<std::thread, 2> workers;
    for (auto& worker : workers) {
        worker = std::thread([&] {
            auto reservation = attempt->ReserveWrite(Data(), 6);
            if (reservation) {
                ++accepted;
            }
            gate.arrive_and_wait();
            if (reservation) {
                EXPECT_EQ(reservation.Value().Finish(6), std::errc::no_buffer_space);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_EQ(accepted, 1);
    EXPECT_EQ(attempt->FirstError(), std::errc::no_buffer_space);
    EXPECT_EQ(attempt->Budget().NewFileBytes, 6);
    EXPECT_EQ(attempt->Budget().ReservedNewFileBytes, 0);
    EXPECT_EQ(attempt->Budget().IoBytes, 6);
    EXPECT_EQ(attempt->Record(Data()).Value().Length, 6);
}

TEST(IoAttempt, ConcurrentWritesAccountEveryAcceptedByte)
{
    auto attempt = Attempt({100, 100});
    RegisterData(attempt);
    std::array<std::thread, 4> workers;
    for (auto& worker : workers) {
        worker = std::thread([&] {
            for (unsigned i = 0; i < 25; ++i) {
                auto permit = attempt->ReserveWrite(Data(), 1);
                ASSERT_TRUE(permit);
                EXPECT_FALSE(permit.Value().Finish(1));
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_FALSE(attempt->FirstError());
    EXPECT_EQ(attempt->Budget().NewFileBytes, 100);
    EXPECT_EQ(attempt->Budget().ReservedNewFileBytes, 0);
    EXPECT_EQ(attempt->Record(Data()).Value().Length, 100);
}

TEST(IoAttempt, PreservesAcceptedPrefixDigestAfterFileFailure)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    EXPECT_EQ(attempt->Record(Data()).Value().Sha256, Digest(""));
    auto prefix = attempt->ReserveWrite(Data(), 8);
    ASSERT_TRUE(prefix);
    ASSERT_FALSE(prefix.Value().Finish(3, Digest("abc")));
    auto retry = attempt->ReserveWrite(Data(), 5);
    ASSERT_TRUE(retry);
    ASSERT_FALSE(retry.Value().Finish(0));
    attempt->FailFile(Data(), std::make_error_code(std::errc::no_space_on_device));
    const auto record = attempt->Record(Data()).Value();
    EXPECT_EQ(record.State, EFileState::Failed);
    EXPECT_EQ(record.Length, 3);
    EXPECT_EQ(record.Sha256, Digest("abc"));
    EXPECT_FALSE(record.Receipt);
}

TEST(IoAttempt, MissingPrefixDigestBecomesKnownAtCompletion)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    auto prefix = attempt->ReserveWrite(Data(), 1);
    ASSERT_TRUE(prefix);
    ASSERT_FALSE(prefix.Value().Finish(1));
    EXPECT_FALSE(attempt->Record(Data()).Value().Sha256);
    auto retry = attempt->ReserveWrite(Data(), 1);
    ASSERT_TRUE(retry);
    ASSERT_FALSE(retry.Value().Finish(0));
    EXPECT_FALSE(attempt->Record(Data()).Value().Sha256);
    ReachDirSynced(attempt, Data());
    ASSERT_TRUE(attempt->CompleteFile(Data(), 1, Digest("a")));
    EXPECT_EQ(attempt->Record(Data()).Value().Sha256, Digest("a"));
    attempt->FailFile(Data(), std::make_error_code(std::errc::io_error));
    EXPECT_EQ(attempt->Record(Data()).Value().Sha256, Digest("a"));
}

TEST(IoAttempt, ConcurrentFailurePreservesAcceptedLengthAndDigest)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    ASSERT_TRUE(attempt->RegisterFile(Metadata()));
    auto permit = attempt->ReserveWrite(Data(), 8);
    ASSERT_TRUE(permit);
    std::thread failure([&] {
        attempt->FailFile(Metadata(), std::make_error_code(std::errc::io_error));
    });
    failure.join();
    EXPECT_EQ(permit.Value().Finish(3, Digest("abc")), std::errc::io_error);
    const auto record = attempt->Record(Data()).Value();
    EXPECT_EQ(record.Length, 3);
    EXPECT_EQ(record.Sha256, Digest("abc"));
    EXPECT_EQ(attempt->Budget().NewFileBytes, 3);
    EXPECT_EQ(attempt->Budget().ReservedNewFileBytes, 0);
}

TEST(IoAttempt, AbandonedPermitsReleaseCapacityAndFailAttempt)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    {
        auto permit = attempt->ReserveWrite(Data(), 8);
        ASSERT_TRUE(permit);
    }
    EXPECT_EQ(attempt->FirstError(), std::errc::operation_canceled);
    EXPECT_EQ(attempt->Budget().ReservedNewFileBytes, 0);
    EXPECT_EQ(attempt->Budget().NewFileBytes, 0);
    EXPECT_EQ(attempt->Budget().IoBytes, 8);
    EXPECT_TRUE(attempt->Receipts().empty());
}

TEST(IoAttempt, ExceptionUnwindingReleasesTheReservation)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    EXPECT_THROW(
        {
            auto permit = attempt->ReserveWrite(Data(), 8).Value();
            throw std::runtime_error("writer failure");
        },
        std::runtime_error);
    EXPECT_EQ(attempt->FirstError(), std::errc::operation_canceled);
    EXPECT_EQ(attempt->Budget().ReservedNewFileBytes, 0);
    EXPECT_EQ(attempt->Budget().NewFileBytes, 0);
}

TEST(IoAttempt, PermitsRetainTheirAttemptAndTransferOwnershipOnce)
{
    std::optional<TWriteReservation> permit;
    std::weak_ptr<TIOAttempt> weak;
    {
        auto attempt = Attempt();
        weak = attempt;
        RegisterData(attempt);
        auto result = attempt->ReserveWrite(Data(), 4);
        ASSERT_TRUE(result);
        permit.emplace(std::move(result).Value());
    }
    EXPECT_FALSE(weak.expired());
    EXPECT_FALSE(permit->Finish(2));
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(permit->Finish(0), std::errc::operation_not_permitted);
}

TEST(IoAttempt, MoveAssignmentAbandonsThePreviousReservation)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    auto first = attempt->ReserveWrite(Data(), 2);
    auto second = attempt->ReserveWrite(Data(), 2);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    first.Value() = std::move(second).Value();
    EXPECT_EQ(attempt->FirstError(), std::errc::operation_canceled);
    EXPECT_EQ(attempt->Budget().ReservedNewFileBytes, 2);
    EXPECT_EQ(first.Value().Finish(1), std::errc::operation_canceled);
    EXPECT_EQ(attempt->Budget().ReservedNewFileBytes, 0);
    EXPECT_EQ(attempt->Budget().NewFileBytes, 1);
}

TEST(IoAttempt, InvalidAcceptedCountFailsWithoutOverflowOrLeakingReservation)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    auto permit = attempt->ReserveWrite(Data(), 2);
    ASSERT_TRUE(permit);
    EXPECT_EQ(permit.Value().Finish(3), std::errc::invalid_argument);
    EXPECT_EQ(attempt->Budget().ReservedNewFileBytes, 0);
    EXPECT_EQ(attempt->Budget().NewFileBytes, 0);
    EXPECT_EQ(attempt->Record(Data()).Value().State, EFileState::Failed);
}

TEST(IoAttempt, SealingRejectsFurtherWriteEventsButAllowsVerification)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    ReachDirSynced(attempt, Data());
    ASSERT_TRUE(attempt->CompleteFile(Data(), 0, {}));
    ASSERT_FALSE(attempt->Seal());
    EXPECT_FALSE(attempt->MarkFormatVerified(Data()));
    EXPECT_FALSE(attempt->CanRead(Data()));
    EXPECT_EQ(attempt->CheckWrite(Data()), std::errc::operation_not_permitted);
    EXPECT_EQ(attempt->Advance(Data(), EFileState::Synced), std::errc::operation_not_permitted);
    EXPECT_EQ(attempt->CompleteFile(Data(), 0, {}).Error(), std::errc::operation_not_permitted);
    EXPECT_TRUE(attempt->Receipts().empty());

    auto empty = Attempt();
    ASSERT_FALSE(empty->Seal());
    EXPECT_EQ(empty->RegisterFile(Metadata()).Error(), std::errc::operation_not_permitted);
}

TEST(IoAttempt, DeactivationRejectsLateCompletionAndPreservesAcceptedAccounting)
{
    auto old = Attempt();
    RegisterData(old);
    auto permit = old->ReserveWrite(Data(), 4);
    ASSERT_TRUE(permit);
    old->Deactivate();
    EXPECT_EQ(permit.Value().Finish(3, Digest("abc")), std::errc::operation_canceled);
    EXPECT_EQ(old->Record(Data()).Value().Length, 3);
    EXPECT_EQ(old->Record(Data()).Value().Sha256, Digest("abc"));
    EXPECT_EQ(old->Budget().ReservedNewFileBytes, 0);
    EXPECT_EQ(old->CompleteFile(Data(), 3, {}).Error(), std::errc::operation_canceled);
    auto spec = Spec();
    spec.AttemptId.back() = '0';
    auto next = TIOAttempt::Make(spec, std::make_shared<TWriteLatch>()).Value();
    EXPECT_NE(next->Id(), old->Id());
    EXPECT_TRUE(next->Receipts().empty());
    EXPECT_FALSE(next->FirstError());
    EXPECT_EQ(old->CheckWrite(Data()), std::errc::operation_canceled);
    EXPECT_FALSE(next->FirstError());
}

TEST(IoAttempt, CompletedDeactivationKeepsReceiptUntilAnInvalidWriteEvent)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    ReachDirSynced(attempt, Data());
    ASSERT_TRUE(attempt->CompleteFile(Data(), 0, {}));
    attempt->Deactivate();
    EXPECT_FALSE(attempt->FirstError());
    EXPECT_EQ(attempt->Receipts().size(), 1);
    EXPECT_FALSE(attempt->CanRead(Data()));
    EXPECT_EQ(attempt->ChargeRead(1), std::errc::operation_canceled);
    EXPECT_EQ(attempt->MarkFormatVerified(Data()), std::errc::operation_canceled);
    EXPECT_EQ(attempt->CheckWrite(Data()), std::errc::operation_canceled);
}

TEST(IoAttempt, DeactivationBeforeReceiptIsStillIncomplete)
{
    auto attempt = Attempt();
    RegisterData(attempt);
    ReachDirSynced(attempt, Data());
    attempt->Deactivate();
    EXPECT_EQ(attempt->FirstError(), std::errc::operation_canceled);
    EXPECT_EQ(attempt->Record(Data()).Value().State, EFileState::Failed);
    EXPECT_TRUE(attempt->Receipts().empty());
}

TEST(IoAttempt, UnknownReadsDoNotMutateAttemptAndUnknownWritesFail)
{
    auto attempt = Attempt();
    EXPECT_FALSE(attempt->CanRead(Metadata()));
    EXPECT_EQ(attempt->Record(Metadata()).Error(), std::errc::no_such_file_or_directory);
    EXPECT_FALSE(attempt->FirstError());
    EXPECT_EQ(attempt->CheckWrite(Metadata()), std::errc::no_such_file_or_directory);
    EXPECT_EQ(attempt->FirstError(), std::errc::no_such_file_or_directory);
    EXPECT_TRUE(attempt->Records().empty());
}

}
}
