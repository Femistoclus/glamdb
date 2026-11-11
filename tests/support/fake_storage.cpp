#include "tests/support/fake_storage.h"

#include "core/api/binary.h"
#include "core/api/codec.h"
#include "core/engine/store_lease.h"

#include <fstream>
#include <iterator>
#include <set>

namespace glamdb::test {
namespace {

using namespace detail;

struct Metadata
{
    BaseIdentity base;
    std::string schema_hash;
    std::vector<AppendOutcome> tags;
};

const AppendResult& TagReceipt(const AppendOutcome& tag)
{
    if (!tag.result.has_value()) {
        throw Failure({"CORRUPTION", "test dependency tag has no receipt", RetryHint::OperatorAction});
    }
    return *tag.result;
}

void Write(const std::filesystem::path& path, const Metadata& metadata)
{
    std::filesystem::create_directories(path.parent_path());
    Encoder out;
    out.Text(EncodeBase(metadata.base));
    out.Text(metadata.schema_hash);
    out.Number(static_cast<uint32_t>(metadata.tags.size()));
    for (const auto& tag : metadata.tags) {
        out.Text(*EncodeOutcome(tag));
    }
    std::ofstream stream(path, std::ios::binary);
    stream.write(out.bytes.data(), static_cast<std::streamsize>(out.bytes.size()));
    stream.close();
    Require(!stream.fail(), "IO_ERROR", "test producer write failed");
}

Metadata Read(const std::filesystem::path& path)
{
    std::ifstream stream(path, std::ios::binary);
    Require(stream.is_open(), "CORRUPTION", "test dependency root is missing");
    const std::string bytes((std::istreambuf_iterator<char>(stream)), {});
    Decoder in{bytes};
    const auto base = DecodeBase(in.Text());
    Require(base.has_value(), "CORRUPTION", "test dependency root identity is invalid");
    Metadata metadata{*base, in.Text(), {}};
    const auto count = in.Number<uint32_t>();
    Require(count <= 4096, "CORRUPTION", "test dependency history is too large");
    for (uint32_t i = 0; i < count; ++i) {
        const auto outcome = DecodeOutcome(in.Text());
        Require(outcome && outcome->result.has_value(), "CORRUPTION", "test dependency tag is invalid");
        metadata.tags.push_back(*outcome);
    }
    in.End();
    return metadata;
}

class Create final : public StagedCreate
{
public:
    Create(FakeStorage& owner, CollectionInfo info, Metadata metadata)
        : owner_(owner)
        , info_(std::move(info))
        , metadata_(std::move(metadata))
    { }
    CollectionInfo Info() const override
    {
        return info_;
    }
    iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) override
    {
        return catalog.InsertTable("columnar", info_.name, metadata_.base.metadata_location);
    }
    Status VerifyClosure(std::string_view location) override
    {
        if (owner_.on_guard) {
            owner_.on_guard();
        }
        try {
            Require(!owner_.fail_guard && location == metadata_.base.metadata_location &&
                    Read(owner_.Physical(location)).base == metadata_.base,
                "IO_ERROR",
                "test dependency closure rejected");
            return {};
        } catch (const Failure& failure) {
            return std::unexpected(failure.detail);
        }
    }

private:
    FakeStorage& owner_;
    CollectionInfo info_;
    Metadata metadata_;
};

class Append final : public StagedAppend
{
public:
    Append(FakeStorage& owner, std::string name, BaseIdentity base, Metadata metadata)
        : owner_(owner)
        , name_(std::move(name))
        , base_(std::move(base))
        , metadata_(std::move(metadata))
    { }
    AppendResult Receipt() const override
    {
        return TagReceipt(metadata_.tags.back());
    }
    iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) override
    {
        const auto result = catalog.UpdateTableMetadataLocation("columnar",
            name_,
            metadata_.base.metadata_location,
            base_.metadata_location,
            base_.metadata_location);
        if (!result) {
            return std::unexpected(result.error());
        }
        if (*result == 1 && owner_.on_pointer) {
            owner_.on_pointer();
        }
        if (*result != 1 || owner_.fail_after_pointer) {
            return iceberg::IOError("test dependency failed after tentative pointer");
        }
        return {};
    }
    Status VerifyClosure(std::string_view location) override
    {
        if (owner_.on_guard) {
            owner_.on_guard();
        }
        try {
            Require(!owner_.fail_guard && location == metadata_.base.metadata_location &&
                    Read(owner_.Physical(location)).base == metadata_.base,
                "IO_ERROR",
                "test dependency closure rejected");
            return {};
        } catch (const Failure& failure) {
            return std::unexpected(failure.detail);
        }
    }

private:
    FakeStorage& owner_;
    std::string name_;
    BaseIdentity base_;
    Metadata metadata_;
};

} // namespace

std::filesystem::path FakeStorage::Physical(std::string_view uri) const
{
    const auto warehouse = uri.find("/warehouse/");
    detail::Require(warehouse != std::string_view::npos, "CORRUPTION", "invalid test URI");
    return root / std::string(uri.substr(warehouse + 1));
}

Result<std::unique_ptr<detail::StagedCreate>>
FakeStorage::PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt)
{
    CollectionInfo info{request.collection_id, request.exact_name, detail::NewUuid(), request.schema, 0, std::nullopt};
    const auto location =
        "store://" + store.store_id + "/warehouse/" + request.collection_id + "/" + std::string(attempt) + ".meta";
    Metadata metadata{{store.store_id, request.collection_id, info.table_uuid, location, std::nullopt, 0, 0},
        *detail::SchemaFingerprint(request.schema),
        {}};
    last_root = Physical(location);
    Write(last_root, metadata);
    return std::make_unique<Create>(*this, std::move(info), std::move(metadata));
}

Result<std::unique_ptr<detail::StagedAppend>> FakeStorage::PrepareAppend(const StoreInfo& store,
    const detail::BoundOperation& binding,
    const CollectionInfo& collection,
    const detail::NormalizedInput& input,
    std::chrono::steady_clock::time_point)
{
    ++prepares;
    if (on_prepare) {
        on_prepare();
    }
    if (fail_prepare) {
        return std::unexpected(ErrorDetail{"IO_ERROR", "first producer error", RetryHint::OperatorAction});
    }
    auto metadata = Read(Physical(binding.base.metadata_location));
    const auto snapshot = int64_t{100} - static_cast<int64_t>(metadata.tags.size()) * 11;
    AppendResult receipt{binding.collection_id,
        binding.operation_id,
        binding.payload_hash,
        1,
        snapshot,
        binding.base.current_snapshot_id,
        static_cast<int64_t>(input.RowCount()),
        1,
        123,
        1700000000000000LL + static_cast<int64_t>(metadata.tags.size()) * 1000,
        store.profile_id,
        1};
    metadata.tags.push_back({binding.operation_id,
        CommitDisposition::Committed,
        CommitPhase::Complete,
        receipt,
        std::nullopt,
        std::nullopt});
    metadata.base.metadata_location =
        "store://" + store.store_id + "/warehouse/" + binding.collection_id + "/" + binding.attempt_id + ".meta";
    metadata.base.current_snapshot_id = snapshot;
    last_root = Physical(metadata.base.metadata_location);
    Write(last_root, metadata);
    return std::make_unique<Append>(*this, collection.name, binding.base, std::move(metadata));
}

Status FakeStorage::VerifyRetained(const StoreInfo&,
    std::span<const detail::CollectionView> collections,
    std::span<const detail::OperationRecord> operations)
{
    if (on_verify) {
        on_verify();
    }
    if (throw_verifier) {
        throw std::runtime_error("verifier raised an exception");
    }
    try {
        for (const auto& collection : collections) {
            const auto metadata = Read(Physical(collection.base.metadata_location));
            detail::Require(metadata.base == collection.base &&
                    metadata.schema_hash == *detail::SchemaFingerprint(collection.info.schema),
                "CORRUPTION",
                "test dependency identity mismatch");
            std::set<std::string> seen;
            std::set<int64_t> snapshots;
            std::optional<int64_t> parent;
            for (const auto& tag : metadata.tags) {
                const auto& receipt = TagReceipt(tag);
                detail::Require(seen.insert(tag.operation_id).second && snapshots.insert(receipt.snapshot_id).second &&
                        receipt.parent_snapshot_id == parent,
                    "CORRUPTION",
                    "test dependency duplicated tag or broken ancestry");
                bool found = false;
                for (const auto& operation : operations) {
                    if (operation.binding.collection_id == collection.info.collection_id &&
                        operation.binding.operation_id == tag.operation_id)
                    {
                        detail::Require(operation.state == detail::OperationState::Committed &&
                                operation.outcome == tag,
                            "CORRUPTION",
                            "test dependency tag/index mismatch");
                        found = true;
                    }
                }
                detail::Require(found, "CORRUPTION", "test dependency tag has no outcome");
                parent = receipt.snapshot_id;
            }
            detail::Require(parent == collection.base.current_snapshot_id,
                "CORRUPTION",
                "test dependency head mismatch");
            for (const auto& operation : operations) {
                if (operation.binding.collection_id == collection.info.collection_id) {
                    detail::Require(seen.contains(operation.binding.operation_id) ==
                            (operation.state == detail::OperationState::Committed),
                        "CORRUPTION",
                        "test dependency outcome has no tag");
                }
            }
        }
        return {};
    } catch (const detail::Failure& failure) {
        return std::unexpected(failure.detail);
    }
}

} // namespace glamdb::test
