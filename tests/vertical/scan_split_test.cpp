#include "tests/vertical/vertical_support.h"

#include "core/engine/storage_mapping.h"
#include "storage/resources.h"

#include <atomic>
#include <iostream>

#include <format>
#include <future>
#include <thread>

namespace glamdb {
namespace {
using namespace vertical;
using namespace std::chrono_literals;

class ScanSplit : public ::testing::Test
{
protected:
    test::TempDir Temporary;
    std::filesystem::path Root = Temporary.path / "store";
    std::shared_ptr<Engine> Store;

    void SetUp() override
    {
        auto created = CreateStore({test::store_id, Root.string()});
        ASSERT_TRUE(created) << created.error().message;
        Store = *created;
        ASSERT_EQ(Store->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }

    AppendResult Append(std::string operation, const std::vector<TInputRow>& rows)
    {
        const auto result = Store->AppendDurable({test::collection_id, std::move(operation), {}, {Batch(rows)}});
        EXPECT_EQ(result.disposition, CommitDisposition::Committed);
        return result.result.value_or(AppendResult{});
    }

    std::shared_ptr<TReadView> View(SnapshotSelector selector = {})
    {
        auto view = Store->AcquireReadView({{{test::collection_id, selector}}, "alice"});
        EXPECT_TRUE(view) << (view ? "" : view.error().message);
        return view ? *view : nullptr;
    }

    std::vector<std::shared_ptr<const TScanSplit>> Splits(const TScanSplitPlan& plan)
    {
        std::vector<std::shared_ptr<const TScanSplit>> splits;
        for (;;) {
            auto page = Store->NextScanSplits(plan, "alice");
            EXPECT_TRUE(page) << (page ? "" : page.error().message);
            if (!page) {
                break;
            }
            splits.insert(splits.end(), page->Splits.begin(), page->Splits.end());
            if (page->Done) {
                break;
            }
        }
        return splits;
    }

    static std::vector<std::string> Rows(Result<std::shared_ptr<ScanCursor>> scan)
    {
        EXPECT_TRUE(scan) << (scan ? "" : scan.error().message);
        std::vector<std::string> rows;
        if (!scan) {
            return rows;
        }
        for (;;) {
            auto next = (*scan)->Next();
            EXPECT_TRUE(next) << (next ? "" : next.error().message);
            if (!next || !*next) {
                break;
            }
            auto part = Render(**next);
            rows.insert(rows.end(), part.begin(), part.end());
        }
        return Sorted(std::move(rows));
    }
};

// Literal typed output oracle: duplicate multiplicity, nulls, timestamp and bool.
const std::vector<std::string> ExpectedRows = {
    "duplicate|1|0.500000|true|ts:10",
    "duplicate|1|0.500000|true|ts:10",
    "duplicate|2|-1.500000|false|ts:20",
    "other|null|null|null|null",
    "tail|9|2.000000|false|ts:99",
    "ключ|3|2.500000|true|ts:30",
};

TEST_F(ScanSplit, ConcurrentPagedWorkersHaveExactLiteralUnionAndRepeatableSplits)
{
    Append(FirstOperation, FirstRows);
    Append(SecondOperation, SecondRows);
    Append(ThirdOperation, {{"tail", 9, 2.0, false, 99}});
    auto view = View();
    ASSERT_NE(view, nullptr);
    auto plan = Store->PlanScan(view, 0, {}, "alice", {.SplitsPerPage = 1, .TargetBytes = 1});
    ASSERT_TRUE(plan) << plan.error().message;
    EXPECT_TRUE((*plan)->Info().output_schema->Equals(*ArrowSchema()));
    EXPECT_EQ(Rows(Store->ScanReadView(*view, 0, {}, "alice")), ExpectedRows);
    auto splits = Splits(**plan);
    ASSERT_EQ(splits.size(), 3U);
    for (const auto& split : splits) {
        EXPECT_EQ(split->FileCount(), 1U);
        EXPECT_GT(split->EstimatedBytes(), 1U);
    }
    std::vector<std::future<std::vector<std::string>>> workers;
    for (const auto& split : splits) {
        workers.push_back(std::async(std::launch::async, [&, split] {
            return Rows(Store->ReadSplit(*split, "alice"));
        }));
    }
    std::vector<std::string> all;
    for (auto& worker : workers) {
        const auto rows = worker.get();
        all.insert(all.end(), rows.begin(), rows.end());
    }
    EXPECT_EQ(Sorted(all), ExpectedRows);
    view.reset(); // Both the plan and independent descriptors must retain the pin.
    plan->reset();
    Append("00000000-0000-4000-8000-0000000000a4", {{"new", 4, 4.0, true, 40}});
    all.clear();
    for (const auto& split : splits) {
        const auto rows = Rows(Store->ReadSplit(*split, "alice"));
        all.insert(all.end(), rows.begin(), rows.end());
    }
    EXPECT_EQ(Sorted(all), ExpectedRows);
}

TEST_F(ScanSplit, ConcurrentZeroColumnWorkersPreserveLiteralCardinalityAcrossUnevenFiles)
{
    Append(FirstOperation, FirstRows);
    Append(SecondOperation, SecondRows);
    Append(ThirdOperation, {{"tail", 9, 2.0, false, 99}});
    auto view = View();
    ASSERT_NE(view, nullptr);
    const TReadViewScanRequest request{.Projection = std::vector<std::string>{}, .BatchRows = 1};
    auto plan = Store->PlanScan(view, 0, request, "alice", {.SplitsPerPage = 1, .TargetBytes = 1});
    ASSERT_TRUE(plan) << plan.error().message;
    EXPECT_EQ((*plan)->Info().output_schema->num_fields(), 0);
    const auto splits = Splits(**plan);
    ASSERT_EQ(splits.size(), 3U);
    const auto countRows = [](Result<std::shared_ptr<ScanCursor>> scan) {
        EXPECT_TRUE(scan) << (scan ? "" : scan.error().message);
        int64_t rows = 0;
        if (!scan) {
            return rows;
        }
        EXPECT_EQ((*scan)->Info().output_schema->num_fields(), 0);
        for (;;) {
            const auto next = (*scan)->Next();
            EXPECT_TRUE(next) << (next ? "" : next.error().message);
            if (!next || !*next) {
                break;
            }
            EXPECT_EQ((*next)->num_columns(), 0);
            rows += (*next)->num_rows();
        }
        return rows;
    };
    EXPECT_EQ(countRows(Store->ScanReadView(*view, 0, request, "alice")), 6);
    std::vector<std::future<int64_t>> workers;
    for (const auto& split : splits) {
        workers.push_back(std::async(std::launch::async, [&, split] {
            return countRows(Store->ReadSplit(*split, "alice"));
        }));
    }
    int64_t total = 0;
    std::vector<int64_t> counts;
    for (auto& worker : workers) {
        const auto count = worker.get();
        total += count;
        counts.push_back(count);
    }
    EXPECT_EQ(total, 6);
    std::ranges::sort(counts);
    EXPECT_EQ(counts, (std::vector<int64_t>{1, 2, 3}));
}

TEST_F(ScanSplit, HistoricalInitialPrunedAndZeroColumnCardinality)
{
    auto emptyView = View();
    const auto first = Append(FirstOperation, FirstRows);
    Append(SecondOperation, SecondRows);
    for (const auto& view : {emptyView, View(SnapshotSelector::InitialEmpty())}) {
        auto plan = Store->PlanScan(view, 0, {}, "alice");
        ASSERT_TRUE(plan);
        EXPECT_TRUE((*plan)->Info().output_schema->Equals(*ArrowSchema()));
        EXPECT_TRUE(Splits(**plan).empty());
    }
    auto view = View(SnapshotSelector::Id(first.snapshot_id));
    auto plan = Store->PlanScan(view, 0, {.Projection = std::vector<std::string>{}}, "alice");
    ASSERT_TRUE(plan);
    const auto splits = Splits(**plan);
    ASSERT_EQ(splits.size(), 1U);
    EXPECT_EQ(Rows(Store->ReadSplit(*splits[0], "alice")), (std::vector<std::string>{"", "", ""}));
    auto filtered =
        Store->PlanScan(view, 0, {.Filter = Predicate::Compare("value", CompareOp::Gt, int64_t{999})}, "alice");
    ASSERT_TRUE(filtered);
    for (const auto& split : Splits(**filtered)) {
        EXPECT_TRUE(Rows(Store->ReadSplit(*split, "alice")).empty());
    }
    EXPECT_FALSE(Store->PlanScan(view, 0, {.Limit = 1}, "alice"));
    EXPECT_FALSE(Store->PlanScan(view, 0, {.Limit = 0}, "alice"));
    EXPECT_FALSE(Store->PlanScan(view, 1, {}, "alice"));
    EXPECT_FALSE(Store->PlanScan(view, 0, {}, "bob"));
    EXPECT_FALSE(Store->PlanScan(view, 0, {}, "alice", {.SplitsPerPage = 0}));
    EXPECT_FALSE(Store->PlanScan(view, 0, {}, "alice", {.TargetBytes = 0}));
}

TEST_F(ScanSplit, PartialPageReservationFailureDoesNotSkipFiles)
{
    Append(FirstOperation, FirstRows);
    Append(SecondOperation, SecondRows);
    auto view = View();
    auto plan = Store->PlanScan(view, 0, {}, "alice", {.SplitsPerPage = 2, .TargetBytes = 1});
    ASSERT_TRUE(plan);
    const auto resources = Store->GetResourceOwner();
    const auto baseline = resources->GetUsage().memory_bytes;
    const auto control = static_cast<size_t>(ResourceClass::Control);
    auto pressure = resources->Reserve(ResourceClass::Control,
        resources->GetLimits().memory_bytes[control] - baseline[control] - 4096);
    ASSERT_TRUE(pressure);
    const auto full = resources->GetUsage().memory_bytes;
    auto refused = Store->NextScanSplits(**plan, "alice");
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(resources->GetUsage().memory_bytes, full);
    pressure->reset();
    const auto splits = Splits(**plan);
    ASSERT_EQ(splits.size(), 2U);
    EXPECT_EQ(Rows(Store->ReadSplit(*splits[0], "alice")),
        (std::vector<std::string>{"duplicate|1|0.500000|true|ts:10",
            "duplicate|1|0.500000|true|ts:10",
            "other|null|null|null|null"}));
}

TEST_F(ScanSplit, TerminalViewsAndCancellationStopAdmissionsButNotOpenedCursors)
{
    Append(FirstOperation, FirstRows);
    for (int mode = 0; mode < 4; ++mode) {
        auto view = View();
        auto plan = Store->PlanScan(view, 0, {}, "alice");
        ASSERT_TRUE(plan);
        const auto splits = Splits(**plan);
        ASSERT_EQ(splits.size(), 1U);
        auto cursor = Store->ReadSplit(*splits[0], "alice");
        ASSERT_TRUE(cursor);
        EXPECT_FALSE(Store->ReadSplit(*splits[0], "bob"));
        if (mode == 0) {
            ASSERT_TRUE(Store->ReleaseReadView(*view, "alice"));
        } else if (mode == 1) {
            ASSERT_TRUE(Store->CancelReadView(*view, "alice"));
        } else if (mode == 2) {
            ASSERT_TRUE(Store->RenewReadView(*view, "alice", 1ms));
            std::this_thread::sleep_for(5ms);
        } else {
            ASSERT_TRUE(Store->CancelScanPlan(**plan, "alice"));
        }
        EXPECT_FALSE(Store->NextScanSplits(**plan, "alice"));
        EXPECT_FALSE(Store->ReadSplit(*splits[0], "alice"));
        EXPECT_EQ(Rows(cursor).size(), 3U);
    }
}

TEST_F(ScanSplit, WrongEngineRestartAndRetainedBuffersHaveIndependentLifetime)
{
    Append(FirstOperation, FirstRows);
    auto view = View();
    auto plan = Store->PlanScan(view, 0, {}, "alice");
    ASSERT_TRUE(plan);
    const auto splits = Splits(**plan);
    ASSERT_EQ(splits.size(), 1U);
    auto other = CreateStore({"00000000-0000-4000-8000-000000000099", (Temporary.path / "other").string()});
    ASSERT_TRUE(other);
    auto wrong = (*other)->ReadSplit(*splits[0], "alice");
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error().code, "READ_VIEW_WRONG_ENGINE");
    auto cursor = Store->ReadSplit(*splits[0], "alice");
    ASSERT_TRUE(cursor);
    auto batch = (*cursor)->Next();
    ASSERT_TRUE(batch);
    ASSERT_NE(*batch, nullptr);
    auto buffer = (*batch)->column(1)->data()->buffers[1];
    ASSERT_TRUE(Store->ReleaseReadView(*view, "alice"));
    ASSERT_TRUE(Store->Shutdown(1s));
    cursor->reset();
    Store.reset();
    EXPECT_EQ(Render(**batch).size(), 3U);
    batch->reset();
    EXPECT_FALSE(OpenExisting({Root.string(), test::store_id}));
    buffer.reset();
    auto reopened = OpenExisting({Root.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    auto stale = (*reopened)->ReadSplit(*splits[0], "alice");
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error().code, "READ_VIEW_RESTARTED");
}

TEST_F(ScanSplit, PlanAndReadErrorsReleaseReservationsAndKeepReplayPossible)
{
    Append(FirstOperation, FirstRows);
    auto view = View();
    const auto resources = Store->GetResourceOwner();
    const auto baseline = resources->GetUsage().memory_bytes;
    EXPECT_FALSE(Store->PlanScan(view, 0, {.Projection = std::vector<std::string>{"missing"}}, "alice"));
    EXPECT_EQ(resources->GetUsage().memory_bytes, baseline);
    auto plan = Store->PlanScan(view, 0, {}, "alice");
    ASSERT_TRUE(plan);
    auto splits = Splits(**plan);
    ASSERT_EQ(splits.size(), 1U);
    const auto metadata = static_cast<size_t>(ResourceClass::Metadata);
    const auto admitted = resources->GetUsage().memory_bytes;
    auto pressure =
        resources->Reserve(ResourceClass::Metadata, resources->GetLimits().memory_bytes[metadata] - admitted[metadata]);
    ASSERT_TRUE(pressure);
    const auto full = resources->GetUsage().memory_bytes;
    auto refused = Store->ReadSplit(*splits[0], "alice");
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(resources->GetUsage().memory_bytes, full);
    pressure->reset();
    auto cursor = Store->ReadSplit(*splits[0], "alice");
    ASSERT_TRUE(cursor);
    (*cursor)->Cancel();
    EXPECT_FALSE((*cursor)->Next());
    cursor->reset();
    // No file was decoded, so metadata-cache residency cannot mask cleanup.
    splits.clear();
    plan->reset();
    EXPECT_EQ(resources->GetUsage().memory_bytes, baseline);
    plan = Store->PlanScan(view, 0, {}, "alice");
    ASSERT_TRUE(plan);
    splits = Splits(**plan);
    EXPECT_EQ(Rows(Store->ReadSplit(*splits[0], "alice")).size(), 3U);
}

TEST_F(ScanSplit, WholeFileSplitReadsAllNativeRowGroups)
{
    const SchemaSpec schema{{{"key", FieldType::String, false}, {"payload", FieldType::String, false}}};
    ASSERT_EQ(Store->CreateCollection({SecondCollection, "wide", schema}).disposition, CommitDisposition::Committed);
    std::vector<Row> input;
    for (int n = 0; n < 10; ++n) {
        input.push_back({"k" + std::to_string(n), std::string(910 * 1024, 'x')});
    }
    const auto batch = detail::MakeRecordBatch(schema, input);
    ASSERT_TRUE(batch);
    ASSERT_EQ(Store->AppendDurable({SecondCollection, FirstOperation, {}, {*batch}}).disposition,
        CommitDisposition::Committed);
    auto files = Store->ListFiles(SecondCollection);
    ASSERT_TRUE(files);
    ASSERT_EQ(files->Files.size(), 1U);
    auto details = Store->GetFileDetails(SecondCollection, files->Files[0].FileID);
    ASSERT_TRUE(details);
    ASSERT_EQ(details->RowGroups.size(), 2U);
    auto view = Store->AcquireReadView({{{SecondCollection}}, "alice"});
    ASSERT_TRUE(view);
    auto plan = Store->PlanScan(*view, 0, {.Projection = std::vector<std::string>{"key"}}, "alice", {.TargetBytes = 1});
    ASSERT_TRUE(plan);
    auto splits = Splits(**plan);
    ASSERT_EQ(splits.size(), 1U);
    EXPECT_EQ(Rows(Store->ReadSplit(*splits[0], "alice")),
        (std::vector<std::string>{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"}));
}

TEST_F(ScanSplit, KVIntervalMembershipSurvivesDeleteCompactionAndUpsert)
{
    const SchemaSpec schema{
        {{"key", FieldType::String, false}, {"value", FieldType::Int64, false}, {"payload", FieldType::String, false}}};
    const auto batch = [&](const std::vector<Row>& rows) {
        return detail::MakeRecordBatch(schema, rows).value();
    };
    const auto key = [](int n) {
        return std::format("key:{:05}", n);
    };
    const auto row = [&](int n) -> Row {
        return {key(n), int64_t{n}, std::string(4096, 'x')};
    };
    CreateCollectionRequest create{SecondCollection, "kv", schema};
    create.mode = CollectionMode::KeyValue;
    create.write_settings.compression = ParquetCompression{ParquetCodec::Uncompressed, std::nullopt};
    create.write_settings.dictionary = DictionaryPolicy::Disabled;
    ASSERT_EQ(Store->CreateCollection(create).disposition, CommitDisposition::Committed);
    DeleteRequest thin{SecondCollection, "00000000-0000-4000-8000-0000000000b4", {}, {}};
    for (int part = 0; part < 4; ++part) {
        std::vector<Row> rows;
        for (int offset = 0; offset < 2048; ++offset) {
            rows.push_back(row(part * 2048 + offset));
            if (offset != 0) {
                thin.keys.push_back(key(part * 2048 + offset));
            }
        }
        ASSERT_EQ(
            Store
                ->UpsertDurable(
                    {SecondCollection, "00000000-0000-4000-8000-0000000000b" + std::to_string(part), {}, {batch(rows)}})
                .disposition,
            CommitDisposition::Committed);
    }
    ASSERT_EQ(Store->DeleteDurable(thin).disposition, CommitDisposition::Committed);
    auto view = Store->AcquireReadView({{{SecondCollection}}, "alice"});
    ASSERT_TRUE(view);
    const TReadViewScanRequest request{.Projection = std::vector<std::string>{"key", "value"}};
    auto plan = Store->PlanScan(*view, 0, request, "alice", {.SplitsPerPage = 1, .TargetBytes = 1});
    ASSERT_TRUE(plan);
    ASSERT_EQ(Store->CompactDurable({SecondCollection, FirstOperation}).disposition, CommitDisposition::Committed);
    auto files = Store->ListFiles(SecondCollection);
    ASSERT_TRUE(files);
    ASSERT_EQ(files->Files.size(), 1U);
    auto changed = row(0);
    changed[1] = int64_t{999};
    ASSERT_EQ(Store->UpsertDurable({SecondCollection, SecondOperation, {}, {batch({changed})}}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Store->DeleteDurable({SecondCollection, ThirdOperation, {}, {key(2048)}}).disposition,
        CommitDisposition::Committed);
    // Page only after replacement publication, then retry each physical split twice.
    auto splits = Splits(**plan);
    ASSERT_EQ(splits.size(), 4U);
    const std::vector<std::string> before{"key:00000|0", "key:02048|2048", "key:04096|4096", "key:06144|6144"};
    EXPECT_EQ(Rows(Store->ScanReadView(**view, 0, request, "alice")), before);
    for (int attempt = 0; attempt < 2; ++attempt) {
        std::vector<std::string> all;
        for (const auto& split : splits) {
            const auto rows = Rows(Store->ReadSplit(*split, "alice"));
            all.insert(all.end(), rows.begin(), rows.end());
        }
        EXPECT_EQ(Sorted(all), before);
    }
    auto latest = Store->AcquireReadView({{{SecondCollection}}, "alice"});
    ASSERT_TRUE(latest);
    auto latestPlan = Store->PlanScan(*latest, 0, request, "alice", {.TargetBytes = 1});
    ASSERT_TRUE(latestPlan);
    std::vector<std::string> current;
    for (const auto& split : Splits(**latestPlan)) {
        const auto rows = Rows(Store->ReadSplit(*split, "alice"));
        current.insert(current.end(), rows.begin(), rows.end());
    }
    EXPECT_EQ(Sorted(current), (std::vector<std::string>{"key:00000|999", "key:04096|4096", "key:06144|6144"}));
}

TEST_F(ScanSplit, TargetGroupingAndDelayedPlanningSurviveSnapshotRetention)
{
    Append(FirstOperation, FirstRows);
    Append(SecondOperation, SecondRows);
    auto view = View();
    auto plan = Store->PlanScan(view, 0, {}, "alice");
    ASSERT_TRUE(plan);
    Append(ThirdOperation, {{"later", 9, 2.0, false, 99}});
    ASSERT_TRUE(Store->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}}));
    auto splits = Splits(**plan);
    ASSERT_EQ(splits.size(), 1U);
    EXPECT_EQ(splits[0]->FileCount(), 2U);
    EXPECT_EQ(Rows(Store->ReadSplit(*splits[0], "alice")),
        (std::vector<std::string>{"duplicate|1|0.500000|true|ts:10",
            "duplicate|1|0.500000|true|ts:10",
            "duplicate|2|-1.500000|false|ts:20",
            "other|null|null|null|null",
            "ключ|3|2.500000|true|ts:30"}));
}

TEST_F(ScanSplit, NativeWorkerMeasurements)
{
    for (int file = 0; file < 4; ++file) {
        std::vector<TInputRow> input;
        for (int row = 0; row < 1000; ++row) {
            input.push_back({"file" + std::to_string(file) + ":" + std::to_string(row), row, 0.5, true, row});
        }
        Append("00000000-0000-4000-8000-0000000000c" + std::to_string(file), input);
    }
    auto view = View();
    for (int workers : {1, 2, 4}) {
        for (int pass = 0; pass < 2; ++pass) {
            const auto started = std::chrono::steady_clock::now();
            auto plan = Store->PlanScan(view, 0, {}, "alice", {.TargetBytes = 1});
            ASSERT_TRUE(plan);
            const auto splits = Splits(**plan);
            ASSERT_EQ(splits.size(), 4U);
            const auto planned = std::chrono::steady_clock::now();
            std::atomic<size_t> next{0};
            struct TMeasurement
            {
                int64_t Rows = 0;
                int64_t Batches = 0;
                uint64_t Bytes = 0;
                double ReadUs = 0;
            };
            std::vector<std::future<TMeasurement>> tasks;
            for (int worker = 0; worker < workers; ++worker) {
                tasks.push_back(std::async(std::launch::async, [&] {
                    TMeasurement measurement;
                    for (size_t index = next.fetch_add(1); index < splits.size(); index = next.fetch_add(1)) {
                        auto cursor = Store->ReadSplit(*splits[index], "alice");
                        EXPECT_TRUE(cursor);
                        if (!cursor) {
                            break;
                        }
                        for (;;) {
                            const auto before = std::chrono::steady_clock::now();
                            auto batch = (*cursor)->Next();
                            EXPECT_TRUE(batch);
                            if (!batch || !*batch) {
                                break;
                            }
                            measurement.ReadUs +=
                                std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - before)
                                    .count();
                            measurement.Rows += (*batch)->num_rows();
                            ++measurement.Batches;
                        }
                        EXPECT_TRUE((*cursor)->Profile().data_bytes_read.value.has_value());
                        measurement.Bytes += (*cursor)->Profile().data_bytes_read.value.value_or(0);
                    }
                    return measurement;
                }));
            }
            TMeasurement total;
            for (auto& task : tasks) {
                const auto part = task.get();
                total.Rows += part.Rows;
                total.Batches += part.Batches;
                total.Bytes += part.Bytes;
                total.ReadUs += part.ReadUs;
            }
            EXPECT_EQ(total.Rows, 4000);
            const auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - planned).count();
            std::cout << "split_bench workers=" << workers << " pass=" << pass
                      << " plan_us=" << std::chrono::duration<double, std::micro>(planned - started).count()
                      << " rows_per_second=" << 4000 / seconds
                      << " successful_next_mean_us=" << total.ReadUs / total.Batches
                      << " positional_bytes=" << total.Bytes << '\n';
        }
    }
}

} // namespace
} // namespace glamdb
