//
// Created by 张艺文 on 2018/11/2.
//

#include <inttypes.h>

#include <monitoring/thread_status_util.h>
#include <table/merging_iterator.h>
#include <db/snapshot_checker.h>
#include <db/compaction_iterator.h>
#include <db/event_helpers.h>
#include "fixed_range_based_flush_job.h"
#include "nvm_cache_options.h"
#include "fixed_range_chunk_based_nvm_write_cache.h"
#include "nvm_write_cache.h"
#include "db/column_family.h"
#include "util/log_buffer.h"
#include "util/event_logger.h"
#include "db/job_context.h"
#include "db/memtable.h"

namespace rocksdb {

    const char *GetFlushReasonString(FlushReason flush_reason) {
        switch (flush_reason) {
            case FlushReason::kOthers:
                return "Other Reasons";
            case FlushReason::kGetLiveFiles:
                return "Get Live Files";
            case FlushReason::kShutDown:
                return "Shut down";
            case FlushReason::kExternalFileIngestion:
                return "External File Ingestion";
            case FlushReason::kManualCompaction:
                return "Manual Compaction";
            case FlushReason::kWriteBufferManager:
                return "Write Buffer Manager";
            case FlushReason::kWriteBufferFull:
                return "Write Buffer Full";
            case FlushReason::kTest:
                return "Test";
            case FlushReason::kDeleteFiles:
                return "Delete Files";
            case FlushReason::kAutoCompaction:
                return "Auto Compaction";
            case FlushReason::kManualFlush:
                return "Manual Flush";
            case FlushReason::kErrorRecovery:
                return "Error Recovery";
            default:
                return "Invalid";
        }
    }

    FixedRangeBasedFlushJob::FixedRangeBasedFlushJob(const std::string &dbname,
                                                     const ImmutableDBOptions &db_options,
                                                     JobContext *job_context,
                                                     EventLogger *event_logger,
                                                     rocksdb::ColumnFamilyData *cfd,
                                                     std::vector<SequenceNumber> existing_snapshots,
                                                     SequenceNumber earliest_write_conflict_snapshot,
                                                     SnapshotChecker *snapshot_checker,
                                                     rocksdb::InstrumentedMutex *db_mutex,
                                                     std::atomic<bool> *shutting_down,
                                                     LogBuffer *log_buffer,
                                                     rocksdb::NVMCacheOptions *nvm_cache_options)
            : dbname_(dbname),
              db_options_(db_options),
              job_context_(job_context),
              event_logger_(event_logger),
              cfd_(cfd),
              existing_snapshots_(std::move(existing_snapshots)),
              earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
              snapshot_checker_(snapshot_checker),
              db_mutex_(db_mutex),
              shutting_down_(shutting_down),
              log_buffer_(log_buffer),
              nvm_cache_options_(nvm_cache_options),
              nvm_write_cache_(dynamic_cast<FixedRangeChunkBasedNVMWriteCache *>(nvm_cache_options_->nvm_write_cache_)),
              cache_stat_(nvm_write_cache_->stats()),
              range_list_(cache_stat_->range_list_) {

    }


    FixedRangeBasedFlushJob::~FixedRangeBasedFlushJob() {

    }

    void FixedRangeBasedFlushJob::Prepare() {
        db_mutex_->AssertHeld();

        // pick memtables from immutable memtable list
        cfd_->imm()->PickMemtablesToFlush(nullptr, &mems_);
        if (mems_.empty()) return;

        //Report flush inpyt size
        ReportFlushInputSize(mems_);
    }


    Status FixedRangeBasedFlushJob::Run() {
        db_mutex_->AssertHeld();
        AutoThreadOperationStageUpdater stage_run(
                ThreadStatus::STAGE_FLUSH_RUN);
        if (mems_.empty()) {
            ROCKS_LOG_BUFFER(log_buffer_, "[%s] Nothing in memtable to flush",
                             cfd_->GetName().c_str());
            return Status::OK();
        }

        Status s = InsertToNVMCache();

        if (s.ok() &&
            (shutting_down_->load(std::memory_order_acquire) || cfd_->IsDropped())) {
            s = Status::ShutdownInProgress(
                    "Database shutdown or Column family drop during flush");
        }

        if (!s.ok()) {
            cfd_->imm()->RollbackMemtableFlush(mems_, 0);
        } else {
            // record this flush to manifest or not?
        }

        return s;

    }

    Status FixedRangeBasedFlushJob::InsertToNVMCache() {
        AutoThreadOperationStageUpdater stage_updater(
                ThreadStatus::STAGE_FLUSH_WRITE_L0);
        db_mutex_->AssertHeld();
        const uint64_t start_micros = db_options_.env->NowMicros();
        Status s;
        {
            db_mutex_->Unlock();
            if (log_buffer_) {
                log_buffer_->FlushBufferToLog();
            }
            std::vector<InternalIterator *> memtables;
            std::vector<InternalIterator *> range_del_iters;
            ReadOptions ro;
            ro.total_order_seek = true;
            Arena arena;
            uint64_t total_num_entries = 0, total_num_deletes = 0;
            size_t total_memory_usage = 0;
            for (MemTable *m : mems_) {
                ROCKS_LOG_INFO(
                        db_options_.info_log,
                        "[%s] [JOB %d] Flushing memtable with next log file: %"
                                PRIu64
                                "\n",
                        cfd_->GetName().c_str(), job_context_->job_id, m->GetNextLogNumber());
                memtables.push_back(m->NewIterator(ro, &arena));
                auto *range_del_iter = m->NewRangeTombstoneIterator(ro);
                if (range_del_iter != nullptr) {
                    range_del_iters.push_back(range_del_iter);
                }
                total_num_entries += m->num_entries();
                total_num_deletes += m->num_deletes();
                total_memory_usage += m->ApproximateMemoryUsage();
            }
            event_logger_->Log()
                    << "job" << job_context_->job_id << "event"
                    << "flush_started"
                    << "num_memtables" << mems_.size() << "num_entries" << total_num_entries
                    << "num_deletes" << total_num_deletes << "memory_usage"
                    << total_memory_usage << "flush_reason"
                    << GetFlushReasonString(cfd_->GetFlushReason());

            {
                ScopedArenaIterator iter(
                        NewMergingIterator(&cfd_->internal_comparator(), &memtables[0],
                                           static_cast<int>(memtables.size()), &arena));
                std::unique_ptr<InternalIterator> range_del_iter(NewMergingIterator(
                        &cfd_->internal_comparator(),
                        range_del_iters.empty() ? nullptr : &range_del_iters[0],
                        static_cast<int>(range_del_iters.size())));
                ROCKS_LOG_INFO(db_options_.info_log,
                               "[%s] [JOB %d] NVM cache flush: started",
                               cfd_->GetName().c_str(), job_context_->job_id);

                int64_t _current_time = 0;
                auto status = db_options_.env->GetCurrentTime(&_current_time);
                // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
                if (!status.ok()) {
                    ROCKS_LOG_WARN(
                            db_options_.info_log,
                            "Failed to get current time to populate creation_time property. "
                            "Status: %s",
                            status.ToString().c_str());
                }
                const uint64_t current_time = static_cast<uint64_t>(_current_time);

                uint64_t oldest_key_time =
                        mems_.front()->ApproximateOldestKeyTime();

                s = BuildChunkAndInsert(iter.get(),
                                        std::move(range_del_iter),
                                        cfd_->internal_comparator(),
                                        std::move(existing_snapshots_),
                                        earliest_write_conflict_snapshot_,
                                        snapshot_checker_,
                                        event_logger_, job_context_->job_id);

                LogFlush(db_options_.info_log);
            }
        }
    }

    Status FixedRangeBasedFlushJob::BuildChunkAndInsert(InternalIterator *iter,
                                                        std::unique_ptr<InternalIterator> range_del_iter,
                                                        const InternalKeyComparator &internal_comparator,
                                                        std::vector<SequenceNumber> snapshots,
                                                        SequenceNumber earliest_write_conflict_snapshot,
                                                        SnapshotChecker *snapshot_checker,
                                                        EventLogger *event_logger, int job_id) {
        Status s;
        // Internal Iterator
        iter->SeekToFirst();
        std::unique_ptr<RangeDelAggregator> range_del_agg(
                new RangeDelAggregator(internal_comparator, snapshots));
        s = range_del_agg->AddTombstones(std::move(range_del_iter));
        if (!s.ok()) {
            // may be non-ok if a range tombstone key is unparsable
            return s;
        }


        if (iter->Valid() || !range_del_agg->IsEmpty()) {
            /*TableBuilder* builder;
            unique_ptr<WritableFileWriter> file_writer;
            {
                unique_ptr<WritableFile> file;
#ifndef NDEBUG
                bool use_direct_writes = env_options.use_direct_writes;
                TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
                s = NewWritableFile(env, fname, &file, env_options);
                if (!s.ok()) {
                    EventHelpers::LogAndNotifyTableFileCreationFinished(
                            event_logger, ioptions.listeners, dbname, column_family_name, fname,
                            job_id, meta->fd, tp, reason, s);
                    return s;
                }
                file->SetIOPriority(io_priority);
                file->SetWriteLifeTimeHint(write_hint);

                file_writer.reset(new WritableFileWriter(std::move(file), fname,
                                                         env_options, ioptions.statistics,
                                                         ioptions.listeners));
                builder = NewTableBuilder(
                        ioptions, mutable_cf_options, internal_comparator,
                        int_tbl_prop_collector_factories, column_family_id,
                        column_family_name, file_writer.get(), compression, compression_opts,
                        level, nullptr *//* compression_dict *//*, false *//* skip_filters *//*,
                        creation_time, oldest_key_time);
            }*/ //no need


            MergeHelper merge(db_options_.env, internal_comparator.user_comparator(),
                              cfd_->ioptions()->merge_operator, nullptr, cfd_->ioptions()->info_log,
                              true /* internal key corruption is not ok */,
                              snapshots.empty() ? 0 : snapshots.back(),
                              snapshot_checker);

            CompactionIterator c_iter(
                    iter, internal_comparator.user_comparator(), &merge, kMaxSequenceNumber,
                    &snapshots, earliest_write_conflict_snapshot, snapshot_checker, db_options_.env,
                    ShouldReportDetailedTime(db_options_.env, cfd_->ioptions()->statistics),
                    true /* internal key corruption is not ok */, range_del_agg.get());
            c_iter.SeekToFirst();
            for (; c_iter.Valid(); c_iter.Next()) {
                const Slice &key = c_iter.key();
                const Slice &value = c_iter.value();
                builder->Add(key, value);
                //meta->UpdateBoundaries(key, c_iter.ikey().sequence);

                // TODO(noetzli): Update stats after flush, too.
                /*if (io_priority == Env::IO_HIGH &&
                    IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
                    ThreadStatusUtil::SetThreadOperationProperty(
                            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
                }*/
            }

            // not supprt range del currently
            /*for (auto it = range_del_agg->NewIterator(); it->Valid(); it->Next()) {
                auto tombstone = it->Tombstone();
                auto kv = tombstone.Serialize();
                builder->Add(kv.first.Encode(), kv.second);
                *//*meta->UpdateBoundariesForRange(kv.first, tombstone.SerializeEndKey(),
                                               tombstone.seq_, internal_comparator);*//*
            }*/

            // Finish and check for builder errors
            //tp = builder->GetTableProperties();
            bool empty = builder->NumEntries() == 0 && tp.num_range_deletions == 0;
            s = c_iter.status();
            if (!s.ok() || empty) {
                builder->Abandon();
            } else {
                s = builder->Finish();
            }

            if (s.ok() && !empty) {
                uint64_t file_size = builder->FileSize();
                meta->fd.file_size = file_size;
                meta->marked_for_compaction = builder->NeedCompact();
                assert(meta->fd.GetFileSize() > 0);
                tp = builder->GetTableProperties(); // refresh now that builder is finished
                if (table_properties) {
                    *table_properties = tp;
                }
            }
            delete builder;

            // Finish and check for file errors
            if (s.ok() && !empty) {
                StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
                s = file_writer->Sync(ioptions.use_fsync);
            }
            if (s.ok() && !empty) {
                s = file_writer->Close();
            }

            /*if (s.ok() && !empty) {
                // Verify that the table is usable
                // We set for_compaction to false and don't OptimizeForCompactionTableRead
                // here because this is a special case after we finish the table building
                // No matter whether use_direct_io_for_flush_and_compaction is true,
                // we will regrad this verification as user reads since the goal is
                // to cache it here for further user reads
                std::unique_ptr<InternalIterator> it(table_cache->NewIterator(
                        ReadOptions(), env_options, internal_comparator, *meta,
                        nullptr *//* range_del_agg *//*,
                        mutable_cf_options.prefix_extractor.get(), nullptr,
                        (internal_stats == nullptr) ? nullptr
                                                    : internal_stats->GetFileReadHist(0),
                        false *//* for_compaction *//*, nullptr *//* arena *//*,
                        false *//* skip_filter *//*, level));
                s = it->status();
                if (s.ok() && paranoid_file_checks) {
                    for (it->SeekToFirst(); it->Valid(); it->Next()) {
                    }
                    s = it->status();
                }
            }*/
        }

        // Check for input iterator errors
        if (!iter->status().ok()) {
            s = iter->status();
        }

        if (!s.ok() || meta->fd.GetFileSize() == 0) {
            env->DeleteFile(fname);
        }

        // Output to event logger and fire events.
        EventHelpers::LogAndNotifyTableFileCreationFinished(
                event_logger, ioptions.listeners, dbname, column_family_name, fname,
                job_id, meta->fd, tp, reason, s);

        return s;
    }


    void FixedRangeBasedFlushJob::Cancle() {

    }

    void FixedRangeBasedFlushJob::ReportFlushInputSize(const rocksdb::autovector<rocksdb::MemTable *> &mems) {
        uint64_t input_size = 0;
        for (auto *mem : mems) {
            input_size += mem->ApproximateMemoryUsage();
        }
        ThreadStatusUtil::IncreaseThreadOperationProperty(
                ThreadStatus::FLUSH_BYTES_MEMTABLES,
                input_size);
    }

}//end rocksdb