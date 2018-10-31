//
// Created by 张艺文 on 2018/10/31.
//

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <limits>
#include <set>
#include <utility>
#include <vector>
#include <string>

#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/logs_with_prep_tracker.h"
#include "db/memtable_list.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/transaction_log.h"
#include "table/scoped_arena_iterator.h"
#include "util/autovector.h"
#include "util/event_logger.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"
#include "utilities/nvm_write_cache/range_memtable_set.h"

namespace rocksdb {

        class DBImpl;
        class MemTable;
        class SnapshotChecker;
        class TableCache;
        class Version;
        class VersionEdit;
        class VersionSet;
        class Arena;

        class NVMFlushJob {
            public:
            // TODO(icanadi) make effort to reduce number of parameters here
            // IMPORTANT: mutable_cf_options needs to be alive while FlushJob is alive
            /*NVMFlushJob(const std::string& dbname, ColumnFamilyData* cfd,
            const ImmutableDBOptions& db_options,
            const MutableCFOptions& mutable_cf_options,
            const uint64_t* max_memtable_id, const EnvOptions& env_options,
                    VersionSet* versions, InstrumentedMutex* db_mutex,
                    std::atomic<bool>* shutting_down,
                    std::vector<SequenceNumber> existing_snapshots,
                    SequenceNumber earliest_write_conflict_snapshot,
                    SnapshotChecker* snapshot_checker, JobContext* job_context,
                    LogBuffer* log_buffer, Directory* db_directory,
                    Directory* output_file_directory, CompressionType output_compression,
                    Statistics* stats, EventLogger* event_logger, bool measure_io_stats,
            const bool sync_output_directory, const bool write_manifest);*/

            NVMFlushJob(const std::string& dbname, ColumnFamilyData* cfd,
                    const ImmutableDBOptions& db_options,
                    const MutableCFOptions& mutableCFOptions
                    );

            ~NVMFlushJob();

            // Require db_mutex held.
            // Once PickMemTable() is called, either Run() or Cancel() has to be called.
            // 获取imm list，并根据imm list与RangeList的交集获取RangeMem的集合以及对应的GlobalBloomFilter
            void Prepare();

            // 通过子线程执行各个range的flush操作
            Status Run();

            // 取消本次flush
            void Cancel();
            TableProperties GetTableProperties() const { return table_properties_; }
            const autovector<MemTable*>& GetMemTables() const { return mems_; }

            private:
            //void ReportStartedFlush();
            //void ReportFlushInputSize(const autovector<MemTable*>& mems);
            //void RecordFlushIOStats();
            //Status WriteLevel0Table();

            const std::string& dbname_;
            ColumnFamilyData* cfd_;
            const ImmutableDBOptions& db_options_;
            const MutableCFOptions& mutable_cf_options_;

            //const uint64_t* max_memtable_id_;
            //const EnvOptions env_options_;
            VersionSet* versions_;
            InstrumentedMutex* db_mutex_;
            std::atomic<bool>* shutting_down_;
            std::vector<SequenceNumber> existing_snapshots_;
            SequenceNumber earliest_write_conflict_snapshot_;
            SnapshotChecker* snapshot_checker_;
            JobContext* job_context_;
            LogBuffer* log_buffer_;
            //Directory* db_directory_;
            //Directory* output_file_directory_;
            CompressionType output_compression_;
            Statistics* stats_;
            EventLogger* event_logger_;
            TableProperties table_properties_;
            bool measure_io_stats_;

            //const bool write_manifest_;

            // Variables below are set by PickMemTable():
            //FileMetaData meta_;
            autovector<MemTable*> mems_;
            VersionEdit* edit_;
            Version* base_;
            bool pick_memtable_called;

            PersistentRangeMemSet* range_mem_set_;
            std::vector<PersistentRangeMem*> range_mems_;

            typedef std::pair<Slice, Slice> KeyRange;
            std::unordered_map<KeyRange, uint64_t> *range_list_;

            std::vector<char*> global_bloom_filters_;

        };

}  // namespace rocksdb

