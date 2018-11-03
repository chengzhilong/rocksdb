//
// Created by 张艺文 on 2018/11/2.
//

#include <monitoring/thread_status_util.h>
#include "fixed_range_based_flush_job.h"
#include "nvm_cache_options.h"
#include "fixed_range_chunk_based_nvm_write_cache.h"
#include "nvm_write_cache.h"
#include "db/column_family.h"
#include "util/log_buffer.h"

namespace rocksdb{
    FixedRangeBasedFlushJob::FixedRangeBasedFlushJob(const std::string &dbname, rocksdb::ColumnFamilyData *cfd,
                                                     rocksdb::InstrumentedMutex *db_mutex,
                                                     std::atomic<bool> *shutting_down,
                                                     LogBuffer* log_buffer,
                                                     rocksdb::NVMCacheOptions *nvm_cache_options)
                                                     :  dbname_(dbname),
                                                        cfd_(cfd),
                                                        db_mutex_(db_mutex),
                                                        shutting_down_(shutting_down),
                                                        log_buffer_(log_buffer),
                                                        nvm_cache_options_(nvm_cache_options),
                                                        nvm_write_cache_(dynamic_cast<FixedRangeChunkBasedNVMWriteCache*>(nvm_cache_options_->nvm_write_cache_)),
                                                        cache_stat_(nvm_write_cache_->stats()),
                                                        range_list_(cache_stat_->range_list_)
                                                        {

    }


    FixedRangeBasedFlushJob::~FixedRangeBasedFlushJob() {

    }

    void FixedRangeBasedFlushJob::Prepare() {
        db_mutex_->AssertHeld();

        // pick memtables from immutable memtable list
        cfd_->imm()->PickMemtablesToFlush(nullptr, &mems_);
        if(mems_.empty()) return;

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

        if(!s.ok()){
            cfd_->imm()->RollbackMemtableFlush(mems_, 0);
        } else{
            // record this flush to manifest or not?
        }

        return s;

    }

    Status FixedRangeBasedFlushJob::InsertToNVMCache() {

    }


    void FixedRangeBasedFlushJob::Cancle() {

    }

    void FixedRangeBasedFlushJob::ReportFlushInputSize(const rocksdb::autovector<rocksdb::MemTable *> &mems) {
        uint64_t input_size = 0;
        for (auto* mem : mems) {
            input_size += mem->ApproximateMemoryUsage();
        }
        ThreadStatusUtil::IncreaseThreadOperationProperty(
                ThreadStatus::FLUSH_BYTES_MEMTABLES,
                input_size);
    }

}//end rocksdb