//
// Created by 张艺文 on 2018/11/2.
//

#pragma once


#include <string>
#include <util/autovector.h>
#include <atomic>
#include <monitoring/instrumented_mutex.h>

#include "utilities/nvm_write_cache/nvm_write_cache.h"

namespace rocksdb{

    class ColumnFamilyData;
    class MemTable;
    struct NVMCacheOptions;
    struct FixedRangeChunkBasedCacheStats;
    class FixedRangeChunkBasedNVMWriteCache;
    class LogBuffer;

    class FixedRangeBasedFlushJob{
    public:

        explicit FixedRangeBasedFlushJob(
                const std::string& dbname,
                ColumnFamilyData* cfd,
                InstrumentedMutex* db_mutex,
                std::atomic<bool>* shutting_down,
                LogBuffer* log_buffer,
                NVMCacheOptions* nvm_cache_options);

        ~FixedRangeBasedFlushJob();

        void Prepare();

        Status Run();

        void Cancle();

    private:

        void ReportFlushInputSize(const autovector<MemTable*>& mems);

        Status InsertToNVMCache();

        const std::string& dbname_;
        ColumnFamilyData* cfd_;
        InstrumentedMutex* db_mutex_;
        std::atomic<bool>* shutting_down_;
        LogBuffer* log_buffer_;

        const NVMCacheOptions* nvm_cache_options_;
        FixedRangeChunkBasedNVMWriteCache* nvm_write_cache_;
        FixedRangeChunkBasedCacheStats* cache_stat_;
        std::unordered_map<KeyRange, uint64_t >* range_list_;

        autovector<MemTable*> mems_;

    };

}//end rocksdb
