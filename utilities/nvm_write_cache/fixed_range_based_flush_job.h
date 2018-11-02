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
    class NVMCacheOptions;
    class FixedRangeChunkBasedCacheStat;
    class FixedRangeChunkBasedNVMWriteCache;

    class FixedRangeBasedFlushJob{
    public:

        explicit FixedRangeBasedFlushJob(
                const std::string& dbname,
                ColumnFamilyData* cfd,
                NVMCacheOptions* nvm_cache_options,
                InstrumentedMutex* db_mutex,
                std::atomic<bool>* shutting_down);

        ~FixedRangeBasedFlushJob();

        void Prepare();

        void Run();

        void Cancle();

    private:

        const std::string& dbname_;
        ColumnFamilyData* cfd_;
        InstrumentedMutex* db_mutex_;
        std::atomic<bool>* shutting_down_;

        const NVMCacheOptions* nvm_cache_options_;
        FixedRangeChunkBasedNVMWriteCache* nvm_write_cache_;
        FixedRangeChunkBasedCacheStat* cache_stat_;
        std::unordered_map<KeyRange, uint64_t >* range_list_;

        autovector<MemTable*> mems_;

    };

}//end rocksdb
