//
// Created by 张艺文 on 2018/11/2.
//

#include "fixed_range_based_flush_job.h"
#include "nvm_cache_options.h"
#include "fixed_range_chunk_based_nvm_write_cache.h"
#include "nvm_write_cache.h"

namespace rocksdb{
    FixedRangeBasedFlushJob::FixedRangeBasedFlushJob(const std::string &dbname, rocksdb::ColumnFamilyData *cfd,
                                                     rocksdb::InstrumentedMutex *db_mutex,
                                                     std::atomic<bool> *shutting_down,
                                                     rocksdb::NVMCacheOptions *nvm_cache_options)
                                                     :  dbname_(dbname),
                                                        cfd_(cfd),
                                                        db_mutex_(db_mutex),
                                                        shutting_down_(shutting_down),
                                                        nvm_cache_options_(nvm_cache_options),
                                                        nvm_write_cache_(dynamic_cast<FixedRangeChunkBasedNVMWriteCache*>(nvm_cache_options_->nvm_write_cache_)),
                                                        cache_stat_(nvm_write_cache_->GetStats()),
                                                        range_list_(cache_stat_->range_list_)
                                                        {

    }

}//end rocksdb