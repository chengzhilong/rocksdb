//
// Created by 张艺文 on 2018/10/31.
//

#include "nvm_flush_job.h"

namespace rocksdb{

    NVMFlushJob::NVMFlushJob(const std::string &dbname, ColumnFamilyData *cfd,
                             const rocksdb::ImmutableDBOptions &db_options,
                             const rocksdb::MutableCFOptions &mutableCFOptions,
                             rocksdb::PersistentRangeMemSet *range_mem_set,
                             std::unordered_map<KeyRange, uint64_t> *range_list)
                             :
                             dbname_(dbname),
                             db_options_(db_options),
                             mutable_cf_options_(mutableCFOptions),
                             range_mem_set_(range_mem_set),
                             range_list_(range_list){

    }

    NVMFlushJob::~NVMFlushJob() {

    }

    // 问题
    // bloom filter是否可以修改
    // 与compaction的同步竞争
    // range的划分
    // chunck的构造问题

    // 获取imm list，并根据imm list与RangeList的交集获取RangeMem的集合以及对应的GlobalBloomFilter
    void NVMFlushJob::Prepare() {


    }

    // 通过子线程执行各个range的flush操作
    Status NVMFlushJob::Run() {

    }

    void NVMFlushJob::Cancel() {

    }

}//end rocksdb