#ifndef PERSISTENT_RANGE_MEM_H
#define PERSISTENT_RANGE_MEM_H

#include <list>
#include <db/db_impl.h>
#include <libpmemobj.h>

#include "persistent_chunk.h"
#include "pmem_hash_map.h"
#include "nvm_cache_options.h"
#include "skiplist/libpmemobj++/make_persistent.hpp"
#include "skiplist/libpmemobj++/make_persistent_array.hpp"
#include "skiplist/libpmemobj++/p.hpp"
#include "skiplist/libpmemobj++/persistent_ptr.hpp"
#include "skiplist/libpmemobj++/pool.hpp"
#include "skiplist/libpmemobj++/transaction.hpp"

using namespace pmem::obj;

namespace rocksdb {

#define CHUNK_BLOOM_FILTER_SIZE 8
        using pmem::obj::persistent_ptr;

        using std::list;

        class freqUpdateInfo {
            public:
            explicit freqUpdateInfo(const Slice& real_start, const Slice& real_end)
            :real_start_(real_start), real_end_(real_end) {

            }
            void update(const Slice& real_start, const Slice& real_end) {
                transaction::run(pop, [&]{
                    real_start_ = make_persistent<char[]>(real_start.size());
                    memcpy(&real_start_[0], real_start.data(), real_start.size());
                    real_end_ = make_persistent<char[]>(real_end.size());
                    memcpy(&real_end_[0], real_end.data(), real_end.size());
                    chunk_num_ = chunk_num_ + 1;
                    seq_num_ = seq_num_ + 1;
                });
            }

            // 实际的range
            // TODO 初始值怎么定
            const p<uint64_t> MAX_CHUNK_SIZE;
            persistent_ptr<char[]> real_start_;
            persistent_ptr<char[]> real_end_;
            p<size_t> chunk_num_;
            p<uint64_t> seq_num_;
            p<uint64_t> total_size_;
        };

        class FixedRangeTab
        {
            struct chunk_blk {
                unsigned char bloom_filter[CHUNK_BLOOM_FILTER_SIZE];
                size_t size;
                char data[];
            };

            public:
            FixedRangeTab(size_t chunk_count, char *data, int filterLen);
            ~FixedRangeTab();

            public:
            // 返回当前RangeMemtable中所有chunk的有序序列
            // 基于MergeIterator
            // 参考 DBImpl::NewInternalIterator
            InternalIterator* NewInternalIterator(ColumnFamilyData* cfd, Arena* arena);
            Status Get(const Slice& key, std::string *value);

            // 返回当前RangeMemtable是否正在被compact
            bool IsCompactWorking();

            // 返回当前RangeMemtable的Global Bloom Filter
//    char* GetBloomFilter();

            // 将新的chunk数据添加到RangeMemtable
            void Append(const char *bloom_data, const Slice& chunk_data,
            const Slice& new_start, const Slice& new_end);

            // 更新当前RangeMemtable的Global Bloom Filter
//    void SetBloomFilter(char* bloom_data);

            // 返回当前RangeMem的真实key range（stat里面记录）
            void GetRealRange(Slice& real_start, Slice& real_end) {
                real_start = info.real_start_;
                real_end = info.real_end_;
            }

            // 更新当前RangeMemtable的状态
            void UpdateStat(const Slice& new_start, const Slice& new_end);

            // 判断是否需要compact，如果需要则将一个RangeMemid加入Compact队列
            void MaybeScheduleCompact();

            // 释放当前RangeMemtable的所有chunk以及占用的空间
            void Release();

            // 重置Stat数据以及bloom filter
            void CleanUp();

            private:
            FixedRangeTab(const FixedRangeTab&) = delete;
            FixedRangeTab& operator=(const FixedRangeTab&) = delete;

            uint64_t max_chunk_num_to_flush() const{ return 100;}
            Status DoInChunkSearch(const Slice& key, std::string* value, std::vector<uint64_t>& off, const char* chunk_data);
            Slice GetKVData(const char* raw, uint64_t item_size)

            // persistent info
            p<char*> raw_;
            p_range::p_node node_in_pmem_map;
            persistent_ptr<freqUpdateInfo> range_info_;

            // volatile info
            FixedRangeBasedOptions* interal_options_;
            std::vector<uint64_t> chunk_offset_;
            Comparator* cmp_;
        };

} // namespace rocksdb

#endif // PERSISTENT_RANGE_MEM_H
