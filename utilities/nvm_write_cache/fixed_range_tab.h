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
#include "skiplist/libpmemobj++/detail/persistent_ptr_base.hpp"
#include "skiplist/libpmemobj++/pool.hpp"
#include "skiplist/libpmemobj++/transaction.hpp"

using namespace pmem::obj;

namespace rocksdb {

    using pmem::obj::persistent_ptr;

    using std::list;

    struct Usage{
        uint64_t chunk_num;
        uint64_t range_size;
        Slice start,end;

    };



class FixedRangeTab
{
  using p_range::p_node;
  //  struct chunk_blk {
  //    unsigned char bloom_filter[CHUNK_BLOOM_FILTER_LEN];
  //    size_t size;
  //    char data[];
  //  };

    public:
        FixedRangeTab(pool_base &pop, p_range::p_node hash_node, FixedRangeBasedOptions *options);

        FixedRangeTab(pool_base &pop, const FixedRangeTab &) = delete;

        FixedRangeTab &operator=(const FixedRangeTab &) = delete;

        ~FixedRangeTab() = default;

        // 返回当前RangeMemtable中所有chunk的有序序列
        // 基于MergeIterator
        // 参考 DBImpl::NewInternalIterator
        InternalIterator *NewInternalIterator(ColumnFamilyData *cfd, Arena *arena);

        Status Get(const InternalKeyComparator &internal_comparator, const Slice &key, std::string *value);

        // 返回当前RangeMemtable是否正在被compact
        bool IsCompactWorking() { return in_compaction_; }

        // 设置compaction状态
        void SetCompactionWorking(bool working){in_compaction_ = working;}

        // 将新的chunk数据添加到RangeMemtable
        void Append(InternalKeyComparator* icmp,
                    const char *bloom_data, const Slice &chunk_data,
                    const Slice &new_start, const Slice &new_end);

        // 返回当前RangeMem的真实key range（stat里面记录）
        void GetRealRange(Slice &real_start, Slice &real_end);


        // 判断是否需要compact，如果需要则将一个RangeMemid加入Compact队列
        Usage RangeUsage();

        // 释放当前RangeMemtable的所有chunk以及占用的空间
        void Release();

        // 重置Stat数据以及bloom filter
        void CleanUp();

        void RebuildFromNode(p_range::p_node pmap_node);

    private:

        uint64_t max_chunk_num_to_flush() const {
            // TODO: set a max chunk num
            return 100;
        }

        Status
        DoInChunkSearch(InternalKeyComparator &icmp, const Slice &key, std::string *value, std::vector<uint64_t> &off,
                        char *chunk_data);

        Slice GetKVData(char *raw, uint64_t item_off);

        void CheckAndUpdateKeyRange(InternalKeyComparator* icmp, const Slice& new_start, const Slice& new_end);

        // persistent info
        p_range::p_node pmap_node_;
        pool_base& pop_;

        // volatile info
        const FixedRangeBasedOptions *interal_options_;
        std::vector<uint64_t> chunk_offset_;
        char *raw_;
        bool in_compaction_;
        //Comparator* cmp_;
    };

} // namespace rocksdb

#endif // PERSISTENT_RANGE_MEM_H
