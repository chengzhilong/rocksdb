#ifndef PERSISTENT_RANGE_MEM_H
#define PERSISTENT_RANGE_MEM_H

#include <list>
#include <db/db_impl.h>

//#include "libpmemobj.h"
#include "libpmemobj++/p.hpp"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/pool.hpp"
#include "libpmemobj++/transaction.hpp"
#include "libpmemobj++/make_persistent.hpp"
#include "libpmemobj++/make_persistent_array.hpp"

#include "persistent_chunk.h"
#include "pmem_hash_map.h"
#include "nvm_cache_options.h"
#include "chunkblk.h"

using namespace pmem::obj;

namespace rocksdb {

using pmem::obj::persistent_ptr;

using std::list;

struct Usage {
    uint64_t chunk_num;
    uint64_t range_size;
    InternalKey start, end;

};



class FixedRangeTab {

public:
    FixedRangeTab(pool_base &pop, p_range::p_node hash_node, FixedRangeBasedOptions *options);

    FixedRangeTab(pool_base &pop, const FixedRangeTab &) = delete;

public:
    // 返回当前RangeMemtable中所有chunk的有序序列
    // 基于MergeIterator
    // 参考 DBImpl::NewInternalIterator
    InternalIterator *NewInternalIterator(const InternalKeyComparator* icmp, Arena *arena);

    Status Get(const InternalKeyComparator &internal_comparator, const Slice &key,
               std::string *value);

    void RebuildBlkList();

    // 返回当前RangeMemtable是否正在被compact
    bool IsCompactWorking() { return in_compaction_; }

    // 设置compaction状态
    void SetCompactionWorking(bool working) { in_compaction_ = working; }

    // 将新的chunk数据添加到RangeMemtable
    void Append(const InternalKeyComparator& icmp,
            const char *bloom_data, const Slice &chunk_data,
            const Slice& start, const Slice& end);

    Usage RangeUsage();

    // 释放当前RangeMemtable的所有chunk以及占用的空间
    void Release();

    // 重置Stat数据以及bloom filter
    void CleanUp();

    uint64_t max_range_size(){
        return pmap_node_->bufSize;
    }

private:

    uint64_t max_chunk_num_to_flush() const {
        // TODO: set a max chunk num
        return 1024;
    }

    // 返回当前RangeMem的真实key range（stat里面记录）
    void GetRealRange(Slice &real_start, Slice &real_end);

    Status searchInChunk(PersistentChunkIterator *iter,
                        const InternalKeyComparator &icmp,
                        const Slice &key, std::string *value);

    Slice GetKVData(char *raw, uint64_t item_off);

    void CheckAndUpdateKeyRange(const InternalKeyComparator &icmp, const Slice &new_start, const Slice &new_end);

    void ConsistencyCheck();

    // persistent info
    p_range::p_node pmap_node_;
    pool_base &pop_;

    // volatile info
    const FixedRangeBasedOptions *interal_options_;
    vector<ChunkBlk> blklist;
    char *raw_;
    bool in_compaction_;


};

} // namespace rocksdb

#endif // PERSISTENT_RANGE_MEM_H
