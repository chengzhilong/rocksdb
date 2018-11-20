

#pragma once
#include <queue>
#include <unordered_map>
#include <rocksdb/iterator.h>
#include "utilities/nvm_write_cache/nvm_write_cache.h"
#include "utilities/nvm_write_cache/nvm_cache_options.h"

#include "skiplist/libpmemobj++/pool.hpp"
#include "skiplist/libpmemobj++/persistent_ptr.hpp"
#include "skiplist/libpmemobj++/p.hpp"

#include "fixed_range_tab.h"
#include "pmem_hash_map.h"

using std::string;
using std::unordered_map;
using namespace pmem::obj;

namespace rocksdb {

    //class PersistentAllocator;

    static inline int
    file_exists(char const *file)
    {
        return access(file, F_OK);
    }

    struct CompactionItem {
        FixedRangeTab *pending_compated_range_;
        Slice start_key_, end_key_;
        uint64_t range_size_, chunk_num_;
    };


    class PersistentAllocator{
    public:
        explicit PersistentAllocator(persistent_ptr<char[]> raw_space, uint64_t total_size){
            raw_ = raw_space;
            total_size_ = total_size;
            cur_ = 0;
        }

        ~PersistentAllocator() =default;

        char* Allocate(size_t alloca_size){
            char* alloc = &raw_[0] + cur_;
            cur_ = cur_ + alloca_size;
            return alloc;
        }

        uint64_t Remain(){
            return total_size_ - cur_;
        }


    private:
        persistent_ptr<char[]> raw_;
        p<uint64_t > total_size_;
        p<uint64_t > cur_;

    };

    class FixedRangeChunkBasedNVMWriteCache : public NVMWriteCache {
    public:
        explicit FixedRangeChunkBasedNVMWriteCache(const string &file, uint64_t pmem_size);

        ~FixedRangeChunkBasedNVMWriteCache();

        // insert data to cache
        // insert_mark is (uint64_t)range_id
//  Status Insert(const Slice& cached_data, void* insert_mark) override;

        // get data from cache
        Status Get(const InternalKeyComparator& internal_comparator, const Slice &key, std::string *value) override;

        void AppendToRange(FixedRangeTab* tab,
                const char* bloom_data, const Slice& chunk_data,
                const Slice &new_start, const Slice &new_end);

        // get iterator of the total cache
        Iterator *NewIterator() override;

        // 获取range_mem_id对应的RangeMemtable结构
//  FixedRangeTab* GetRangeMemtable(uint64_t range_mem_id);

        // return there is need for compaction or not
        bool NeedCompaction() override { return !vinfo_->range_queue_.empty(); }

        //get iterator of data that will be drained
        // get 之后释放没有 ?
        CompactionItem *GetCompactionData() {
            CompactionItem *item = vinfo_->range_queue_.front();
            return item;
        }

        void addCompactionRangeTab(FixedRangeTab *tab);

        // add a range with a new prefix to range mem
        // return the id of the range
        FixedRangeTab* NewRange(const std::string &prefix);

        // get internal options of this cache
        const FixedRangeBasedOptions *internal_options() { return vinfo_->internal_options_; }

        void MaybeScheduleCompaction();

        unordered_map<string, FixedRangeTab>* GetRangeList();

        // get stats of this cache
        //FixedRangeChunkBasedCacheStats *stats() { return cache_stats_; }

    private:

        void RebuildFromPersistentNode();

        struct PersistentInfo {
            p<bool> inited_;
            p<uint64_t> allocated_bits_;
            persistent_ptr<p_range::pmem_hash_map> range_map_;
            persistent_ptr<PersistentAllocator> allocator_;
        };

        pool<PersistentInfo> pop_;
        persistent_ptr<PersistentInfo> pinfo_;

        struct VolatileInfo{
            unordered_map<string, FixedRangeTab> prefix2range;
            const FixedRangeBasedOptions *internal_options_;
            std::queue<CompactionItem*> range_queue_;
            uint64_t range_seq_;
        };

        VolatileInfo* vinfo_;
    };

} // namespace rocksdb