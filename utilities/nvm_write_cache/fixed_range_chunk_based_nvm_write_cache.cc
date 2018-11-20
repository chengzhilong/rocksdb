#include "skiplist/libpmemobj++/make_persistent.hpp"
#include "skiplist/libpmemobj++/make_persistent_array.hpp"
#include "skiplist/libpmemobj++/p.hpp"
#include "skiplist/libpmemobj++/persistent_ptr.hpp"
#include "skiplist/libpmemobj++/pool.hpp"
#include "skiplist/libpmemobj++/transaction.hpp"

#include "fixed_range_chunk_based_nvm_write_cache.h"

#include <ex_common.h>

namespace rocksdb {

    using p_range::pmem_hash_map;
    using p_range::p_node;

    FixedRangeChunkBasedNVMWriteCache::FixedRangeChunkBasedNVMWriteCache(const string &file, uint64_t pmem_size) {
        //bool justCreated = false;
        if (file_exists(file.c_str()) != 0) {
            pop_ = pmem::obj::pool<PersistentInfo>::create(file.c_str(), "FixedRangeChunkBasedNVMWriteCache", pmem_size,
                                                           CREATE_MODE_RW);
            //justCreated = true;

        } else {
            pop_ = pmem::obj::pool<PersistentInfo>::open(file.c_str(), "FixedRangeChunkBasedNVMWriteCache");
        }

        pinfo_ = pop_.root();
        if (!pinfo_->inited_) {
            transaction::run(pop_, [&] {
                // TODO 配置
                pinfo_->range_map = make_persistent<p_range::pmem_hash_map>();
                pinfo_->range_map_->tabLen = 0;
                pinfo_->range_map_->tab = make_persistent<p_node[]>(p_map->tabLen);
                pinfo_->range_map_->loadFactor = 0.75f;
                pinfo_->range_map_->threshold = p_map->tabLen * p_map->loadFactor;
                pinfo_->range_map_->size = 0;

                persistent_ptr<char[]> data_space = make_persistent<char[]>(pmem_size);
                pinfo_->allocator_ = make_persistent<PersistentAllocator>(data_space, pmem_size);

                pinfo_->inited_ = true;
            });
        }

    }

    FixedRangeChunkBasedNVMWriteCache::~FixedRangeChunkBasedNVMWriteCache() {
        pop_.close();
    }

    Status FixedRangeChunkBasedNVMWriteCache::Get(const InternalKeyComparator &internal_comparator, const Slice &key,
                                                  std::string *value) {
        std::string prefix = (*vinfo_->internal_options_->prefix_extractor_)(key.data(), key.size());
        auto found_tab = vinfo_->prefix2range.find(prefix);
        if(found_tab == vinfo_->prefix2range.end()){
            // not found
            return Status::NotFound("no this range");
        }else{
            // found
            FixedRangeTab* tab = found_tab->second;
            return tab->Get(internal_comparator, key, value);
        }
    }


    void FixedRangeChunkBasedNVMWriteCache::NewRange(const std::string &prefix) {
        size_t bufSize = 1 << 27; // 128 MB
        uint64_t _hash;
        _hash = pinfo_->range_map_->put(pop_, prefix, bufSize);

        p_range::p_node new_node = pinfo_->range_map_->get_node(_hash, prefix);
        FixedRangeTab *range = new FixedRangeTab(pop_, new_node, vinfo_->internal_options_);
        vinfo_->prefix2range.insert({prefix, range});
    }

    void FixedRangeChunkBasedNVMWriteCache::MaybeNeedCompaction() {
        // TODO more reasonable compaction threashold
        if(pinfo_->allocator_->Remain() < pinfo_->allocator_->Capacity() * 0.75){
            uint64_t max_range_size = 0;
            FixedRangeTab* pendding_range = nullptr;
            Usage pendding_range_usage;
            for(auto range : vinfo_->prefix2range){
                Usage range_usage = range.second.RangeUsage();
                if(max_range_size < range_usage.range_size){
                    pendding_range = range.second;
                    pendding_range_usage = range_usage;
                }
            }

            CompactionItem* compaction_item = new CompactionItem;
            compaction_item->pending_compated_range_ = pendding_range;
            compaction_item->range_size_ = pendding_range_usage.range_size;
            compaction_item->chunk_num_ = pendding_range_usage.chunk_num;
            compaction_item->start_key_ = pendding_range_usage.start;
            compaction_item->end_key_ = pendding_range_usage.end;

            vinfo_->range_queue_.push(compaction_item);
        }
    }



} // namespace rocksdb

