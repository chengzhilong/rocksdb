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

    FixedRangeChunkBasedNVMWriteCache::FixedRangeChunkBasedNVMWriteCache(const string& file, const string& layout)
            :file_path(file)
            ,LAYOUT(layout)
            ,POOLSIZE(1 << 26)
    {
        //    file_path.data()
        bool justCreated = false;
        if (file_exists(file_path) != 0) {
            pop = pool<pmem_hash_map>::create(file_path, LAYOUT, POOLSIZE, CREATE_MODE_RW);
            justCreate = true;

        } else {
            pop = pool<pmem_hash_map>::open(file_path, LAYOUT);
        }
//  pmem_ptr = pop.root();

        transaction::run(pop, [&] {
            persistent_ptr<p_range::pmem_hash_map> p_map = pop.root();
            if (justCreated) {
                // TODO 配置
                p_map->tabLen = ;
                p_map->tab = make_persistent<p_node[]>(p_map->tabLen);
                p_map->loadFactor = 0.75f;
                p_map->threshold = p_map->tabLen * p_map->loadFactor;
                p_map->size = 0;
            }
        });

//  if (file_exists(file_path) != 0) {
//    if ((pop = pmemobj_create(file_path, POBJ_LAYOUT_NAME(range_mem),
//                              PMEMOBJ_MIN_POOL, 0666)) == NULL) {
//      perror("failed to create pool\n");
//      return 1;
//    }
//  } else {
//    if ((pop = pmemobj_open(file_path,
//                            POBJ_LAYOUT_NAME(range_mem))) == NULL) {
//      perror("failed to open pool\n");
//      return 1;
//    }
//  }
    }

    FixedRangeChunkBasedNVMWriteCache::~FixedRangeChunkBasedNVMWriteCache()
    {
        if (pop)
            pop.close();
//    pmemobj_close(pop);
    }

    Status FixedRangeChunkBasedNVMWriteCache::Get(const Slice &key, std::string *value)
    {
        FixedRangeTab *tab;
        PrefixExtractor* prefix_extractor = internal_options_->prefix_extractor_;
        tab = cache_stats_->range_list_[prefix_extractor(std::string(key.data(), key.size()))];
        if(tab != nullptr){
            // 找到了对应的tab
            return tab->Get(key, value);
        }else{
            // 没有对应的tab，返回未找到
            return Status::kNotFound;
        }
        // TODO
        // 1. calc target FixedRangeTab
        // 2.
    }

    void FixedRangeChunkBasedNVMWriteCache::addCompactionRangeTab(FixedRangeTab *tab)
    {
        range_queue_.pu
    }

    uint64_t FixedRangeChunkBasedNVMWriteCache::NewRange(const std::string &prefix)
    {
        // ( const void * key, int len, unsigned int seed );
//  uint64_ _hash = CityHash64WithSeed(prefix, prefix.size(), 16);
        persistent_ptr<p_range::pmem_hash_map> p_map = pop.root();
        size_t bufSize = 1 << 26; // 64 MB
        uint64_t _hash;
        _hash = p_map->put(pop, prefix, bufSize);
        return _hash;
    }

} // namespace rocksdb


