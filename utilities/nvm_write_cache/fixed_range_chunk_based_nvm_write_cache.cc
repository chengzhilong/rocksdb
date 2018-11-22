#include "utilities/nvm_write_cache/skiplist/test_common.h"
#include "fixed_range_chunk_based_nvm_write_cache.h"

namespace rocksdb {

    using p_range::pmem_hash_map;
    using p_range::p_node;

    FixedRangeChunkBasedNVMWriteCache::FixedRangeChunkBasedNVMWriteCache(
            const FixedRangeBasedOptions* ioptions,
            const string &file, uint64_t pmem_size) {
        //bool justCreated = false;
        vinfo_ = new VolatileInfo(ioptions);
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
                pinfo_->range_map_->tab = make_persistent<p_node[]>(pinfo_->range_map_->tabLen);
                pinfo_->range_map_->loadFactor = 0.75f;
                pinfo_->range_map_->threshold = pinfo_->range_map_->tabLen * pinfo_->range_map_->loadFactor;
                pinfo_->range_map_->size = 0;

                persistent_ptr<char[]> data_space = make_persistent<char[]>(pmem_size);
                pinfo_->allocator_ = make_persistent<PersistentAllocator>(data_space, pmem_size);

                pinfo_->inited_ = true;
            });
        }else{
            RebuildFromPersistentNode();
        }

    }

    FixedRangeChunkBasedNVMWriteCache::~FixedRangeChunkBasedNVMWriteCache() {
        delete vinfo_;
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

    void FixedRangeChunkBasedNVMWriteCache::AppendToRange(const rocksdb::InternalKeyComparator &icmp,
                                                          const char *bloom_data, const rocksdb::Slice &chunk_data,
                                                          const rocksdb::ChunkMeta &meta) {
        /*
         * 1. 获取prefix
         * 2. 调用tangetab的append
         * */
        FixedRangeTab* now_range = nullptr;
        auto tab_found = vinfo_->prefix2range.find(meta.prefix);
        assert(tab_found != vinfo_->prefix2range.end());
        now_range = &tab_found->second;
        now_range->Append(icmp, bloom_data, chunk_data, meta.cur_start, meta.cur_end);
    }


    FixedRangeTab* FixedRangeChunkBasedNVMWriteCache::NewRange(const std::string &prefix) {
        size_t bufSize = 1 << 27; // 128 MB
        uint64_t _hash;
        _hash = pinfo_->range_map_->put(pop_, prefix, bufSize);

        p_range::p_node new_node = pinfo_->range_map_->get_node(_hash, prefix);
        FixedRangeTab *range = new FixedRangeTab(pop_, new_node, vinfo_->internal_options_);
        vinfo_->prefix2range.insert({prefix, range});
        return range;
    }

    void FixedRangeChunkBasedNVMWriteCache::MaybeNeedCompaction() {
        // 选择所有range中数据大小占总容量80%的range并按照总容量的大小顺序插入compaction queue
        std::vector<CompactionItem> pendding_compact;
        for(auto range : vinfo_->prefix2range){
            Usage range_usage = range.second.RangeUsage();
            if(range_usage.range_size >= range.second.max_range_size() * 0.8){
                pendding_compact.emplace_back(range.second);
            }
        }
        std::sort(pendding_compact.begin(), pendding_compact.end(),
                [](FixedRangeTab* lrange, FixedRangeTab* rrange){
                    return lrange->RangeUsage().range_size < rrange->RangeUsage().range_size;
        });

        for(auto pendding_range : pendding_compact){
            vinfo_->range_queue_.push(std::move(pendding_range));
        }
    }

    using p_range::p_node ;
    void FixedRangeChunkBasedNVMWriteCache::RebuildFromPersistentNode() {
        // 遍历每个Node，重建vinfo中的prefix2range
        // 遍历pmem_hash_map中的每个tab，每个tab是一个p_node指针数组
        PersistentInfo* vpinfo = pinfo_.get();
        pmem_hash_map* vhash_map = vpinfo->range_map_.get();

        size_t hash_map_size = vhash_map->tabLen;

        for(size_t i = 0; i < hash_map_size; i++){
            p_node pnode = vhash_map->tab[0];
            if(pnode != nullptr){
                p_node tmp = pnode;
                do{
                    // 重建FixedRangeTab
                    FixedRangeTab* recovered_tab = new FixedRangeTab(pop_, tmp, vinfo_->internal_options_);
                    p_range::Node* tmp_node = tmp.get();
                    std::string prefix(tmp_node->prefix_.get(), tmp_node->prefixLen);
                    vinfo_->prefix2range[prefix] = recovered_tab;
                    tmp = tmp_node->next;
                }while(tmp != nullptr);
            }
        }



    }


    InternalIterator* FixedRangeChunkBasedNVMWriteCache::NewIterator(const InternalKeyComparator* icmp, Arena* arena) {
        InternalIterator* internal_iter;
        MergeIteratorBuilder merge_iter_builder(icmp, arena);
        for (auto range : vinfo_->prefix2range) {
            merge_iter_builder.AddIterator(range.second.NewInternalIterator(icmp, arena));
        }

        internal_iter = merge_iter_builder.Finish();
        return internal_iter;
    }

    void FixedRangeChunkBasedNVMWriteCache::RangeExistsOrCreat(const std::string &prefix) {
        auto tab_idx = vinfo_->prefix2range.find(prefix);
        if(tab_idx == vinfo_->prefix2range.end()){
            NewRange(prefix);
        }
    }


} // namespace rocksdb

