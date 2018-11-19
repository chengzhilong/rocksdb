#include "util/coding.h"
#include "fixed_range_tab.h"

#include <table/merging_iterator.h>

#include <persistent_chunk.h>

namespace rocksdb {

    using pmem::obj::persistent_ptr;
#define MAX_BUF_LEN 4096

//POBJ_LAYOUT_BEGIN(range_mem);
//POBJ_LAYOUT_ROOT(range_mem, struct my_root);
//POBJ_LAYOUT_TOID(range_mem, struct foo_el);
//POBJ_LAYOUT_TOID(range_mem, struct bar_el);
//POBJ_LAYOUT_END(range_mem);

    struct my_root {
        size_t length; // mark end of chunk block sequence
        unsigned char data[MAX_BUF_LEN];
    };


    FixedRangeTab::FixedRangeTab(size_t chunk_count, char *data, int filterLen) {
        // new
    }

    FixedRangeTab::~FixedRangeTab() {

    }

//| prefix data | prefix size |
//| cur_ | seq_ |
//| chunk blmFilter | chunk len | chunk data ..| 不定长
//| chunk blmFilter | chunk len | chunk data ...| 不定长
//| chunk blmFilter | chunk len | chunk data .| 不定长


    InternalIterator *FixedRangeTab::NewInternalIterator(
            ColumnFamilyData *cfd, Arena *arena) {
        InternalIterator *internal_iter;
        MergeIteratorBuilder merge_iter_builder(&cfd->internal_comparator(),
                                                arena);
        // TODO
        // 预设 range 持久化
//  char *chunkBlkOffset = data_ + sizeof(stat.used_bits_) + sizeof(stat.start_)
//      + sizeof(stat.end_);

        persistent_ptr<char[]> chunkBlkOffset = node_in_pmem_map->buf;

        PersistentChunk pchk;
        for (int i = 0; i < info.chunk_num; ++i) {
//    chunk_blk *blk = reinterpret_cast<chunk_blk*>(chunkBlkOffset);
            persistent_ptr<char[]> sizeOffset = chunkBlkOffset + CHUNK_BLOOM_FILTER_SIZE;
            size_t blkSize;
            memcpy(&blkSize, sizeOffset, sizeof(blkSize));
            pchk.reset(CHUNK_BLOOM_FILTER_SIZE, blkSize, sizeOffset + sizeof(blkSize));
            merge_iter_builder.AddIterator(pchk.NewIterator(arena));

            chunkBlkOffset = sizeOffset + sizeof(blkSize) + blkSize;
        }

        internal_iter = merge_iter_builder.Finish();
    }

    void FixedRangeTab::Append(const char *bloom_data,
                               const Slice &chunk_data,
                               const Slice &new_start,
                               const Slice &new_end) {


        if (range_info_->total_size + chunk_data.size_ >= range_info_->MAX_CHUNK_SIZE
            || range_info_.chunk_num_ > max_chunk_num_to_flush()) {
            // TODO
            // mark as flush
        }

        //size_t cur_len = node_in_pmem_map->dataLen;
        size_t chunk_blk_len = interal_options_->chunk_bloom_bits_ + sizeof(chunk_data.size_)
                               + chunk_data.size_;
        uint64_t raw_cur = DecodeFixed64(raw_ - 2 * sizeof(uint64_t));
        uint64_t last_srq = DecodeFixed64(raw_ - sizeof(uint64_t));
        char *dst = raw_ + raw_cur;

        // append data
        memcpy(dst, bloom_data, interal_options_->chunk_bloom_bits_);
        memcpy(dst + interal_options_->chunk_bloom_bits_, chunk_data.size_, sizeof(uint64_t));

        dst += interal_options_->chunk_bloom_bits_ + sizeof(chunk_data.size_);

        memcpy(dst, chunk_data.data_, chunk_data.size_);

        // update cur and seq
        EncodeFixed64(raw_ - 2 * sizeof(uint64_t), raw_cur + chunk_blk_len);
        EncodeFixed64(raw_ - sizeof(uint64_t), last_srq + 1);

        // update meta info
        range_info_->update(new_start, new_end);

        // record this offset to volatile vector
        chunk_offset_.push_back(raw_cur);
    }

    void FixedRangeTab::Release() {

    }

    Status FixedRangeTab::Get(const rocksdb::Slice &key, std::string *value) {
        // 1.从下往上遍历所有的chunk
        uint64_t bloom_bits = interal_options_->chunk_bloom_bits_;
        for (size_t i = chunk_offset_.size() - 1; i >= 0; i--) {
            uint64_t off = chunk_offset_[i];
            char* pchunk_data = raw_ + off;
            char *chunk_start = pchunk_data;
            // 2.获取当前chunk的bloom data，查找这个bloom data判断是否包含对应的key
            if (interal_options_->filter_policy_->KeyMayMatch(key, Slice(chunk_start, bloom_bits))) {
                // 3.如果有则读取元数据进行chunk内的查找
                uint64_t chunk_len = DecodeFixed64(chunk_start + bloom_bits);
                chunk_start += (bloom_bits + sizeof(uint64_t) + chunk_len);
                uint64_t item_num = DecodeFixed64(chunk_start - sizeof(uint64_t));
                chunk_start -= (item_num + 1) * sizeof(uint64_t);
                std::vector<uint64_t> item_offs;
                while(item_num-- > 0){
                    item_offs.push_back(DecodeFixed64(chunk_start));
                    chunk_start += sizeof(uint64_t);
                }
                Status s = DoInChunkSearch(key, value, item_offs, pchunk_data + bloom_bits + sizeof(uint64_t));
                if(s.ok()) return s;
            }else{
                continue;
            }
        }

        // 4.循环直到查找完所有的chunk
    }

    Status FixedRangeTab::DoInChunkSearch(const rocksdb::Slice &key, std::string *value, std::vector<uint64_t> &off,
                                          const char *chunk_data) {
        size_t left = 0, right = off.size() - 1;
        size_t middle = (left + right) / 2;
        while(left < right){
            Slice ml_key = GetKVData(chunk_data, off[middle]);
            int result = cmp_->Compare(ml_key, key);
            if(result == 0){
                //found
                uint64_t value_off = DecodeFixed64(chunk_data + off[middle] + ml_key.size());
                Slice raw_value = GetKVData(chunk_data, value_off);
                value = new std::string(raw_value.data(), raw_value.size());
                return Status::OK();
            }else if( result < 0){
                // middle < key
                left = middle + 1;
            }else if(result > 0){
                // middle >= key
                right = middle - 1;
            }
        }
        return Status::NotFound("not found");
    }

} // namespace rocksdb
