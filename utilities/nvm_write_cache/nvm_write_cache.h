//
// Created by 张艺文 on 2018/11/2.
//

#pragma once

#include <include/rocksdb/iterator.h>
#include <unordered_map>
#include <vector>


namespace rocksdb{


    typedef std::pair<Slice, Slice> KeyRange;

    struct CacheStats{
        uint64_t used_bits_;
    };

    struct FixedRangeChunkBasedCacheStats : public CacheStats{


        std::unordered_map<KeyRange, uint64_t> range_list_;

        std::vector<std::string*> chunk_bloom_data_;

    };


    class NVMWriteCache{
    public:
        NVMWriteCache() =default;

        virtual ~NVMWriteCache() = default;

        virtual Status Insert(const Slice& cached_data, const Slice& meta_data) = 0;

        virtual Status Get(const Slice& key, std::string* value) = 0;

        virtual Iterator* NewIterator() = 0;

        virtual Iterator* GetDraineddata() = 0;

        virtual void* GetOptions() = 0;

        virtual CacheStats* GetStats() = 0;

    };

} // end rocksdb
