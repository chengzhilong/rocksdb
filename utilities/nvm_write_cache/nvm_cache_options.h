//
// Created by 张艺文 on 2018/11/2.
//

#pragma once

#include "include/rocksdb/options.h"
#include "utilities/nvm_write_cache/prefix_extractor.h"
#include "utilities/nvm_write_cache/nvm_write_cache.h"
namespace rocksdb{

    class NVMWriteCache;
    class PrefixExtractor;

    struct PMemInfo{
        std::string pmem_path_;
        uint64_t pmem_size_;
    };

    enum NVMCacheType{
        kRangeFixedChunk,
        kRangeDynamic,
        kTreeBased,
    };

    enum DrainStrategy{
        kCompaction;
        kNoDrain;
    };

    struct NVMCacheOptions{
        NVMCacheOptions();

        explicit NVMCacheOptions(Options& options);

        ~NVMCacheOptions();

        bool use_nvm_write_cache_;

        PMemInfo pmem_info_;

        NVMCacheType nvm_cache_type_;

        NVMWriteCache* nvm_write_cache_;

        DrainStrategy drain_strategy_;

    };

    struct FixedRangeBasedOptions{

        const uint16_t chunk_bloom_bits_ = 16;

        const uint16_t prefix_bits_ = 3;

        const PrefixExtractor* prefix_extractor_ = nullptr;

        const uint64_t range_num_threshold_ = 0;

        const uint64_t range_size_threashold_ = 64ul << 20;

    };

    struct DynamicRangeBasedOptions{

    };

    struct TreeBasedOptions{

    };

} //end rocksdb

