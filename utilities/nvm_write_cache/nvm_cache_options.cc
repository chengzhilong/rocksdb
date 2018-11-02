//
// Created by 张艺文 on 2018/11/2.
//

#include "nvm_cache_options.h"

namespace rocksdb{
    NVMCacheOptions::NVMCacheOptions()
        :   use_nvm_write_cache_(false)
    {

    }

    NVMCacheOptions::NVMCacheOptions(Options &options)
        :   use_nvm_write_cache_(options.nvm_cache_options_->use_nvm_write_cache_),
            pmem_info_(options.nvm_cache_options_->pmem_info_),
            nvm_cache_type_(options.nvm_cache_options_->nvm_cache_type_),
            nvm_write_cahce_(options.nvm_cache_options_->nvm_write_cahce_),
            drain_strategy_(options.nvm_cache_options_->drain_strategy_)
    {

    }


    NVMCacheOptions::~NVMCacheOptions() {
        delete nvm_write_cahce_;
    }



}//end rocksdb
