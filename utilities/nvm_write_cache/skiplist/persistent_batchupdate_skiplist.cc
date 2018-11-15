//
// Created by 张艺文 on 2018/11/14.
//

#include "persistent_batchupdate_skiplist.h"

namespace rocksdb{

    void EncodeFixed64(char* buf, uint64_t value) {

            buf[0] = value & 0xff;
            buf[1] = (value >> 8) & 0xff;
            buf[2] = (value >> 16) & 0xff;
            buf[3] = (value >> 24) & 0xff;
            buf[4] = (value >> 32) & 0xff;
            buf[5] = (value >> 40) & 0xff;
            buf[6] = (value >> 48) & 0xff;
            buf[7] = (value >> 56) & 0xff;
    }

    uint32_t DecodeFixed32(const char* ptr) {
            return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
                    | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
                    | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
                    | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
    }

    uint64_t DecodeFixed64(const char* ptr) {
            uint64_t lo = DecodeFixed32(ptr);
            uint64_t hi = DecodeFixed32(ptr + 4);
            return (hi << 32) | lo;
    }

    VolatileSkipList::VolatileSkipList(int32_t max_height, int32_t branching_factor)
            :
            kMaxHeight_(static_cast<uint16_t>(max_height)),
            kBranching_(static_cast<uint16_t>(branching_factor)),
            kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),

            max_height_(1) {

        head_ = NewNode(" ", max_height_);
        prev_ = static_cast<Node**>(malloc(sizeof(Node*) * kMaxHeight_));
        first_inserted_ = nullptr;
        prev_inserted_ = nullptr;

        for (int i = 0; i < kMaxHeight_; i++) {
            head_->SetNext(i, nullptr);
            prev_[i] = head_;
        }

        prev_height_ = 1;
    }

    VolatileSkipList::~VolatileSkipList() {
        Node* start = head_;
        Node* prev = start;
        while(start->Next(0) != nullptr){
            start = start->Next(0);
            delete  prev;
        }
        delete start;
    }

    Node* VolatileSkipList::NewNode(const std::string &key, int height) {
        Node* n;
        n = new Node(key, height);
        n->index_ = seq_num_++;
        return n;
    }

// when n < key returns true
// n should be at behind of key means key is after node
    bool VolatileSkipList::KeyIsAfterNode(const std::string& key, Node* n) const {
        return (n != nullptr) && (n->key_.compare(key));
    }

    Node* VolatileSkipList::FindLessThan(const std::string &key,
                                         Node* prev[]) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        Node* last_not_after;
        while(true){
            Node* next = x->Next(level);
            if(next != last_not_after && KeyIsAfterNode(key, next)){
                x = next;
            }else{
                prev[level] = x;
                if(level ==0 ){
                    return x;
                }else{
                    last_not_after = next;
                    level--;
                }
            }
        }
    }

    void VolatileSkipList::Insert(const char *key, int height) {
        // key < prev[0]->next(0) && prev[0] is head or key < prev[0]
        if (!KeyIsAfterNode(key, prev_[0]->Next(0)) &&
            (prev_[0] == head_ || KeyIsAfterNode(key, prev_[0]))) {
            for (uint32_t i = 1; i < prev_height_; i++) {
                prev_[i] = prev_[0];
            }
        } else {
            FindLessThan(key, prev_);
        }

        if(height > GetMaxHeight()){
            for(int i = GetMaxHeight(); i < height; i++){
                prev_[i] = head_;
            }
            max_height_ = static_cast<uint16_t >(height);
        }


        Node* x = NewNode(key, height);

        {
            if(prev_inserted_ == nullptr){
                first_inserted_ = x;
            }
            prev_inserted_->next_by_insert_ = x;
            prev_inserted_ = x;
        }

        for(int i = 0; i < height; i++){
            x->SetNext(i, prev_[i]->Next(i));
            prev_[i]->SetNext(i ,x);
        }
        prev_[0] = x;
        prev_height_ = static_cast<uint16_t >(height);

    }


    Node* VolatileSkipList::FindGreaterOrEqual(const std::string &key) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        Node* last_bigger;
        while(true){
            Node* next = x->Next(level);
            int cmp = (next == nullptr || next == last_bigger) ? 1 : next->key_.compare(key);
            if(cmp == 0 || (cmp > 0 && level ==0)){
                return next;
            }else if(cmp < 0){
                x = next;
            }else{
                last_bigger = next;
                level--;
            }
        }

    }

    void VolatileSkipList::Print() const {
        Node* start = head_;
        while(start->Next(0) != nullptr){
            start = start->Next(0);
        }
    }

    void VolatileSkipList::GetIndex(uint64_t &size, std::vector<uint64_t >& height, std::vector<int>& result) {
        std::vector<int> index;
        size_t seq = 0;
        Node* next_node = first_inserted_;
        do{
            // get next in all height
            for(size_t i = 0; i < height[seq]; i++){
                index.push_back(next_node->Next(i) == nullptr ? next_node->Next(i)->index_:-1);
            }
            // go to the next
            next_node = next_node->next_by_insert_;
        }while(next_node != nullptr);

    }

    PersistentBatchUpdateSkiplist::PersistentBatchUpdateSkiplist(pool_base &pop, int32_t max_height,
                                                                 int32_t branching_factor):
                                                                 pop_(pop),
                                                                 kMaxHeight_(max_height),
                                                                 kBranching_(branching_factor),
                                                                 kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_){
        //vlist_ = new VolatileSkipList(max_height, branching_factor);
        transaction::run(pop, [&]{
            off_array_ = make_persistent<int[]>(10010);
            key_log_ = make_persistent<char[]>(2ul * 1024 * 1024 * 1024);
            index_log_ = nullptr;
            array_cur_ = 0;
            key_cur_ = 0;
            head_ = 0;
            pendding_sort_ = 0;
            count_ = 0;
            index_cur_ = 0;
            index_size_ = 0;
        });

    }

    PersistentBatchUpdateSkiplist::~PersistentBatchUpdateSkiplist() {
        delete vlist_;

    }

    void PersistentBatchUpdateSkiplist::Insert(const std::string &key) {
        char* buf = new char[key.size() + 8 + 8];
        int height = RandomHeight();
        EncodeFixed64(buf, static_cast<uint64_t >(key.size()));
        memcpy(buf, key.c_str(), key.size());
        EncodeFixed64(buf, static_cast<uint64_t >(index_cur_ + height - 1));

        // append to key_log_
        uint64_t off = AppendToKeyLog(buf, key.size() + 8 + 8);

        // append actived
        // maybe transaction?
        {
            off_array_[array_cur_] = off;
            array_cur_ = array_cur_ + 1;
            index_cur_ = index_cur_ + height;
            count_ = count_ + 1;
        }

        if(count_ > 100){
            BuildNewListAndPersist();
            count_ = 0;
        }
    }

    uint64_t PersistentBatchUpdateSkiplist::AppendToKeyLog(const char *data, size_t size) {
        memcpy(&key_log_[0] + key_cur_, data, size);
        key_cur_ = key_cur_ + size;
        return key_cur_ - size;

    }

    void PersistentBatchUpdateSkiplist::BuildNewListAndPersist() {

        vlist_ = new VolatileSkipList(kMaxHeight_, kBranching_);
        std::vector<uint64_t > height;
        // read pre-build sorted index
        TransferIntoVolatile(height);

        // build new index
        uint64_t next_sort = pendding_sort_;
        while(next_sort < array_cur_){
            std::string cur_key;
            uint64_t prev_accumu_height;
            uint64_t accumu_height;
            GetKey(off_array_[next_sort], cur_key, accumu_height, prev_accumu_height);
            vlist_->Insert(cur_key.c_str(), accumu_height - prev_accumu_height);
            height.push_back(accumu_height - prev_accumu_height);
            next_sort++;
        }

        // persist new index
        uint64_t index_size;
        std::vector<int> new_index;
        vlist_->GetIndex(index_size, height, new_index);

        persistent_ptr<int[]> new_persistent_index;
        transaction::run(pop_, [&]{
            new_persistent_index = make_persistent<int[]>(new_index.size());
            size_t i = 0;
            for(auto idx_num : new_index){
                index_log_[i++] = idx_num;
            }
        });

        persistent_ptr<int[]> old_index = index_log_;
        int old_size = index_size;
        index_log_ = new_persistent_index;
        index_size = new_index.size();
        delete_persistent<int[]>(old_index, old_size);

        // set new pendding_sort pos
        pendding_sort_ = array_cur_;
        head_ = vlist_->head();

        delete vlist_;
    }

    void PersistentBatchUpdateSkiplist::TransferIntoVolatile(std::vector<uint64_t >& height) {
        if(index_log_ != nullptr){
            // Build Volatile Skiplist from persistent index
            size_t next_key = head_;
            std::string cur_key;
            uint64_t prev_accumu_height = 0;
            uint64_t accumu_height = 0;
            GetKey(off_array_[next_key], cur_key, accumu_height, prev_accumu_height);
            vlist_->Insert(cur_key.c_str(), static_cast<int>(accumu_height - prev_accumu_height));
            height.push_back(accumu_height - prev_accumu_height);
            while(index_log_[prev_accumu_height + 1] != -1){
                next_key = index_log_[prev_accumu_height + 1];
                GetKey(off_array_[next_key], cur_key, accumu_height, prev_accumu_height);
                vlist_->Insert(cur_key.c_str(), static_cast<int>(accumu_height - prev_accumu_height));
            }
        }else{
            // no prebuild sorted index
            return;
        }
    }

    void PersistentBatchUpdateSkiplist::GetKey(uint64_t offset, std::string &key, uint64_t &accumu_height,
                                               uint64_t prev_accumu_height) {
        uint64_t key_size = DecodeFixed64(&key_log_[0] + offset);
        accumu_height = DecodeFixed64(&key_log_[0] + offset + key_size + 8);
        prev_accumu_height = DecodeFixed64(&key_log_[0] + offset - 8);
        key = std::string(&key_log_[0] + offset + 8, key_size);
    }



    int PersistentBatchUpdateSkiplist::RandomHeight() {
        auto rnd = Random::GetTLSInstance();

        // Increase height with probability 1 in kBranching
        int height = 1;
        while (height < kMaxHeight_ && rnd->Next() < kScaledInverseBranching_) {
            height++;
        }
        assert(height > 0);
        assert(height <= kMaxHeight_);
        return height;
    }

    void SkiplistWrapper::Init(pool_base &pop, uint32_t max_height, uint32_t branching_factor) {
        if(!inited){
            transaction::run(pop, [&]{
                batchskiplist = make_persistent<PersistentBatchUpdateSkiplist>(pop, max_height, branching_factor);
                inited = true;
            });
        }
    }



} //end rocksdb

