//
// Created by 张艺文 on 2018/11/5.
//

#pragma once

#include <cstring>
#include <string>
#include <util/random.h>
#include "libpmemobj++/p.hpp"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/transaction.hpp"
#include "libpmemobj++/pool.hpp"
#include "libpmemobj++/make_persistent.hpp"

using namespace pmem::obj;

namespace rocksdb {

    const int kMaxHeight = 12;

    struct Node {
        explicit Node(const std::string &key, int height)
                :
                    key_(make_persistent<std::string>(key)){
        };

        ~Node() = default;

        persistent_ptr<Node> Next(int n) {
            assert(n >= 0);
            return next_[n];
        }

        void SetNext(int n, persistent_ptr<Node> next) {
            assert(n >= 0);
            next_[n] = next;
        };

        //p<int> height_;
        persistent_ptr<std::string> key_;
        persistent_ptr<Node> next_[kMaxHeight];
    };

    class persistent_SkipList {
    public:
        explicit persistent_SkipList(const std::string &path, int32_t max_height = 12, int32_t branching_factor = 4);

        ~persistent_SkipList() {

        }

        void Insert(const char *key);

        //bool Contains(const char *key);

    private:
        pool_base pool_;
        persistent_ptr<Node> head_;
        persistent_ptr<Node> prev_[kMaxHeight];
        p<uint32_t> prev_height_;
        p<uint16_t> kMaxHeight_;
        p<uint16_t> kBranching_;
        p<uint32_t> kScaledInverseBranching_;

        p<uint16_t> max_height_;


        inline int GetMaxHeight() const {
            return max_height_;
        }

        persistent_ptr<Node> NewNode(const std::string &key, int height);

        int RandomHeight();

        bool Equal(const char *a, const char *b) {
            return strcmp(a, b) == 0;
        }

        bool LessThan(const char *a, const char *b) {
            return strcmp(a, b) < 0;
        }

        bool KeyIsAfterNode(const std::string& key, persistent_ptr<Node> n) const;

        persistent_ptr<Node> FindGreaterOrEqual(const std::string& key) const;

        persistent_ptr<Node> FindLessThan(const std::string& key, persistent_ptr<Node> *prev = nullptr) const;

        //persistent_ptr<Node> FindLast() const;


    };


    persistent_SkipList::persistent_SkipList(const std::string &path, int32_t max_height, int32_t branching_factor)
            :
            kMaxHeight_(static_cast<uint16_t>(max_height)),
            kBranching_(static_cast<uint16_t>(branching_factor)),
            kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),

            max_height_(1) {
        if (file_exists(path) != 0) {
            pool_ = pool<persistent_SkipList>::create(path, "layout", PMEMOBJ_MIN_POOL, CREATE_MODE_RW);
        } else {
            pool_ = pool<persistent_SkipList>::open(path, "layout");
        }

        head_ = NewNode(0, max_height_);

        //Node* v_head = pmemobj_direct(head_.raw());
        for (int i = 0; i < kMaxHeight_; i++) {
            head_->SetNext(i, nullptr);
            prev_[i] = head_;
        }

        prev_height_ = 1;
    }

    persistent_ptr<Node> persistent_SkipList::NewNode(const std::string &key, int height) {
        persistent_ptr<Node> n;
        transaction::run(pool_, [&] {
            n = make_persistent<Node>(key, height);
        });
        return n;
    }

    int persistent_SkipList::RandomHeight() {
        auto rnd = Random::GetTLSInstance();
        int height = 1;
        while (height < kMaxHeight_ && rnd->Next() < kScaledInverseBranching_) {
            height++;
        }
        return height;
    }

    // when n < key returns true
    // n should be at behind of key means key is after node
    bool persistent_SkipList::KeyIsAfterNode(const std::string& key, persistent_ptr<rocksdb::Node> n) const {
        return (n != nullptr) && (n->key_ < key);
    }

    persistent_ptr<Node> persistent_SkipList::FindLessThan(const std::string &key,
                                                           persistent_ptr<rocksdb::Node> prev[]) const {
        persistent_ptr<Node> x = head_;
        int level = GetMaxHeight() - 1;
        persistent_ptr<Node> last_not_after;
        while(true){
            persistent_ptr<Node> next = x->Next(level);
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

    void persistent_SkipList::Insert(const char *key) {
        // key < prev[0]->next(0) && prev[0] is head or key < prev[0]
        if (!KeyIsAfterNode(key, prev_[0]->Next(0)) &&
            (prev_[0] == head_ || KeyIsAfterNode(key, prev_[0]))) {
            for (uint32_t i = 1; i < prev_height_; i++) {
                prev_[i] = prev_[0]
            }
        } else {
            FindLessThan(key, prev_);
        }

        int height = RandomHeight();
        if(height > GetMaxHeight()){
            for(int i = GetMaxHeight(); i < height; i++){
                prev_[i] = head_;
            }
            max_height_ = static_cast<uint16_t >(height);
        }


        persistent_ptr<Node> x = NewNode(key, height);
        for(int i = 0; i < height; i++){
            x->SetNext(i, prev_[i]->Next(i));
            prev_[i]->SetNext(i ,x);
        }
        prev_[0] = x;
        prev_height_ = static_cast<uint16_t >(height);

    }


    persistent_ptr<Node> persistent_SkipList::FindGreaterOrEqual(const std::string &key) const {
        persistent_ptr<Node> x = head_;
        int level = GetMaxHeight() - 1;
        persistent_ptr<Node> last_bigger;
        while(true){
            persistent_ptr<Node> next = x->Next(level);
            int cmp = (next == nullptr || next == last_bigger) ? 1 : next->key_->compacre(key);
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
} // end rocksdb


int main(int argc, char* argv[]){
    std::string path(argv[1]);
    auto skiplist = new rocksdb::persistent_SkipList(path, 12, 4);
    skiplist->Insert("a");
    return 0;
}
