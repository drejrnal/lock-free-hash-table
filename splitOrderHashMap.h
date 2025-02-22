//
// Created by luoxiYun on 2022/3/12.
//

#include <atomic>
#include <cmath>
#include <type_traits>
#include <folly/synchronization/Hazptr.h>

/*
 * 1. segment space allocation : need to consider concurrent construction circumstance
 *    segment空间采用渐进式的分配方式，测试示例需检测并发多线程并发分配空间时 程序仍能正确运行
 * 2. organize hashmap using split ordering algorithm
 *    implement insert/delete/find interface
 * 3. using folly::hazard pointer tool to deal with GC
 */
#define R2(n)     n,     n + 2*64,     n + 1*64,     n + 3*64
#define R4(n) R2(n), R2(n + 2*16), R2(n + 1*16), R2(n + 3*16)
#define R6(n) R4(n), R4(n + 2*4 ), R4(n + 1*4 ), R4(n + 3*4 )

using folly::hazptr_local;
using folly::hazptr_holder;
using folly::hazptr_obj_base;

// Lookup table that store the reverse of each table
constexpr unsigned long lookuptable[256] = { R6(0), R6(2), R6(1), R6(3) };

constexpr int segmentDefaultSize = 32;
constexpr int segmentDefaultMaxLevel = 4;

class LockFreeHashMap{

    /*struct Node;
    struct DummyNode;
    struct DataNode;*/
    struct Segment;

    using HashKey = size_t;
    using RevHashKey = size_t;
    //using Buckets = std::atomic<Node*>;


public:

    struct Node;
    struct DummyNode;
    struct DataNode;
    using Buckets = std::atomic<Node*>;

    LockFreeHashMap(): pow_of_2_(0), size_(0), load_factor(0.8){
        int level = 1;
        Segment *segments = segments_;
        while ( level++ <= segmentDefaultMaxLevel - 2 ){
            Segment* subSegment = NewSegment(level);
            segments[0].container.store(subSegment, std::memory_order_release);
            segments = subSegment;
        }
        Buckets *buckets = NewBuckets();
        segments[0].container.store(buckets, std::memory_order_release);

        auto *dummy = new DummyNode(0);
        buckets[0].store(dummy, std::memory_order_release);
    }

    size_t bucketSize(){
        return 1 << pow_of_2_.load(std::memory_order_relaxed);
    }

    size_t size(){
        return size_.load(std::memory_order_acquire);
    }

#ifdef DEBUG
    void traverseTopLevel(){
        for( int i = 0; i < segmentDefaultSize; i++ ){
            if( segments_[i].container.load() != nullptr )
                std::cout<<i<<" reference next level "<<std::endl;
        }
    }
#endif
    std::atomic<Node *>& getBucketHeadByHash( HashKey hash, Buckets*& dummyPtr,
                                              hazptr_holder<std::atomic> *hptr0,
                                              hazptr_holder<std::atomic> *hptr1){
        size_t index = hash & (bucketSize() - 1);
        Node* head = getBucketHeadByIndex( index, dummyPtr );
        if( head == nullptr )
            initializeBuckets(index,dummyPtr, hptr0, hptr1);
        return *dummyPtr;
    }

    bool insert( int key, int value ){
        Buckets *dummyPtr;
        hazptr_local<2, std::atomic> hazptrs;
        auto *dataNode = new DataNode(std::hash<int>()(key),key, value);
        auto& dummyHead = getBucketHeadByHash(std::hash<int>()(key), dummyPtr,
                                              &hazptrs[0], &hazptrs[1]);
        return insertDataNode( dummyHead, dataNode, &hazptrs[0], &hazptrs[1] );
    }

    //添加新的Find接口，内部调用searchDataNode, 避免find(...)内部构造hazptr_local
    //根据key找到对应的value,返回是否找到，若找到，值存到value中
    bool find( int key, int &value ){
        Node *prev = nullptr, *cur = nullptr;
        /* referrer(HazptrHolder.h hazptr_local)
         * There can only be one hazptr_local active for the same
         * thread at any time. This is not tracked and checked by the
         * implementation (except in debug mode) because it would negate the
         * performance gains of this class.
         */
        //根据hazptr_local限制，此处hazptrs用于dummyHead的获取及
        //作为参数传入searchDataNode function
        hazptr_local<2, std::atomic> hazptrs;
        Buckets *dummyPtr;
        //bug:此处内部又定义了hazptr_local，不满足hazptr_local的限制条件
        auto& dummyHead = getBucketHeadByHash(std::hash<int>()(key), dummyPtr, &hazptrs[0], &hazptrs[1]);
        DataNode target(std::hash<int>()(key), key);
        bool found = searchDataNode(dummyHead, &target, &prev, &cur, &hazptrs[0], &hazptrs[1]);
        if( found )
            value = ((DataNode *)cur)->value_.load();
        return found;
    }

    bool erase( int key ){
        hazptr_local<2, std::atomic> hazptrs;
        Buckets *dummyPtr;
        auto& dummyHead = getBucketHeadByHash(std::hash<int>()(key), dummyPtr, &hazptrs[0], &hazptrs[1]);
        DataNode target(std::hash<int>()(key), key);
        return deleteDataNode( dummyHead, &target, &hazptrs[0], &hazptrs[1] );
    }


public:
    struct Node: hazptr_obj_base<Node, std::atomic>{
        Node( HashKey hashKey, bool dummy ):hashKey_(hashKey){
            //calculate reverse HashKey in derived class
            isDummy = dummy;
            reverseHashKey_ = isDummy ? dummyReverse(hashKey) : dataReverse(hashKey);
            next_.store(nullptr, std::memory_order_relaxed);
        }

        /*bool operator==(const Node &other) const {
            return reverseHashKey_ == other.reverseHashKey_;
        }*/
        //for Node comparison in split-order list
        /*auto operator<=>(Node &other)  {
            if( this->reverseHashKey_ != other.reverseHashKey_ )
            //if( (*this) != other )
                return reverseHashKey_ <=> other.reverseHashKey_;
            else {
                auto *right = static_cast<DataNode *>(&other);
                return static_cast<DataNode *>(this)->key_ <=> right->key_;
            }
        }*/

        static RevHashKey reverse( HashKey hashKey ){
            return lookuptable[hashKey >> 56] | (lookuptable[(hashKey>>48) & 0xff] << 8)|
                    (lookuptable[(hashKey>>40) & 0xff]<<16) | (lookuptable[(hashKey>>32)&0xff]<<24)|
                    (lookuptable[(hashKey>>24) & 0xff]<<32) | (lookuptable[(hashKey>>16)&0xff]<<40) |
                    (lookuptable[(hashKey>>8) & 0xff]<<48) | (lookuptable[hashKey & 0xff] << 56) ;
        }

        static RevHashKey dummyReverse( HashKey hashKey ){
            return reverse( hashKey );
        }

        static RevHashKey dataReverse( HashKey hashKey ){
            return reverse( hashKey | (0x8000000000000000) );
        }

        Node *get_next(){
            return next_.load(std::memory_order_acquire);
        }

        std::atomic<Node *> *ptr_next(){
            return &next_;
        }

        bool isDummy;
        HashKey hashKey_;
        RevHashKey reverseHashKey_;
        std::atomic<Node *> next_;
    };
    //DummyNode内的哈希值reverse后末位为0
    struct DummyNode: Node{
        explicit DummyNode( HashKey hashKey ): Node(hashKey, true){}
    };
    //DataNode内的hash值为hash(key)计算后的结果
    //DataNode内的哈希值reverse后末位置为1
    struct DataNode: Node{
        DataNode( HashKey hashKey, int key, int value ): Node( hashKey, false ){
            key_ = key;
            value_.store( value, std::memory_order_relaxed);
        }
        DataNode( HashKey hashKey, int key ): Node( hashKey, false ){
            key_ = key;
            value_.store(0xdead, std::memory_order_relaxed);
        }
        //todo::key and value type should be template parameter
        int key_;
        std::atomic<int> value_;
    };

    bool nodeLess(Node *lhs, Node *rhs){
        if( lhs->reverseHashKey_ != rhs->reverseHashKey_ ){
            return lhs->reverseHashKey_ < rhs->reverseHashKey_;
        }
        if( lhs->isDummy || rhs->isDummy )
            return false;

        return static_cast<DataNode *>(lhs)->key_ <
                    static_cast<DataNode *>(rhs)->key_;
    }

    bool nodeEqual(Node *lhs, Node *rhs){
        return (!nodeLess(lhs, rhs)) & (!nodeLess(rhs, lhs));
    }

private:
    struct Segment{
        Segment():level_(0), container(nullptr){;}
        explicit Segment( int level ):level_(level),container(nullptr){;}
        Buckets *get_buckets(){
            return static_cast<Buckets *>(container.load(std::memory_order_acquire));
        }
        Segment *get_sub_segment(){
            return static_cast<Segment *>(container.load(std::memory_order_acquire));
        }
        ~Segment(){
            if( container == nullptr )
                return;
            if( level_ < segmentDefaultMaxLevel - 1 ){
                Segment *segments = get_sub_segment();
                delete[] segments;
            }else{
                Buckets *buckets = get_buckets();
                delete[] buckets;
            }
        }
        int level_;
        std::atomic<void *> container;
    };

    Segment *NewSegment( int level ){
        Segment *segments = new Segment[segmentDefaultSize];
        for( int i = 0; i < segmentDefaultSize; i++ ){
            segments[i].level_ = level;
            segments[i].container = nullptr;
        }
        return segments;
    }
    //return atomic<DummyNode*>[segmentDefaultSize]
    Buckets *NewBuckets(){
        auto *buckets = new Buckets[segmentDefaultSize];
        for( int i = 0; i < segmentDefaultSize; i++ )
            buckets[i].store(nullptr, std::memory_order_release);
        return buckets;
    }

    bool insertDummyNode( std::atomic<Node *> &parentDummy, DummyNode *newDummy, DummyNode **realDummy,
                          hazptr_holder<std::atomic> *hptr0,
                          hazptr_holder<std::atomic> *hptr1);
    bool insertDataNode( std::atomic<Node *> &dummy, DataNode *new_node,
                         hazptr_holder<std::atomic> *hptr0,
                         hazptr_holder<std::atomic> *hptr1);
    bool searchDataNode( std::atomic<Node *> &head, Node *target, Node **prev, Node **current,
                         hazptr_holder<std::atomic> *,
                         hazptr_holder<std::atomic> *);
    bool deleteDataNode( std::atomic<Node *> &head, DataNode *target,
                         hazptr_holder<std::atomic> *,
                         hazptr_holder<std::atomic> *);

    /*
     * constraint:
     *   1. 返回的DummyNode *不能为nullptr
     *   2. thread safety: 确定linearization point,即每个segment项内的container是否成功设置
     *   分配DummyNode之后，需要将其插入到parent buckets中的node list中，所以需要知晓
     *   parent bucket的位置
     */
    Node* initializeBuckets( size_t hashIndex, Buckets*& bucket,
                             hazptr_holder<std::atomic> *hptr0,
                             hazptr_holder<std::atomic> *hptr1) {
        size_t parentIndex = getParentBucketIndex(hashIndex);
        Node* parenthead = getBucketHeadByIndex(parentIndex, bucket);
        if( parenthead == nullptr )
            initializeBuckets(parentIndex, bucket, hptr0, hptr1);
        //此时bucket应该为parent atomic<Node *>
        Buckets *parentBucket = bucket;
        Segment *seg = segments_;
        int level = 1;
        int range = pow( segmentDefaultSize, segmentDefaultMaxLevel - level );
        while( level++ <= segmentDefaultMaxLevel - 2 ){
            Segment& segentry = seg[(hashIndex / range) % segmentDefaultSize];
            Segment *subseg = segentry.get_sub_segment();
            if( subseg == nullptr ){
                //allocate segment and do cas on segentry
                Segment *newseg = NewSegment(level);
                void *expected = nullptr;
                if( !segentry.container.compare_exchange_strong( expected, newseg,
                                                                std::memory_order_release)){
                    //another thread contend successfully
                    delete[] newseg;
                    subseg = static_cast<Segment *>(expected);
                }else{
                    //this thread acquire successfully
                    subseg = newseg;
                }
            }
            seg = static_cast<Segment *>(segentry.container.load(std::memory_order_acquire));
            range /= segmentDefaultSize;
        }
        CHECK_EQ(range, segmentDefaultSize)<<"Top-down traverse error:incorrect range value";
        Segment& segentry = seg[(hashIndex / segmentDefaultSize) % segmentDefaultSize];
        Buckets *buckets = segentry.get_buckets();
        if( buckets == nullptr ){
            Buckets *newbuck = NewBuckets();
            void *expected = nullptr;
            if( !segentry.container.compare_exchange_strong(expected, newbuck,
                                                  std::memory_order_release)){
                delete[] newbuck;
                buckets = static_cast<Buckets *>(expected);
            }else{
                buckets = newbuck;
            }
        }

        bucket = &buckets[hashIndex % segmentDefaultSize];
        Node *dummy = bucket->load(std::memory_order_acquire);
        if(dummy == nullptr){
            DummyNode *newdummy = new DummyNode(hashIndex);
            /*if(!bucket->compare_exchange_strong(dummy, newdummy,
                                              std::memory_order_release)){
                delete newdummy;
            }else{
                dummy = newdummy;
            }*/
            //若newdummy成功插入，则完成，并返回newdummy
            DummyNode *retDummy;
            if(insertDummyNode(*parentBucket, newdummy, &retDummy, hptr0, hptr1) ){
                bucket->store(newdummy, std::memory_order_release);
                dummy = newdummy;
            }else {
                delete newdummy;
                dummy = retDummy;
            }
            //否则，delete newdummy, 返回其他线程插入的dummynode
        }
        return static_cast<Node *>(dummy);
    }

    /*
     * 如果降级(decreasing level)情况下某一级存在nullptr 则返回nullptr
     * if( @return == nullptr )
     *     return InitializeBucket()
     * 返回DummyNode所在地址后，在此node后插入dataNode
     */
    Node* getBucketHeadByIndex( size_t hashIndex, Buckets*& dummyPtr ){
        //根据hashIndex向下查找具体的segment
        int level = 1;
        int range = pow(segmentDefaultSize, segmentDefaultMaxLevel - level);
        Segment *segment = segments_;
        while( level <= segmentDefaultMaxLevel - 2 ){
            Segment *subsegment = segment[(hashIndex / range) % segmentDefaultSize].get_sub_segment();
            if( subsegment == nullptr )
                return nullptr;
            segment = subsegment;
            level++;
            range /= segmentDefaultSize;
        }

        Buckets *buckets = segment[(hashIndex / range) % segmentDefaultSize].get_buckets();
        if( buckets == nullptr )
            return nullptr;
        dummyPtr = &buckets[hashIndex % segmentDefaultSize];
        //此处虽然bucket已有，确需重新层次遍历，获取parent node
        //parent node的目的是正确插入dummyNode，
        //所以需要回到InitializeBukets的逻辑中
        return static_cast<Node *>(dummyPtr->load(std::memory_order_acquire));
        //return bucket;
    }

    /*
     * helper function
     */
    size_t getParentBucketIndex( size_t hashIndex ){
        //__builtin_clzl(p) return number of leading zero bits of p
        size_t leading_0_cnt = __builtin_clzl(hashIndex);
        //reset hashIndex's most significant bit
        return (~(0x8000000000000000 >> leading_0_cnt) & hashIndex);
    }

    Node *getUnMarkNode( Node *node ){
        return reinterpret_cast<Node *>
                        (reinterpret_cast<unsigned long>(node) & (~0x1));
    }

    bool isMarkReference( Node *node ){
        return (reinterpret_cast<unsigned long>(node) & (0x1)) == 0x01;
    }

    Node *getMarkReference( Node *node ){
        return reinterpret_cast<Node *>
                        ( reinterpret_cast<unsigned long>(node) | (0x1) );
    }

    Segment segments_[segmentDefaultSize];
    //bucket size is 2^(pow_of_2_)
    std::atomic<int> pow_of_2_;
    //number of items
    std::atomic<int> size_;
    double load_factor;
};


//static_assert(std::is_standard_layout_v<LockFreeHashMap>);

bool LockFreeHashMap::searchDataNode(std::atomic<Node *> &head, Node *target,
                                     Node **previous, Node **current,
                                     hazptr_holder<std::atomic> *hptr0,
                                     hazptr_holder<std::atomic> *hptr1 ) {
try_again:
    //atomic variable not copy constructive
    const std::atomic<Node *> *traverse = &head;
    Node *prev = traverse->load(std::memory_order_acquire);
    Node *curr = prev;
    while ( true ){
        if(!hptr0->try_protect(curr, *traverse))
            goto try_again;
        if( curr == nullptr ){
            *previous = prev;
            *current = curr;
            return false;
        }

        Node *next = curr->get_next();
        //并发删除的情形，待删除节点的next字段被修改
        if( isMarkReference(next) ){
            Node *real_next = getUnMarkNode(next);
            //helping uncompleted deletion
            //if cas not success, then curr would equal real_next
            if(!prev->next_.compare_exchange_strong( curr,
                                        real_next, std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
                //因为此时可能有并发插入操作，curr前可能有新插入的节点
                //所以需从头遍历，获取curr节点前新插入的节点
                goto try_again;
            }
            else {
                this->size_--;
                //回收节点
                hptr0->reset_protection();
                curr->retire();
                //若此时有并发插入操作，则此时循环初的try_protect失败，从头遍历
                //此处traverse赋值时需考虑next有效性（即是否prev被删除）
                //若prev被删除，prev应被hptr1保护，其指向的内存不会被释放。
                //且ptr_next返回的是一个地址值，其不等于real_next
                //这种情形在下一轮循环的try_protect()可判断出来
                traverse = prev->ptr_next();
                curr = real_next;
            }
        }else{
            /*if( curr >= target ){
                *previous = prev;
                *current = curr;
                auto *cmp = static_cast<DataNode *>(curr);
                return cmp->key_ == static_cast<DataNode *>(target)->key_;
            }*/
            //split-order comparison
            if( !nodeLess(curr, target) ){
                *previous = prev;
                *current = curr;
                return nodeEqual(curr, target);
                //auto *cmp = static_cast<DataNode *>(curr);
                //return cmp->key_ == static_cast<DataNode *>(target)->key_;
            }
            //此处traverse赋值时需考虑next有效性（即是否curr被删除）
            //如果curr被删除，curr->ptr_next()返回的地址值不会等于next
            //这在下一轮循环的try_protect()中可以判断
            traverse = curr->ptr_next();
            prev = curr;
            curr = next;
            std::swap( hptr0, hptr1 );
        }

    }
}

bool LockFreeHashMap::insertDummyNode(std::atomic<Node *> &parentDummy,
                                      DummyNode *newDummy,
                                      DummyNode **realDummy,
                                      hazptr_holder<std::atomic> *hptr0,
                                      hazptr_holder<std::atomic> *hptr1) {
    Node *prev = nullptr, *curr = nullptr;

    do {
        if(searchDataNode(parentDummy, newDummy, &prev, &curr, hptr0, hptr1)){
            *realDummy = static_cast<DummyNode *>(curr);
            return false;
        }
        //此时，如果有并发的删除操作，需保证curr指向的空间未被释放
        newDummy->next_.store(curr, std::memory_order_relaxed);
    }while(!prev->next_.compare_exchange_weak(curr,
                                                newDummy, std::memory_order_acq_rel) );

    return true;
}


bool LockFreeHashMap::insertDataNode(std::atomic<Node *> &dummy, DataNode *new_node,
                                     hazptr_holder<std::atomic> *hptr0,
                                     hazptr_holder<std::atomic> *hptr1) {
    Node *prev = nullptr, *curr = nullptr;
    do {
        if (searchDataNode(dummy, new_node, &prev, &curr, hptr0, hptr1)) {
            //待插入的节点已存在，插入操作变更新操作
            auto *cur_node = static_cast<DataNode *>(curr);
            //TODO 这里需要思考原来的值占据的空间如何回收？
            cur_node->value_.exchange(new_node->value_.load(), std::memory_order_release);
            delete new_node;
            return false;
        }
        //此时，如果有并发的删除操作，需保证curr指向的空间未被释放
        new_node->next_.store(curr, std::memory_order_relaxed);
    }while(!prev->next_.compare_exchange_weak(curr,
                                                new_node, std::memory_order_acq_rel) );

    this->size_++;
    int cur_size = this->size_.load();
    int pow = pow_of_2_.load();
    if( cur_size >= ( 1 << pow ) * load_factor ){
        //rehash
        pow_of_2_.compare_exchange_strong( pow, pow+1, std::memory_order_release);
    }
    return true;
}

bool LockFreeHashMap::deleteDataNode(std::atomic<Node *> &head, DataNode *target,
                                     hazptr_holder<std::atomic> *hptr0,
                                     hazptr_holder<std::atomic> *hptr1) {
    Node *prev = nullptr, *cur = nullptr;
    Node *cur_next = nullptr;
    //hazptr_local<2, std::atomic> hazptrs;
    do {
        do {
            if (!searchDataNode(head, target, &prev, &cur, hptr0, hptr1)) {
                return false;
            }
            //处理并发删除与删除的情景
            cur_next = cur->get_next();
        } while (isMarkReference( cur_next ));
        //处理并发删除与删除的情景
    } while( !cur->next_.compare_exchange_weak( cur_next, getMarkReference(cur_next),
                                                std::memory_order_acq_rel,
                                                std::memory_order_relaxed));
    //处理并发searchNode与删除的情景
    if( prev->next_.compare_exchange_strong(cur, getUnMarkNode(cur_next),
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed)){
        this->size_--;
        //回收节点
        hptr0->reset_protection();
        cur->retire();
    }
    return true;
}