#include <iostream>
#include <typeinfo>
#include <random>
#include <thread>
#include <folly/concurrency/ConcurrentHashMap.h>
#include "splitOrderHashMap.h"
#include "synchronization/Barrier.h"

using namespace std;

class Foo {
public:
    typedef uint32_t integer_type;
};

template<class T>
class Bar {
public:
    typedef T value_type;
};

template<class T>
void bar(){
    Foo::integer_type i;
    typename Bar<T>::value_type t;
    std::cout << typeid(t).name() <<std::endl;
}

//单线程创建测试
//为方便测试，初始化bucket size为2^20，及pow_of_2设置为20
void test_segment_initialize(){
    LockFreeHashMap threadsafe_table;
    using Node = LockFreeHashMap::DummyNode;

    using Buckets = LockFreeHashMap::Buckets;
    constexpr size_t ops = 2000000;
    random_device rd;

    uniform_int_distribution<int> ud(0, pow(segmentDefaultSize, segmentDefaultMaxLevel)-1);
    mt19937 gen(rd());
    for( int i = 0; i < ops; i++ ){
        Buckets *bucket;
        //Buckets *n = &threadsafe_table.getBucketHeadByHash(ud(rd), bucket);
        //CHECK_EQ(bucket, n);
        //CHECK_NOTNULL(n);
    }

}

//单线程插入测试
//测试插入节点是否按顺序(split-ordering)排列
//test-case:dummy(0)-->8(00010001)-->dummy(2)-->10(01010001)-->dummy(1)-->
//             9(10010001)-->13(10110001)-->dummy(3)-->7(11100001)
//添加删除功能，测试erase函数
void test_splitOrdering_insert(){
    LockFreeHashMap threadSafe_table;
    vector<int> test_case = {8, 10, 9, 13, 7};
    for( int e : test_case )
        threadSafe_table.insert(e, e);
    //todo need debugging
    cout<<"Before erasing:"<<endl;
    bool found;
    for( int e : test_case ) {
        int val;
        threadSafe_table.find( e, val );
        cout<<"Value of Key "<<e<<" is "<<val<<endl;
        CHECK_EQ( e, val);
    }
    bool erased = false;
    erased = threadSafe_table.erase(9);
    CHECK_EQ( true, erased );
    erased = threadSafe_table.erase(7);
    CHECK_EQ( true, erased );
    erased = threadSafe_table.erase( 24 );
    CHECK_EQ( false, erased );
    cout<<"After erasing:"<<endl;
    for( int e : test_case ) {
        int val;
        found = threadSafe_table.find( e, val );
        if( found )
            cout<<"Value of Key "<<e<<" is "<<val<<endl;
    }
}

//多线程正确性测试
//一个线程执行查找操作，另一线程执行删除操作
void test_splitOrdering_multithread(){
    LockFreeHashMap threadSafe_table;
    vector<int> test_case = {8, 10, 9, 13, 7};
    for( int e : test_case )
        threadSafe_table.insert(e, e);

    LOG(INFO)<<"Before deletion bucket size:"<<threadSafe_table.bucketSize();

    LOG(INFO)<<"Multithread test begin:";

    thread findThread([&threadSafe_table, &test_case]{
        bool found;
        int val;
        /*found = threadSafe_table.find(test_case[1], val);
        if( found )
            LOG(INFO)<<"Value is "<<val;

        found = threadSafe_table.find(test_case[3], val);
        if( found )
            LOG(INFO)<<"Value is "<<val;*/
        for( int e : test_case ) {
            int val;
            found = threadSafe_table.find( e, val );
            if( found )
                CHECK_EQ(e, val);
        }
    });

    thread deleteThread([&threadSafe_table, &test_case]{
        bool erased;
        erased = threadSafe_table.erase( test_case[1] );
        LOG(INFO)<<"Result of erasement is "<<erased;
        erased = threadSafe_table.erase(test_case[3]);
        LOG(INFO)<<"Result of erasement is "<<erased;
        erased = threadSafe_table.erase(test_case[0]);
        LOG(INFO)<<"Result of erasement is "<<erased;
    });

    findThread.join();
    deleteThread.join();


    LOG(INFO)<<"Multithread test end";

    bool exist;
    int val;
    exist = threadSafe_table.find(test_case[0], val);
    CHECK_EQ(false, exist);
    exist = threadSafe_table.find( test_case[4], val);
    CHECK_EQ(true, exist);
    CHECK_EQ(2, threadSafe_table.size());
    LOG(INFO)<<"After deletion bucket size:"<<threadSafe_table.bucketSize();
}

//独立测试并发插入、查找、删除
//比较不同线程数下，并发操作的性能差异
void test_concurrent_opt(int nthreads){
    LockFreeHashMap thread_safe_map;

    vector<thread> threads;

    threads.reserve(nthreads);
    constexpr size_t opt = 2000000;
    //数据并发插入
    for( int i = 0; i < nthreads; i++ ) {
        threads.emplace_back([ &, nthreads, opt, i ]() {
            for (int j = i; j < opt; j += nthreads)
                thread_safe_map.insert(j, j);
        });
    }
    for( int i = 0; i < nthreads; i++ )
        threads[i].join();
    //数据查找
    vector<vector<int>> partitions;
    partitions.reserve(nthreads);
    random_device rd;
    uniform_int_distribution<int> ud(0, opt-1);
    for( int i = 0; i < nthreads; i++ ){
        vector<int> tmp;
        for( int j = i; j < opt; j+=nthreads ){
            tmp.push_back( ud(rd) );
        }
        partitions.emplace_back(tmp);
        //LOG(INFO)<<"Partition "<<i<<" size:"<<partitions[i].size();
    }
    for( int i = 0; i < nthreads; i++ ){
        threads[i] = thread([ &, opt, nthreads, i](){
            int val;
            for( int ele : partitions[i] ){
                //LOG(INFO)<<"Thread "<<i<<" find ele "<<ele;
                thread_safe_map.find( ele, val );
                CHECK_EQ( ele, val );
            }
        });
    }
    for( int i = 0; i < nthreads; i++ )
        threads[i].join();
    //数据删除
    for( int i = 0; i < nthreads; i++ ){
        threads[i] = thread([&, i](){
            for( int j = i; j < partitions[0].size(); j+=nthreads )
                thread_safe_map.erase( partitions[0][j] );
        });
    }
    for( int i = 0; i < nthreads; i++ )
        threads[i].join();
    int res_del;
    for( int i = 0; i < partitions[0].size(); i++ ) {
        CHECK_EQ(false, thread_safe_map.find(partitions[0][i],res_del));
    }
}

//性能测试
//测试混合负载下，并发操作
void test_mix_opt( int nthreads ){
    LockFreeHashMap thread_safe_map;
    constexpr size_t opt = 2000000;
    vector<thread> threads;
    threads.reserve( nthreads );
    //与性能测试计时相关的变量
    Barrier barrier( nthreads + 1 );


    for( int i = 0; i < nthreads; i++ ){
        threads.emplace_back([&, nthreads, i](){
            random_device rd;
            uniform_int_distribution<int> ud(0,99);
            int find_res;
            barrier.wait();
            barrier.wait();
            for( int t = i; t < opt; t+=nthreads ){
                if( ud(rd) < 80 ){
                    thread_safe_map.find(t, find_res);
                }else if( ud(rd) < 90 ){
                    thread_safe_map.insert( t, 0xdeadbeaf );
                }else
                    thread_safe_map.erase( t );
            }
        });
    }

    barrier.wait();
    chrono::time_point<chrono::steady_clock> begin = chrono::steady_clock::now();
    barrier.wait();

    for( int i = 0; i < nthreads; i++ )
        threads[i].join();

    chrono::time_point<chrono::steady_clock> end = chrono::steady_clock::now();
    chrono::duration<int,nano> elapse = chrono::duration_cast<chrono::nanoseconds>(end-begin);
    double us_elapse = elapse.count()/ 1000.0;

    double result = opt/us_elapse;

    LOG(INFO)<<nthreads<<" threads "<< "spent total amount of time cost "<<(elapse.count()/1000000)<<"ms";
    LOG(INFO)<<"Total throughtput: "<<result<<" Mops";

}

void test_concurrent_opt_bench( int nthreads ){
    vector<thread> threads;
    threads.reserve( nthreads );
    size_t opt = 1000000;
    int cases = nthreads/3;
    LockFreeHashMap thread_safe_table;
    random_device rd;
    uniform_int_distribution<int> ud(0,opt - 1);
    atomic<int> count(0);

    Barrier b( cases * 3 + 1 );

    for( int i = 0; i < cases; i++ ){
        //插入线程
        threads.emplace_back([&](){
            b.wait();
            b.wait();
            for( int k = i; k < opt; k +=cases ) {
                int rnd = ud(rd);
                thread_safe_table.insert(rnd, rnd);
                    //count++;
            }
        });
        //删除线程
        threads.emplace_back([&](){
            b.wait();
            b.wait();
            for( int k = i; k < opt; k +=cases ) {
                int rnd = ud(rd);
                thread_safe_table.erase(rnd);
                    //count--;
            }
        });

        //查找线程
        threads.emplace_back([&](){
            b.wait();
            b.wait();
            int val, rnd = ud(rd);
            for( int k = i; k < opt; k +=cases ) {
                thread_safe_table.find(rnd, val);
            }
        });
    }

    b.wait();
    chrono::time_point<chrono::steady_clock> begin = chrono::steady_clock::now();
    b.wait();

    for( int i = 0; i < threads.size(); i++ )
        threads[i].join();

    chrono::time_point<chrono::steady_clock> end = chrono::steady_clock::now();
    chrono::duration<int,nano> elapse = chrono::duration_cast<chrono::nanoseconds>(end-begin);
    double us_elapse = elapse.count()/ 1000.0;
    double result = opt / us_elapse;

    //int size = thread_safe_table.size();
    //CHECK_EQ( count.load(), size );

    LOG(INFO)<<nthreads<<" threads "<< "spent total amount of time cost "<<(elapse.count()/1000000)<<"ms";
    LOG(INFO)<<"Total throughtput: "<<result<<" Mops";
}

void folly_map_test( int nthread ) {
    using threadsafeTable = folly::ConcurrentHashMap<int, int>;
    int total_ops = 1000000;

    threadsafeTable kv_table;

    random_device rd;
    uniform_int_distribution<int> ud(0,total_ops - 1);

    vector<thread> threads(nthread);
    Barrier b(nthread+1);
    for (int i = 0; i < nthread; i++) {
        threads[i] = thread([&, i, total_ops]() {
            b.wait();
            b.wait();
            for (int k = i; k < total_ops; k +=nthread) {
                int tmp = ud(rd);
                kv_table.insert( tmp, tmp );
            }
        });
    }
    b.wait();
    chrono::time_point<chrono::steady_clock> begin = chrono::steady_clock::now();
    b.wait();
    for (int i = 0; i < nthread; i++)
        threads[i].join();

    chrono::time_point<chrono::steady_clock> end = chrono::steady_clock::now();
    chrono::duration<int, nano> elapse = chrono::duration_cast<chrono::nanoseconds>(end - begin);

    LOG(INFO)<<nthread<<" threads operates "<<"spending total amount of time cost "<<(elapse.count()/1000000)<<"ms";
    //LOG(INFO)<<"Average cost "<<us_elapse/FLAGS_ops<<"us"<<" ops "<<ops<<"Mops";
    double us_elapse = elapse.count()/1000.0;
    LOG(INFO)<<"Throughput "<<total_ops/us_elapse<<"Mops";

}


int main( int argc, char *argv[0] ) {
    /*Foo foo;
    Foo::integer_type fit;
    bar<int>();*/
    FLAGS_logtostderr = 1;
    google::InitGoogleLogging(argv[0]);
    //test_segment_initialize();
    //test_splitOrdering_insert();
    //test_splitOrdering_multithread();
    //test_concurrent_opt(8);
    //test_mix_opt(8);
    //folly_map_test(8);
    cout<<"Lock free bench test"<<endl;
    test_concurrent_opt_bench(8);
    return 0;
}