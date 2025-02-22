//
// Created by luoxiYun on 2022/3/7.
//

#include <cstdint>
#include <random>
#include <iostream>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <folly/concurrency/ConcurrentHashMap.h>

#include "synchronization/Barrier.h"

using namespace std;

DEFINE_int32(ops, 1000 * 100 * 10, "Number of operations");
DEFINE_int32(size, 1000 * 100 * 10, "Size of a hashmap");

DEFINE_int32(value_size, 20, "size of a value string");

/*
 * 1. concurrent map find bench
 * 2. unordered map find multi-thread parallel/independent execution
 *  compare the result of above circumstances.
 * 3. map bench : observe performance on different size of (key, value) pair.
 */

void random_string_generator( const int length ){
    static const char alphanum[] = "1234567890"
                                   "abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::random_device rd;
    std::uniform_int_distribution<int> ud( 0, sizeof(alphanum) - 2 );
    std::mt19937 gen(rd());
    unordered_set<string> strset;
    for( int i = 0; i < FLAGS_ops; i++ ) {
        std::string random_str;
        random_str.reserve(length);
        //cout<<"random index : "<<endl;
        for (int i = 0; i < length; i++) {
            //std::cout <<" "<<ud(gen);
            random_str += (alphanum[ud(gen)]);
        }
        //std::cout << random_str << endl;
        strset.insert(random_str);
    }
    cout<<strset.size()<<endl;
}

void basicline_test( ){
    string prefix = "key_partition_";
    static const char alphanum[] = "1234567890"
                                   "abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    random_device rd;
    uniform_int_distribution<int> ud(0, sizeof(alphanum) - 2);
    unordered_map<string, string> basemap;
    mt19937 gen(rd());


    for (int i = 0; i < FLAGS_size; i++) {
        string tmp_key = prefix + to_string(i);
        string tmp_val;
        for (int j = 0; j < FLAGS_value_size; j++) {
            tmp_val += alphanum[ud(gen)];
        }
        basemap.insert({tmp_key, tmp_val});
    }
    auto start = chrono::steady_clock::now();
    for( int i = 0; i < FLAGS_ops; i+=2 ){
        auto it = basemap.find(prefix+ to_string(i));
    }
    for( int i = 1; i < FLAGS_ops; i+=2 ){
        auto it = basemap.find(prefix+ to_string(i));
    }
    auto end = chrono::steady_clock::now();
    auto elapse = chrono::duration_cast<chrono::nanoseconds>(end - start);

    double us_elapse = elapse.count()/ 1000.0;
    double result = FLAGS_ops/us_elapse;

    LOG(INFO)<<"Average cost "<<us_elapse/FLAGS_ops<<"us "<<result<<"Mops";
}

void workload_base_bench( ){
    string prefix = "key_partition_";
    static const char alphanum[] = "1234567890"
                                   "abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    random_device rd;
    uniform_int_distribution<int> ud(0, sizeof(alphanum) - 2);
    unordered_map<string, string> basemap;
    mt19937 gen(rd());

    for (int i = 0; i < FLAGS_size; i++) {
        string tmp_key = prefix + to_string(i);
        string tmp_val;
        for (int j = 0; j < FLAGS_value_size; j++) {
            tmp_val += alphanum[ud(gen)];
        }
        basemap.insert({tmp_key, tmp_val});
    }

    uniform_int_distribution<int> ud2(0,99);
    auto start = chrono::steady_clock::now();
    for (int k = 0; k < FLAGS_ops; k +=1) {
        int ops_type = ud2(rd);
        //LOG(INFO)<<"Thread "<<i<<" number:"<<ops_type;
        if( ops_type <= 89 )
            basemap.find(prefix+ to_string(k));
        else if( ops_type <= 94 )
            basemap.insert_or_assign(prefix+ to_string(k), "updated value");
        else
            basemap.erase(prefix+ to_string(k));
    }

    auto end = chrono::steady_clock::now();
    auto elapse = chrono::duration_cast<chrono::nanoseconds>(end - start);

    double us_elapse = elapse.count()/ 1000.0;
    double result = FLAGS_ops/us_elapse;

    LOG(INFO)<<"Average cost "<<us_elapse/FLAGS_ops<<"us "<<result<<"Mops";


}

//使用unordered map并行访问
void bench_traverse_map( int nthreads ){
    LOG(INFO)<<"Parallel unordered map traverse";
    vector<unordered_map<string, string>> tables(nthreads);
    string prefix = "key_partition_";
    static const char alphanum[] = "1234567890"
                                   "abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    random_device rd;
    uniform_int_distribution<int> ud(0, sizeof(alphanum) - 2);
    mt19937 gen(rd());
    int ops_per_thread = FLAGS_size / nthreads;
    for( int i = 0; i < nthreads; i++ ) {
        for (int k = 0; k < ops_per_thread; k++) {
            string tmp_key = prefix + to_string(i) + "_" + to_string(k);
            string tmp_val;
            for (int j = 0; j < FLAGS_value_size; j++) {
                tmp_val += alphanum[ud(gen)];
            }
            tables[i].insert({tmp_key, tmp_val});
        }
    }

    atomic<bool> start = false;
    atomic<int> ready_threads(0);
    Barrier b(nthreads+1);
    vector<thread> threads(nthreads);
    int ops = FLAGS_ops;
    for( int i = 0; i < nthreads; i++ ){
        threads[i] = thread([&,i,nthreads, ops](){
            string local_prefix = "key_partition_" + to_string(i) + "_";
            int local_ops = ops / nthreads;
            //LOG(INFO) << "Thread " << this_thread::get_id() << endl;
            /*ready_threads++;
            while (!start.load()){
                this_thread::yield();
            }*/
            b.wait();
            b.wait();
            for (int k = 0; k < local_ops; k++) {
                auto it = tables[i].find(local_prefix + to_string(k));
                //LOG(INFO) << "[" << it->first << " " << it->second << "]";
                //LOG(INFO)<<local_prefix+ to_string(k);
            }

        });
    }
    //while ( ready_threads.load() != nthreads ){;}
    b.wait();
    chrono::time_point<chrono::steady_clock> begin = chrono::steady_clock::now();
    //start.store(true);
    b.wait(); //threads execution begin
    for( int i = 0; i < nthreads; i++ )
        threads[i].join();

    chrono::time_point<chrono::steady_clock> end = chrono::steady_clock::now();
    chrono::duration<int,nano> elapse = chrono::duration_cast<chrono::nanoseconds>(end-begin);
    double us_elapse = elapse.count()/ 1000.0;
    double result = FLAGS_ops/us_elapse;

    LOG(INFO)<<nthreads<<" threads traverse "<<FLAGS_size<<" amount of pairs spent total amount of time cost "<<(elapse.count()/1000000)<<"ms";
    LOG(INFO)<<"Average cost "<<us_elapse/FLAGS_ops<<"us"<<" ops "<<result<<"Mops";

}

//使用concurrentmap并发访问
void bench_traverse( int nthreads ){
    LOG(INFO)<<"Concurrent hashmap traverse";
    using threadsafeTable = folly::ConcurrentHashMap<string, string>;
    string prefix = "key_partition_";
    static const char alphanum[] = "1234567890"
                                   "abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    random_device rd;
    uniform_int_distribution<int> ud(0, sizeof(alphanum) - 2);
    mt19937 gen(rd());
    threadsafeTable kv_str;

    for (int i = 0; i < FLAGS_size; i++) {
        string tmp_key = prefix + to_string(i);
        string tmp_val;
        for (int j = 0; j < FLAGS_value_size; j++) {
            tmp_val += alphanum[ud(gen)];
        }
        kv_str.insert(tmp_key, tmp_val);
    }
    Barrier b(nthreads+1);
    vector<thread> threads(nthreads);
    int ops = FLAGS_ops;
    for( int i = 0; i < nthreads; i++ ){
        threads[i] = thread([&,i, nthreads, ops](){
            LOG(INFO)<<"Thread "<<i<<" id:"<<this_thread::get_id()<<endl;
            b.wait();
            b.wait();
            for( int k = i; k < ops; k +=nthreads ){
                auto it = kv_str.find(prefix+ to_string(k));
                //LOG(INFO)<<"[" << it->first << " " << it->second << "]";
            }
        });
    }
    b.wait();
    chrono::time_point<chrono::steady_clock> begin = chrono::steady_clock::now();
    b.wait();
    LOG(INFO)<<"Threads execution start";
    for( int i = 0; i < nthreads; i++ )
        threads[i].join();

    chrono::time_point<chrono::steady_clock> end = chrono::steady_clock::now();
    chrono::duration<int,nano> elapse = chrono::duration_cast<chrono::nanoseconds>(end-begin);
    double us_elapse = elapse.count()/ 1000.0;
    //double ops = FLAGS_ops/us_elapse;

    LOG(INFO)<<nthreads<<" threads traverse "<<kv_str.size()<<" amount of pairs spent total amount of time cost "<<(elapse.count()/1000000)<<"ms";
    //LOG(INFO)<<"Average cost "<<us_elapse/FLAGS_ops<<"us"<<" ops "<<ops<<"Mops";
    LOG(INFO)<<"Average cost "<<us_elapse/FLAGS_ops<<"us";
}

void workload_bench( int nthread ) {
    using threadsafeTable = folly::ConcurrentHashMap<string, string>;
    int total_ops = FLAGS_ops;
    string prefix = "key_partition_";
    static const char alphanum[] = "1234567890"
                                   "abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    int vallength = 1000;
    random_device rd;
    uniform_int_distribution<int> ud(0, sizeof(alphanum) - 2);
    mt19937 gen(rd());
    threadsafeTable kv_str;
    for (int i = 0; i < FLAGS_size; i++) {
        string tmp_key = prefix + to_string(i);
        string tmp_val;
        for (int j = 0; j < vallength; j++) {
            tmp_val += alphanum[ud(gen)];
        }
        kv_str.insert(tmp_key, tmp_val);
    }

    vector<thread> threads(nthread);
    Barrier b(nthread+1);
    for (int i = 0; i < nthread; i++) {
        threads[i] = thread([&, i, total_ops, prefix]() {
            random_device rd;
            uniform_int_distribution<int> ud(0,99);
            b.wait();
            b.wait();
            for (int k = i; k < total_ops; k+=nthread) {
                int ops_type = ud(rd);
                //LOG(INFO)<<"Thread "<<i<<" number:"<<ops_type;
                if( ops_type <= 89 )
                    kv_str.find(prefix+ to_string(k));
                else if( ops_type <= 94 )
                    kv_str.assign(prefix+ to_string(k), "updated value");
                else
                    kv_str.erase(prefix+ to_string(k));
            }
        });
    }
    b.wait();
    chrono::time_point<chrono::steady_clock> begin = chrono::steady_clock::now();
    b.wait();
    for (int i = 0; i < nthread; i++)
        threads[i].join();
    LOG(INFO)<<"After operations size of map is "<<kv_str.size();
    chrono::time_point<chrono::steady_clock> end = chrono::steady_clock::now();
    chrono::duration<int, nano> elapse = chrono::duration_cast<chrono::nanoseconds>(end - begin);

    LOG(INFO)<<nthread<<" threads operates "<<"spending total amount of time cost "<<(elapse.count()/1000000)<<"ms";
    //LOG(INFO)<<"Average cost "<<us_elapse/FLAGS_ops<<"us"<<" ops "<<ops<<"Mops";
    double us_elapse = elapse.count()/1000.0;
    LOG(INFO)<<"Throughput "<<FLAGS_ops/us_elapse<<"Mops";

}

int main( int argc, char *argv[] ){
    //random_string_generator(10); //slow!
    FLAGS_minloglevel = 0;
    FLAGS_log_dir = "/Users/luoxi/Documents/ClionProjects/tesench/logs/"; //specify logging position
    google::InitGoogleLogging(argv[0]);
    //basicline_test();
    //traverse_threads(10);
    //bench_traverse(4); // concurrent map
    //bench_traverse_map(4); //parallel unordered map
    workload_base_bench();
    //workload_bench(8);
    return 0;
}

