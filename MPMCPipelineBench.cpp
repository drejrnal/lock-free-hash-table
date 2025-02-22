//
// Created by luoxiYun on 2022/3/15.
//

#include <iostream>
#include <thread>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <random>
#include <folly/MPMCQueue.h>
#include <folly/MPMCPipeline.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <experimental/coroutine>

#include "synchronization/Barrier.h"

using namespace std;

DEFINE_int32(ops, 1000000, "Total number of operations");
DEFINE_int32(size,1029, "length of data");

//copy large payload of data
void copy_cost( int iter ){
    static const char alphanum[] = "1234567890"
                                   "abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    random_device rd;
    uniform_int_distribution<int> ud(0, sizeof(alphanum) - 2);
    mt19937 gen(rd());
    vector<string> data(iter,"");
    for( int i = 0; i < iter; i++ ) {
        for (int j = 0; j < FLAGS_size; j++) {
             data[i]+= alphanum[ud(gen)];
        }
    }
    char contain[FLAGS_size+1];
    auto start = chrono::steady_clock::now();
    for( int i = 0; i < iter; i++ ){
        memcpy(contain, data[i].c_str(), FLAGS_size+1);
        //contain[FLAGS_size] = '\0';
        CHECK_EQ(contain[FLAGS_size], '\0');
        CHECK_EQ(contain[FLAGS_size-1], data[i][FLAGS_size-1]);
    }
    auto end = chrono::steady_clock::now();
    auto elapse = chrono::duration_cast<chrono::nanoseconds>(end - start);

    double us_elapse = elapse.count()/ 1000.0;

    cout<<"Average cost "<<us_elapse/iter<<"us";

}

void mpmcqueue_bench( const int nthreads ){

    char region[FLAGS_size*nthreads + nthreads];
    folly::MPMCQueue<string> mpmc_queue(2);
    folly::ConcurrentHashMap<string, string> kv_store;
    /*
     * 初始化kv_store
     */
    static const char alphanum[] = "1234567890"
                                   "abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::random_device rd;
    std::uniform_int_distribution<int> ud( 0, sizeof(alphanum) - 2 );
    std::mt19937 gen(rd());
    string prefix = "key_";
    for (int i = 0; i < FLAGS_ops; i++) {
        string tmp_key = prefix + to_string(i);
        string tmp_val;
        for (int j = 0; j < FLAGS_size; j++) {
            tmp_val += alphanum[ud(gen)];
        }
        kv_store.insert(tmp_key, tmp_val);
    }

    //consumer threads
    //read key from queue, find key-value pair and memcpy
    int valSize = FLAGS_size;
    int totalOps = FLAGS_ops;
    vector<thread> threads;
    threads.reserve( nthreads );
    for( int i = 0; i < nthreads; i++ ){
        threads.emplace_back([&,i, valSize, totalOps, nthreads](){
            size_t offset = i * (valSize +1);
            string key;
            for( int k = i; k < totalOps; k+=nthreads ){
                mpmc_queue.blockingRead(key);
                auto iter = kv_store.find( key );
                memcpy( region + offset, iter->second.c_str(), valSize );
                region[offset+valSize] = '\0';
                //LOG(INFO)<<"Consumer "<<i<<":[ "<<key<<","<<iter->second<<" ]";
                CHECK_EQ(region[offset], iter->second[0]);
                CHECK_EQ(region[offset+FLAGS_size-1], iter->second[FLAGS_size-1]);
            }
        });
    }

    //producer thread
    //write key into queue
    auto start = chrono::steady_clock::now();

    for( int i = 0; i < FLAGS_ops; i++ )
        mpmc_queue.blockingWrite(prefix+ to_string(i));

    for( int i = 0; i < nthreads; i++ )
        threads[i].join();

    auto end = chrono::steady_clock::now();
    auto elapse = chrono::duration_cast<chrono::nanoseconds>(end - start);

    double us_elapse = elapse.count()/ 1000.0;
    double avg_lat = us_elapse/FLAGS_ops;

    LOG(INFO)<<"Average Latency: "<<avg_lat<<"us";
}

void mpmcpipeline_bench( const size_t numThreadsPerStage ) {
    constexpr size_t stages = 2;
    folly::MPMCPipeline<string, string, string> a(10, 10, 10);
    folly::ConcurrentHashMap<string, string> kv_store;
    static const char alphanum[] = "1234567890"
                                   "abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::random_device rd;
    std::uniform_int_distribution<int> ud( 0, sizeof(alphanum) - 2 );
    std::mt19937 gen(rd());
    string prefix = "key_";
    for (int i = 0; i < FLAGS_ops; i++) {
        string tmp_key = prefix + to_string(i);
        string tmp_val;
        for (int j = 0; j < FLAGS_size; j++) {
            tmp_val += alphanum[ud(gen)];
        }
        kv_store.insert(tmp_key, tmp_val);
    }

    vector<thread> threads;
    threads.reserve(numThreadsPerStage * stages + 1);
    for (size_t i = 0; i < numThreadsPerStage; ++i) {
        threads.emplace_back([&a,&kv_store] {

            for (;;) {
                string val;
                auto ticket = a.blockingReadStage<0>(val);
                if (val.empty()) { // stop
                    // We still need to propagate
                    a.blockingWriteStage<0>(ticket, "");
                    break;
                }
                //processing
                auto it = kv_store.find(val);
                a.blockingWriteStage<0>(ticket, string(it->second));
            }
        });
    }

    for (size_t i = 0; i < numThreadsPerStage; ++i) {
        threads.emplace_back([&a,i] {
            //char container[FLAGS_size+1];
            for (;;) {
                std::string val;
                auto ticket = a.blockingReadStage<1>(val);
                if (val.empty()) { // stop
                    // We still need to propagate
                    a.blockingWriteStage<1>(ticket, "");
                    break;
                }
                //copy data to memory
                //memcpy(container, val.c_str(), FLAGS_size+1);
                //LOG(INFO)<<"Stage 2,Thread:"<<i<<":"<<val;
                a.blockingWriteStage<1>(ticket, string(val));
            }
        });
    }

    threads.emplace_back([&a]() {
        for (;;) {
            std::string val;
            a.blockingRead(val);
            if (val.empty()) {
                break;
            }
            //io
            //LOG(INFO)<<"Stage 3:"<<val;
        }
    });


    //write request
    auto start = chrono::steady_clock::now();

    for (size_t i = 0; i < FLAGS_ops; ++i) {
        a.blockingWrite(prefix+ to_string(i));
    }
    for (size_t i = 0; i < numThreadsPerStage; ++i) {
        a.blockingWrite("");
    }

    for( thread &t : threads )
        t.join();
    auto end = chrono::steady_clock::now();
    auto elapse = chrono::duration_cast<chrono::nanoseconds>(end - start);

    double us_elapse = elapse.count()/ 1000.0;
    double avg_lat = us_elapse/FLAGS_ops;

    LOG(INFO)<<"Average Latency: "<<avg_lat<<"us";
}

int main( int argc, char *argv[0] ){
    //copy_cost(100);
    FLAGS_logtostderr = 1;
    google::InitGoogleLogging(argv[0]);
    //copy_cost(100);
    mpmcpipeline_bench(2);
    //mpmcqueue_bench(4);
    return 0;
}