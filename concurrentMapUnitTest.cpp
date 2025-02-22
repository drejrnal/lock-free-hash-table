//
// Created by luoxiYun on 2022/3/9.
//

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/hash/Hash.h>
#include "gtest/gtest.h"
#include <thread>
#include <experimental/coroutine>

using namespace std;

TEST(ConcurrentMap, eraseStressTest){
    int size = 2, num_threads = 32;
    int ops = 2 * 128 * size;
    folly::ConcurrentHashMap<unsigned long, unsigned long> cm(size);
    for( int i = 0; i < size; i++ ){
        unsigned long k = folly::hash::jenkins_rev_mix32(i);
        cm.insert(k, k);
    }
    vector<thread> threads;
    threads.reserve( num_threads );

    for( int i = 0; i < num_threads; i++ ){
        threads[i] = thread([&, i](){
            uint32_t offset = ( i * ops ) / num_threads;
            for( int iter = 0; iter < (ops / num_threads); iter++ ){
                unsigned long tmp = folly::hash::jenkins_rev_mix32(offset+iter);
                auto res = cm.insert(tmp, tmp).second;
                if( res ){
                    if( iter % 2 == 0 ){
                        res = cm.erase( tmp );
                    }else{
                        res = cm.erase_if_equal( tmp, tmp );
                    }

                    if( !res ){
                        cout<<"Failed to erase element for thread "<<i<<"val: "<<tmp<<endl;
                        exit(0);
                    }

                    EXPECT_TRUE(res);
                }
                res = cm.insert( tmp, tmp ).second;
                if( res ){
                    res = bool(cm.assign( tmp, tmp ));
                    if( !res ){
                        cout<<"Failed to erase element for thread "<<i<<"val: "<<tmp<<endl;
                        exit(0);
                    }
                    EXPECT_TRUE(res);
                    auto find_res = cm.find( tmp );
                    if( find_res == cm.cend() ){
                        cout<<"Failed to erase element for thread "<<i<<"val: "<<tmp<<endl;
                        exit(0);
                    }
                    EXPECT_EQ( tmp, find_res->second);
                }
            }
        });
    }
    for( int i = 0; i < num_threads; i++ ){
        threads[i].join();
    }

}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}