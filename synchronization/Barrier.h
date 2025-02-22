//
// Created by luoxiYun on 2022/4/3.
//
#pragma once

#ifndef TESENCH_BARRIER_H
#define TESENCH_BARRIER_H


#include <stdint.h>

#include <condition_variable>
#include <mutex>

struct Barrier {
    explicit Barrier(size_t count) : lock_(), cv_(), count_(count) {}

    void wait() {
        std::unique_lock<std::mutex> lockHeld(lock_);
        auto gen = gen_;
        if (++num_ == count_) {
            num_ = 0;
            gen_++;
            cv_.notify_all();
        } else {
            cv_.wait(lockHeld, [&]() { return gen != gen_; });
        }
    }

private:
    std::mutex lock_;
    std::condition_variable cv_;
    size_t num_{0};
    size_t count_;
    size_t gen_{0};
};

#endif //TESENCH_BARRIER_H
