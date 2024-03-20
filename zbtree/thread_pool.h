#ifndef THREAD_WORKER_H
#define THREAD_WORKER_H
#include <atomic>
#include <memory>
#include <thread>
#include <vector>
#include <condition_variable>
#include <xmmintrin.h>
#include <cassert>
#include <iostream>
#include <functional>
// #include <barrier>
#include <boost/thread/barrier.hpp>
#include "zbtree.h"
#include "rwlatch.h"
// using KeyType = int;
// using ValueType = int;
namespace btreeolc
{
struct work {
    KeyType* keys;
    ValueType* values;
    int count;
    work(KeyType* k, ValueType* v, int c);
    work(const work& w) {
        keys = w.keys;
        values = w.values;
        count = w.count;
    };
    ~work();
};

struct thread_pool {
    using q_type = std::vector<work>;
    void queue(q_type& w);
    explicit thread_pool(int n, std::shared_ptr<btreeolc::BTree> tree);
    ~thread_pool();
private:
	std::shared_ptr<btreeolc::BTree> device_tree;
    q_type work_queue;
    std::vector<std::shared_ptr<std::thread>> threads;
    std::atomic<bool> start;
    std::atomic<bool> end;
    std::mutex wakeup_mutex;
    std::mutex start_mutex;
    ReaderWriterLatch start_rdlock;
    std::condition_variable wakeup_cv;
	boost::barrier syncpoint;
    void yield(int trial);
    void run();
    void do_work(const int index);
    void notify_all();
};
}

#endif  // THREAD_WORKER_H