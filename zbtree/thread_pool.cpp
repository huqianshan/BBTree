#include "thread_pool.h"

std::mutex m;
namespace btreeolc
{
work::work(KeyType* k, ValueType* v, int c) : keys(k), values(v), count(c) {}
work::~work()
{
}
thread_pool::thread_pool(int n, std::shared_ptr<btreeolc::BTree> tree)
    : syncpoint(n, [&](){
        start=false;
    }), start(false), end(false), device_tree(tree)
{
    assert(n > 0);
    for(int i = 0; i < n; ++i)
    {
        threads.push_back(std::make_shared<std::thread>(&thread_pool::do_work, this, i));
    }
}
thread_pool::~thread_pool()
{
    int trial = 0;
    while(start.load())
    {
        yield(trial++);
    }
    end = true;
    notify_all();
    std::unique_lock<std::mutex> lock(m);
    std::cout << "Joining threads..." << std::endl;
    lock.unlock();
    for(auto& t : threads)
    {
        t->join();
    }
}

void thread_pool::notify_all()
{
    std::unique_lock<std::mutex> lock(wakeup_mutex);
    wakeup_cv.notify_all();
}

void thread_pool::yield(int trial)
{
    if(trial < 3)
    {
        sched_yield();
    }
    else
    {
        _mm_pause();
    }
}

void thread_pool::queue(thread_pool::q_type& w)
{
    int trial = 0;
    while(start.load())
    {
        yield(trial++);
    }
    start_rdlock.WLock();
	work_queue.swap(w);
    run();
    start_rdlock.WUnlock();
	for(auto& wk : w)
	{
		delete[] wk.keys;
		delete[] wk.values;
	}
}

void thread_pool::run()
{
    start = true;
    notify_all();
}

void thread_pool::do_work(const int index)
{
    while(!end.load())
    {
        // int trial = 0;
        while(!start.load())
        {
            std::unique_lock<std::mutex> lock(wakeup_mutex);
            // wakeup_cv.wait_for(lock, std::chrono::nanoseconds(100), [this]() {return start.load() || end.load();});
            wakeup_cv.wait(lock, [this]() {return start.load() || end.load();});
            if(end.load())
            {
                return;
            }
        }
        start_rdlock.RLock();
        for(int i = index; i < work_queue.size(); i += threads.size())
        {
			device_tree->BatchInsert(work_queue[i].keys, work_queue[i].values, work_queue[i].count);
            // delete[] work_queue[i].keys;
            // delete[] work_queue[i].values;
        }
		syncpoint.count_down_and_wait();
        start = false;
        start_rdlock.RUnlock();
		// if(start.load())
		// {
		// 	std::lock_guard<std::mutex> lock(start_mutex);
		// 	if(start.load()) {
        //         start = false;
        //         work_queue = q_type();
        //     }
		// }
    }
}
}  // namespace btreeolc