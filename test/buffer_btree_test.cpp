#include "buffer_btree.h"
#include <iostream>
#include <thread>
#include <vector>
using namespace std;

using buffer_type = btreeolc::buffer_btree::BufferBTree<uint64_t, uint64_t>;

void insert(buffer_type& buffer)
{
    for(int i = 0; i < 100000; ++i)
    {
        buffer.insert(i, i);
    }
}

constexpr int thread_num = 16;

int main()
{
    vector<thread> threads;
    btreeolc::buffer_btree::BufferBTree<uint64_t, uint64_t> buffer;
    for(int i = 0; i < thread_num; ++i)
    {
        thread t(insert);
        threads.push_back(move(t));
    }
    for(int i = 0; i< thread_num; ++i)
    {
        threads[i].join();
    }
    return 0;
}