#ifndef BUFFER_BTREE_H
#define BUFFER_BTREE_H

#include <cassert>
#include <cstring>
#include <atomic>
#include <immintrin.h>
#include <sched.h>
#include <limits>
#include <queue>
#include <unordered_set>
#include <iostream>
#include <thread>
#include <shared_mutex>
#include <mutex>
#include "zbtree.h"

// #define DEBUG_BUFFER

namespace btreeolc
{
namespace buffer_btree
{

    enum class PageType : uint8_t
    {
        BTreeInner = 1,
        BTreeLeaf = 2
    };

    static const uint64_t pageSize = 4 * 1024;

    struct OptLock
    {
        std::atomic<uint64_t> typeVersionLockObsolete{0b100};

        bool isLocked(uint64_t version)
        {
            return ((version & 0b10) == 0b10);
        }

        uint64_t readLockOrRestart(bool &needRestart)
        {
            uint64_t version;
            version = typeVersionLockObsolete.load();
            if (isLocked(version) || isObsolete(version))
            {
                _mm_pause();
                needRestart = true;
            }
            return version;
        }

        void writeLockOrRestart(bool &needRestart)
        {
            uint64_t version;
            version = readLockOrRestart(needRestart);
            if (needRestart)
                return;

            upgradeToWriteLockOrRestart(version, needRestart);
            if (needRestart)
                return;
        }

        void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart)
        {
            if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10))
            {
                version = version + 0b10;
            }
            else
            {
                _mm_pause();
                needRestart = true;
            }
        }

        void writeUnlock()
        {
            typeVersionLockObsolete.fetch_add(0b10);
        }

        bool isObsolete(uint64_t version)
        {
            return (version & 1) == 1;
        }

        void checkOrRestart(uint64_t startRead, bool &needRestart) const
        {
            readUnlockOrRestart(startRead, needRestart);
        }

        void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const
        {
            needRestart = (startRead != typeVersionLockObsolete.load());
        }

        void writeUnlockObsolete()
        {
            typeVersionLockObsolete.fetch_add(0b11);
        }
    };

    struct NodeBase : public OptLock
    {
        PageType type;
        uint16_t count;
        uint32_t access_count;
    };

    struct BTreeLeafBase : public NodeBase
    {
        static const PageType typeMarker = PageType::BTreeLeaf;
    };

    template <class Key, class Payload>
    struct BTreeLeaf : public BTreeLeafBase
    {
        struct Entry
        {
            Key k;
            Payload p;
        };

        static const uint64_t maxEntries = (pageSize - sizeof(NodeBase)) / (sizeof(Key) + sizeof(Payload));

        Key keys[maxEntries];
        Payload payloads[maxEntries];

        BTreeLeaf()
        {
            count = 0;
            type = typeMarker;
            access_count = 0;
        }

        bool isFull() { return count == maxEntries; };

        unsigned lowerBound(Key k)
        {
            unsigned lower = 0;
            unsigned upper = count;
            do
            {
                unsigned mid = ((upper - lower) / 2) + lower;
                if (k < keys[mid])
                {
                    upper = mid;
                }
                else if (k > keys[mid])
                {
                    lower = mid + 1;
                }
                else
                {
                    return mid;
                }
            } while (lower < upper);
            return lower;
        }

        unsigned lowerBoundBF(Key k)
        {
            auto base = keys;
            unsigned n = count;
            while (n > 1)
            {
                const unsigned half = n / 2;
                base = (base[half] < k) ? (base + half) : base;
                n -= half;
            }
            return (*base < k) + base - keys;
        }

        void insert(Key k, Payload p)
        {
            assert(count < maxEntries);
            if (access_count < std::numeric_limits<uint32_t>::max())
                access_count++;
            if (count)
            {
                unsigned pos = lowerBound(k);
                if ((pos < count) && (keys[pos] == k))
                {
                    // Upsert
                    payloads[pos] = p;
                    return;
                }
                memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos));
                memmove(payloads + pos + 1, payloads + pos, sizeof(Payload) * (count - pos));
                keys[pos] = k;
                payloads[pos] = p;
            }
            else
            {
                keys[0] = k;
                payloads[0] = p;
            }
            count++;
        }

        BTreeLeaf *split(Key &sep)
        {
            BTreeLeaf *newLeaf = new BTreeLeaf();
            newLeaf->count = count - (count / 2);
            count = count - newLeaf->count;
            memcpy(newLeaf->keys, keys + count, sizeof(Key) * newLeaf->count);
            memcpy(newLeaf->payloads, payloads + count, sizeof(Payload) * newLeaf->count);
            sep = keys[count - 1];
            return newLeaf;
        }
    };

    struct BTreeInnerBase : public NodeBase
    {
        static const PageType typeMarker = PageType::BTreeInner;
    };

    template <class Key>
    struct BTreeInner : public BTreeInnerBase
    {
        static const uint64_t maxEntries = (pageSize - sizeof(NodeBase)) / (sizeof(Key) + sizeof(NodeBase *));
        NodeBase *children[maxEntries];
        Key keys[maxEntries];

        BTreeInner()
        {
            count = 0;
            type = typeMarker;
        }

        bool isFull() { return count == (maxEntries - 1); };

        unsigned lowerBoundBF(Key k)
        {
            auto base = keys;
            unsigned n = count;
            while (n > 1)
            {
                const unsigned half = n / 2;
                base = (base[half] < k) ? (base + half) : base;
                n -= half;
            }
            return (*base < k) + base - keys;
        }

        unsigned lowerBound(Key k)
        {
            unsigned lower = 0;
            unsigned upper = count;
            do
            {
                unsigned mid = ((upper - lower) / 2) + lower;
                if (k < keys[mid])
                {
                    upper = mid;
                }
                else if (k > keys[mid])
                {
                    lower = mid + 1;
                }
                else
                {
                    return mid;
                }
            } while (lower < upper);
            return lower;
        }

        BTreeInner *split(Key &sep)
        {
            BTreeInner *newInner = new BTreeInner();
            newInner->count = count - (count / 2);
            count = count - newInner->count - 1;
            sep = keys[count];
            memcpy(newInner->keys, keys + count + 1, sizeof(Key) * (newInner->count + 1));
            memcpy(newInner->children, children + count + 1, sizeof(NodeBase *) * (newInner->count + 1));
            return newInner;
        }

        void insert(Key k, NodeBase *child)
        {
            assert(count < maxEntries - 1);
            unsigned pos = lowerBound(k);
            memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
            memmove(children + pos + 1, children + pos, sizeof(NodeBase *) * (count - pos + 1));
            keys[pos] = k;
            children[pos] = child;
            std::swap(children[pos], children[pos + 1]);
            count++;
        }
        ~BTreeInner()
        {
            for (int i = 0; i < count; i++)
            {
                delete children[i];
            }
        }
    };

    template <class Key, class Value>
    struct BufferBTreeImp
    {
        std::atomic<NodeBase *> root;
        std::atomic<uint64_t> leaf_count;
        std::atomic<bool> full;
        std::atomic<uint64_t> using_count;
        std::shared_ptr<BTree> device_tree;
        using leaf_type = BTreeLeaf<Key, Value>;
        using inner_type = BTreeInner<Key>;

        explicit BufferBTreeImp(std::shared_ptr<BTree> device_tree) : device_tree(device_tree)
        {
            root = new BTreeLeaf<Key, Value>();
            leaf_count = 1;
            full = false;
            using_count = 0;
        }

        void makeRoot(Key k, NodeBase *leftChild, NodeBase *rightChild)
        {
            BTreeInner<Key>* inner = new BTreeInner<Key>();
            inner->count = 1;
            inner->keys[0] = k;
            inner->children[0] = leftChild;
            inner->children[1] = rightChild;
            root = inner;
        }

        void yield(int count)
        {
            if (count > 3)
                sched_yield();
            else
                _mm_pause();
        }


        void insert(Key k, Value v)
        {
            int restartCount = 0;
        restart:
            if (restartCount++)
                yield(restartCount);
            bool needRestart = false;

            // Current node
            NodeBase *node = root;
            uint64_t versionNode = node->readLockOrRestart(needRestart);
            if (needRestart || (node != root))
                goto restart;

            // Parent of current node
            BTreeInner<Key> *parent = nullptr;
            uint64_t versionParent;

            while (node->type == PageType::BTreeInner)
            {
                BTreeInner<Key> *inner = (BTreeInner<Key> *)node;

                // Split eagerly if full
                if (inner->isFull())
                {
                    // Lock
                    if (parent)
                    {
                        parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
                        if (needRestart)
                            goto restart;
                    }
                    node->upgradeToWriteLockOrRestart(versionNode, needRestart);
                    if (needRestart)
                    {
                        if (parent)
                            parent->writeUnlock();
                        goto restart;
                    }
                    if (!parent && (node != root))
                    { // there's a new parent
                        node->writeUnlock();
                        goto restart;
                    }
                    // Split
                    Key sep;
                    BTreeInner<Key> *newInner = inner->split(sep);
                    if (parent)
                        parent->insert(sep, newInner);
                    else
                        makeRoot(sep, inner, newInner);
                    // Unlock and restart
                    node->writeUnlock();
                    if (parent)
                        parent->writeUnlock();
                    goto restart;
                }

                if (parent)
                {
                    parent->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent = inner;
                versionParent = versionNode;

                node = inner->children[inner->lowerBound(k)];
                inner->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;
            }

            BTreeLeaf<Key, Value> *leaf = (BTreeLeaf<Key, Value> *)node;

            // Split leaf if full
            if (leaf->count == leaf->maxEntries)
            {
                // Lock
                if (parent)
                {
                    parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }
                node->upgradeToWriteLockOrRestart(versionNode, needRestart);
                if (needRestart)
                {
                    if (parent)
                        parent->writeUnlock();
                    goto restart;
                }
                if (!parent && (node != root))
                { // there's a new parent
                    node->writeUnlock();
                    goto restart;
                }
                // Split
                Key sep;
                BTreeLeaf<Key, Value> *newLeaf = leaf->split(sep);
                leaf_count++;
                if (parent)
                    parent->insert(sep, newLeaf);
                else
                    makeRoot(sep, leaf, newLeaf);
                // Unlock and restart
                node->writeUnlock();
                if (parent)
                    parent->writeUnlock();
                goto restart;
            }
            else
            {
                // only lock leaf node
                node->upgradeToWriteLockOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                if (parent)
                {
                    parent->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                    {
                        node->writeUnlock();
                        goto restart;
                    }
                }
                leaf->insert(k, v);
                node->writeUnlock();
                // return; // success
                // not return cause we need to check leaf count
                // and if leaf count is too large, we need to flush it
            }

            if (leaf_count > max_leaf_count)
            {
                full = true;
            }
        }

        struct LeafCompare
        {
            bool operator()(const leaf_type* lhs, const leaf_type* rhs) const
            {
                return lhs->access_count < rhs->access_count;
            }
        };
        // max heap
        using vec_type = std::vector<leaf_type*>;
        using pq_type = std::priority_queue<leaf_type*, vec_type, LeafCompare>;
        vec_type flush()
        {
            std::pair<vec_type, vec_type> ret = distinguish_leaves(root);
            vec_type flush_leaf = std::move(ret.first);
            vec_type keep_leaf = std::move(ret.second);
            // todo
            // merge insert leaf operation
            for(auto leaf : flush_leaf)
            {
                for(int i = 0; i < leaf->count; i++)
                {
                    device_tree->Insert(leaf->keys[i], leaf->payloads[i]);
                }
            }
            return keep_leaf;
        }

        void flush_all()
        {
            std::pair<vec_type, vec_type> ret = distinguish_leaves(root);
            vec_type flush_leaf = std::move(ret.first);
            vec_type keep_leaf = std::move(ret.second);
            // todo
            // merge insert leaf operation
            for(auto leaf : flush_leaf)
            {
                for(int i = 0; i < leaf->count; i++)
                {
                    device_tree->Insert(leaf->keys[i], leaf->payloads[i]);
                }
            }
            for(auto leaf : keep_leaf)
            {
                for(int i = 0; i < leaf->count; i++)
                {
                    device_tree->Insert(leaf->keys[i], leaf->payloads[i]);
                }
            }
        }

    private:
        // return flush_leaf and keep_leaf
        std::pair<vec_type, vec_type> distinguish_leaves(NodeBase *node)
        {
            pq_type pq;
            vec_type another;
            traverse(node, pq, another);
            vec_type flush_leaf;
            while(!pq.empty())
            {
                flush_leaf.push_back(pq.top());
                pq.pop();
            }
            return std::make_pair(std::move(flush_leaf), std::move(another));
        }
        void traverse(NodeBase* node, pq_type& pq/*flush*/, vec_type& another)
        {
            if(node == nullptr)
                return;
            if(node->type == PageType::BTreeInner)
            {
                inner_type* inner = (inner_type*)node;
                for(int i = 0; i < inner->count; i++)
                {
                    traverse(inner->children[i], pq, another);
                }
            }
            else
            {
                leaf_type* leaf = (leaf_type*)node;
                pq.push(leaf);
                if(pq.size() > max_leaf_count - keep_leaf_count)
                {
                    another.push_back(pq.top());
                    pq.pop();
                }
            }
        }

    public:
        bool lookup(Key k, Value &result)
        {
            int restartCount = 0;
        restart:
            if (restartCount++)
                yield(restartCount);
            bool needRestart = false;

            NodeBase *node = root;
            uint64_t versionNode = node->readLockOrRestart(needRestart);
            if (needRestart || (node != root))
                goto restart;

            // Parent of current node
            BTreeInner<Key> *parent = nullptr;
            uint64_t versionParent;

            while (node->type == PageType::BTreeInner)
            {
                auto inner = static_cast<BTreeInner<Key> *>(node);

                if (parent)
                {
                    parent->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent = inner;
                versionParent = versionNode;

                node = inner->children[inner->lowerBound(k)];
                inner->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;
            }

            BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
            unsigned pos = leaf->lowerBound(k);
            bool success;
            if ((pos < leaf->count) && (leaf->keys[pos] == k))
            {
                success = true;
                result = leaf->payloads[pos];
            }
            if (parent)
            {
                parent->readUnlockOrRestart(versionParent, needRestart);
                if (needRestart)
                    goto restart;
            }
            node->readUnlockOrRestart(versionNode, needRestart);
            if (needRestart)
                goto restart;

            if(!success)
            {
                return device_tree->Get(k, result);
            }
            return success;
        }

        uint64_t scan(Key k, int range, Value *output)
        {
            int restartCount = 0;
        restart:
            if (restartCount++)
                yield(restartCount);
            bool needRestart = false;

            NodeBase *node = root;
            uint64_t versionNode = node->readLockOrRestart(needRestart);
            if (needRestart || (node != root))
                goto restart;

            // Parent of current node
            BTreeInner<Key> *parent = nullptr;
            uint64_t versionParent;

            while (node->type == PageType::BTreeInner)
            {
                auto inner = static_cast<BTreeInner<Key> *>(node);

                if (parent)
                {
                    parent->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent = inner;
                versionParent = versionNode;

                node = inner->children[inner->lowerBound(k)];
                inner->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;
            }

            BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
            unsigned pos = leaf->lowerBound(k);
            int count = 0;
            for (unsigned i = pos; i < leaf->count; i++)
            {
                if (count == range)
                    break;
                output[count++] = leaf->payloads[i];
            }

            if (parent)
            {
                parent->readUnlockOrRestart(versionParent, needRestart);
                if (needRestart)
                    goto restart;
            }
            node->readUnlockOrRestart(versionNode, needRestart);
            if (needRestart)
                goto restart;

            return count;
        }
        ~BufferBTreeImp()
        {
            delete root.load();
            root = nullptr;
        }
    };

    /*
    *  BufferBTree is a thread-safe B+ tree
    */
    template<typename Key, typename Value>
    struct BufferBTree
    {
        BufferBTreeImp<Key, Value>* volatile current;
        std::shared_mutex mtx;
        std::shared_ptr<BTree> device_tree;
        
        BufferBTree(std::shared_ptr<BTree> device_tree) : device_tree(device_tree)
        {
            current = new BufferBTreeImp<Key, Value>(device_tree);
        }

        void Insert(Key k, Value v)
        {
        restart_insert:
            std::shared_lock<std::shared_mutex> share_lock(mtx);

            if(current->full)
            {
                share_lock.unlock();
                std::unique_lock<std::shared_mutex> u_lock(mtx);
                if(current->full)
                {
                    // rebuild the tree base on keep_leaf and we restart insert
                    auto keep_leaf = std::move(current->flush());
#ifdef DEBUG_BUFFER
                    std::cerr << "triger flush" << std::endl;
                    int leaf_cnt = 0;
                    for(auto leaf : keep_leaf)
                    {
                        printf("leaf: %d, count: %d, access count %d\n", leaf_cnt++, leaf->count, leaf->access_count);
                    }
#endif
                    std::unique_ptr<BufferBTreeImp<Key, Value>> old_tree(current);
                    current = nullptr;
                    current = new BufferBTreeImp<Key, Value>(device_tree);
                    for(auto leaf : keep_leaf)
                    {
                        for(int i = 0; i < leaf->count; i++)
                        {
                            current->insert(leaf->keys[i], leaf->payloads[i]);
                        }
                    }
                }
                u_lock.unlock();
                goto restart_insert;
            }
            else
            {
                current->insert(k, v);
            }
        }

        bool Get(Key k, Value &result)
        {
        restart_lookup:
            std::shared_lock<std::shared_mutex> share_lock(mtx);
            if(current->full)
            {
                share_lock.unlock();
                yield();
                goto restart_lookup;
            }
            else
            {
                return current->lookup(k, result);
            }
        }

        uint64_t scan(Key k, int range, Value *output)
        {
        restart_scan:
            std::shared_lock<std::shared_mutex> share_lock(mtx);
            if(current->full)
            {
                share_lock.unlock();
                yield();
            }
            else
            {
                return current->scan(k, range, output);
            }
        }
        
        void yield()
        {
            sched_yield();
        }

        void flush_all()
        {
            std::unique_lock<std::shared_mutex> u_lock(mtx);
            current->flush_all();
            // std::shared_ptr<BufferBTreeImp<Key, Value>> old(current);
            // current = nullptr;
            // current = new BufferBTreeImp<Key, Value>(device_tree);
        }
        ~BufferBTree()
        {
            flush_all();
            delete current;
            current = nullptr;
        }
    };

}
}

#endif