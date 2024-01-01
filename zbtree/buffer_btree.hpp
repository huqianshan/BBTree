#ifndef _BUFFER_TREE_H_
#define _BUFFER_TREE_H_

#include <cassert>
#include <cstring>
#include <atomic>
#include <immintrin.h>
#include <sched.h>
#include <numeric>
#include <queue>

namespace btreeolc
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
            if(access_count < std::numeric_limits<uint32_t>::max())
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
    };

    template <class Key, class Value>
    struct BufferBTree
    {
        std::atomic<NodeBase *> root;
        std::atomic<uint64_t> leaf_count;
        static const uint32_t max_leaf_count = 100;
        static const uint32_t keep_leaf_count = 10;
        using leaf_type = BTreeLeaf<Key, Value>;

        BufferBTree()
        {
            root = new BTreeLeaf<Key, Value>();
            leaf_count = 1;
        }

        void makeRoot(Key k, NodeBase *leftChild, NodeBase *rightChild)
        {
            auto inner = new BTreeInner<Key>();
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
                BTreeInner<Key>* inner = (BTreeInner<Key> *)node;

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

            BTreeLeaf<Key, Value>* leaf = (BTreeLeaf<Key, Value> *)node;

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
            
            if(leaf_count > max_leaf_count)
            {
                flush();
            }
        }

        void flush()
        {
            // todo
            // find all least accessed leaf nodes
            // flush all data to disk
            std::priority_queue<std::pair<uint32_t, BTreeLeaf<Key, Value>*>> pq;
            traverse(root, pq);
        }
private:
        void traverse(NodeBase *node, std::priority_queue<std::pair<uint32_t, leaf_type*>> &result)
        {
            if(node == nullptr)
                return;
            if (node->type == PageType::BTreeInner)
            {
                auto inner = dynamic_cast<BTreeInner<Key> *>(node);
                for (unsigned i = 0; i < inner->count; i++)
                {
                    traverse(inner->children[i], result);
                }
            }
            else
            {
                auto leaf = dynamic_cast<leaf_type*>(node);
                result.emplace(leaf->access_count, leaf);
                if(result.size() > keep_leaf_count)
                {
                    result.pop();
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
    };

}
#endif