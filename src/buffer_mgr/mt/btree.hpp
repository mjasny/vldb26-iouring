#pragma once

#include "btree_node.hpp"
#include "config.hpp"
#include "types.hpp"

#include <span>

static const PID metadataPageId = 0;

struct MetaDataPage {
    PID roots[pageSize / sizeof(PID)];

    PID getRoot(unsigned slot) { return roots[slot]; }
};

static_assert(sizeof(MetaDataPage) == pageSize);

struct BTree {
private:
    action_t trySplit(GuardX<BTreeNode>&& node, GuardX<BTreeNode>&& parent, std::span<u8> key, unsigned payloadLen);
    void ensureSpace(PID toSplit, std::span<u8> key, unsigned payloadLen);

public:
    unsigned slotId;
    bool splitOrdered;

    BTree();
    ~BTree();

    GuardO<BTreeNode> findLeafO(std::span<u8> key) {
        for (u64 repeatCounter = 0;; repeatCounter++) {
            {
                GuardO<MetaDataPage> meta(metadataPageId);
                if (meta.retry()) {
                    goto restart;
                }
                GuardO<BTreeNode> node(meta->getRoot(slotId), meta);
                if (node.retry()) {
                    goto restart;
                }
                meta.release();

                while (node->isInner()) {
                    GuardO<BTreeNode> child(node->lookupInner(key), node);
                    if (child.retry()) {
                        goto restart;
                    }
                    node = std::move(child);
                }
                return node;
            }
        restart:
            bm.handleRestart();
        }
    }

    GuardS<BTreeNode> findLeafSChecked(std::span<u8> key) {
        for (u64 repeatCounter = 0;; repeatCounter++) {
            GuardS<BTreeNode> node(std::move(findLeafO(key)));
            if (!node.retry()) {
                return node;
            }
            bm.handleRestart();
        }
    }

    // point lookup, returns payload len on success, or -1 on failure
    int lookup(std::span<u8> key, u8* payloadOut, unsigned payloadOutSize) {
        GuardS<BTreeNode> node = findLeafSChecked(key);
        bool found;
        unsigned pos = node->lowerBound(key, found);
        if (!found)
            return -1;

        memcpy(payloadOut, node->getPayload(pos).data(), min(node->slot[pos].payloadLen, payloadOutSize));
        return node->slot[pos].payloadLen;
    }

    template <class Fn>
    bool lookup(std::span<u8> key, Fn fn) {
        GuardS<BTreeNode> node = findLeafSChecked(key);
        bool found;
        unsigned pos = node->lowerBound(key, found);
        if (!found)
            return false;

        fn(node->getPayload(pos));
        return true;
    }

    void insert(std::span<u8> key, std::span<u8> payload);
    bool remove(std::span<u8> key);

    template <class Fn>
    bool updateInPlace(std::span<u8> key, Fn fn) {
        for (u64 repeatCounter = 0;; repeatCounter++) {
            {
                GuardO<BTreeNode> node(std::move(findLeafO(key)));
                if (node.retry()) {
                    goto restart;
                }
                bool found;
                unsigned pos = node->lowerBound(key, found);
                if (!found)
                    return false;

                GuardX<BTreeNode> nodeLocked(std::move(node), LockConflictPolicy::Restart);
                if (nodeLocked.retry()) {
                    goto restart;
                }
                fn(nodeLocked->getPayload(pos));
                return true;
            }
        restart:
            bm.handleRestart();
        }
    }

    GuardS<BTreeNode> findLeafS(std::span<u8> key) {
        for (u64 repeatCounter = 0;; repeatCounter++) {
            {
                GuardO<MetaDataPage> meta(metadataPageId);
                if (meta.retry()) {
                    goto restart;
                }
                GuardO<BTreeNode> node(meta->getRoot(slotId), meta);
                if (node.retry()) {
                    goto restart;
                }
                meta.release();

                while (node->isInner()) {
                    GuardO<BTreeNode> child(node->lookupInner(key), node);
                    if (child.retry()) {
                        goto restart;
                    }
                    node = std::move(child);
                }
                GuardS<BTreeNode> leaf(std::move(node));
                if (leaf.retry()) {
                    goto restart;
                }
                return leaf;
            }
        restart:
            bm.handleRestart();
        }
    }

    template <class Fn>
    void scanAsc(std::span<u8> key, Fn fn) {
        GuardS<BTreeNode> node(std::move(findLeafSChecked(key)));

        bool found;
        int pos = node->lowerBound(key, found);
        for (u64 repeatCounter = 0;; repeatCounter++) { // XXX
            if (pos < node->count) {
                if (!fn(*node.ptr, pos))
                    return;
                pos++;
            } else {
                if (!node->hasRightNeighbour()) {
                    return;
                }
                pos = 0;
                PID nextLeaf = node->nextLeafNode;
                node.release();
                for (;;) {
                    GuardS<BTreeNode> next(nextLeaf);
                    if (!next.retry()) {
                        node = std::move(next);
                        break;
                    }
                    bm.handleRestart();
                }
            }
        }
    }


    template <class Fn>
    void scanDesc(std::span<u8> key, Fn fn) {
        GuardS<BTreeNode> node(std::move(findLeafSChecked(key)));
        bool exactMatch;

        // PID ppid = reinterpret_cast<Page*>(node->ptr()) - bm.virtMem;
        // ensure(node->hdr.lpid == ppid);

        int pos = node->lowerBound(key, exactMatch);
        if (pos == node->count) {
            pos--;
            exactMatch = true; // XXX:
        }
        for (u64 repeatCounter = 0;; repeatCounter++) { // XXX
            while (pos >= 0) {
                if (!fn(*node.ptr, pos, exactMatch))
                    return;
                pos--;
            }
            if (!node->hasLowerFence())
                return;

            node = findLeafS(node->getLowerFence());
            pos = node->count - 1;
        }
    }
};
