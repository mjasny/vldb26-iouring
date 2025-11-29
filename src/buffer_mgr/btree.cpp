#include "btree.hpp"

#include "buffer_mgr/bm.hpp"
#include "buffer_mgr/types.hpp"
#include "utils/utils.hpp"

#include <cassert>

static unsigned btreeslotcounter = 0;

BTree::BTree() : splitOrdered(false) {
    GuardX<MetaDataPage> page(metadataPageId);
    AllocGuard<BTreeNode> rootNode(true);
    slotId = btreeslotcounter++;
    page->roots[slotId] = rootNode.pid;
}

BTree::~BTree() {}

action_t BTree::trySplit(GuardX<BTreeNode>&& node, GuardX<BTreeNode>&& parent, std::span<u8> key, unsigned payloadLen) {

    // create new root if necessary
    if (parent.pid == metadataPageId) {
        MetaDataPage* metaData = reinterpret_cast<MetaDataPage*>(parent.ptr);
        AllocGuard<BTreeNode> newRoot(false); // can pagefault, no changes
        if (newRoot.retry()) {
            return action_t::RESTART;
        }
        newRoot->upperInnerNode = node.pid;
        metaData->roots[slotId] = newRoot.pid;
        parent = std::move(newRoot);
    }

    // split
    BTreeNode::SeparatorInfo sepInfo = node->findSeparator(splitOrdered);
    u8 sepKey[sepInfo.len];
    node->getSep(sepKey, sepInfo);

    if (parent->hasSpaceFor(sepInfo.len, sizeof(PID))) { // is there enough space in the parent for the separator?
        auto ret = node->splitNode(node.pid, parent.ptr, sepInfo.slot, {sepKey, sepInfo.len});

        //  this can page-fault, no changes
        return ret;
    }

    // must split parent to make space for separator, restart from root to do this
    node.release();
    parent.release();
    ensureSpace(parent.ptr, {sepKey, sepInfo.len}, sizeof(PID));

    return action_t::OK;
}

void BTree::ensureSpace(BTreeNode* toSplit, std::span<u8> key, unsigned payloadLen) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
        {
            GuardS<BTreeNode> parent(metadataPageId);
            if (parent.retry()) {
                goto restart;
            }
            GuardS<BTreeNode> node(reinterpret_cast<MetaDataPage*>(parent.ptr)->getRoot(slotId));
            if (node.retry()) {
                goto restart;
            }

            while (node->isInner() && (node.ptr != toSplit)) {
                parent = std::move(node);
                node = GuardS<BTreeNode>(parent->lookupInner(key));
                if (node.retry()) {
                    goto restart;
                }
            }
            if (node.ptr == toSplit) {
                if (node->hasSpaceFor(key.size(), payloadLen))
                    return; // someone else did split concurrently

                // upgrade always succeeds
                GuardX<BTreeNode> parentLocked(std::move(parent));
                GuardX<BTreeNode> nodeLocked(std::move(node));
                auto ret = trySplit(std::move(nodeLocked), std::move(parentLocked), key, payloadLen);
                if (ret == action_t::RESTART) {
                    goto restart;
                }
            }
            return;
        }
    restart:
        bm.handleRestart();
    }
}

void BTree::insert(std::span<u8> key, std::span<u8> payload) {
    assert((key.size() + payload.size()) <= BTreeNode::maxKVSize);

    for (u64 repeatCounter = 0;; repeatCounter++) {
        {
            GuardS<BTreeNode> parent(metadataPageId);
            if (parent.retry()) {
                goto restart;
            }
            GuardS<BTreeNode> node(reinterpret_cast<MetaDataPage*>(parent.ptr)->getRoot(slotId));
            if (node.retry()) {
                goto restart;
            }

            while (node->isInner()) {
                parent = std::move(node);
                node = GuardS<BTreeNode>(parent->lookupInner(key));
                if (node.retry()) {
                    goto restart;
                }
            }

            if (node->hasSpaceFor(key.size(), payload.size())) {
                // only lock leaf
                GuardX<BTreeNode> nodeLocked(std::move(node));
                parent.release();
                nodeLocked->insertInPage(key, payload);
                return; // success
            }

            // lock parent and leaf
            GuardX<BTreeNode> parentLocked(std::move(parent));
            GuardX<BTreeNode> nodeLocked(std::move(node));
            auto ret = trySplit(std::move(nodeLocked), std::move(parentLocked), key, payload.size());
            if (ret == action_t::RESTART) {
                goto restart;
            }
            // insert hasn't happened, restart from root
            continue;
        }
    restart:
        bm.handleRestart();
    }
}

bool BTree::remove(std::span<u8> key) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
        {
            GuardS<BTreeNode> parent(metadataPageId);
            if (parent.retry()) {
                goto restart;
            }
            GuardS<BTreeNode> node(reinterpret_cast<MetaDataPage*>(parent.ptr)->getRoot(slotId));
            if (node.retry()) {
                goto restart;
            }

            u16 pos;
            while (node->isInner()) {
                pos = node->lowerBound(key);
                PID nextPage = (pos == node->count) ? node->upperInnerNode : node->getChild(pos);
                parent = std::move(node);
                node = GuardS<BTreeNode>(nextPage);
                if (node.retry()) {
                    goto restart;
                }
            }

            bool found;
            unsigned slotId = node->lowerBound(key, found);
            if (!found)
                return false;

            unsigned sizeEntry = node->slot[slotId].keyLen + node->slot[slotId].payloadLen;
            if ((node->freeSpaceAfterCompaction() + sizeEntry >= BTreeNodeHeader::underFullSize) && (parent.pid != metadataPageId) && (parent->count >= 2) && ((pos + 1) < parent->count)) {
                // underfull
                GuardX<BTreeNode> parentLocked(std::move(parent));
                GuardX<BTreeNode> nodeLocked(std::move(node));
                GuardX<BTreeNode> rightLocked(parentLocked->getChild(pos + 1)); // this might PageFault, no previous modifications
                if (rightLocked.retry()) {
                    goto restart;
                }
                nodeLocked->removeSlot(slotId);
                if (rightLocked->freeSpaceAfterCompaction() >= (pageSize - BTreeNodeHeader::underFullSize)) {
                    if (nodeLocked->mergeNodes(nodeLocked.pid, pos, parentLocked.ptr, rightLocked.ptr)) {
                        // XXX: should reuse page Id
                        // we can mark right node for eviction directly?
                    }
                }
            } else {
                GuardX<BTreeNode> nodeLocked(std::move(node));
                parent.release();
                nodeLocked->removeSlot(slotId);
            }
            return true;
        }
    restart:
        bm.handleRestart();
    }
}
