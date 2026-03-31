#include "btree.hpp"

#include "bm.hpp"
#include "types.hpp"
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
    const auto sep = std::span<u8> {sepKey, sepInfo.len};

    if (parent->hasSpaceFor(sepInfo.len, sizeof(PID))) { // is there enough space in the parent for the separator?
        auto ret = node->splitNode(node.pid, parent.ptr, sepInfo.slot, sep);
        return ret;
    }

    // must split parent to make space for separator, restart from root to do this
    const PID parent_pid = parent.pid;
    node.release();
    parent.release();
    ensureSpace(parent_pid, sep, sizeof(PID));

    return action_t::OK;
}

void BTree::ensureSpace(PID toSplit, std::span<u8> key, unsigned payloadLen) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
        {
            GuardO<MetaDataPage> meta(metadataPageId);
            if (meta.retry()) {
                goto restart;
            }
            GuardO<BTreeNode> node(reinterpret_cast<MetaDataPage*>(meta.ptr)->getRoot(slotId), meta);
            if (node.retry()) {
                goto restart;
            }
            if (node.pid == toSplit) {
                if (node->hasSpaceFor(key.size(), payloadLen)) {
                    return;
                }

                const PID optimistic_node_pid = node.pid;
                node.release();
                GuardX<BTreeNode> parentLocked(metadataPageId);
                if (parentLocked.retry()) {
                    goto restart;
                }
                if (reinterpret_cast<MetaDataPage*>(parentLocked.ptr)->getRoot(slotId) != optimistic_node_pid) {
                    goto restart;
                }
                GuardX<BTreeNode> nodeLocked(optimistic_node_pid, LockConflictPolicy::Restart);
                if (nodeLocked.retry()) {
                    goto restart;
                }
                auto ret = trySplit(std::move(nodeLocked), std::move(parentLocked), key, payloadLen);
                if (ret == action_t::RESTART) {
                    goto restart;
                }
                return;
            }

            meta.release();
            GuardO<BTreeNode> parent(std::move(node));
            const PID first_pid = parent->lookupInner(key);
            assert(first_pid != 0);
            GuardO<BTreeNode> child(first_pid, parent);
            if (child.retry()) {
                goto restart;
            }

            while (child->isInner() && (child.pid != toSplit)) {
                parent = std::move(child);
                const PID next_pid = parent->lookupInner(key);
                assert(next_pid != 0);
                child = GuardO<BTreeNode>(next_pid, parent);
                if (child.retry()) {
                    goto restart;
                }
            }

            if (child.pid == toSplit) {
                if (child->hasSpaceFor(key.size(), payloadLen)) {
                    return; // someone else did split concurrently
                }
                GuardX<BTreeNode> parentLocked(std::move(parent), LockConflictPolicy::Restart);
                if (parentLocked.retry()) {
                    goto restart;
                }
                GuardX<BTreeNode> nodeLocked(std::move(child), LockConflictPolicy::Restart);
                if (nodeLocked.retry()) {
                    goto restart;
                }
                auto ret = trySplit(std::move(nodeLocked), std::move(parentLocked), key, payloadLen);
                if (ret == action_t::RESTART) {
                    goto restart;
                }
                return;
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
            GuardO<MetaDataPage> meta(metadataPageId);
            if (meta.retry()) {
                goto restart;
            }
            GuardO<BTreeNode> node(reinterpret_cast<MetaDataPage*>(meta.ptr)->getRoot(slotId), meta);
            if (node.retry()) {
                goto restart;
            }

            if (!node->isInner()) {
                meta.release();
                if (node->hasSpaceFor(key.size(), payload.size())) {
                    GuardX<BTreeNode> nodeLocked(std::move(node), LockConflictPolicy::Restart);
                    if (nodeLocked.retry()) {
                        goto restart;
                    }
                    nodeLocked->insertInPage(key, payload);
                    return;
                }
                const PID child_pid = node.pid;
                node.release();
                GuardX<BTreeNode> parentLocked(metadataPageId, LockConflictPolicy::Restart);
                if (parentLocked.retry()) {
                    goto restart;
                }
                if (reinterpret_cast<MetaDataPage*>(parentLocked.ptr)->getRoot(slotId) != child_pid) {
                    goto restart;
                }
                GuardX<BTreeNode> nodeLocked(child_pid, LockConflictPolicy::Restart);
                if (nodeLocked.retry()) {
                    goto restart;
                }
                auto ret = trySplit(std::move(nodeLocked), std::move(parentLocked), key, payload.size());
                if (ret == action_t::RESTART) {
                    goto restart;
                }
                goto restart;
            }

            meta.release();
            GuardO<BTreeNode> parent(std::move(node));
            const PID first_pid = parent->lookupInner(key);
            assert(first_pid != 0);
            GuardO<BTreeNode> child(first_pid, parent);
            if (child.retry()) {
                goto restart;
            }

            while (child->isInner()) {
                parent = std::move(child);
                const PID next_pid = parent->lookupInner(key);
                assert(next_pid != 0);
                child = GuardO<BTreeNode>(next_pid, parent);
                if (child.retry()) {
                    goto restart;
                }
            }

            if (child->hasSpaceFor(key.size(), payload.size())) {
                GuardX<BTreeNode> nodeLocked(std::move(child), LockConflictPolicy::Restart);
                if (nodeLocked.retry()) {
                    goto restart;
                }
                parent.release();
                nodeLocked->insertInPage(key, payload);
                return;
            }
            GuardX<BTreeNode> parentLocked(std::move(parent), LockConflictPolicy::Restart);
            if (parentLocked.retry()) {
                goto restart;
            }
            GuardX<BTreeNode> nodeLocked(std::move(child), LockConflictPolicy::Restart);
            if (nodeLocked.retry()) {
                goto restart;
            }
            auto ret = trySplit(std::move(nodeLocked), std::move(parentLocked), key, payload.size());
            if (ret == action_t::RESTART) {
                goto restart;
            }
            continue;
        }
    restart:
        bm.handleRestart();
    }
}

bool BTree::remove(std::span<u8> key) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
        {
            GuardO<MetaDataPage> meta(metadataPageId);
            if (meta.retry()) {
                goto restart;
            }
            GuardO<BTreeNode> parent(reinterpret_cast<MetaDataPage*>(meta.ptr)->getRoot(slotId), meta);
            if (parent.retry()) {
                goto restart;
            }
            meta.release();
            GuardO<BTreeNode> node(std::move(parent));
            if (node.retry()) {
                goto restart;
            }
            u16 pos;
            while (node->isInner()) {
                pos = node->lowerBound(key);
                PID nextPage = (pos == node->count) ? node->upperInnerNode : node->getChild(pos);
                parent = std::move(node);
                node = GuardO<BTreeNode>(nextPage, parent);
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
                GuardX<BTreeNode> parentLocked(std::move(parent), LockConflictPolicy::Restart);
                if (parentLocked.retry()) {
                    goto restart;
                }
                GuardX<BTreeNode> nodeLocked(std::move(node), LockConflictPolicy::Restart);
                if (nodeLocked.retry()) {
                    goto restart;
                }
                GuardX<BTreeNode> rightLocked(parentLocked->getChild(pos + 1));
                if (rightLocked.retry()) {
                    goto restart;
                }
                nodeLocked->removeSlot(slotId);
                if (rightLocked->freeSpaceAfterCompaction() >= (pageSize - BTreeNodeHeader::underFullSize)) {
                    (void)nodeLocked->mergeNodes(nodeLocked.pid, pos, parentLocked.ptr, rightLocked.ptr);
                }
            } else {
                GuardX<BTreeNode> nodeLocked(std::move(node), LockConflictPolicy::Restart);
                if (nodeLocked.retry()) {
                    goto restart;
                }
                parent.release();
                nodeLocked->removeSlot(slotId);
            }
            return true;
        }
    restart:
        bm.handleRestart();
    }
}
