#pragma once

#include "bm.hpp"
#include "config.hpp"
#include "guards.hpp"
#include "types.hpp"
#include "utils.hpp"

#include <cassert>
#include <span>

struct BTreeNodeHeader {
    static constexpr unsigned underFullSize = (pageSize / 2) + (pageSize / 4); // merge nodes more empty
    static constexpr u64 noNeighbour = ~0ull;

    struct FenceKeySlot {
        u16 offset;
        u16 len;
    };

    union {
        PID upperInnerNode;             // inner
        PID nextLeafNode = noNeighbour; // leaf
    };

    bool hasRightNeighbour() { return nextLeafNode != noNeighbour; }

    FenceKeySlot lowerFence = {0, 0}; // exclusive
    FenceKeySlot upperFence = {0, 0}; // inclusive

    bool hasLowerFence() { return !!lowerFence.len; };

    u16 count = 0;
    bool isLeaf;
    u16 spaceUsed = 0;
    u16 dataOffset = static_cast<u16>(pageSize);
    u16 prefixLen = 0;

    static constexpr unsigned hintCount = 16;
    u32 hint[hintCount];
    u32 padding;

    BTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf) {}
    ~BTreeNodeHeader() {}
};


struct BTreeNode : public BTreeNodeHeader {
    struct Slot {
        u16 offset;
        u16 keyLen;
        u16 payloadLen;
        union {
            u32 head;
            u8 headBytes[4];
        };
    } __attribute__((packed));
    union {
        Slot slot[(pageSize - sizeof(BTreeNodeHeader)) / sizeof(Slot)]; // grows from front
        u8 heap[pageSize - sizeof(BTreeNodeHeader)];                    // grows from back
    };

    static constexpr unsigned maxKVSize = ((pageSize - sizeof(BTreeNodeHeader) - (2 * sizeof(Slot)))) / 4;

    BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf) {}

    u8* ptr() { return reinterpret_cast<u8*>(this); }
    bool isInner() { return !isLeaf; }
    std::span<u8> getLowerFence() { return {ptr() + lowerFence.offset, lowerFence.len}; }
    std::span<u8> getUpperFence() { return {ptr() + upperFence.offset, upperFence.len}; }
    u8* getPrefix() { return ptr() + lowerFence.offset; } // any key on page is ok

    unsigned freeSpace() { return dataOffset - (reinterpret_cast<u8*>(slot + count) - ptr()); }
    unsigned freeSpaceAfterCompaction() { return pageSize - (reinterpret_cast<u8*>(slot + count) - ptr()) - spaceUsed; }

    bool hasSpaceFor(unsigned keyLen, unsigned payloadLen) {
        return spaceNeeded(keyLen, payloadLen) <= freeSpaceAfterCompaction();
    }

    u8* getKey(unsigned slotId) { return ptr() + slot[slotId].offset; }
    std::span<u8> getPayload(unsigned slotId) { return {ptr() + slot[slotId].offset + slot[slotId].keyLen, slot[slotId].payloadLen}; }

    PID getChild(unsigned slotId) { return loadUnaligned<PID>(getPayload(slotId).data()); }

    // How much space would inserting a new key of len "keyLen" require?
    unsigned spaceNeeded(unsigned keyLen, unsigned payloadLen) {
        return sizeof(Slot) + (keyLen - prefixLen) + payloadLen;
    }

    void makeHint() {
        unsigned dist = count / (hintCount + 1);
        for (unsigned i = 0; i < hintCount; i++)
            hint[i] = slot[dist * (i + 1)].head;
    }

    void updateHint(unsigned slotId) {
        unsigned dist = count / (hintCount + 1);
        unsigned begin = 0;
        if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
            begin = (slotId / dist) - 1;
        for (unsigned i = begin; i < hintCount; i++)
            hint[i] = slot[dist * (i + 1)].head;
    }

    void searchHint(u32 keyHead, u16& lowerOut, u16& upperOut) {
        if (count > hintCount * 2) {
            u16 dist = upperOut / (hintCount + 1);
            u16 pos, pos2;
            for (pos = 0; pos < hintCount; pos++)
                if (hint[pos] >= keyHead)
                    break;
            for (pos2 = pos; pos2 < hintCount; pos2++)
                if (hint[pos2] != keyHead)
                    break;
            lowerOut = pos * dist;
            if (pos2 < hintCount)
                upperOut = (pos2 + 1) * dist;
        }
    }

    // lower bound search, foundExactOut indicates if there is an exact match, returns slotId
    u16 lowerBound(std::span<u8> skey, bool& foundExactOut) {
        foundExactOut = false;

        // check prefix
        int cmp = memcmp(skey.data(), getPrefix(), min(skey.size(), prefixLen));
        if (cmp < 0) // key is less than prefix
            return 0;
        if (cmp > 0) // key is greater than prefix
            return count;
        if (skey.size() < prefixLen) // key is equal but shorter than prefix
            return 0;
        u8* key = skey.data() + prefixLen;
        unsigned keyLen = skey.size() - prefixLen;

        // check hint
        u16 lower = 0;
        u16 upper = count;
        u32 keyHead = head(key, keyLen);
        searchHint(keyHead, lower, upper);

        // binary search on remaining range
        while (lower < upper) {
            u16 mid = ((upper - lower) / 2) + lower;
            if (keyHead < slot[mid].head) {
                upper = mid;
            } else if (keyHead > slot[mid].head) {
                lower = mid + 1;
            } else { // head is equal, check full key
                int cmp = memcmp(key, getKey(mid), min(keyLen, slot[mid].keyLen));
                if (cmp < 0) {
                    upper = mid;
                } else if (cmp > 0) {
                    lower = mid + 1;
                } else {
                    if (keyLen < slot[mid].keyLen) { // key is shorter
                        upper = mid;
                    } else if (keyLen > slot[mid].keyLen) { // key is longer
                        lower = mid + 1;
                    } else {
                        foundExactOut = true;
                        return mid;
                    }
                }
            }
        }
        return lower;
    }

    // lowerBound wrapper ignoring exact match argument (for convenience)
    u16 lowerBound(std::span<u8> key) {
        bool ignore;
        return lowerBound(key, ignore);
    }


    // upper bound search: returns the first slot with key > skey
    u16 upperBound(std::span<u8> skey) {
        // check prefix
        int cmp = memcmp(skey.data(), getPrefix(), min(skey.size(), prefixLen));
        if (cmp < 0) // key is less than prefix => everything here is greater
            return 0;
        if (cmp > 0) // key is greater than prefix => nothing here is greater
            return count;
        if (skey.size() < prefixLen) // key is equal but shorter than prefix => everything here is greater
            return 0;

        u8* key = skey.data() + prefixLen;
        unsigned keyLen = skey.size() - prefixLen;

        // check hint
        u16 lower = 0;
        u16 upper = count;
        u32 keyHead = head(key, keyLen);
        searchHint(keyHead, lower, upper);

        // binary search on remaining range
        while (lower < upper) {
            u16 mid = ((upper - lower) / 2) + lower;
            if (keyHead < slot[mid].head) {
                upper = mid;
            } else if (keyHead > slot[mid].head) {
                lower = mid + 1;
            } else { // head is equal, check full key
                int cmp = memcmp(key, getKey(mid), min(keyLen, slot[mid].keyLen));
                if (cmp < 0) {
                    upper = mid; // skey < slot[mid] -> candidate upper bound
                } else if (cmp > 0) {
                    lower = mid + 1; // skey > slot[mid]
                } else {
                    // equal prefix + equal bytes up to min length: compare lengths
                    if (keyLen < slot[mid].keyLen) {
                        upper = mid; // shorter => skey < slot[mid]
                    } else if (keyLen > slot[mid].keyLen) {
                        lower = mid + 1; // longer  => skey > slot[mid]
                    } else {
                        // exact match: for *upper* bound we move right of the match
                        lower = mid + 1;
                    }
                }
            }
        }
        return lower;
    }

    // insert key/value pair
    void insertInPage(std::span<u8> key, std::span<u8> payload) {
        unsigned needed = spaceNeeded(key.size(), payload.size());
        if (needed > freeSpace()) {
            assert(needed <= freeSpaceAfterCompaction());
            compactify();
        }
        unsigned slotId = lowerBound(key);
        memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
        storeKeyValue(slotId, key, payload);
        count++;
        updateHint(slotId);
    }

    bool removeSlot(unsigned slotId) {
        spaceUsed -= slot[slotId].keyLen;
        spaceUsed -= slot[slotId].payloadLen;
        memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
        count--;
        makeHint();
        return true;
    }

    bool removeInPage(std::span<u8> key) {
        bool found;
        unsigned slotId = lowerBound(key, found);
        if (!found)
            return false;
        return removeSlot(slotId);
    }

    void copyNode(BTreeNodeHeader* dst, BTreeNodeHeader* src) {
        u64 ofs = offsetof(BTreeNodeHeader, upperInnerNode);
        memcpy(reinterpret_cast<u8*>(dst) + ofs, reinterpret_cast<u8*>(src) + ofs, sizeof(BTreeNode) - ofs);
    }

    void compactify() {
        unsigned should = freeSpaceAfterCompaction();
        static_cast<void>(should);
        BTreeNode tmp(isLeaf);
        tmp.setFences(getLowerFence(), getUpperFence());
        copyKeyValueRange(&tmp, 0, 0, count);
        tmp.upperInnerNode = upperInnerNode;
        copyNode(this, &tmp);
        makeHint();
        assert(freeSpace() == should);
    }

    // merge right node into this node
    bool mergeNodes(PID pid, unsigned slotId, BTreeNode* parent, BTreeNode* right) {
        if (!isLeaf)
            // TODO: implement inner merge
            return true;

        assert(right->isLeaf);
        assert(parent->isInner());
        BTreeNode tmp(isLeaf);
        tmp.setFences(getLowerFence(), right->getUpperFence());
        unsigned leftGrow = (prefixLen - tmp.prefixLen) * count;
        unsigned rightGrow = (right->prefixLen - tmp.prefixLen) * right->count;
        unsigned spaceUpperBound =
            spaceUsed + right->spaceUsed + (reinterpret_cast<u8*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
        if (spaceUpperBound > pageSize)
            return false;
        copyKeyValueRange(&tmp, 0, 0, count);
        right->copyKeyValueRange(&tmp, count, 0, right->count);
        memcpy(parent->getPayload(slotId + 1).data(), &pid, sizeof(PID));
        parent->removeSlot(slotId);
        tmp.makeHint();
        tmp.nextLeafNode = right->nextLeafNode;

        copyNode(this, &tmp);
        return true;
    }

    // store key/value pair at slotId
    void storeKeyValue(u16 slotId, std::span<u8> skey, std::span<u8> payload) {
        // slot
        u8* key = skey.data() + prefixLen;
        unsigned keyLen = skey.size() - prefixLen;
        slot[slotId].head = head(key, keyLen);
        slot[slotId].keyLen = keyLen;
        slot[slotId].payloadLen = payload.size();
        // key
        unsigned space = keyLen + payload.size();
        dataOffset -= space;
        spaceUsed += space;
        slot[slotId].offset = dataOffset;
        assert(getKey(slotId) >= reinterpret_cast<u8*>(&slot[slotId]));
        memcpy(getKey(slotId), key, keyLen);
        memcpy(getPayload(slotId).data(), payload.data(), payload.size());
    }

    void copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, unsigned srcCount) {
        if (prefixLen <= dst->prefixLen) { // prefix grows
            unsigned diff = dst->prefixLen - prefixLen;
            for (unsigned i = 0; i < srcCount; i++) {
                unsigned newKeyLen = slot[srcSlot + i].keyLen - diff;
                unsigned space = newKeyLen + slot[srcSlot + i].payloadLen;
                dst->dataOffset -= space;
                dst->spaceUsed += space;
                dst->slot[dstSlot + i].offset = dst->dataOffset;
                u8* key = getKey(srcSlot + i) + diff;
                memcpy(dst->getKey(dstSlot + i), key, space);
                dst->slot[dstSlot + i].head = head(key, newKeyLen);
                dst->slot[dstSlot + i].keyLen = newKeyLen;
                dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
            }
        } else {
            for (unsigned i = 0; i < srcCount; i++)
                copyKeyValue(srcSlot + i, dst, dstSlot + i);
        }
        dst->count += srcCount;
        assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<u8*>(dst->slot + dst->count));
    }

    void copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot) {
        unsigned fullLen = slot[srcSlot].keyLen + prefixLen;
        u8 key[fullLen];
        memcpy(key, getPrefix(), prefixLen);
        memcpy(key + prefixLen, getKey(srcSlot), slot[srcSlot].keyLen);
        dst->storeKeyValue(dstSlot, {key, fullLen}, getPayload(srcSlot));
    }

    void insertFence(FenceKeySlot& fk, std::span<u8> key) {
        assert(freeSpace() >= key.size());
        dataOffset -= key.size();
        spaceUsed += key.size();
        fk.offset = dataOffset;
        fk.len = key.size();
        memcpy(ptr() + dataOffset, key.data(), key.size());
    }

    void setFences(std::span<u8> lower, std::span<u8> upper) {
        insertFence(lowerFence, lower);
        insertFence(upperFence, upper);
        for (prefixLen = 0; (prefixLen < min(lower.size(), upper.size())) && (lower[prefixLen] == upper[prefixLen]); prefixLen++)
            ;
    }

    action_t splitNode(PID leftPID, BTreeNode* parent, unsigned sepSlot, std::span<u8> sep) {
        assert(sepSlot > 0);
        assert(sepSlot < (pageSize / sizeof(PID)));

        BTreeNode tmp(isLeaf);
        BTreeNode* nodeLeft = &tmp;

        AllocGuard<BTreeNode> newNode(isLeaf);
        if (newNode.retry()) {
            return action_t::RESTART;
        }
        BTreeNode* nodeRight = newNode.ptr;

        nodeLeft->setFences(getLowerFence(), sep);
        nodeRight->setFences(sep, getUpperFence());

        u16 oldParentSlot = parent->lowerBound(sep);
        if (oldParentSlot == parent->count) {
            assert(parent->upperInnerNode == leftPID);
            parent->upperInnerNode = newNode.pid;
        } else {
            assert(parent->getChild(oldParentSlot) == leftPID);
            memcpy(parent->getPayload(oldParentSlot).data(), &newNode.pid, sizeof(PID));
        }
        parent->insertInPage(sep, {reinterpret_cast<u8*>(&leftPID), sizeof(PID)});

        if (isLeaf) {
            copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
            copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
            nodeLeft->nextLeafNode = newNode.pid;
            nodeRight->nextLeafNode = this->nextLeafNode;
        } else {
            // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
            copyKeyValueRange(nodeLeft, 0, 0, sepSlot);
            copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
            nodeLeft->upperInnerNode = getChild(nodeLeft->count);
            nodeRight->upperInnerNode = upperInnerNode;
        }
        nodeLeft->makeHint();
        nodeRight->makeHint();
        copyNode(this, nodeLeft);
        return action_t::OK;
    }

    struct SeparatorInfo {
        unsigned len;     // len of new separator
        unsigned slot;    // slot at which we split
        bool isTruncated; // if true, we truncate the separator taking len bytes from slot+1
    };

    unsigned commonPrefix(unsigned slotA, unsigned slotB) {
        assert(slotA < count);
        unsigned limit = min(slot[slotA].keyLen, slot[slotB].keyLen);
        u8 *a = getKey(slotA), *b = getKey(slotB);
        unsigned i;
        for (i = 0; i < limit; i++)
            if (a[i] != b[i])
                break;
        return i;
    }

    SeparatorInfo findSeparator(bool splitOrdered) {
        assert(count > 1);
        if (isInner()) {
            // inner nodes are split in the middle
            unsigned slotId = count / 2;
            return SeparatorInfo{static_cast<unsigned>(prefixLen + slot[slotId].keyLen), slotId, false};
        }

        // find good separator slot
        unsigned bestPrefixLen, bestSlot;

        if (splitOrdered) {
            bestSlot = count - 2;
        } else if (count > 16) {
            unsigned lower = (count / 2) - (count / 16);
            unsigned upper = (count / 2);

            bestPrefixLen = commonPrefix(lower, 0);
            bestSlot = lower;

            if (bestPrefixLen != commonPrefix(upper - 1, 0))
                for (bestSlot = lower + 1; (bestSlot < upper) && (commonPrefix(bestSlot, 0) == bestPrefixLen); bestSlot++)
                    ;
        } else {
            bestSlot = (count - 1) / 2;
        }


        // try to truncate separator
        unsigned common = commonPrefix(bestSlot, bestSlot + 1);
        if ((bestSlot + 1 < count) && (slot[bestSlot].keyLen > common) && (slot[bestSlot + 1].keyLen > (common + 1)))
            return SeparatorInfo{prefixLen + common + 1, bestSlot, true};

        return SeparatorInfo{static_cast<unsigned>(prefixLen + slot[bestSlot].keyLen), bestSlot, false};
    }

    void getSep(u8* sepKeyOut, SeparatorInfo info) {
        memcpy(sepKeyOut, getPrefix(), prefixLen);
        memcpy(sepKeyOut + prefixLen, getKey(info.slot + info.isTruncated), info.len - prefixLen);
    }

    PID lookupInner(std::span<u8> key) {
        unsigned pos = lowerBound(key);
        if (pos == count)
            return upperInnerNode;
        return getChild(pos);
    }

    PID lookupInnerUpper(std::span<u8> key) {
        unsigned pos = upperBound(key);
        if (pos == count)
            return upperInnerNode;
        return getChild(pos);
    }
};

static_assert(sizeof(BTreeNode) == pageSize, "btree node size problem");
