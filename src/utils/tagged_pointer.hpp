#pragma once

#include <cassert>
#include <stdint.h>

template <typename T, int alignedTo>
class TaggedPointer {
private:
    static_assert(
        alignedTo != 0 && ((alignedTo & (alignedTo - 1)) == 0),
        "Alignment parameter must be power of two");

    // for 8 byte alignment tagMask = alignedTo - 1 = 8 - 1 = 7 = 0b111
    // i.e. the lowest three bits are set, which is where the tag is stored
    static const intptr_t tagMask = alignedTo - 1;

    // pointerMask is the exact contrary: 0b...11111000
    // i.e. all bits apart from the three lowest are set, which is where the pointer is stored
    static const intptr_t pointerMask = ~tagMask;

    // save us some reinterpret_casts with a union
    union {
        T* asPointer = nullptr;
        intptr_t asBits;
    };

public:
    inline TaggedPointer() = default;

    inline TaggedPointer(T* pointer, int tag) {
        set(pointer, tag);
    }

    inline static TaggedPointer from_u64(uint64_t value) {
        TaggedPointer ptr;
        ptr.asBits = value;
        return ptr;
    }

    inline uint64_t as_u64() {
        return asBits;
    }

    inline void set(T* pointer, int tag = 0) {
        // make sure that the pointer really is aligned
        assert((reinterpret_cast<intptr_t>(pointer) & tagMask) == 0);
        // make sure that the tag isn't too large
        assert((tag & pointerMask) == 0);

        asPointer = pointer;
        asBits |= tag;
    }

    inline T* getPointer() const {
        return reinterpret_cast<T*>(asBits & pointerMask);
    }
    inline int getTag() const {
        return asBits & tagMask;
    }

    inline intptr_t getBits() const {
        return asBits;
    }
};

// https://nikic.github.io/2012/02/02/Pointer-magic-for-efficient-dynamic-value-representations.html
// double number = 17.0;
// TaggedPointer<double, 8> taggedPointer(&number, 5);
// taggedPointer.getPointer(); // == &number
// taggedPointer.getTag(); // == 5
