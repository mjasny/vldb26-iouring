#pragma once

#include "bm.hpp"
#include "types.hpp"
#include "utils.hpp"

#include <cassert>


template <class T>
struct GuardO {
    PID pid = moved;
    BID bid = 0;
    T* ptr = nullptr;
    u64 version = 0;
    static const u64 moved = ~0ull;

    GuardO() = delete;

    explicit GuardO(u64 pid) : pid(pid) {
        ptr = reinterpret_cast<T*>(bm.fixO(pid, version));
        if (ptr) {
            bid = bm.to_bid(reinterpret_cast<Page*>(ptr));
        }
    }

    template <class U>
    GuardO(u64 pid, const GuardO<U>& parent) : pid(pid) {
        if (!parent.validate()) {
            ptr = nullptr;
            bm.request_restart(ValidationRestart {});
            return;
        }
        ptr = reinterpret_cast<T*>(bm.fixO(pid, version));
        if (ptr && !parent.validate()) {
            ptr = nullptr;
            this->pid = moved;
            bid = 0;
            version = 0;
            bm.request_restart(ValidationRestart {});
        } else if (ptr) {
            bid = bm.to_bid(reinterpret_cast<Page*>(ptr));
        }
    }

    GuardO(GuardO&& other) noexcept
        : pid(other.pid), bid(other.bid), ptr(other.ptr), version(other.version) {
        other.pid = moved;
        other.bid = 0;
        other.ptr = nullptr;
        other.version = 0;
    }

    GuardO& operator=(GuardO&& other) noexcept {
        if (this == &other)
            return *this;
        pid = other.pid;
        bid = other.bid;
        ptr = other.ptr;
        version = other.version;
        other.pid = moved;
        other.bid = 0;
        other.ptr = nullptr;
        other.version = 0;
        return *this;
    }

    GuardO(const GuardO&) = delete;
    GuardO& operator=(const GuardO&) = delete;

    T* operator->() {
        assert(pid != moved);
        return ptr;
    }

    bool validate() const {
        return pid != moved && ptr && bm.validateO(pid, bid, version);
    }

    bool retry() const {
        return ptr == nullptr;
    }

    void release() {
        pid = moved;
        bid = 0;
        ptr = nullptr;
        version = 0;
    }
};

template <class T>
struct GuardS {
    PID pid = moved;
    BID bid = 0;
    T* ptr = nullptr;
    static const u64 moved = ~0ull;

    GuardS() = delete;

    // constructor
    explicit GuardS(u64 pid) : pid(pid) {
        ptr = reinterpret_cast<T*>(bm.fixS(pid));
        if (ptr) {
            bid = bm.to_bid(reinterpret_cast<Page*>(ptr));
        }
    }

    explicit GuardS(GuardO<T>&& other) {
        assert(other.pid != moved);
        pid = other.pid;
        ptr = reinterpret_cast<T*>(bm.fixS(other.pid, other.bid, other.version));
        if (ptr) {
            bid = bm.to_bid(reinterpret_cast<Page*>(ptr));
            other.release();
        } else {
            pid = moved;
            bid = 0;
        }
    }

    GuardS(GuardS&& other) : pid(other.pid), bid(other.bid), ptr(other.ptr) {
        other.pid = moved;
        other.bid = 0;
        other.ptr = nullptr;
    }

    // assignment operator
    GuardS& operator=(const GuardS&) = delete;

    // move assignment operator
    GuardS& operator=(GuardS&& other) {
        if (this == &other) {
            return *this;
        }
        if (pid != moved && ptr)
            bm.unfixS_bid(bid);
        pid = other.pid;
        bid = other.bid;
        ptr = other.ptr;
        other.pid = moved;
        other.bid = 0;
        other.ptr = nullptr;
        return *this;
    }

    // copy constructor
    GuardS(const GuardS&) = delete;

    // destructor
    ~GuardS() {
        if (pid != moved && ptr)
            bm.unfixS_bid(bid);
    }

    T* operator->() {
        assert(pid != moved);
        return ptr;
    }

    void release() {
        if (pid != moved && ptr) {
            bm.unfixS_bid(bid);
            pid = moved;
            bid = 0;
        }
    }


    bool retry() {
        return ptr == nullptr;
    }
};

template <class T>
struct GuardX {
    PID pid = moved;
    BID bid = 0;
    T* ptr;
    static const u64 moved = ~0ull;

    // constructor
    GuardX() : pid(moved), ptr(nullptr) {}

    // constructor
    explicit GuardX(u64 pid) : pid(pid) {
        ptr = reinterpret_cast<T*>(bm.fixX(pid));
        if (ptr) {
            bid = bm.to_bid(reinterpret_cast<Page*>(ptr));
        }
        // ptr->hdr.dirty = true;
    }

    explicit GuardX(u64 pid, LockConflictPolicy wait_policy) : pid(pid) {
        ptr = reinterpret_cast<T*>(bm.fixX(pid, wait_policy));
        if (ptr) {
            bid = bm.to_bid(reinterpret_cast<Page*>(ptr));
        }
    }


    explicit GuardX(GuardS<T>&& other) = delete;

    explicit GuardX(GuardO<T>&& other, LockConflictPolicy wait_policy = LockConflictPolicy::Wait) {
        assert(other.pid != moved);
        pid = other.pid;
        ptr = reinterpret_cast<T*>(bm.fixX(other.pid, other.bid, other.version, wait_policy));
        if (ptr) {
            bid = bm.to_bid(reinterpret_cast<Page*>(ptr));
            other.release();
        } else {
            pid = moved;
            bid = 0;
        }
    }

    GuardX(GuardX&& other) : pid(other.pid), bid(other.bid), ptr(other.ptr) {
        other.pid = moved;
        other.bid = 0;
        other.ptr = nullptr;
    }

    // assignment operator
    GuardX& operator=(const GuardX&) = delete;

    // move assignment operator
    GuardX& operator=(GuardX&& other) {
        if (this == &other) {
            return *this;
        }
        if (pid != moved && ptr) {
            bm.unfixX_bid(bid);
        }
        pid = other.pid;
        bid = other.bid;
        ptr = other.ptr;
        other.pid = moved;
        other.bid = 0;
        other.ptr = nullptr;
        return *this;
    }

    // copy constructor
    GuardX(const GuardX&) = delete;

    // destructor
    ~GuardX() {
        if (pid != moved && ptr)
            bm.unfixX_bid(bid);
    }

    T* operator->() {
        assert(pid != moved);
        return ptr;
    }

    void release() {
        if (pid != moved && ptr) {
            bm.unfixX_bid(bid);
            pid = moved;
            bid = 0;
        }
    }

    bool retry() {
        return ptr == nullptr;
    }
};

template <class T>
struct AllocGuard : public GuardX<T> {
    template <typename... Params>
    AllocGuard(Params&&... params) {
        GuardX<T>::ptr = reinterpret_cast<T*>(bm.allocPage());
        if (GuardX<T>::ptr) {
            new (GuardX<T>::ptr) T(std::forward<Params>(params)...);
            GuardX<T>::bid = bm.to_bid(reinterpret_cast<Page*>(GuardX<T>::ptr));
            GuardX<T>::pid = bm.buffer_frames[GuardX<T>::bid].pid;
        } else {
            GuardX<T>::pid = GuardX<T>::moved;
            GuardX<T>::bid = 0;
        }
    }
};
