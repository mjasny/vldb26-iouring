#pragma once

#include "bm.hpp"
#include "types.hpp"
#include "utils.hpp"

#include <cassert>


template <class T>
struct GuardS {
    PID pid = moved;
    T* ptr;
    static const u64 moved = ~0ull;

    GuardS() = delete;

    // constructor
    explicit GuardS(u64 pid) : pid(pid) {
        ptr = reinterpret_cast<T*>(bm.fixS(pid));
    }

    GuardS(GuardS&& other) {
        assert(pid != other.pid);
        if (pid != moved && ptr)
            bm.unfixS(pid);
        pid = other.pid;
        ptr = other.ptr;
        other.pid = moved;
        other.ptr = nullptr;
    }

    // assignment operator
    GuardS& operator=(const GuardS&) = delete;

    // move assignment operator
    GuardS& operator=(GuardS&& other) {
        assert(pid != other.pid);
        if (pid != moved && ptr)
            bm.unfixS(pid);
        pid = other.pid;
        ptr = other.ptr;
        other.pid = moved;
        other.ptr = nullptr;
        return *this;
    }

    // copy constructor
    GuardS(const GuardS&) = delete;

    // destructor
    ~GuardS() {
        if (pid != moved && ptr)
            bm.unfixS(pid);
    }

    T* operator->() {
        assert(pid != moved);
        return ptr;
    }

    void release() {
        if (pid != moved && ptr) {
            bm.unfixS(pid);
            pid = moved;
        }
    }


    bool retry() {
        return ptr == nullptr;
    }
};

template <class T>
struct GuardX {
    PID pid = moved;
    T* ptr;
    static const u64 moved = ~0ull;

    // constructor
    GuardX() : pid(moved), ptr(nullptr) {}

    // constructor
    explicit GuardX(u64 pid) : pid(pid) {
        ptr = reinterpret_cast<T*>(bm.fixX(pid));
        // ptr->hdr.dirty = true;
    }


    explicit GuardX(GuardS<T>&& other) {
        assert(other.pid != moved);

        pid = other.pid;
        ptr = other.ptr;
        other.pid = moved;
        other.ptr = nullptr;
    }

    GuardX(GuardX&& other) {
        assert(pid != other.pid);
        if (pid != moved && ptr)
            bm.unfixX(pid);
        pid = other.pid;
        ptr = other.ptr;
        other.pid = moved;
        other.ptr = nullptr;
    }

    // assignment operator
    GuardX& operator=(const GuardX&) = delete;

    // move assignment operator
    GuardX& operator=(GuardX&& other) {
        assert(pid != other.pid);
        if (pid != moved && ptr) {
            bm.unfixX(pid);
        }
        pid = other.pid;
        ptr = other.ptr;
        other.pid = moved;
        other.ptr = nullptr;
        return *this;
    }

    // copy constructor
    GuardX(const GuardX&) = delete;

    // destructor
    ~GuardX() {
        if (pid != moved && ptr)
            bm.unfixX(pid);
    }

    T* operator->() {
        assert(pid != moved);
        return ptr;
    }

    void release() {
        if (pid != moved && ptr) {
            bm.unfixX(pid);
            pid = moved;
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
        }
        // GuardX<T>::pid = bm.toPID(GuardX<T>::ptr);
        GuardX<T>::pid = bm.allocCount - 1; // TODO make this nicer with allocPage
    }
};
