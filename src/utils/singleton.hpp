#pragma once


template <typename T>
struct Singleton {
    static T& get() {
        static T instance;
        return instance;
    }
};
