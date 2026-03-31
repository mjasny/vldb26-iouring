#pragma once

#include "bm.hpp"
#include "config.hpp"

#include <string_view>

namespace mt_test {

enum class Backend {
    Posix,
    Uring,
};

inline Backend parse_backend(int argc, char** argv) {
    Backend backend = Backend::Posix;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--backend=posix") {
            backend = Backend::Posix;
        } else if (arg == "--backend=uring") {
            backend = Backend::Uring;
        } else {
            ensure(false, "unknown backend arg");
        }
    }
    return backend;
}

inline void apply_backend(Backend backend) {
    auto& cfg = Config::get();
    cfg.sync_variant = true;
    cfg.posix_variant = (backend == Backend::Posix);
    BufferManager::sync_variant = true;
    BufferManager::posix_variant = (backend == Backend::Posix);
}

inline const char* backend_name(Backend backend) {
    return backend == Backend::Posix ? "posix" : "uring";
}

} // namespace mt_test
