#include "hugepages.hpp"

#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"

#include <fcntl.h>
#include <mutex>
#include <numa.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>

namespace {

void log_hugepage_fallback_once(size_t size) {
    static std::once_flag once;
    std::call_once(once, [size] {
        Logger::info("HugePages fallback to regular mmap size=", size);
    });
}

void* mmap_with_fallback(size_t size) {
    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (ptr != MAP_FAILED) {
        memset(ptr, 0, size);
        return ptr;
    }

    ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        throw std::runtime_error("mmap failed size=" + std::to_string(size));
    }

    madvise(ptr, size, MADV_HUGEPAGE);
    memset(ptr, 0, size);
    log_hugepage_fallback_once(size);
    return ptr;
}

} // namespace


HugePages::HugePages() : size(0), addr(nullptr) {}
HugePages::HugePages(size_t size) : size(size), addr(malloc(size)) {}
HugePages::HugePages(size_t size, int numa_node) : size(size), addr(malloc_on_socket(size, numa_node)) {}

HugePages::~HugePages() {
    if (addr && size > 0) {
        free(addr, size);
    }
}


void* HugePages::malloc(size_t size) {
    size = roundToPageSize(size);
    return mmap_with_fallback(size);
}


void* HugePages::malloc_on_socket(size_t size, int numa_node) {
    size = roundToPageSize(size);
    void* ptr = mmap_with_fallback(size);

    numa_tonode_memory(ptr, size, numa_node);
    return ptr;
}

void* HugePages::malloc_interleaved(size_t size) {
    size = roundToPageSize(size);
    void* ptr = mmap_with_fallback(size);
    // Distribute physical pages round-robin across all NUMA nodes (chiplets on
    // single-socket EPYC). This gives all worker groups equal average latency
    // instead of letting first-touch skew everything to one chiplet.
    if (numa_available() >= 0) {
        struct bitmask* all = numa_all_nodes_ptr;
        numa_interleave_memory(ptr, size, all);
    }
    return ptr;
}

void* HugePages::malloc_file_backed(size_t size) {
    static const char* hugepath = "/mnt/huge/hugefile";

    int fd = open(hugepath, O_CREAT | O_RDWR, 0755);
    if (fd < 0) {
        throw std::runtime_error("open(hugepage file) failed size=" + std::to_string(size));
    }

    if (ftruncate(fd, size) != 0) {
        close(fd);
        throw std::runtime_error("truncate failed size=" + std::to_string(size));
    }

    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        throw std::runtime_error("mallocHugePages failed size=" + std::to_string(size));
    }
    close(fd);
    return ptr;
}

void HugePages::free(void* ptr, size_t size) {
    size = roundToPageSize(size);
    check_ret(munmap(ptr, size));
}
