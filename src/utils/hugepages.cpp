#include "hugepages.hpp"

#include "utils/my_asserts.hpp"

#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>


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
    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (ptr == MAP_FAILED) {
        throw std::runtime_error("mallocHugePages failed size=" + std::to_string(size));
    }

    memset(ptr, 0, size);
    return ptr;
}


void* HugePages::malloc_on_socket(size_t size, int numa_node) {
    size = roundToPageSize(size);
    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (ptr == MAP_FAILED) {
        throw std::runtime_error("mallocHugePages failed size=" + std::to_string(size));
    }

    numa_tonode_memory(ptr, size, numa_node);

    memset(ptr, 0, size);
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
