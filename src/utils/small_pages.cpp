#include "small_pages.hpp"

#include "my_asserts.hpp"

#include <stdexcept>
#include <string>
#include <sys/mman.h>


SmallPages::SmallPages(size_t size) : size(size), addr(malloc(size)) {}

SmallPages::~SmallPages() {
    if (addr && size > 0) {
        free(addr, size);
    }
}


void* SmallPages::malloc(size_t size) {
    size = roundToPageSize(size);
    void* ptr = ::malloc(size);
    check_ptr(ptr);

    memset(ptr, 0, size);
    return ptr;
}


void SmallPages::free(void* ptr, size_t size) {
    size = roundToPageSize(size);
    ::free(ptr);
}
