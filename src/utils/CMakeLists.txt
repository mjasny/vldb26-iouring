
add_library(utils
    stats_printer.cpp
    cpu_map.cpp
    rdtsc_clock.cpp
    types.cpp
    hugepages.cpp
    small_pages.cpp
)
target_link_libraries(utils numa)
