add_library(buffer_mgr_mt_utils
    ../tpcc/random_generator.cpp
    ../tpcc/types.cpp
    ../tpcc/tpcc_globals.cpp

    btree.cpp
    config.cpp
    bm.cpp
)
target_include_directories(buffer_mgr_mt_utils PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(buffer_mgr_mt_utils utils liburing)
target_link_libraries(buffer_mgr_mt_utils boost_fiber)

add_executable(buffer_mgr_mt buffer_mgr_mt.cpp)
target_include_directories(buffer_mgr_mt PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(buffer_mgr_mt liburing utils buffer_mgr_mt_utils)
target_link_libraries(buffer_mgr_mt boost_fiber)
target_link_libraries(buffer_mgr_mt aio)

add_executable(buffer_mgr_mt_smoke_test test/mt_smoke_test.cpp)
target_include_directories(buffer_mgr_mt_smoke_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(buffer_mgr_mt_smoke_test liburing utils boost_fiber aio)

add_executable(buffer_mgr_mt_runtime_test test/mt_runtime_test.cpp)
target_include_directories(buffer_mgr_mt_runtime_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(buffer_mgr_mt_runtime_test liburing utils buffer_mgr_mt_utils boost_fiber aio)

add_executable(buffer_mgr_mt_wake_bench test/mt_wake_bench.cpp)
target_include_directories(buffer_mgr_mt_wake_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(buffer_mgr_mt_wake_bench liburing utils buffer_mgr_mt_utils boost_fiber aio)

add_executable(buffer_mgr_mt_system_test test/mt_system_test.cpp)
target_include_directories(buffer_mgr_mt_system_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(buffer_mgr_mt_system_test liburing utils buffer_mgr_mt_utils boost_fiber aio)

add_executable(buffer_mgr_mt_eviction_test test/mt_eviction_test.cpp)
target_include_directories(buffer_mgr_mt_eviction_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(buffer_mgr_mt_eviction_test liburing utils buffer_mgr_mt_utils boost_fiber aio)

add_executable(buffer_mgr_mt_multiworker_eviction_test test/mt_multiworker_eviction_test.cpp)
target_include_directories(buffer_mgr_mt_multiworker_eviction_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(buffer_mgr_mt_multiworker_eviction_test liburing utils buffer_mgr_mt_utils boost_fiber aio)

add_executable(buffer_mgr_mt_multiworker_progress_test test/mt_multiworker_progress_test.cpp)
target_include_directories(buffer_mgr_mt_multiworker_progress_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(buffer_mgr_mt_multiworker_progress_test liburing utils buffer_mgr_mt_utils boost_fiber aio)

add_executable(buffer_mgr_mt_all_tests test/mt_all_tests.cpp)
target_include_directories(buffer_mgr_mt_all_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(buffer_mgr_mt_all_tests utils)

if(BUILD_TESTING)
    add_test(NAME buffer_mgr_mt_smoke_test_posix COMMAND buffer_mgr_mt_smoke_test --backend=posix)
    add_test(NAME buffer_mgr_mt_smoke_test_uring COMMAND buffer_mgr_mt_smoke_test --backend=uring)

    add_test(NAME buffer_mgr_mt_system_test_posix COMMAND buffer_mgr_mt_system_test --backend=posix)
    add_test(NAME buffer_mgr_mt_system_test_uring COMMAND buffer_mgr_mt_system_test --backend=uring)

    add_test(NAME buffer_mgr_mt_eviction_test_posix COMMAND buffer_mgr_mt_eviction_test --backend=posix)
    add_test(NAME buffer_mgr_mt_eviction_test_uring COMMAND buffer_mgr_mt_eviction_test --backend=uring)

    add_test(NAME buffer_mgr_mt_multiworker_eviction_test_posix COMMAND buffer_mgr_mt_multiworker_eviction_test --backend=posix)
    add_test(NAME buffer_mgr_mt_multiworker_eviction_test_uring COMMAND buffer_mgr_mt_multiworker_eviction_test --backend=uring)

    add_test(NAME buffer_mgr_mt_multiworker_progress_test_posix_2 COMMAND buffer_mgr_mt_multiworker_progress_test --backend=posix --workers=2)
    add_test(NAME buffer_mgr_mt_multiworker_progress_test_posix_4 COMMAND buffer_mgr_mt_multiworker_progress_test --backend=posix --workers=4)
    add_test(NAME buffer_mgr_mt_multiworker_progress_test_uring_2 COMMAND buffer_mgr_mt_multiworker_progress_test --backend=uring --workers=2)
    add_test(NAME buffer_mgr_mt_multiworker_progress_test_uring_4 COMMAND buffer_mgr_mt_multiworker_progress_test --backend=uring --workers=4)

    add_test(NAME buffer_mgr_mt_runtime_test_posix COMMAND buffer_mgr_mt_runtime_test --backend=posix)
    add_test(NAME buffer_mgr_mt_runtime_test_uring COMMAND buffer_mgr_mt_runtime_test --backend=uring)

    add_test(NAME buffer_mgr_mt_all_tests_posix COMMAND buffer_mgr_mt_all_tests --backend=posix)
    add_test(NAME buffer_mgr_mt_all_tests_uring COMMAND buffer_mgr_mt_all_tests --backend=uring)
endif()
