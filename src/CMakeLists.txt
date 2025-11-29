add_subdirectory(utils)
add_subdirectory(shuffle)
add_subdirectory(buffer_mgr)


set(SOURCES
    bench_buf_reg.cpp
    bench_nop.cpp
    bench_fsync.cpp
    bench_sqpoll_wakeup.cpp
    bench_ssd_batches.cpp
    epoll_server.cpp epoll_client.cpp
    server_bw.cpp client_bw.cpp
    server.cpp client.cpp
    zc_recv.cpp
)

foreach(SRC_FILE ${SOURCES})
    get_filename_component(EXE_NAME ${SRC_FILE} NAME_WE) # Extract filename without extension
    add_executable(${EXE_NAME} ${SRC_FILE})
    target_link_libraries(${EXE_NAME} liburing utils)
endforeach()



