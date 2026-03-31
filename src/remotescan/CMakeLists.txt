add_executable(remotescan
    main.cpp
    common.cpp
    config.cpp
    raid.cpp
    worker_base.cpp
    pinning.cpp
    server.cpp
    client.cpp
    epoll_libaio.cpp
)
target_link_libraries(remotescan liburing utils aio)
