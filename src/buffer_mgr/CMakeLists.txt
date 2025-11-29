
add_library(buffer_mgr_utils
    tpcc/random_generator.cpp
    tpcc/types.cpp
    tpcc/tpcc_globals.cpp

    btree.cpp
    config.cpp
    bm.cpp

)
target_link_libraries(buffer_mgr_utils utils liburing)


set(SOURCES
    buffer_mgr.cpp
)

foreach(SRC_FILE ${SOURCES})
    get_filename_component(EXE_NAME ${SRC_FILE} NAME_WE) # Extract filename without extension
    add_executable(${EXE_NAME} ${SRC_FILE})
    target_link_libraries(${EXE_NAME} liburing utils buffer_mgr_utils)


    #target_compile_definitions(${EXE_NAME} PRIVATE BOOST_FIBERS_NO_ATOMICS)
    target_link_libraries(${EXE_NAME} boost_fiber)
    target_link_libraries(${EXE_NAME} aio)
endforeach()
