set(SOURCES
    shufflev2.cpp
)

foreach(SRC_FILE ${SOURCES})
    get_filename_component(EXE_NAME ${SRC_FILE} NAME_WE) # Extract filename without extension
    add_executable(${EXE_NAME} ${SRC_FILE})
    target_link_libraries(${EXE_NAME} liburing utils)
endforeach()
