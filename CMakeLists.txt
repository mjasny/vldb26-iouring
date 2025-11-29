cmake_minimum_required(VERSION 3.20)
project(ringding)

# Set C++ Standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE INTERNAL "")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()


add_compile_options("-fcoroutines")
add_compile_options("-g")

set(CMAKE_CXX_FLAGS "-Wall -Wextra") # Enable debugging and define DEBUG

set(CMAKE_CXX_FLAGS "-Waddress-of-packed-member -march=native -mavx512f -mavx512bw -march=native") 


# Set the different build type flags
set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG -fsanitize=address -fsanitize=undefined -fno-sanitize=alignment") # Enable debugging and define DEBUG
set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG") 

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -flto") # Enable optimizations for Release



# Include ExternalProject module
include(ExternalProject)

# Define liburing as an external project
ExternalProject_Add(liburing_build
    PREFIX ${CMAKE_BINARY_DIR}/liburing
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/libs/liburing
    CONFIGURE_COMMAND ""  # No configure command
    BUILD_COMMAND $(MAKE)  # Default build command
    BUILD_IN_SOURCE TRUE   # Build in source
    INSTALL_COMMAND ""    # No install command
    BUILD_ALWAYS TRUE 
)

add_library(liburing STATIC IMPORTED)
set_target_properties(liburing PROPERTIES
    IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/libs/liburing/src/liburing.a
    INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_SOURCE_DIR}/libs/liburing/src/include
)

# Since liburing is an external project, ensure it is built before it is used
add_dependencies(liburing liburing_build)


include(FetchContent)

set(BOOST_INCLUDE_LIBRARIES fiber)
set(BOOST_ENABLE_CMAKE ON CACHE BOOL "" FORCE)
set(BOOST_INCLUDE_TEST OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
  Boost
  URL https://github.com/boostorg/boost/releases/download/boost-1.89.0/boost-1.89.0-cmake.7z
  DOWNLOAD_EXTRACT_TIMESTAMP ON
)
FetchContent_MakeAvailable(Boost)



include_directories(src/)

add_subdirectory(src)



