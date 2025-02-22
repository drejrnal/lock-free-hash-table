cmake_minimum_required(VERSION 3.21)
project(tesench)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_INSTALL_DIR lib/cmake/folly CACHE STRING
        "The subdirectory where CMake package config files should be installed")

set(FOLLY_PACKAGE_CONFIG_FILE "/Users/luoxi/Library/CPPThirdPartyLibrary/folly/${CMAKE_INSTALL_DIR}")
set(CMAKE_PREFIX_PATH  ${FOLLY_PACKAGE_CONFIG_FILE})

add_subdirectory(third_party)

find_package(FOLLY CONFIG REQUIRED)
find_package(gflags CONFIG REQUIRED)
#find_package(GTest MODULE REQUIRED)
find_package(glog  REQUIRED)
#find_library(gflags NAMES GFlags_shared)

#MESSAGE(STATUS glog::glog )

include_directories(synchronization)

add_executable(main main.cpp splitOrderHashMap.h)

add_executable(concurrent_map_bench concurrentMapBench.cpp )
add_executable(mpmc_pipe MPMCPipelineBench.cpp)
add_executable(concurrent_map_unit concurrentMapUnitTest.cpp)

if (FOLLY_FOUND )
    message(STATUS "Folly library: ${FOLLY_LIBRARIES}")
    target_link_libraries(concurrent_map_bench ${FOLLY_LIBRARIES})
    target_link_libraries(concurrent_map_unit ${FOLLY_LIBRARIES})
    target_include_directories(concurrent_map_bench AFTER PUBLIC ${FOLLY_INCLUDE_DIRS})
    target_include_directories(concurrent_map_unit AFTER PUBLIC ${FOLLY_INCLUDE_DIRS})
    target_link_libraries(mpmc_pipe ${FOLLY_LIBRARIES})
    target_include_directories(mpmc_pipe AFTER PUBLIC ${FOLLY_INCLUDE_DIRS})
endif ()

if ( gflags_FOUND )
    message(STATUS "Found gflags library ${GFLAGS_LIBRARIES}")
    target_link_libraries(concurrent_map_bench ${GFLAGS_LIBRARIES})
    target_link_libraries(concurrent_map_unit ${GFLAGS_LIBRARIES})
    target_link_libraries(mpmc_pipe ${GFLAGS_LIBRARIES})
    target_link_libraries(main ${GFLAGS_LIBRARIES})
endif ()

target_link_libraries(main glog::glog)
target_link_libraries(main ${FOLLY_LIBRARIES})
target_include_directories(main AFTER PUBLIC ${FOLLY_INCLUDE_DIRS})

get_target_property(GTEST_INCLUDE_DIR gtest INCLUDE_DIRECTORIES)
MESSAGE(STATUS "GTest include: ${GTEST_INCLUDE_DIR}")

target_include_directories(concurrent_map_unit AFTER PUBLIC "${GTEST_INCLUDE_DIR}")
target_link_libraries(concurrent_map_unit gtest gmock_main)
#[[
if ( GTest_FOUND )
    message(STATUS "FOUND GTest library ${GTEST_LIBRARIES}")
    target_link_libraries(concurrent_map_unit ${GTEST_LIBRARIES})
    target_link_libraries(concurrent_map_bench ${GTEST_LIBRARIES})
endif()]]
