cmake_minimum_required(VERSION 3.16)
include_directories(../include)
file(GLOB BTREE_CPPS "../include/*.cpp")

set(CMAKE_CXX_FLAGS "-std=c++1z")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -mavx512f -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g3 -march=native ")

add_executable(runUnitTests
        gtest.cpp
        insert_delete_test.cpp ../zbtree/BTreeOLC.h 
         ${BTREE_CPPS})

target_link_libraries(runUnitTests
        gtest
        pthread)

# set(CMAKE_CXX_FLAGS_DEBUG "-g -O3 -march=native -DDEBUG")

add_executable(btree-olc benchmark.cpp ../zbtree/BTreeOLC.h    ${BTREE_CPPS})
target_link_libraries(btree-olc
        jemalloc
        pthread)


add_executable(smart smart.cpp    ${BTREE_CPPS})