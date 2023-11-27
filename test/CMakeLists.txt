include_directories(../btree)
file(GLOB BBTREE_CPPS "../btree/*.cpp")

add_executable(runUnitTests
        gtest.cpp
        insert_delete_test.cpp 
        concurrent_test.cpp ${BBTREE_CPPS})

target_link_libraries(runUnitTests
        gtest
        pthread)

set(CMAKE_CXX_FLAGS "-std=c++1z")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")

add_executable(bbtree benchmark.cpp    ${BBTREE_CPPS})
target_link_libraries(bbtree
        jemalloc
        pthread)


add_executable(smart smart.cpp    ${BBTREE_CPPS})