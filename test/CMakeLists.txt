include_directories(../btree)
file(GLOB BBTREE_CPPS "../btree/*.cpp")

add_executable(runUnitTests
        gtest.cpp
        insert_delete_test.cpp 
        concurrent_test.cpp ${BBTREE_CPPS})

target_link_libraries(runUnitTests
        gtest
        pthread)