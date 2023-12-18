cmake_minimum_required(VERSION 3.14)
project(TestBBTree)

include_directories(../zbtree)
include_directories(../zns)
file(GLOB ZBTREE_CPPS "../zbtree/*.cpp")
file(GLOB ZNS_CPPS "../zns/*.cpp")

set(CMAKE_CXX_FLAGS "-std=c++11 ")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -mavx512f -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g3 -march=native ")

include(FetchContent)
FetchContent_Declare(
  googletest
  # Specify the commit you depend on and update it regularly.
  URL https://github.com/google/googletest/archive/5376968f6948923e2411081fd9372e71a59d8e77.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Now simply link against gtest or gtest_main as needed. Eg
add_executable(UnitTests insert_delete_test.cpp ${ZBTREE_CPPS} ${ZNS_CPPS})
target_link_libraries(UnitTests gtest_main pthread zbd)
add_test(NAME runUnitTests COMMAND UnitTests)


add_executable(btree-olc benchmark.cpp ${ZBTREE_CPPS} ${ZNS_CPPS})
target_link_libraries(btree-olc
        jemalloc
        pthread
        zbd)


add_executable(zns_test ztest.cpp ${ZNS_CPPS} )
target_link_libraries(zns_test
        jemalloc
        pthread
        zbd)

# add_executable(smart smart.cpp )