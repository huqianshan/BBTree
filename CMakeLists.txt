cmake_minimum_required(VERSION 3.2)

if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif(POLICY CMP0048)
project(BBTree VERSION 0.1)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type selected, default to Release")
  set(CMAKE_BUILD_TYPE Release)
else()
  message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

##
## compiler flags
##
set(CMAKE_CXX_FLAGS "-Wall -Werror -pthread -std=c++1z")
set(CMAKE_CXX_FLAGS_DEBUG "-O2 -g3 -fno-inline -fno-omit-frame-pointer") #-fsanitize=leak,address,undefined
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")


include_directories(./btree)
set(ZNS_LIBS zbd)
set(TEST_LIBS gtest gmock pthread)
file(GLOB BBTREE_CPPS "./btree/*.cpp")

## test.cpp
# add_executable(bbtree test.cpp ${BBTREE_CPPS})
# target_link_libraries(bbtree)


## functional test
# add_executable(bbtest ./test/gtest.cpp ./test/btree_concurrent_test.cpp  ${BBTREE_CPPS})
# target_link_libraries(bbtest ${TEST_LIBS})
# enable_testing()
# 查找 GTest 库
# find_package(GTest REQUIRED)
add_subdirectory(test)





