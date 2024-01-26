# 设置CMake的最小版本要求
cmake_minimum_required(VERSION 3.10)

# 定义项目名称和版本
project(FDTREE VERSION 1.0)

# 设置源文件变量
set(SOURCE_FILES main.cpp)
include_directories(./src)
file(GLOB FDTREE_CPPS "./src/*.cpp")

# 添加一个可执行文件
add_executable(fdtree ${SOURCE_FILES} ${FDTREE_CPPS})

# 链接库（如果有的话）
# target_link_libraries(MyProject SomeLibrary)