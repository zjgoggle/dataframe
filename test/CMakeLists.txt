project(Unittests)
# cmake -G "Eclipse CDT4 - Unix Makefiles" -D_ECLIPSE_VERSION=4.2 -DCMAKE_BUILD_TYPE=Debug ../build
cmake_minimum_required( VERSION 2.6 )

set(PROJNAME DataFrameUnittests)

add_definitions(-std=c++17)

file(GLOB SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/*.h
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
)

add_executable( ${PROJNAME} ${SRC} )

target_include_directories(${PROJNAME} PRIVATE ./ ../src)
target_link_libraries( ${PROJNAME} dataframe)

