project(memoRDMA)

cmake_minimum_required(VERSION 3.16)

set(SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)

set(CMAKE_CXX_STANDARD 14)
set(warnings "-Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${warnings}")

include_directories(${INC_DIR})

file(GLOB_RECURSE SOURCES ${SRC_DIR}/*.cpp)
file(GLOB_RECURSE HEADERS ${INC_DIR}/*.h)

set(SRC_SRV ${SOURCES})
set(SRC_CLT ${SOURCES})

list(REMOVE_ITEM SRC_SRV "${CMAKE_CURRENT_SOURCE_DIR}/src/memoRDMA_client.cpp")
list(REMOVE_ITEM SRC_CLT "${CMAKE_CURRENT_SOURCE_DIR}/src/memoRDMA_server.cpp")

#add_executable(memoRDMA_client ${SRC_CLT} ${HEADERS})
add_executable(memoRDMA_server ${SRC_SRV} ${HEADERS})

#target_link_libraries(memoRDMA_client "ibverbs" "pthread")
target_link_libraries(memoRDMA_server "ibverbs" "pthread")