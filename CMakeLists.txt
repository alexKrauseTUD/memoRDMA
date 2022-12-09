project(memoRDMA)

cmake_minimum_required(VERSION 3.16)

set(SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)

set(CMAKE_CXX_STANDARD 20)
set(warnings "-Wall -Wextra -pedantic")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${warnings}")
set(OPTIMIZE_OPTIONS "-O3")

include_directories(${INC_DIR})

file(GLOB_RECURSE SOURCES ${SRC_DIR}/*.cpp)
file(GLOB_RECURSE HEADERS ${INC_DIR}/*.h)

set(SRC_SRV ${SOURCES})

if(EXISTS "${PROJECT_SOURCE_DIR}/memo.conf")
    CONFIGURE_FILE("memo.conf" ${CMAKE_BINARY_DIR}/ COPYONLY)
else()
    message( "Beware! Existing Config in binary dir but none found in root dir.")
endif()

# Remove all definitions from CMakeCache
remove_definitions(-DMEMO_NOLOGGING -DMEMO_NOINFOLOGGING -DMEMO_NODEBUGLOGGING)

# Add Logger Configuration as you please
# add_definitions(-DMEMO_NOLOGGING)
# add_definitions(-DMEMO_NOINFOLOGGING)
# add_definitions(-DMEMO_NODEBUGLOGGING)

add_executable(memoRDMA_server ${SRC_SRV} ${HEADERS})
target_compile_options(memoRDMA_server PRIVATE "${OPTIMIZE_OPTIONS}")
target_link_libraries(memoRDMA_server "ibverbs" "pthread")
