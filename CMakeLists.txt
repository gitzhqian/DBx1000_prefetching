cmake_minimum_required(VERSION 3.17)
project(Dbx1000)

SET (CMAKE_C_COMPILER "gcc")
SET (CMAKE_CXX_COMPILER "g++")
SET (CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

######################## Intel PCM ########################
add_custom_command(OUTPUT libPCM.a
        COMMAND make lib
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/pcm)

add_custom_target(pcm DEPENDS libPCM.a)
###########################################################

add_definitions(-DNOGRAPHITE=1)

# include header files
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/benchmarks/ ${PROJECT_SOURCE_DIR}/concurrency_control/ ${PROJECT_SOURCE_DIR}/storage/ ${PROJECT_SOURCE_DIR}/system/ ${PROJECT_SOURCE_DIR}/pcm/)
# lib files
#LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/libs)
file(GLOB_RECURSE SRC_FILES benchmarks/*.cpp concurrency_control/*.cpp storage/*.cpp system/*.cpp config.cpp)
add_executable(rundb ${SRC_FILES})
#target_link_libraries(rundb libpthread.so libjemalloc.so ${PROJECT_SOURCE_DIR}/pcm/libPCM.a)
target_link_libraries(rundb tbb libpthread.so libjemalloc.so libcityhash.so)