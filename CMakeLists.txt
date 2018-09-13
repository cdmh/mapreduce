cmake_minimum_required(VERSION 3.12)
project(mapreduce)
set(CMAKE_CXX_STANDARD 14)
set(Boost_USE_STATIC_LIBS        ON) # only find static libs
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost 1.36 REQUIRED system filesystem iostreams)

MESSAGE(STATUS "find boost ${Boost_INCLUDE_DIRS} ${Boost_LIBRARIES}")

include_directories(${Boost_INCLUDE_DIRS})
include_directories(include)
include_directories(include/detail)
include_directories(include/detail/intermediates)
include_directories(include/detail/schedule_policy)

add_executable(wordcount examples/wordcount/wordcount.cpp)
add_executable(prime examples/prime/prime.cpp)
add_executable(friends examples/friends/friends.cpp)
target_link_libraries(wordcount ${Boost_LIBRARIES} pthread)
target_link_libraries(friends ${Boost_LIBRARIES} pthread)
target_link_libraries(prime ${Boost_LIBRARIES} pthread)