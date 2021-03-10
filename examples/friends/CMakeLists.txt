project(friends)
add_executable(friends friends.cpp)
target_link_libraries(friends ${Boost_LIBRARIES} pthread)
