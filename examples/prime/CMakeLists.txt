project(prime)
add_executable(prime prime.cpp)
target_link_libraries(prime ${Boost_LIBRARIES} pthread)
