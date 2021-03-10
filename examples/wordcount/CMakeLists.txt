project(wordcount)
add_executable(wordcount wordcount.cpp)
target_link_libraries(wordcount ${Boost_LIBRARIES} pthread)
