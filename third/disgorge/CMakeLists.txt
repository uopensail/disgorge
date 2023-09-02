cmake_minimum_required(VERSION 3.16)
project(disgorge)

set(CMAKE_CXX_STANDARD 17)

include_directories(/usr/local/include)
include_directories(/usr/include)
include_directories(include)
link_directories(/usr/local/lib)

SET(SOURCE include/disgorge.h src/disgorge.cpp include/instance.hpp include/json.hpp include/query.hpp)

add_library(disgorge SHARED ${SOURCE})

target_link_libraries(disgorge rocksdb)

add_library(disgorge_static STATIC ${SOURCE})
target_link_libraries(disgorge_static rocksdb)