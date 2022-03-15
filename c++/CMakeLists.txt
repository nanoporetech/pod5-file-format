
find_package(Arrow REQUIRED CONFIG)

add_library(mkr_format
    mkr.cpp
    mkr.h
)

add_subdirectory(test)