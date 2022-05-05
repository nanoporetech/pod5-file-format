add_executable(find_all_read_ids
    find_all_read_ids.cpp
)

target_link_libraries(find_all_read_ids
    mkr_format
    Boost::headers
    Boost::filesystem
)

add_executable(find_specific_read_ids
    find_specific_read_ids.cpp
)

target_link_libraries(find_specific_read_ids
    mkr_format
    Boost::headers
    Boost::filesystem
)