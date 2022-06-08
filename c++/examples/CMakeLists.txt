add_executable(find_all_read_ids
    find_all_read_ids.cpp
)

target_link_libraries(find_all_read_ids
    pod5_format
    Boost::headers
)

add_executable(find_specific_read_ids
    find_specific_read_ids.cpp
)

target_link_libraries(find_specific_read_ids
    pod5_format
    Boost::headers
)


add_executable(find_specific_read_ids_with_signal
    find_specific_read_ids_with_signal.cpp
)

target_link_libraries(find_specific_read_ids_with_signal
    pod5_format
    Boost::headers
)