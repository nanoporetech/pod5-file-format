
find_package(Arrow REQUIRED CONFIG)
find_package(Boost REQUIRED CONFIG)

configure_file(
    mkr_format/version.h.in
    mkr_format/version.h
)

add_library(mkr_format
    mkr_format/mkr.cpp
    mkr_format/mkr.h
    mkr_format/schema_metadata.cpp
    mkr_format/schema_metadata.h
    mkr_format/signal_table_writer.cpp
    mkr_format/signal_table_writer.cpp
    mkr_format/signal_table_writer.h
    mkr_format/signal_table_reader.cpp
    mkr_format/signal_table_reader.h
    mkr_format/signal_table_schema.cpp
    mkr_format/signal_table_schema.h

    mkr_format/errors.cpp
    mkr_format/errors.h
    mkr_format/result.h
    mkr_format/types.cpp
    mkr_format/types.h
)

target_include_directories(mkr_format
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(mkr_format
    PUBLIC
        arrow::arrow
        Boost::headers
)

generate_export_header(mkr_format EXPORT_FILE_NAME mkr_format/mkr_format_export.h)

add_subdirectory(test)