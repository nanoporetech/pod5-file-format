

if (USE_CONAN)
    find_package(Arrow REQUIRED CONFIG)
    find_package(Boost REQUIRED CONFIG)
    find_package(Flatbuffers REQUIRED CONFIG)
    find_package(zstd REQUIRED CONFIG)
else()
    find_package(Arrow REQUIRED)
    find_package(
        Boost REQUIRED
        COMPONENTS
            filesystem
    )
    find_package(Flatbuffers REQUIRED)
    find_package(zstd REQUIRED)
endif()

find_program(
    FLATBUFFERS_FLATC_EXECUTABLE
    flatc
)
include(BuildFlatBuffers)

configure_file(
    mkr_format/version.h.in
    mkr_format/version.h
)

set(mkr_library_type STATIC)
if (BUILD_SHARED_LIB)
    set(mkr_library_type SHARED)
endif()

add_library(mkr_format ${mkr_library_type}
    mkr_format/file_writer.cpp
    mkr_format/file_writer.h
    mkr_format/file_reader.cpp
    mkr_format/file_reader.h

    mkr_format/schema_metadata.cpp
    mkr_format/schema_metadata.h

    mkr_format/read_table_reader.cpp
    mkr_format/read_table_reader.h
    mkr_format/read_table_schema.cpp
    mkr_format/read_table_schema.h
    mkr_format/read_table_writer.cpp
    mkr_format/read_table_writer.h
    mkr_format/read_table_writer_utils.cpp
    mkr_format/read_table_writer_utils.h
    mkr_format/read_table_utils.cpp
    mkr_format/read_table_utils.h

    mkr_format/signal_compression.cpp
    mkr_format/signal_compression.h
    mkr_format/signal_table_reader.cpp
    mkr_format/signal_table_reader.h
    mkr_format/signal_table_schema.cpp
    mkr_format/signal_table_schema.h
    mkr_format/signal_table_writer.cpp
    mkr_format/signal_table_writer.h
    mkr_format/signal_table_utils.h
    
    mkr_format/c_api.cpp
    mkr_format/c_api.h

    mkr_format/errors.cpp
    mkr_format/errors.h
    mkr_format/result.h
    mkr_format/table_reader.cpp
    mkr_format/table_reader.h
    mkr_format/types.cpp
    mkr_format/types.h

    mkr_format/internal/combined_file_utils.h
    mkr_format/internal/schema_utils.h

    mkr_format/svb16/common.hpp
    mkr_format/svb16/decode.hpp
    mkr_format/svb16/decode_scalar.hpp
    mkr_format/svb16/decode_x64.hpp
    mkr_format/svb16/encode.hpp
    mkr_format/svb16/encode_scalar.hpp
    mkr_format/svb16/encode_x64.hpp
    mkr_format/svb16/intrinsics.hpp
    mkr_format/svb16/shuffle_tables.hpp
    mkr_format/svb16/simd_detect_x64.hpp
    mkr_format/svb16/svb16.c
    mkr_format/svb16/svb16.h
)

set(public_headers)
list(APPEND public_headers
    mkr_format/file_writer.h
    mkr_format/file_reader.h

    mkr_format/schema_metadata.h

    mkr_format/read_table_reader.h
    mkr_format/read_table_schema.h
    mkr_format/read_table_writer.h
    mkr_format/read_table_writer_utils.h
    mkr_format/read_table_utils.h

    mkr_format/signal_compression.h
    mkr_format/signal_table_reader.h
    mkr_format/signal_table_schema.h
    mkr_format/signal_table_writer.h
    mkr_format/signal_table_utils.h

    mkr_format/c_api.h

    mkr_format/errors.h
    mkr_format/result.h
    mkr_format/table_reader.h
    mkr_format/types.h

    ${CMAKE_CURRENT_BINARY_DIR}/mkr_format/mkr_format_export.h
)

set_target_properties(mkr_format 
    PROPERTIES
        POSITION_INDEPENDENT_CODE 1
        CXX_STANDARD 14
        PUBLIC_HEADER "${public_headers}"
)

target_link_libraries(mkr_format
    PUBLIC
        arrow::arrow
        flatbuffers::flatbuffers
        Boost::headers
        Boost::filesystem
    PRIVATE
        mkr_flatbuffers
        zstd::zstd
)

target_include_directories(mkr_format
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}
)

flatbuffers_generate_headers(
    TARGET mkr_flatbuffers
    SCHEMAS
        mkr_format/flatbuffers/footer.fbs
    INCLUDE_PREFIX ""
    FLAGS --cpp
)

if (NOT MSVC)
    target_compile_options(mkr_format PRIVATE -Werror -Wall -Wno-comment)
endif()
generate_export_header(mkr_format EXPORT_FILE_NAME mkr_format/mkr_format_export.h)

install(
    TARGETS mkr_format
    PUBLIC_HEADER DESTINATION "include/mkr_format"
)

add_subdirectory(examples)
add_subdirectory(test)

if (BUILD_PYTHON_WHEEL)
    add_subdirectory(mkr_format_pybind)
endif()
