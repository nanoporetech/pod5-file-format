

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
    pod5_format/version.h.in
    pod5_format/version.h
)

set(pod5_library_type STATIC)
if (BUILD_SHARED_LIB)
    set(pod5_library_type SHARED)
endif()

add_library(pod5_format ${pod5_library_type}
    pod5_format/file_writer.cpp
    pod5_format/file_writer.h
    pod5_format/file_reader.cpp
    pod5_format/file_reader.h

    pod5_format/schema_metadata.cpp
    pod5_format/schema_metadata.h

    pod5_format/read_table_reader.cpp
    pod5_format/read_table_reader.h
    pod5_format/read_table_schema.cpp
    pod5_format/read_table_schema.h
    pod5_format/read_table_writer.cpp
    pod5_format/read_table_writer.h
    pod5_format/read_table_writer_utils.cpp
    pod5_format/read_table_writer_utils.h
    pod5_format/read_table_utils.cpp
    pod5_format/read_table_utils.h

    pod5_format/signal_compression.cpp
    pod5_format/signal_compression.h
    pod5_format/signal_table_reader.cpp
    pod5_format/signal_table_reader.h
    pod5_format/signal_table_schema.cpp
    pod5_format/signal_table_schema.h
    pod5_format/signal_table_writer.cpp
    pod5_format/signal_table_writer.h
    pod5_format/signal_table_utils.h
    
    pod5_format/c_api.cpp
    pod5_format/c_api.h

    pod5_format/errors.cpp
    pod5_format/errors.h
    pod5_format/result.h
    pod5_format/table_reader.cpp
    pod5_format/table_reader.h
    pod5_format/types.cpp
    pod5_format/types.h

    pod5_format/internal/combined_file_utils.h
    pod5_format/internal/schema_utils.h

    pod5_format/svb16/common.hpp
    pod5_format/svb16/decode.hpp
    pod5_format/svb16/decode_scalar.hpp
    pod5_format/svb16/decode_x64.hpp
    pod5_format/svb16/encode.hpp
    pod5_format/svb16/encode_scalar.hpp
    pod5_format/svb16/encode_x64.hpp
    pod5_format/svb16/intrinsics.hpp
    pod5_format/svb16/shuffle_tables.hpp
    pod5_format/svb16/simd_detect_x64.hpp
)

set(public_headers)
list(APPEND public_headers
    pod5_format/file_writer.h
    pod5_format/file_reader.h

    pod5_format/schema_metadata.h

    pod5_format/read_table_reader.h
    pod5_format/read_table_schema.h
    pod5_format/read_table_writer.h
    pod5_format/read_table_writer_utils.h
    pod5_format/read_table_utils.h

    pod5_format/signal_compression.h
    pod5_format/signal_table_reader.h
    pod5_format/signal_table_schema.h
    pod5_format/signal_table_writer.h
    pod5_format/signal_table_utils.h

    pod5_format/c_api.h

    pod5_format/errors.h
    pod5_format/result.h
    pod5_format/table_reader.h
    pod5_format/types.h

    ${CMAKE_CURRENT_BINARY_DIR}/pod5_format/pod5_format_export.h
)

set_target_properties(pod5_format 
    PROPERTIES
        POSITION_INDEPENDENT_CODE 1
        CXX_STANDARD 14
        PUBLIC_HEADER "${public_headers}"
)

target_link_libraries(pod5_format
    PUBLIC
        arrow::arrow
        flatbuffers::flatbuffers
        Boost::headers
        Boost::filesystem
    PRIVATE
        pod5_flatbuffers
        zstd::zstd
)

if (NOT BUILD_SHARED_LIB AND INSTALL_THIRD_PARTY)
    set(pod5_libs arrow zstd)
    foreach (lib ${pod5_libs})
            
        install(
            FILES $<IF:$<CONFIG:Debug>,${${lib}_LIBS_DEBUG},${${lib}_LIBS_RELEASE}>
            TYPE LIB
            COMPONENT third_party
        )
    endforeach()

    set(lib_ext ".a")
    set(lib_prefix "lib")
    if (WIN32)
        set(lib_ext ".lib")
    set(lib_prefix "")
    endif()
    foreach(boost_lib filesystem)
        set(debug_path "${Boost_${boost_lib}_LIB_DIRS_DEBUG}/${lib_prefix}${Boost_${boost_lib}_LIBS_DEBUG}${lib_ext}")
        set(release_path "${Boost_${boost_lib}_LIB_DIRS_RELEASE}/${lib_prefix}${Boost_${boost_lib}_LIBS_RELEASE}${lib_ext}")
        
        install(
            FILES $<IF:$<CONFIG:Debug>,${debug_path},${release_path}>
            TYPE LIB
            COMPONENT third_party
        )
    endforeach()
endif()

target_include_directories(pod5_format
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}
)

flatbuffers_generate_headers(
    TARGET pod5_flatbuffers
    SCHEMAS
        pod5_format/flatbuffers/footer.fbs
    INCLUDE_PREFIX ""
    FLAGS --cpp
)

if (NOT MSVC)
    target_compile_options(pod5_format PRIVATE -Werror -Wall -Wno-comment)
endif()
generate_export_header(pod5_format EXPORT_FILE_NAME pod5_format/pod5_format_export.h)

install(
    TARGETS pod5_format
    PUBLIC_HEADER DESTINATION "include/pod5_format"
)

add_subdirectory(examples)
add_subdirectory(test)

if (BUILD_PYTHON_WHEEL)
    add_subdirectory(pod5_format_pybind)
endif()
