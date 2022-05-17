cmake_minimum_required(VERSION 3.16.0)

project(MKR)

include(${PROJECT_SOURCE_DIR}/cmake/MKRVersion.cmake)

set(CMAKE_PROJECT_VERSION ${MKR_NUMERIC_VERSION})

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake")

option(USE_CONAN "Use conan for dependency management" OFF)
option(BUILD_SHARED_LIB "Build a shared library" OFF)
option(INSTALL_THIRD_PARTY "Install third party libraries" ON)

option(BUILD_PYTHON_WHEEL "Build a python wheel for mkr" ON)

if (USE_CONAN AND CMAKE_COMPILER_IS_GNUCXX AND
        CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "9.0" AND
        CMAKE_CXX_COMPILER_VERSION VERSION_LESS "10.0")
    # We build MKR on CentOS 7 in CI, where we have GCC 9 but only the pre-C++11 ABI
    # See https://gcc.gnu.org/onlinedocs/libstdc++/manual/using_dual_abi.html
    # This forces GCC 9 on other platforms (eg: Ubuntu Focal) to use the same ABI.
    # The main gain here is being able to use the same conan packages.
    add_compile_definitions(_GLIBCXX_USE_CXX11_ABI=0)
endif()

# Enable finding conan modules:
set(CMAKE_PREFIX_PATH "${CMAKE_BINARY_DIR}")
include_directories("third_party/include")

foreach (config "Release" "Debug")
    string(TOUPPER "${config}" config_upper)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${config}/lib)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${config}/lib)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${config}/bin)
endforeach()

# debug symbols don't depend on the build type, only on this option
option(DISABLE_DEBUG_SYMBOLS "Force debug symbols to be disabled" OFF)
if (NOT DISABLE_DEBUG_SYMBOLS)
    if (MSVC)
        # Z7 embeds deubgging info into .obj files, which is easier to manage for
        # build accelerators (note that a .pdb will still be generated for libs)
        # https://docs.microsoft.com/en-us/cpp/build/reference/z7-zi-zi-debug-information-format
        add_compile_options(/Z7)
        # this will use fastlink in the IDE and full link from the command link
        # https://docs.microsoft.com/en-us/cpp/build/reference/debug-generate-debug-info
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
        set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
        # /DEBUG option is not recognised for STATIC lib linking
    elseif (CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        add_compile_options(-g)
    endif()
endif()

set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "archive")

include(GenerateExportHeader)

enable_testing()

if (BUILD_PYTHON_WHEEL)
    find_package (Python ${PYTHON_VERSION} EXACT COMPONENTS Interpreter Development)
    add_subdirectory(third_party/pybind11)
    
    add_subdirectory(python)
endif()

add_subdirectory(c++)

include(mkr_packaging)
