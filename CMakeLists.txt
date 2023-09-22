cmake_minimum_required(VERSION 3.16.0)

project(POD5)

include(${PROJECT_SOURCE_DIR}/cmake/POD5Version.cmake)

set(CMAKE_PROJECT_VERSION ${POD5_NUMERIC_VERSION})

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake")

# use compiler cache if available
option(DISABLE_CCACHE "Do not try to use ccache to speed compilation" NO)
if (NOT DISABLE_CCACHE)
    find_program(CCACHE_EXECUTABLE ccache
        HINTS "C:/Program\ Files/ccache/"
        )
    if (CCACHE_EXECUTABLE)
        message(STATUS "Using ccache: ${CCACHE_EXECUTABLE}")
        set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
        set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
    endif()
endif()

if (NOT DEFINED ENABLE_CONAN)
    option(ENABLE_CONAN "Enable conan for dependency installation" OFF)
endif()
option(BUILD_SHARED_LIB "Build a shared library" OFF)
option(INSTALL_THIRD_PARTY "Install third party libraries" ON)

option(POD5_DISABLE_TESTS "Disable building all tests" OFF)
option(POD5_BUILD_EXAMPLES "Enable building all examples" ON)

if (NOT DEFINED ENABLE_POD5_PACKAGING)
    option(ENABLE_POD5_PACKAGING "Enable packaging support" ON)
endif()

option(BUILD_PYTHON_WHEEL "Build a python wheel for pod5" ON)

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


# FIXME: DISABLE CONDITIONAL TO WORK ON BIONIC
if (ENABLE_CONAN AND CMAKE_COMPILER_IS_GNUCXX AND
        CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "9.0" AND
        CMAKE_CXX_COMPILER_VERSION VERSION_LESS "10.0")
    # We build POD5 on CentOS 7 in CI, where we have GCC 9 but only the pre-C++11 ABI
    # See https://gcc.gnu.org/onlinedocs/libstdc++/manual/using_dual_abi.html
    # This forces GCC 9 on other platforms (eg: Ubuntu Focal) to use the same ABI.
    # The main gain here is being able to use the same conan packages.
    add_compile_definitions(_GLIBCXX_USE_CXX11_ABI=0)
endif()

include_directories("third_party/include")

foreach (config "Release" "Debug")
    string(TOUPPER "${config}" config_upper)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${config}/lib)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${config}/lib)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${config}/bin)
endforeach()

set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "archive")

include(GenerateExportHeader)

enable_testing()

if (BUILD_PYTHON_WHEEL)
    find_package (Python ${PYTHON_VERSION} EXACT COMPONENTS Interpreter Development)
    add_subdirectory(third_party/pybind11)
endif()

add_subdirectory(c++)

if (ENABLE_POD5_PACKAGING)
    include(pod5_packaging)
endif()
