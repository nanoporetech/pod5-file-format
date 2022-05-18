
set(CPACK_PACKAGE_NAME "pod5-file-format")
set(CPACK_PACKAGE_VENDOR "Oxford Nanopore")
set(CPACK_VERBATIM_VARIABLES true)
set(CPACK_PACKAGE_VERSION_MAJOR ${POD5_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${POD5_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${POD5_VERSION_REV})
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE.md")

include(CPack)