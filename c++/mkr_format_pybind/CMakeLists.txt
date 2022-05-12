
pybind11_add_module(mkr_format_pybind
    bindings.cpp
)

target_link_libraries(mkr_format_pybind
    PRIVATE
        mkr_format
)


set_target_properties(mkr_format_pybind 
    PROPERTIES
        POSITION_INDEPENDENT_CODE 1
        CXX_STANDARD 14
)
