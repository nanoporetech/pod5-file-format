
pybind11_add_module(pod5_format_pybind
    api.h
    bindings.cpp
    repacker.h
    utils.h
)

target_link_libraries(pod5_format_pybind
    PRIVATE
        pod5_format
)


set_target_properties(pod5_format_pybind 
    PROPERTIES
        POSITION_INDEPENDENT_CODE 1
        CXX_STANDARD 14
)
