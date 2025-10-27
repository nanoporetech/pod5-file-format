#include <pybind11/stl/filesystem.h>

#include <filesystem>
#include <map>
#include <string>
#include <vector>

void subset_pod5s_with_mapping(
    std::vector<std::filesystem::path> inputs,
    std::filesystem::path output,
    std::map<std::string, std::vector<std::string>> read_id_to_dest,
    bool missing_ok,
    bool duplicate_ok,
    bool force_overwrite);
