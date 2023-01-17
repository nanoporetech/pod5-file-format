from conans import CMake, ConanFile, tools


class Pod5Conan(ConanFile):
    name = "pod5_file_format"
    license = "MPL 2.0"
    url = "https://github.com/nanoporetech/pod5-file-format"
    description = "POD5 File format"
    topics = "arrow"
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "nanopore_internal_build": [True, False]}
    default_options = {
        "shared": False,
        "nanopore_internal_build": False,
        "boost:header_only": True,
    }
    generators = "cmake_find_package_multi"
    exports_sources = [
        "c++/*",
        "cmake/*",
        "python/*",
        "third_party/*",
        "CMakeLists.txt",
        "LICENSE.md",
    ]

    def configure(self):
        if self.options.nanopore_internal_build:
            self.options["boost"].header_only = False

    def requirements(self):
        package_suffix = ""
        if self.options.nanopore_internal_build:
            package_suffix = "@nanopore/stable"
            self.requires(f"arrow/8.0.0.1{package_suffix}")
            self.requires(f"boost/1.78.0.1{package_suffix}")
        else:
            self.requires("arrow/8.0.0")
            self.requires("boost/1.78.0")

        self.requires(f"flatbuffers/2.0.0{package_suffix}")
        self.requires(f"zstd/1.4.8{package_suffix}")

        if not (self.settings.os == "Windows" or self.settings.os == "Macos"):
            self.requires(f"jemalloc/5.2.1{package_suffix}")

    def package_id(self):
        # Windows configurations do not specify a C++ standard version.
        if self.settings.os == "Windows":
            del self.info.settings.compiler.cppstd

    def compatibility(self):
        # Packages that are built with C++14 should be compatible with clients that use C++17 & 20.
        if self.settings.compiler.cppstd in ["17", "20"]:
            return [{"settings": [("compiler.cppstd", "14")]}]

    def build(self):
        cmake = CMake(self)
        shared = "ON" if self.options.shared else "OFF"
        self.run(
            "cmake . -DENABLE_CONAN=ON -DBUILD_PYTHON_WHEEL=OFF "
            f"-DINSTALL_THIRD_PARTY=OFF {cmake.command_line} "
            f"-DBUILD_SHARED_LIB={shared}"
        )
        self.run(f"cmake --build . {cmake.build_config}")

    def package(self):
        cmake = CMake(self)
        cmake.install()

        # Copy the license files
        self.copy("LICENSE.md", src=".", dst="licenses")

    def package_info(self):
        self.cpp_info.libs = tools.collect_libs(self)
        self.cpp_info.requires = [
            "arrow::arrow",
            "boost::headers",
            "flatbuffers::flatbuffers",
            "zstd::zstd",
        ]
        if not (self.settings.os == "Windows" or self.settings.os == "Macos"):
            self.cpp_info.requires.append("jemalloc::jemalloc")
