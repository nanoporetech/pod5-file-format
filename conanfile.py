from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, CMakeDeps
from conan.tools.files import collect_libs, copy
from conan.tools.build import cross_building


class Pod5Conan(ConanFile):
    name = "pod5_file_format"
    license = "MPL 2.0"
    url = "https://github.com/nanoporetech/pod5-file-format"
    description = "POD5 File format"
    topics = "nanopore", "sequencing", "genomic", "dna", "arrow"
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False]}
    default_options = {
        "shared": False,
        "arrow:with_zstd": True,
    }
    exports_sources = [
        "c++/*",
        "cmake/*",
        "python/*",
        "third_party/*",
        "CMakeLists.txt",
        "LICENSE.md",
    ]

    """
    When building a static library, we need to pack arrow, zstd and if on linux jemalloc,
    alongside pod5 static lib to avoid linking errors. This function copies those libs to
    a folder called third_party in the build directory. The ci/install.sh ensures they end
    up in the correct location to be deployed, if install is done via cmake.
    """

    def _setup_third_party_deps_packaging(self):
        deps_to_pack = (
            ["arrow", "zstd", "jemalloc"]
            if self.settings.os == "Linux"
            else ["arrow", "zstd"]
        )
        static_lib_ext_wildcard = "*.a" if self.settings.os != "Windows" else "*.lib"
        for dep in deps_to_pack:
            if dep == "jemalloc":
                static_lib_ext_wildcard = (
                    "*_pic.a" if self.settings.os != "Windows" else "*_pic.lib"
                )
            dep_object = self.dependencies[dep]
            copy(
                self,
                static_lib_ext_wildcard,
                dep_object.cpp_info.libdir,
                f"{self.build_folder}/third_party/libs",
            )

    def requirements(self):
        self.requires("arrow/12.0.0@")
        self.requires("flatbuffers/2.0.0@")
        self.requires("zstd/1.5.5@")
        self.requires("zlib/1.2.13@")
        if not (
            self.settings.os == "Windows"
            or self.settings.os == "Macos"
            or self.settings.os == "iOS"
        ):
            self.requires("jemalloc/5.2.1@")

    """
    When cross compiling we need pre compiled flatbuffers for flatc to run on the build machine
    which is not the target.
    The flatbuffers version is most likely available already; it is on the master branch and
    quite likely already built on the development branch. However, it seems that conan
    doesn't realise this since it is the same package that it tries to build, even though it
    is a different revision, flatbuffers on the other hand is downloaded.
    """

    def build_requirements(self):
        if hasattr(self, "settings_build") and cross_building(self):
            # We are using an older version of flatbuffers not available on CCI.
            # @TODO: Update to a version that exists in CCI
            # When this line changes a corresponding change in .gitlab-ci.yml is required where this
            # package is uninstalled.
            self.build_requires("flatbuffers/2.0.0@")

    def generate(self):
        if not self.options.shared:
            self._setup_third_party_deps_packaging()

        tc = CMakeToolchain(self)
        tc.variables["ENABLE_CONAN"] = "ON"
        tc.variables["BUILD_PYTHON_WHEEL"] = "OFF"
        tc.variables["POD5_DISABLE_TESTS"] = "ON"
        tc.variables["POD5_BUILD_EXAMPLES"] = "OFF"
        tc.variables["BUILD_SHARED_LIB"] = "ON" if self.options.shared else "OFF"
        tc.generate()

        deps = CMakeDeps(self)
        deps.check_components_exist = True

        # This ensures that target names in cmake would be in the form of libname::libname
        deps.set_property("zstd", "cmake_target_name", None)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

        # Copy the license files
        copy(self, "LICENSE.md", ".", "licenses")

        # Package the required third party libs after installing pod5 static
        if not self.options.shared:
            src = f"{self.build_folder}/third_party/libs/"
            dst = f"{self.build_folder}/{self.settings.build_type}/lib/"
            copy(self, "*", src, dst)

    def package_info(self):
        # Note: package_info collects information in self.cpp_info. It is called from the Conan
        # application.
        #
        # This call is made immediately after the pre_package_info hook and before the
        # post_package_info hook. To get more information, we can "import traceback" and "import inspect",
        # then call traceback.print_stack() to print the complete call stack, or examine
        # inspect.stack().
        #
        # The caller has created self.cpp_info with the name set to the name of self, with a rootpath,
        # version and description from self, env_info and user_info set with default values,
        # public_deps set to an array with the names of public requirements in conanfile.requires.items.

        # Additions for this package. Note that everything in requirements needs to be mentioned
        # here. Except for Windows and Macos, jemalloc is also needed.

        self.cpp_info.libs = collect_libs(self)
        self.cpp_info.requires = [
            "arrow::arrow",
            "flatbuffers::flatbuffers",
            "zstd::zstd",
            "zlib::zlib",
        ]

        # self.cpp
        if self.settings.os == "Linux":
            self.cpp_info.requires.append("jemalloc::jemalloc")
