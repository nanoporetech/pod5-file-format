import os

from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, CMakeDeps
from conan.tools.files import collect_libs
from conan.tools.build import cross_building


class Pod5Conan(ConanFile):
    name = "pod5_file_format"
    license = "MPL 2.0"
    url = "https://github.com/nanoporetech/pod5-file-format"
    description = "POD5 File format"
    topics = "nanopore", "sequencing", "genomic", "dna", "arrow"
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False]}
    default_options = {"shared": False}
    exports_sources = [
        "c++/*",
        "cmake/*",
        "python/*",
        "third_party/*",
        "CMakeLists.txt",
        "LICENSE.md",
    ]

    """
    When cross building, we cannot build the "examples". Change a file in the build directory
    for this version, file CMakeLists.txt at the top level. Note there are several
    CMakeLists.txt in various places, it's the one at the top level. Change line from:

        option(POD5_BUILD_EXAMPLES "Disable building all examples" ON)
     to
        option(POD5_BUILD_EXAMPLES "Disable building all examples" OFF)

    Note that the comment is misleading: ON turns buildin examples on, OFF turns building
    examples off. And note that this must be done before calling _configure_cmake() otherwise
    it will have no effect anymore.
    """

    def _toggle_tests_flag_for_cross_build(self):
        fileToPatchName = os.path.join(self.source_folder, "CMakeLists.txt")
        oldString = 'option(POD5_BUILD_EXAMPLES "Disable building all examples" ON)'
        newString = 'option(POD5_BUILD_EXAMPLES "Disable building all examples" OFF)'
        with open(fileToPatchName, "r+") as text_file:
            file_content = text_file.read()
            new_content = file_content.replace(oldString, newString)
            text_file.seek(0)
            text_file.truncate(0)
            text_file.write(new_content)

    def requirements(self):
        self.requires("arrow/8.0.0@")
        self.requires("boost/1.78.0@")
        # We are using an older version of flatbuffers not available on CCI.
        # @TODO: Update to a version that exists in CCI
        # When this line changes a corresponding change in .gitlab-ci.yml is required where this
        # package is uninstalled.
        self.requires("flatbuffers/2.0.0@nanopore/testing")
        self.requires("zstd/1.5.4@")
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
            self.build_requires("flatbuffers/2.0.0@nanopore/testing")

    def generate(self):
        if cross_building(self):
            self._toggle_tests_flag_for_cross_build()
        tc = CMakeToolchain(self)
        tc.variables["ENABLE_CONAN"] = "ON"
        tc.variables["BUILD_PYTHON_WHEEL"] = "OFF"
        tc.variables["INSTALL_THIRD_PARTY"] = "OFF"
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
        self.copy("LICENSE.md", src=".", dst="licenses")

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
            "boost::headers",
            "flatbuffers::flatbuffers",
            "zstd::zstd",
            "zlib::zlib",
        ]

        # self.cpp
        if not (
            self.settings.os == "Windows"
            or self.settings.os == "Macos"
            or self.settings.os == "iOS"
        ):
            self.cpp_info.requires.append("jemalloc::jemalloc")
