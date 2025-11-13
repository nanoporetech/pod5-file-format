from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, CMakeDeps
from conan.tools.files import collect_libs, copy
from conan.tools.build import cross_building
from conan.tools.cmake import cmake_layout
import os


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

    def _licences_path(self):
        # This needs to match the install step inside CMake.
        return os.path.join(self.build_folder, "pod5_conan_licences")

    def _copy_licences(self):
        # Copy each dependency's licences.
        for require, dependency in self.dependencies.items():
            # package_folder will be None if this dependency isn't used.
            if dependency.package_folder is not None:
                copy(
                    self,
                    "license*",
                    dependency.package_folder,
                    os.path.join(self._licences_path(), dependency.ref.name),
                    ignore_case=True,
                )
        # Copy the ones in third_party
        copy(
            self,
            "*",
            os.path.join(self.source_folder, "third_party/licenses"),
            self._licences_path(),
        )

    def layout(self):
        cmake_layout(self, "Ninja Multi-Config")

    def requirements(self):
        self.requires("arrow/18.0.0")
        self.requires("flatbuffers/2.0.0")
        self.requires("zstd/[>=1.4.8 <=2.0.0]")
        self.requires("zlib/[>=1.2.11 <=2.0.0]")
        if not (
            self.settings.os == "Windows"
            or self.settings.os == "Macos"
            or self.settings.os == "iOS"
        ):
            self.requires("jemalloc/5.2.1")

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
            self.tool_requires("flatbuffers/2.0.0")

    def generate(self):
        if not self.options.shared:
            self._setup_third_party_deps_packaging()

        self._copy_licences()

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
        licence_dst = os.path.join(self.package_folder, "licenses")
        copy(self, "LICENSE.md", self.source_folder, licence_dst)
        copy(self, "*", self._licences_path(), licence_dst)

        # Package the required third party libs after installing pod5 static
        if not self.options.shared:
            src = f"{self.build_folder}/third_party/libs/"
            dst = f"{self.build_folder}/lib/"
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

        # Workaround for broken Arrow package - ensure transitive includes are available
        # Since our headers include Arrow headers, we need Arrow's includes to be transitively available
        try:
            arrow_dep = self.dependencies["arrow"]
            arrow_include_path = os.path.join(arrow_dep.package_folder, "include")
            if os.path.exists(arrow_include_path):
                self.cpp_info.includedirs.append(arrow_include_path)
        except Exception:
            # Arrow dependency not found or other issue - let it fail naturally
            pass

        # self.cpp
        if self.settings.os == "Linux":
            self.cpp_info.requires.append("jemalloc::jemalloc")

        if self.settings.os in ["iOS", "Macos"]:
            self.cpp_info.frameworks = ["CoreFoundation"]
