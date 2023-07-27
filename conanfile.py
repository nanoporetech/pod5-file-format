import os

from conans import CMake, ConanFile, tools


class Pod5Conan(ConanFile):
    name = "pod5_file_format"
    license = "MPL 2.0"
    url = "https://github.com/nanoporetech/pod5-file-format"
    description = "POD5 File format"
    topics = "arrow"
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "nanopore_internal_build": [True, False]}

    _cmake = None

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

    def setVersionsAndSuffixes(self):
        self.package_suffix = ""
        self.arrow_version = "8.0.0"
        self.boost_version = "1.78.0"
        if self.options.nanopore_internal_build:
            self.output.info("Using internal dependencies")
            self.package_suffix = "@nanopore/stable"
            self.arrow_version = f"{self.arrow_version}.4"
            self.boost_version = f"{self.boost_version}.1"

    def requirements(self):
        self.setVersionsAndSuffixes()

        self.requires(f"arrow/{self.arrow_version}{self.package_suffix}")
        self.requires(f"boost/{self.boost_version}{self.package_suffix}")
        self.requires(f"flatbuffers/2.0.0{self.package_suffix}")
        self.requires(f"zstd/1.5.4{self.package_suffix}")
        self.requires(f"zlib/1.2.13{self.package_suffix}")

        if not (
            self.settings.os == "Windows"
            or self.settings.os == "Macos"
            or self.settings.os == "iOS"
        ):
            self.requires(f"jemalloc/5.2.1{self.package_suffix}")

    def package_id(self):
        # Windows configurations do not specify a C++ standard version.
        if self.settings.os == "Windows":
            del self.info.settings.compiler.cppstd
        return super().package_id()

    def compatibility(self):
        # Packages that are built with C++14 should be compatible with clients that use C++17 & 20.
        if self.settings.compiler.cppstd in ["17", "20"]:
            return [{"settings": [("compiler.cppstd", "14")]}]

    def build_requirements(self):
        # When cross compiling we need pre compiled flatbuffers for flatc to run on the build machine
        # which is not the target.
        #
        # The flatbuffers version is most likely available already; it is on the master branch and
        # quite likely already built on the development branch. However, it seems that conan
        # doesn't realise this since it is the same package that it tries to build, even though it
        # is a different revision, flatbuffers on the other hand is downloaded.

        self.setVersionsAndSuffixes()
        if hasattr(self, "settings_build") and tools.cross_building(self):
            self.build_requires(f"flatbuffers/2.0.0{self.package_suffix}")

    def _configure_cmake(self):
        if self._cmake:
            return self._cmake
        self._cmake = CMake(self)
        self._cmake.definitions["ENABLE_CONAN"] = "ON"
        self._cmake.definitions["BUILD_PYTHON_WHEEL"] = "OFF"
        self._cmake.definitions["INSTALL_THIRD_PARTY"] = "OFF"
        self._cmake.definitions["POD5_DISABLE_TESTS"] = "ON"
        self._cmake.definitions["POD5_BUILD_EXAMPLES"] = "OFF"
        self._cmake.definitions["BUILD_SHARED_LIB"] = (
            "ON" if self.options.shared else "OFF"
        )
        self._cmake.configure()
        return self._cmake

    def configure(self):
        if self.options.nanopore_internal_build:
            self.options["boost"].header_only = False
            # ensure that linking against pod5 doesn't override the global allocator
            self.options["jemalloc"].prefix = "je_"

    def build(self):
        # When cross building, we cannot build the "examples". Change a file in the build directory
        # for this version, file CMakeLists.txt at the top level. Note there are several
        # CMakeLists.txt in various places, it's the one at the top level. Change line from
        #
        #     option(POD5_BUILD_EXAMPLES "Disable building all examples" ON)
        # to
        #     option(POD5_BUILD_EXAMPLES "Disable building all examples" OFF)
        #
        # Note that the comment is misleading: ON turns buildin examples on, OFF turns building
        # examples off. And note that this must be done before calling _configure_cmake() otherwise
        # it will have no effect anymore.
        if tools.cross_building(self):
            fileToPatchName = os.path.join(self.source_folder, "CMakeLists.txt")
            oldString = 'option(POD5_BUILD_EXAMPLES "Disable building all examples" ON)'
            newString = (
                'option(POD5_BUILD_EXAMPLES "Disable building all examples" OFF)'
            )
            with open(fileToPatchName, "r+") as text_file:
                file_content = text_file.read()
                new_content = file_content.replace(oldString, newString)
                text_file.seek(0)
                text_file.truncate(0)
                text_file.write(new_content)

        cmake = self._configure_cmake()
        cmake.build()

    def package(self):
        cmake = self._configure_cmake()
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
        self.cpp_info.libs = tools.collect_libs(self)
        self.cpp_info.requires = [
            "arrow::arrow",
            "boost::headers",
            "flatbuffers::flatbuffers",
            "zstd::zstd",
            "zlib::zlib",
        ]
        if not (
            self.settings.os == "Windows"
            or self.settings.os == "Macos"
            or self.settings.os == "iOS"
        ):
            self.cpp_info.requires.append("jemalloc::jemalloc")

        super().package_info()

    def run(
        self,
        command,
        output=True,
        cwd=None,
        win_bash=False,
        subsystem=None,
        msys_mingw=True,
        ignore_errors=False,
        run_environment=False,
        with_login=True,
        env="conanbuild",
    ):
        super().run(
            command=command,
            output=output,
            cwd=cwd,
            win_bash=win_bash,
            subsystem=subsystem,
            msys_mingw=msys_mingw,
            ignore_errors=ignore_errors,
            run_environment=run_environment,
            with_login=with_login,
            env=env,
        )

    def test(self):
        if tools.cross_building(self):
            self.output.warn(
                "Pod5Conan: self.test not performed because cross_building"
            )
            return
        return super().test()
