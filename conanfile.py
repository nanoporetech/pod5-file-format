import os

from conans import CMake, ConanFile, tools
from conans.errors import ConanException


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

    def printSettings(self):
        # Print important settings, useful for debugging.
        self.output.warn("Pod5Conan: self.settings.os = " + str(self.settings.os))
        self.output.warn(
            "Pod5Conan: tools.is_apple_os(self.settings.os) = "
            + str(tools.is_apple_os(self.settings.os))
        )
        self.output.warn("Pod5Conan: self.settings.arch = " + str(self.settings.arch))
        try:
            self.output.warn(
                "Pod5Conan: self.settings.os.sdk = " + str(self.settings.os.sdk)
            )
        except ConanException:
            self.output.warn(
                "Pod5Conan: self.settings.os.sdk does not exist (Exception)"
            )
        self.output.warn(
            "Pod5Conan: self.settings.compiler = " + str(self.settings.compiler)
        )
        try:
            self.output.warn(
                "Pod5Conan: self.settings.os.version = " + str(self.settings.os.version)
            )
        except ConanException:
            self.output.warn(
                "Pod5Conan: self.settings.os.version does not exist (Exception)"
            )

        try:
            self.output.warn(
                "Pod5Conan: self.settings.os.sdk = " + str(self.settings.os.sdk)
            )
        except ConanException:
            self.output.warn(
                "Pod5Conan: self.settings.os.sdk does not exist (Exception)"
            )

    def setVersionsAndSuffixes(self):
        self.package_suffix = ""
        self.arrow_version = "8.0.0"
        self.boost_version = "1.78.0"
        if self.options.nanopore_internal_build:
            self.output.warn(
                "Pod5Conan: setVersionsAndSuffixes for internal build, add .1"
            )
            self.package_suffix = "@nanopore/stable"
            self.arrow_version = f"{self.arrow_version}.3"
            self.boost_version = f"{self.boost_version}.1"
        else:
            self.output.warn(
                "Pod5Conan: setVersionsAndSuffixes, not for internal build"
            )

        self.output.warn("Pod5Conan: package_suffix = " + self.package_suffix)
        self.output.warn("Pod5Conan: arrow_version = " + self.arrow_version)
        self.output.warn("Pod5Conan: boost_version = " + self.boost_version)

    def requirements(self):
        self.output.warn("Pod5Conan: self.requirements has been called, optional.")
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
            self.output.warn("Pod5Conan: self.requirements, requires jemalloc")
        else:
            self.output.warn("Pod5Conan: self.requirements, does not require jemalloc")
            self.output.warn(
                f"Pod5Conan: self.requirements, tools.is_apple_os(self.settings.os) = "
                f"{tools.is_apple_os(self.settings.os)}"
            )
            self.output.warn(
                f"Pod5Conan: self.requirements, self.settings.os = {self.settings.os}"
            )

        self.output.warn("Pod5Conan: self.requirements, before printing settings")
        self.printSettings()
        self.output.warn("Pod5Conan: self.requirements, after printing settings")

    def package_id(self):
        self.output.warn("Pod5Conan: Calling self.package_id")
        # Windows configurations do not specify a C++ standard version.
        if self.settings.os == "Windows":
            del self.info.settings.compiler.cppstd
        result = super().package_id()
        self.output.warn(f"Pod5Conan: Called super.package_id, returns {result}")
        return result

    def compatibility(self):
        self.output.warn("Pod5Conan: Calling compatibility")
        # Packages that are built with C++14 should be compatible with clients that use C++17 & 20.
        if self.settings.compiler.cppstd in ["17", "20"]:
            result = [{"settings": [("compiler.cppstd", "14")]}]
        else:
            result = super().compatibility()
        self.output.warn("Pod5Conan: Called compatibility")
        return result

    def build_requirements(self):
        self.output.warn(
            "Pod5Conan: self.build_requirements has been called, optional."
        )

        # When cross compiling we need pre compiled flatbuffers for flatc to run on the build machine
        # which is not the target.
        #
        # The flatbuffers version is most likely available already; it is on the master branch and
        # quite likely already built on the development branch. However, it seems that conan
        # doesn't realise this since it is the same package that it tries to build, even though it
        # is a different revision, flatbuffers on the other hand is downloaded.

        self.setVersionsAndSuffixes()
        if hasattr(self, "settings_build") and tools.cross_building(self):
            self.output.warn(
                "Pod5Conan: self.build_requirements, cross-building the build part"
            )
            self.output.warn(
                "Pod5Conan: self.build_requirements, adding requirement for same package"
            )
            # self.build_requires('pod5_file_format/{}@nanopore/stable'.format(self.version))
            self.build_requires(f"flatbuffers/2.0.0{self.package_suffix}")
        else:
            self.output.warn("Pod5Conan: self.build_requirements, not cross-building")

    def generate(self):
        self.output.warn("Pod5Conan: self.generate has been called, optional")

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
        self.output.warn("Pod5Conan: Calling self.configure.")
        if self.options.nanopore_internal_build:
            self.options["boost"].header_only = False
            # ensure that linking against pod5 doesn't override the global allocator
            self.options["jemalloc"].prefix = "je_"

    def build(self):
        self.output.warn("Pod5Conan: Calling self.build, required.")
        self.output.warn("Pod5Conan: self.build, before printing settings")
        self.printSettings()
        self.output.warn("Pod5Conan: self.build, after printing settings")

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
            self.output.warn(
                "Pod5Conan: Starting to patch CMakeLists.txt, changing POD5_BUILD_EXAMPLES"
            )
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
            self.output.warn(
                "Pod5Conan: Finished to patch CMakeLists.txt, changing POD5_BUILD_EXAMPLES"
            )
            self.output.warn(
                "Pod5Conan: If there are linker errors complaining about find_all_read_ids,"
            )
            self.output.warn(
                "Pod5Conan: check whether CMakeLists.txt in the build directory has"
            )
            self.output.warn("Pod5Conan: been changed correctly.")

        cmake = self._configure_cmake()

        self.output.warn(
            "Pod5Conan: cmake.command_line = {}".format(cmake.command_line)
        )
        self.output.warn(
            "Pod5Conan: cmake.build_config = {}".format(cmake.build_config)
        )

        cmake.build()

    def package(self):
        self.output.warn("Pod5Conan: Calling self.package, required.")
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
        self.output.warn(
            "Calling Pod5Conan package_info from _call_package_info via _handle_node_cache "
            "in conans/client/installer.py"
        )
        self.output.warn(f"Pod5Conan: cpp_info = {self.cpp_info}")
        # We would like to inspect self.cpp_info to see the contents, but it is not iterable.
        # self.output.warn(
        #    f"Pod5Conan: cpp_info members = {inspect.getmembers(self.cpp_info)}"
        # )
        self.output.warn(f"Pod5Conan: cpp_info.rootpath = {self.cpp_info.rootpath}")
        self.output.warn(f"Pod5Conan: cpp_info.version = {self.cpp_info.version}")
        self.output.warn(
            f"Pod5Conan: cpp_info.description = {self.cpp_info.description}"
        )
        # This creates an error: self.cpp_info.components cannot be used with self.cpp_info
        # configs (release/debug/...) at the same time
        # self.output.warn(f"Pod5Conan: cpp_info.env_info = {self.cpp_info.env_info}")
        # self.output.warn(f"Pod5Conan: cpp_info.user_info = {self.cpp_info.user_info}")
        self.output.warn(
            f"Pod5Conan: cpp_info.public_deps = {self.cpp_info.public_deps}"
        )

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
        self.output.warn("Calling Pod5Conan run")
        self.output.warn("Calling Pod5Conan run, command = " + command)
        if output is not True:
            self.output.warn("Calling Pod5Conan run, output = " + output)
        if cwd is not None:
            self.output.warn("Calling Pod5Conan run, cwd = " + cwd)
        if win_bash is not False:
            self.output.warn("Calling Pod5Conan run, win_bash = " + win_bash)
        if subsystem is not None:
            self.output.warn("Calling Pod5Conan run, subsystem = " + subsystem)
        if msys_mingw is not True:
            self.output.warn("Calling Pod5Conan run, msys_mingw = " + msys_mingw)
        if ignore_errors is not False:
            self.output.warn("Calling Pod5Conan run, ignore_errors = " + ignore_errors)
        if run_environment is not False:
            self.output.warn(
                "Calling Pod5Conan run, run_environment = " + run_environment
            )
        if with_login is not True:
            self.output.warn("Calling Pod5Conan run, with_login = " + with_login)
        if env != "conanbuild":
            self.output.warn("Calling Pod5Conan run, env = " + env)
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
        self.output.warn("Pod5Conan: Calling self.test")
        if tools.cross_building(self):
            self.output.warn(
                "Pod5Conan: self.test not performed because cross_building"
            )
            return
        self.output.warn("Pod5Conan: self.test performed because not cross_building")
        return super().test()
