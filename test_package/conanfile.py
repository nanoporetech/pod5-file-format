import os

from conan import ConanFile
from conan.tools.build import can_run
from conan.tools.cmake import cmake_layout, CMake


class TestPackageConan(ConanFile):
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "compiler.sanitizer": [
            None,
            "AddressStatic",
            "ThreadStatic",
            "UndefinedBehaviorStatic",
        ]
    }
    default_options = {"compiler.sanitizer": None}
    generators = "CMakeDeps", "CMakeToolchain", "VirtualRunEnv"
    test_type = "explicit"

    def requirements(self):
        self.requires(self.tested_reference_str)

    def layout(self):
        cmake_layout(self)

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    @property
    def _test_executable(self):
        return os.path.join(self.cpp.build.bindirs[0], "test_package")

    def test(self):
        if can_run(self):
            self.run(self._test_executable, env="conanrun")
        else:
            self.output.warn("Pod5Conan test: cross_building is true")
