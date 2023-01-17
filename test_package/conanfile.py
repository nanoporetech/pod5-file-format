import os

from conans import CMake, ConanFile, tools


class TestPackageConan(ConanFile):
    settings = "os", "arch", "compiler", "build_type"
    generators = "cmake", "cmake_find_package_multi"

    def build(self):
        self.output.warn("Pod5Conan test: Creating cmake")
        cmake = CMake(self)
        self.output.warn("Pod5Conan test: Calling configure")
        cmake.configure()
        self.output.warn("Pod5Conan test: Calling build")
        cmake.build()
        self.output.warn("Pod5Conan test: Finished build")

    def test(self):
        self.output.warn("Pod5Conan test: Entering test")
        if not tools.cross_building(self):
            self.output.warn("Pod5Conan test: Not cross_building")
            bin_path = os.path.join("bin", "test_package")
            self.run(bin_path, run_environment=True)
        else:
            self.output.warn("Pod5Conan test: cross_building is true")
