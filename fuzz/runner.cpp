#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

#ifdef NDEBUG
#error "asserts aren't enabled"
#endif

extern "C" {
int LLVMFuzzerInitialize(int * argc, char *** argv);
int LLVMFuzzerTestOneInput(uint8_t const * data, size_t size);
}

namespace {

void run_one(bool verbose, std::filesystem::path const & path)
{
    if (verbose) {
        std::cout << "Running " << path.string() << std::endl;
    }

    // Read it into a buffer.
    std::vector<uint8_t> data(std::filesystem::file_size(path));
    std::ifstream file(path, std::ios::in | std::ios::binary);
    file.read(reinterpret_cast<char *>(data.data()), data.size());
    assert(file);

    // Run it.
    LLVMFuzzerTestOneInput(data.data(), data.size());
}

void run_recursive(bool verbose, std::filesystem::path const & path)
{
    for (auto const & dirent : std::filesystem::directory_iterator(path)) {
        if (std::filesystem::is_directory(dirent)) {
            run_recursive(verbose, dirent.path());
        } else if (std::filesystem::is_regular_file(dirent)) {
            run_one(verbose, dirent.path());
        }
    }
}

}  // namespace

int main(int argc, char ** argv)
{
    // Parse args.
    char const * corpus = nullptr;
    bool verbose = false;
    if (argc == 3) {
        verbose = true;
        corpus = argv[2];
    } else if (argc == 2) {
        corpus = argv[1];
    } else {
        std::cerr << "Usage: " << argv[0] << " [-v] <corpus>" << std::endl;
        return EXIT_FAILURE;
    }

    // Setup.
    LLVMFuzzerInitialize(&argc, &argv);

    // Run it.
    std::filesystem::path corpus_path(corpus);
    if (std::filesystem::is_directory(corpus_path)) {
        std::cout << "Running all corpus files" << std::endl;
        run_recursive(verbose, corpus_path);
    } else if (std::filesystem::is_regular_file(corpus_path)) {
        std::cout << "Running single file" << std::endl;
        run_one(verbose, corpus_path);
    } else {
        std::cerr << "Unknown file type: " << corpus << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Success!" << std::endl;
    return EXIT_SUCCESS;
}
