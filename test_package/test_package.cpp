#include "pod5_format/c_api.h"

#include <iostream>

int main()
{
    std::cout << "Initializing POD5...." << std::endl;
    if (pod5_init() == POD5_OK) {
        std::cout << "Pod5 successfully initialized." << std::endl;
    } else {
        std::cerr << "Failed to initialize Pod5!" << std::endl;
    }

    std::cout << "Shutting down POD5 gracefully...." << std::endl;
    if (pod5_terminate() == POD5_OK) {
        std::cout << "Pod5 successfully terminated." << std::endl;
    } else {
        std::cerr << "Failed to shut down Pod5!" << std::endl;
    }
}
