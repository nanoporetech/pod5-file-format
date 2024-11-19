#include "memory_pool.h"

#ifdef _WIN32
#include <windows.h>
#elif !defined(__FreeBSD__)
#include <unistd.h>
#endif

namespace {

// Referenced from the jemalloc source:
// https://github.com/jemalloc/jemalloc/blob/b82333fdec6e5833f88780fcf1fc50b799268e1b/src/pages.c#L596C1-L616C2
size_t os_page_detect(void)
{
#ifdef _WIN32
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return si.dwPageSize;
#elif defined(__FreeBSD__)
    /*
	 * This returns the value obtained from
	 * the auxv vector, avoiding a syscall.
	 */
    return getpagesize();
#else
    long result = sysconf(_SC_PAGESIZE);
    if (result == -1) {
        return 4095 * 16;  // Default to safe, large page size
    }
    return (size_t)result;
#endif
}

}  // namespace

namespace pod5 {

arrow::MemoryPool * default_memory_pool()
{
    // Default to system memory pool for systems with large pages:
    if (os_page_detect() > 4096) {
        return arrow::system_memory_pool();
    }
    return arrow::default_memory_pool();
}

}  // namespace pod5
