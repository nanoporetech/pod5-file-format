#pragma once

// Restrict qualifier portable across the compilers POD5 targets.
#ifdef _MSC_VER
#define pdz_restrict __restrict
#elif defined(__clang__)
#define pdz_restrict
#else
#define pdz_restrict __restrict__
#endif
