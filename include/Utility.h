#ifndef MEMORDMA_HELPER
#define MEMORDMA_HELPER

// #include <assert.h>
#include <byteswap.h>
#include <endian.h>
#include <errno.h>
// #include <infiniband/verbs.h>
#include <inttypes.h>
#include <netdb.h>
#include <string.h>
// #include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
// #include <unistd.h>

#include <iomanip>
// #include <iostream>
// #include <random>
#include <cstdint>
#include <sstream>
#include <string>

#include "Logger.h"

namespace memordma {

using namespace memordma;
class Utility {
   public:
    /*
    Corresponding Macro:
    #define CHECK(expr)                  \
    {                                \
        int rc = (expr);             \
        if (rc != 0) {               \
            perror(strerror(errno)); \
            exit(EXIT_FAILURE);      \
        }                            \
    }
    */
    static void checkOrDie(size_t val) {
        if (val != 0) {
            LOG_ERROR("Check failed: " << strerror(errno) << std::endl;)
            exit(EXIT_FAILURE);
        }
    };

#if __BYTE_ORDER == __LITTLE_ENDIAN
    static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
    static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
    static inline uint64_t htonll(uint64_t x) { return x; }
    static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

    static double BtoMB(uint64_t byte) {
        return static_cast<double>(byte) / 1024 / 1024;
    }

    // https://stackoverflow.com/a/11124118
    // Returns the human-readable file size for an arbitrary, 64-bit file size
    // The default format is "0.### XB", e.g. "4.2 KB" or "1.434 GB"
    static std::string GetBytesReadable(std::size_t i) {
        // Determine the suffix and readable value
        std::string suffix;
        double readable;
        if (i >= 0x1000000000000000) {  // Exabyte
            suffix = "EB";
            readable = (i >> 50);
        } else if (i >= 0x4000000000000) {  // Petabyte
            suffix = "PB";
            readable = (i >> 40);
        } else if (i >= 0x10000000000) {  // Terabyte
            suffix = "TB";
            readable = (i >> 30);
        } else if (i >= 0x40000000) {  // Gigabyte
            suffix = "GB";
            readable = (i >> 20);
        } else if (i >= 0x100000) {  // Megabyte
            suffix = "MB";
            readable = (i >> 10);
        } else if (i >= 0x400) {  // Kilobyte
            suffix = "KB";
            readable = i;
        } else {
            return std::to_string(i) + " B";  // Byte
        }

        // Divide by 1024 to get fractional value
        readable = (readable / 1024);
        std::stringstream stream;
        stream << std::fixed << std::setprecision(2) << readable << suffix;

        // Return formatted number with suffix
        return stream.str();
    }

    // trim from start (in place)
    static inline void ltrim(std::string &s) {
        s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                    return !std::isspace(ch);
                }));
    }

    // trim from end (in place)
    static inline void rtrim(std::string &s) {
        s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
                    return !std::isspace(ch);
                }).base(),
                s.end());
    }

    // trim from both ends (in place)
    static inline void trim(std::string &s) {
        ltrim(s);
        rtrim(s);
    }

    static inline void trim_any_of(std::string &s, std::string charsToRemove) {
        for (auto del_if : charsToRemove) {
            s.erase(s.find_last_not_of(del_if) + 1, std::string::npos);
            s.erase(0, std::min(s.find_first_not_of(del_if), s.size() - 1));
        }
    }

   private:
    Utility();
};

}  // namespace memordma

#endif