#ifndef TUDDBS_MEMORDMA_INCLUDE_PERFORMANCE_TESTS_HPP
#define TUDDBS_MEMORDMA_INCLUDE_PERFORMANCE_TESTS_HPP

#include <atomic>
#include <chrono>

class PerformanceTests {
   public:
    static PerformanceTests& getInstance() {
        static PerformanceTests instance;
        return instance;
    }

    int continuousConsumeBenchmark(size_t conId, size_t seconds);

   private:
    PerformanceTests();
    PerformanceTests(PerformanceTests const&);             // copy ctor hidden
    PerformanceTests& operator=(PerformanceTests const&);  // assign op. hidden
    ~PerformanceTests();

    std::chrono::time_point<std::chrono::high_resolution_clock> continuousBenchmarkCheckpoint = std::chrono::high_resolution_clock::now();
    std::atomic<size_t> continuousBenchmarkReceivedBytes{0};
};

#endif // TUDDBS_MEMORDMA_INCLUDE_PERFORMANCE_TESTS_HPP