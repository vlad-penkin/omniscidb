// component headers
#include <avxbmp.h>

// std headers
#include <functional>
#include <iomanip>
#include <iostream>
#include <vector>

static size_t find_median(std::vector<size_t> & measurements) {
  std::sort(std::begin(measurements), std::end(measurements), std::less<size_t>());
  return measurements[measurements.size() / 2];
}

template <typename TYPE, typename FUNCTION, typename RESPONDER>
static void profiler(FUNCTION&& bitmap_creator,
                     RESPONDER&& responder,
                     size_t size,
                     size_t iter_count = 200) {
  std::vector<TYPE> values(size, 0);
  std::vector<uint8_t> bitmap_data(avxbmp::computeBitmapSize(values.size()), 0);
  int64_t null_count = 0;

  // populating values data with stuff
  for (size_t i = 0; i < values.size(); i++) {
    values[i] = (i % 2 == 0) ? 0 : avxbmp::helpers::null_builder<TYPE>();
  }

  //  measuring execution time profiling
  std::vector<size_t> durations(iter_count, 0);

  for (size_t i = 0; i < iter_count; i++) {
    auto start = std::chrono::high_resolution_clock::now();
    std::invoke(bitmap_creator, bitmap_data, null_count, values);
    durations[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                     std::chrono::high_resolution_clock::now() - start)
                     .count();
  }

  std::invoke(responder, find_median(durations));
}

template <typename TYPE>
static void profileAVX512_parallel(size_t size, size_t iter_count = 200) {
  auto bitmap_creator = [](std::vector<uint8_t>& bitmap_data,
                           int64_t& null_count_out,
                           const std::vector<TYPE>& vals) {
    return avxbmp::createBitmapParallelForAVX512<TYPE>(bitmap_data, null_count_out, vals);
  };

  auto responder = [&iter_count, &size](size_t dur_per_iter_ns) {
    double throughput_gibs =
        size * sizeof(TYPE) * 1.0e9 / dur_per_iter_ns / (1024 * 1024 * 1024);

    std::cout << "[AVX512 Parallel];\tSource size: " << std::setw(12) << std::right
              << size << ", type: " << std::setw(8)
              << avxbmp::helpers::get_type_name<TYPE>() << " (" << std::setw(0)
              << sizeof(TYPE) << " byte)"
              << ", elapsed time, usec: " << std::setw(7) << std::setprecision(6)
              << std::left << dur_per_iter_ns / 1000.0
              << ", throughput, GiB/sec: " << std::setw(8) << std::setprecision(3)
              << std::left << throughput_gibs << std::endl;
  };

  profiler<TYPE>(std::move(bitmap_creator), std::move(responder), size, iter_count);
}

template <typename TYPE>
static void profileAVX512_single_thread(size_t size, size_t iter_count = 200) {
  auto bitmap_creator = [](std::vector<uint8_t>& bitmap_data,
                           int64_t& null_count_out,
                           const std::vector<TYPE>& vals) {
    return avxbmp::createBitmapAVX512<TYPE>(bitmap_data, null_count_out, vals);
  };

  auto responder = [&iter_count, &size](size_t dur_per_iter_ns) {
    double throughput_gibs =
        size * sizeof(TYPE) * 1.0e9 / dur_per_iter_ns / (1024 * 1024 * 1024);

    std::cout << "[AVX512 Single Thread];\tSource size: " << std::setw(12) << std::right
              << size << ", type: " << std::setw(8)
              << avxbmp::helpers::get_type_name<TYPE>() << " (" << std::setw(0)
              << sizeof(TYPE) << " byte)"
              << ", elapsed time, usec: " << std::setw(7) << std::setprecision(6)
              << std::left << dur_per_iter_ns / 1000.0
              << ", throughput, GiB/sec: " << std::setw(8) << std::setprecision(3)
              << std::left << throughput_gibs << std::endl;
  };

  profiler<TYPE>(std::move(bitmap_creator), std::move(responder), size, iter_count);
}

template <typename TYPE>
static void profileDefault(size_t size, size_t iter_count = 200) {
  auto bitmap_creator = [](std::vector<uint8_t>& bitmap_data,
                           int64_t& null_count_out,
                           const std::vector<TYPE>& vals) {
    return avxbmp::createBitmap<TYPE>(bitmap_data, null_count_out, vals);
  };

  auto responder = [&iter_count, &size](size_t dur_per_iter_ns) {
    double throughput_gibs =
        size * sizeof(TYPE) * 1.0e9 / dur_per_iter_ns / (1024 * 1024 * 1024);

    std::cout << "[Default];             \tSource size: " << std::setw(12) << std::right
              << size << ", type: " << std::setw(8)
              << avxbmp::helpers::get_type_name<TYPE>() << " (" << std::setw(0)
              << sizeof(TYPE) << " byte)"
              << ", elapsed time, usec: " << std::setw(7) << std::setprecision(6)
              << std::left << dur_per_iter_ns / 1000.0
              << ", throughput, GiB/sec: " << std::setw(8) << std::setprecision(3)
              << std::left << throughput_gibs << std::endl;
  };

  profiler<TYPE>(std::move(bitmap_creator), std::move(responder), size, iter_count);
}

void major_profile(size_t size = 30'000'000) {
  std::cout << "\nAVX512 implementation profiling (single thread)\n";
  profileAVX512_single_thread<int8_t>(size, 1000);
  profileAVX512_single_thread<uint8_t>(size, 1000);

  profileAVX512_single_thread<int32_t>(size);
  profileAVX512_single_thread<uint32_t>(size);

  profileAVX512_single_thread<int64_t>(size);
  profileAVX512_single_thread<uint64_t>(size);

  profileAVX512_single_thread<float>(size);
  profileAVX512_single_thread<double>(size);

  std::cout << "\nAVX512 implementation profiling (parallel)\n";
  profileAVX512_parallel<int8_t>(size, 1000);
  profileAVX512_parallel<uint8_t>(size, 1000);

  profileAVX512_parallel<int32_t>(size);
  profileAVX512_parallel<uint32_t>(size);

  profileAVX512_parallel<int64_t>(size);
  profileAVX512_parallel<uint64_t>(size);

  profileAVX512_parallel<float>(size);
  profileAVX512_parallel<double>(size);

  std::cout << "\nThe usual C++ implementation profiling (single thread)\n";
  profileDefault<int8_t>(size);
  profileDefault<uint8_t>(size);

  profileDefault<int32_t>(size);
  profileDefault<uint32_t>(size);

  profileDefault<int64_t>(size);
  profileDefault<uint64_t>(size);

  profileDefault<float>(size);
  profileDefault<double>(size);
}

int main() try {
  major_profile(10'000'000);
  major_profile(3'000'000);
  major_profile(300'000);
  major_profile(30'000);
  major_profile(3'000);
  major_profile(64);
  return 0;
} catch (std::runtime_error& e) {
  std::cout << e.what() << std::endl;
  return 255;
}