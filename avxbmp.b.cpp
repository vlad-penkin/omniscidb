// component headers
#include <avxbmp.h>

// std headers
#include <iomanip>
#include <iostream>
#include <vector>

namespace {

template <typename TYPE>
void profile_avx512(size_t size, size_t iter_count = 200) {
  size_t avx512_batches_count = 64 / sizeof(TYPE);

  if (size % avx512_batches_count != 0) {
    throw std::runtime_error("Provided size (" + std::to_string(size) +
                             ") is not a multiple of " +
                             std::to_string(avx512_batches_count) + ".  Aborting.");
  }

  std::vector<TYPE> nulldata(size, 0);

  for (size_t i = 0; i < nulldata.size(); i++) {
    nulldata[i] = (i % 2 == 0) ? 0 : avxbmp::helpers::null_builder<TYPE>();
  }

  size_t bitmap_size = avxbmp::computeBitmapSize(nulldata.size());
  std::vector<uint8_t> bitmap_data(bitmap_size, 0);
  size_t null_count = 0;

  auto start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < iter_count; i++) {
    avxbmp::gen_bitmap_avx512(
        bitmap_data.data(), &null_count, nulldata.data(), nulldata.size());
  }
  size_t dur = std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count();
  double dur_per_iter = dur / iter_count;
  double throughput_gibs =
      size * sizeof(TYPE) * 1.0e9 / dur_per_iter / (1024 * 1024 * 1024);

  std::cout << "[AVX512];\tSource size: " << std::setw(12) << std::right << size
            << ", type: " << std::setw(8) << avxbmp::helpers::get_type_name<TYPE>()
            << " (" << std::setw(0) << sizeof(TYPE) << " byte)"
            << ", elapsed time, usec: " << std::setw(7) << std::setprecision(6)
            << std::left << dur_per_iter / 1000.0
            << ", throughput, GiB/sec: " << std::setw(8) << std::setprecision(3)
            << std::left << throughput_gibs << std::endl;
}

template <typename TYPE>
void profile_default(size_t size, size_t num_iter = 200) {
  size_t avx512_batches_count = 64 / sizeof(TYPE);

  if (size % avx512_batches_count != 0) {
    throw std::runtime_error("Provided size (" + std::to_string(size) +
                             ") is not a multiple of " +
                             std::to_string(avx512_batches_count) + ".  Aborting.");
  }

  std::vector<TYPE> nulldata(size, 0);

  for (size_t i = 0; i < nulldata.size(); i++) {
    nulldata[i] = (i % 2 == 0) ? 0 : avxbmp::helpers::null_builder<TYPE>();
  }

  size_t bitmap_size = avxbmp::computeBitmapSize(nulldata.size());
  std::vector<uint8_t> bitmap_data(bitmap_size, 0);
  int64_t null_count = 0;

  int iter_count = num_iter / sizeof(TYPE);
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iter_count; i++) {
    avxbmp::createBitmap(bitmap_data, null_count, nulldata);
  }
  size_t dur = std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count();

  double dur_per_iter = dur / iter_count;
  double throughput_gibs =
      size * sizeof(TYPE) * 1.0e9 / dur_per_iter / (1024 * 1024 * 1024);
  std::cout << "[DEFAULT];\tSource size: " << std::setw(12) << std::right << size
            << ", type: " << std::setw(8) << avxbmp::helpers::get_type_name<TYPE>()
            << " (" << std::setw(0) << sizeof(TYPE) << " byte)"
            << ", elapsed time, usec: " << std::setw(7) << std::setprecision(6)
            << std::left << dur_per_iter / 1000.0
            << ", throughput, GiB/sec: " << std::setw(8) << std::setprecision(3)
            << std::left << throughput_gibs << std::endl;
}

}  // anonymous namespace

void major_profile(size_t size = 30'000'000) {
  std::cout << "\nAVX512 implementation profiling (single thread)\n";

  profile_avx512<uint8_t>(size);
  profile_avx512<int8_t>(size);

  profile_avx512<int32_t>(size);
  profile_avx512<uint32_t>(size);

  profile_avx512<int64_t>(size);
  profile_avx512<uint64_t>(size);

  profile_avx512<float>(size);
  profile_avx512<double>(size);

  std::cout << "\nThe usual C++ implementation profiling (single thread)\n";

  //  default (cpu) implementation profiling
  profile_default<int8_t>(size);
  profile_default<uint8_t>(size);

  profile_default<int32_t>(size);
  profile_default<uint32_t>(size);

  profile_default<int64_t>(size);
  profile_default<uint64_t>(size);

  profile_default<float>(size);
  profile_default<double>(size);
}

int main() try {
  major_profile(3'000'000);
  major_profile(300'032);
  major_profile(30'016);
  major_profile(3'008);
  major_profile(64);

  return 0;
} catch (std::runtime_error& e) {
  std::cout << e.what() << std::endl;
  return 255;
}
