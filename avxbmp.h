#ifndef INCLUDED_AVXBMP_H
#define INCLUDED_AVXBMP_H

//  component headers
#include <avxbmp_helpers.h>

//  std headers
#include <atomic>
// #include<bitset>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <type_traits>
#include <vector>

//  tbb
#include <tbb/parallel_for.h>

// #include <x86intrin.h>
// #include <immintrin.h>
// #include <nmmintrin.h>
// #include <xmmintrin.h>

//  =====================================================================
//  TODO:
//    4. If all goes well, convert to intrinsics
//    5. Implement Bitmap creation for AVX2 (tentative)
//  FINISHED:
//    1. Finish createBitmapAVX512 to hangle odd (not divisible by 64 as
//    length of raw data) arrays -- DONE, 2021.12.27
//    2. Implement createBitmapParallelForAVX512() -- DONE, 2021.12.28
//    3. Optimize performance createBitmapParallelForAVX512() for the size
//    of the block range in the parallel for -- DONE, 2021.12.28 (using
//    heuristic).
//  =====================================================================

//  External dependences (implemented in .S file)
extern "C" size_t gen_bitmap_avx512_8(uint8_t* bitmap,
                                      size_t* null_count,
                                      uint8_t* data,
                                      size_t size,
                                      uint64_t null_val);
extern "C" size_t gen_bitmap_avx512_32(uint8_t* bitmap,
                                       size_t* null_count,
                                       uint32_t* data,
                                       size_t size,
                                       uint64_t null_val);
extern "C" size_t gen_bitmap_avx512_64(uint8_t* bitmap,
                                       size_t* null_count,
                                       uint64_t* data,
                                       size_t size,
                                       uint64_t null_val);
extern "C" size_t gen_bitmap_avx512_8_intr(uint8_t* bitmap,
                                           size_t* null_count_out,
                                           uint8_t* data,
                                           size_t size,
                                           uint64_t null_val);
extern "C" size_t gen_bitmap_avx512_32_intr(uint8_t* bitmap,
                                            size_t* null_count,
                                            uint32_t* data,
                                            size_t size,
                                            uint64_t null_val);
extern "C" size_t gen_bitmap_avx512_64_intr(uint8_t* bitmap,
                                            size_t* null_count,
                                            uint64_t* data,
                                            size_t size,
                                            uint64_t null_val);

namespace avxbmp {
template <typename TYPE>
size_t __attribute__((hot))
gen_bitmap_avx512(uint8_t* bitmap, size_t* null_count, TYPE* data, size_t size);

template <typename TYPE>
void createBitmapAVX512(std::vector<uint8_t>& bitmap_data,
                        int64_t& null_count_out,
                        const std::vector<TYPE>& vals);

template <typename TYPE>
void createBitmapParallelFor(std::vector<uint8_t>& bitmap_data,
                             int64_t& null_count_out,
                             const std::vector<TYPE>& vals);

template <typename TYPE>
void createBitmapParallelForAVX512(std::vector<uint8_t>& bitmap_data,
                                   int64_t& null_count_out,
                                   const std::vector<TYPE>& vals);

template <typename TYPE>
void createBitmap(std::vector<uint8_t>& bitmap_data,
                  int64_t& null_count_out,
                  const std::vector<TYPE>& vals);

void printBitmap(const std::vector<uint8_t>& bitmap_data, bool reverse = true);
size_t computeBitmapSize(size_t data_size);
size_t diffBitmap(std::vector<uint8_t>& bm1,
                  std::vector<uint8_t>& bm2,
                  bool verbose = false);
};  // namespace avxbmp

//  =================================
//  Inlined functions implementations
//  =================================
#define USE_AVX512_INTRINSICS

template <typename TYPE>
size_t avxbmp::gen_bitmap_avx512(uint8_t* bitmap,
                                 size_t* null_count,
                                 TYPE* data,
                                 size_t size) {
#ifdef USE_AVX512_INTRINSICS
  if constexpr (std::is_same<TYPE, int8_t>::value) {
    return gen_bitmap_avx512_8_intr(
        bitmap, null_count, reinterpret_cast<uint8_t*>(data), size, 0x8080808080808080);
  } else if constexpr (std::is_same<TYPE, uint8_t>::value) {
    return gen_bitmap_avx512_8_intr(
        bitmap, null_count, reinterpret_cast<uint8_t*>(data), size, 0xFFFFFFFFFFFFFFFF);
  } else if constexpr (std::is_same<TYPE, int32_t>::value) {
    return gen_bitmap_avx512_32_intr(
        bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0x8000000080000000);
  } else if constexpr (std::is_same<TYPE, uint32_t>::value) {
    return gen_bitmap_avx512_32_intr(
        bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0xFFFFFFFFFFFFFFFF);
  } else if constexpr (std::is_same<TYPE, int64_t>::value) {
    return gen_bitmap_avx512_64_intr(bitmap,
                                     null_count,
                                     reinterpret_cast<uint64_t*>(data),
                                     size,
                                     avxbmp::helpers::null_builder<int64_t>());
  } else if constexpr (std::is_same<TYPE, uint64_t>::value) {
    return gen_bitmap_avx512_64_intr(bitmap,
                                     null_count,
                                     reinterpret_cast<uint64_t*>(data),
                                     size,
                                     avxbmp::helpers::null_builder<uint64_t>());
  }
  if constexpr (std::is_same<TYPE, float>::value) {
    return gen_bitmap_avx512_32_intr(
        bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0x0080000000800000);
  }
  if constexpr (std::is_same<TYPE, double>::value) {
    return gen_bitmap_avx512_64_intr(
        bitmap, null_count, reinterpret_cast<uint64_t*>(data), size, 0x0010000000000000);
  } else {
    throw std::runtime_error("avxbm::gen_bitmap_avx512() -- Unsupported type: " +
                             avxbmp::helpers::get_type_name<TYPE>() + ". Aborting.");
  }
#else   // USE ASSEMBLY CODE
  if constexpr (std::is_same<TYPE, int8_t>::value) {
    return gen_bitmap_avx512_8(
        bitmap, null_count, reinterpret_cast<uint8_t*>(data), size, 0x8080808080808080);
  } else if constexpr (std::is_same<TYPE, uint8_t>::value) {
    return gen_bitmap_avx512_8(
        bitmap, null_count, reinterpret_cast<uint8_t*>(data), size, 0xFFFFFFFFFFFFFFFF);
  } else if constexpr (std::is_same<TYPE, int32_t>::value) {
    return gen_bitmap_avx512_32(
        bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0x8000000080000000);
  } else if constexpr (std::is_same<TYPE, uint32_t>::value) {
    return gen_bitmap_avx512_32(
        bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0xFFFFFFFFFFFFFFFF);
  } else if constexpr (std::is_same<TYPE, int64_t>::value) {
    return gen_bitmap_avx512_64(bitmap,
                                null_count,
                                reinterpret_cast<uint64_t*>(data),
                                size,
                                avxbmp::helpers::null_builder<int64_t>());
  } else if constexpr (std::is_same<TYPE, uint64_t>::value) {
    return gen_bitmap_avx512_64(bitmap,
                                null_count,
                                reinterpret_cast<uint64_t*>(data),
                                size,
                                avxbmp::helpers::null_builder<uint64_t>());
  }
  if constexpr (std::is_same<TYPE, float>::value) {
    return gen_bitmap_avx512_32(
        bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0x0080000000800000);
  }
  if constexpr (std::is_same<TYPE, double>::value) {
    return gen_bitmap_avx512_64(
        bitmap, null_count, reinterpret_cast<uint64_t*>(data), size, 0x0010000000000000);
  } else {
    throw std::runtime_error("avxbm::gen_bitmap_avx512() -- Unsupported type: " +
                             avxbmp::helpers::get_type_name<TYPE>() + ". Aborting.");
  }
#endif  // USE_AVX512_INTRINSICS
}

namespace {
template <typename TYPE>
std::pair<size_t, size_t> compute_adjusted_sizes(const std::vector<TYPE>& v) {
  __uint128_t size_bytes = v.size() * sizeof(TYPE);
  __uint128_t rem_size_bytes = size_bytes & 0b00111111;
  __uint128_t rounded_size_bytes = size_bytes - rem_size_bytes;
  return {rounded_size_bytes / sizeof(TYPE), rem_size_bytes / sizeof(TYPE)};
}
}  // namespace

template <typename TYPE>
void avxbmp::createBitmapAVX512(std::vector<uint8_t>& bitmap_data,
                                int64_t& null_count_out,
                                const std::vector<TYPE>& vals) {
  size_t null_count = 0;
  auto [avx512_processing_count, cpu_processing_count] = compute_adjusted_sizes(vals);

  gen_bitmap_avx512<TYPE>(bitmap_data.data(),
                          &null_count,
                          const_cast<TYPE*>(vals.data()),
                          avx512_processing_count);

  if (cpu_processing_count > 0) {
    TYPE null_val = avxbmp::helpers::null_builder<TYPE>();
    uint8_t valid_byte = 0;
    size_t remaining_bits = 0;
    int64_t local_null_count = 0;
    for (size_t i = 0; i < cpu_processing_count; ++i) {
      size_t valid = vals[avx512_processing_count + i] != null_val;
      remaining_bits |= valid << i;
      null_count += !valid;
    }

    int left_bytes_encoded_count = (cpu_processing_count + 7) / 8;
    for (size_t i = 0; i < left_bytes_encoded_count; i++) {
      uint8_t encoded_byte = 0xFF & (remaining_bits >> (8 * i));
      bitmap_data[avx512_processing_count / 8 + i] = encoded_byte;
    }
  }

  null_count_out = null_count;
}

template <typename TYPE>
void avxbmp::createBitmapParallelFor(std::vector<uint8_t>& bitmap_data,
                                     int64_t& null_count_out,
                                     const std::vector<TYPE>& vals) {
  TYPE null_val = avxbmp::helpers::null_builder<TYPE>();
  size_t chunk_rows_count = vals.size();
  size_t unroll_count = chunk_rows_count & 0xFFFFFFFFFFFFFFF8ULL;

  std::atomic<int64_t> null_count = 0;
  // tbb::parallel_for(
  //   tbb::blocked_range<size_t>(static_cast<size_t>(0), unroll_count / 8),
  //     [&](auto r) {
  // for (auto i = r.begin() * 8; i < r.end() * 8; i += 8) {

  constexpr size_t block_size = 64 * 1024;

  tbb::parallel_for(static_cast<size_t>(0), unroll_count, block_size, [&](size_t idx) {
    int64_t local_null_count = 0;

    for (auto i = idx; i < std::min(idx + block_size, unroll_count); i += 8) {
      uint8_t valid_byte = 0;
      uint8_t valid;
      valid = vals[i + 0] != null_val;
      valid_byte |= valid << 0;
      local_null_count += !valid;
      valid = vals[i + 1] != null_val;
      valid_byte |= valid << 1;
      local_null_count += !valid;
      valid = vals[i + 2] != null_val;
      valid_byte |= valid << 2;
      local_null_count += !valid;
      valid = vals[i + 3] != null_val;
      valid_byte |= valid << 3;
      local_null_count += !valid;
      valid = vals[i + 4] != null_val;
      valid_byte |= valid << 4;
      local_null_count += !valid;
      valid = vals[i + 5] != null_val;
      valid_byte |= valid << 5;
      local_null_count += !valid;
      valid = vals[i + 6] != null_val;
      valid_byte |= valid << 6;
      local_null_count += !valid;
      valid = vals[i + 7] != null_val;
      valid_byte |= valid << 7;
      local_null_count += !valid;
      bitmap_data[i >> 3] = valid_byte;
    }
    null_count += local_null_count;
  });

  if (unroll_count != chunk_rows_count) {
    uint8_t valid_byte = 0;
    int64_t local_null_count = 0;
    for (size_t i = unroll_count; i < chunk_rows_count; ++i) {
      bool valid = vals[i] != null_val;
      valid_byte |= valid << (i & 7);
      local_null_count += !valid;
    }
    bitmap_data[unroll_count >> 3] = valid_byte;
    null_count += local_null_count;
  }

  null_count_out = null_count.load();
}

template <typename TYPE>
void avxbmp::createBitmapParallelForAVX512(std::vector<uint8_t>& bitmap_data,
                                           int64_t& null_count_out,
                                           const std::vector<TYPE>& vals) {
  static_assert(sizeof(TYPE) <= 64 && (64 % sizeof(TYPE) == 0),
                "Size of type must not exceed 64 and should devide 64.");

  auto [avx512_processing_count, cpu_processing_count] = compute_adjusted_sizes(vals);

  // Heuristic to compute appropriate block size for better load balance
  const size_t min_block_size = 64 / sizeof(TYPE);
  const size_t cpu_count = std::thread::hardware_concurrency();

  size_t blocks_per_thread =
      std::max<size_t>(1, avx512_processing_count / (min_block_size * cpu_count));
  size_t block_size = blocks_per_thread * min_block_size;

  // std::mutex mtx;
  std::atomic<int64_t> null_count = 0;

  //  Note: for large sizes of values data it is undesirable
  //  to use this function in parallel loop as the performance
  //  drops drastically
  auto simple_par_processor = [&](size_t idx) {
    size_t local_null_count = 0;
    size_t processing_count = min_block_size;
    uint8_t* bitmap_data_ptr = bitmap_data.data() + idx / 8;
    const TYPE* values_data_ptr = vals.data() + idx;

    gen_bitmap_avx512<TYPE>(bitmap_data_ptr,
                            &local_null_count,
                            const_cast<TYPE*>(values_data_ptr),
                            processing_count);
    null_count += local_null_count;
  };

  auto par_processor = [&](size_t idx) {
    size_t local_null_count = 0;
    size_t processing_count = std::min(block_size, avx512_processing_count - idx);
    uint8_t* bitmap_data_ptr = bitmap_data.data() + idx / 8;
    const TYPE* values_data_ptr = vals.data() + idx;

    // {
    //   std::lock_guard lock(mtx);
    //   std::cout
    //     << "Processing range: " << idx << " - " << idx+processing_count
    //     << ",\tblock size: " << block_size
    //     << ",\tprocessing_count: " << processing_count
    //     << std::endl;
    // }

    gen_bitmap_avx512<TYPE>(bitmap_data_ptr,
                            &local_null_count,
                            const_cast<TYPE*>(values_data_ptr),
                            processing_count);
    null_count += local_null_count;
  };

  // tbb::parallel_for(static_cast<size_t>(0), avx512_processing_count, min_block_size,
  // simple_par_processor);
  tbb::parallel_for(
      static_cast<size_t>(0), avx512_processing_count, block_size, par_processor);

  if (cpu_processing_count > 0) {
    TYPE null_val = avxbmp::helpers::null_builder<TYPE>();
    uint8_t valid_byte = 0;
    size_t remaining_bits = 0;
    int64_t cpus_null_count = 0;
    for (size_t i = 0; i < cpu_processing_count; ++i) {
      size_t valid = vals[avx512_processing_count + i] != null_val;
      remaining_bits |= valid << i;
      cpus_null_count += !valid;
    }

    int left_bytes_encoded_count = (cpu_processing_count + 7) / 8;
    for (size_t i = 0; i < left_bytes_encoded_count; i++) {
      uint8_t encoded_byte = 0xFF & (remaining_bits >> (8 * i));
      bitmap_data[avx512_processing_count / 8 + i] = encoded_byte;
    }
    null_count += cpus_null_count;
  }

  null_count_out = null_count.load();
}

template <typename TYPE>
void avxbmp::createBitmap(std::vector<uint8_t>& bitmap_data,
                          int64_t& null_count_out,
                          const std::vector<TYPE>& vals) {
  TYPE null_val = avxbmp::helpers::null_builder<TYPE>();
  size_t chunk_rows_count = vals.size();
  size_t unroll_count = chunk_rows_count & 0xFFFFFFFFFFFFFFF8ULL;

  std::atomic<int64_t> null_count = 0;
  int64_t local_null_count = 0;

  size_t idx = 0;
  for (auto i = 0; i < unroll_count; i += 8) {
    uint8_t valid_byte = 0;
    uint8_t valid;
    valid = vals[i + 0] != null_val;
    valid_byte |= valid << 0;
    local_null_count += !valid;
    valid = vals[i + 1] != null_val;
    valid_byte |= valid << 1;
    local_null_count += !valid;
    valid = vals[i + 2] != null_val;
    valid_byte |= valid << 2;
    local_null_count += !valid;
    valid = vals[i + 3] != null_val;
    valid_byte |= valid << 3;
    local_null_count += !valid;
    valid = vals[i + 4] != null_val;
    valid_byte |= valid << 4;
    local_null_count += !valid;
    valid = vals[i + 5] != null_val;
    valid_byte |= valid << 5;
    local_null_count += !valid;
    valid = vals[i + 6] != null_val;
    valid_byte |= valid << 6;
    local_null_count += !valid;
    valid = vals[i + 7] != null_val;
    valid_byte |= valid << 7;
    local_null_count += !valid;
    bitmap_data[i >> 3] = valid_byte;
  }
  null_count += local_null_count;

  if (unroll_count != chunk_rows_count) {
    uint8_t valid_byte = 0;
    int64_t local_null_count = 0;
    for (size_t i = unroll_count; i < chunk_rows_count; ++i) {
      bool valid = vals[i] != null_val;
      valid_byte |= valid << (i & 7);
      local_null_count += !valid;
    }
    bitmap_data[unroll_count >> 3] = valid_byte;
    null_count += local_null_count;
  }

  null_count_out = null_count.load();
}

#endif  // INCLUDED_AVXBMP_H