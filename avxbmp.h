#ifndef INCLUDED_AVXBMP_H
#define INCLUDED_AVXBMP_H

//  component headers
#include<avxbmp_helpers.h>

//  std headers
#include<atomic>
#include<cstdint>
#include<type_traits>
#include<stdexcept>
#include<vector>

//  tbb
#include<tbb/parallel_for.h>

// #include <x86intrin.h>
// #include <immintrin.h>
// #include <nmmintrin.h>
// #include <xmmintrin.h>

//  =====================================================================
//  TODO:
//    1. Finish createBitmapAVX512 to hangle odd (not divisible by 64 as
//    length of raw data) arrays
//    2. Implement createBitmapParallelForAVX512()
//    3. Optimize performance createBitmapParallelForAVX512() for the size 
//    of the block range in the parallel for
//    4. If all goes well, convert to intrinsics
//    5. Implement Bitmap creation for AVX2 (tentative)
//  =====================================================================

//  External dependences (implemented in .S file)
extern "C" size_t gen_bitmap_avx512_8(uint8_t *bitmap, size_t *null_count, uint8_t *data, size_t size, uint64_t null_val);
extern "C" size_t gen_bitmap_avx512_32(uint8_t *bitmap, size_t *null_count, uint32_t *data, size_t size, uint64_t null_val);
extern "C" size_t gen_bitmap_avx512_64(uint8_t *bitmap, size_t *null_count, uint64_t *data, size_t size, uint64_t null_val);

namespace avxbmp {
    template<typename TYPE>
    size_t __attribute__((hot))  gen_bitmap_avx512(uint8_t *bitmap, size_t *null_count, TYPE *data, size_t size);

    template <typename TYPE>
    void createBitmapAVX512(std::vector<uint8_t>& bitmap_data, 
                            int64_t& null_count_out, 
                            const std::vector<TYPE>& vals);

    template <typename TYPE>
    void createBitmapParallelFor(
                            std::vector<uint8_t>& bitmap_data, 
                            int64_t& null_count_out, 
                            const std::vector<TYPE>& vals);


    template <typename TYPE>
    void createBitmapParallelForAVX512(
                            std::vector<uint8_t>& bitmap_data, 
                            int64_t& null_count_out, 
                            const std::vector<TYPE>& vals);


    template <typename TYPE>
    void createBitmap(std::vector<uint8_t>& bitmap_data, 
                    int64_t& null_count_out, 
                    const std::vector<TYPE>& vals);

    void printBitmap(const std::vector<uint8_t>& bitmap_data, bool reverse = true);
    size_t computeBitmapSize(size_t data_size);
    size_t diffBitmap(std::vector<uint8_t>& bm1, std::vector<uint8_t>& bm2);
};
                                    //  =================================
                                    //  Inlined functions implementations
                                    //  =================================
template<typename TYPE>
size_t avxbmp::gen_bitmap_avx512(uint8_t *bitmap, size_t *null_count, TYPE *data, size_t size)
{
    if constexpr (std::is_same<TYPE,  int8_t>::value) {
        return gen_bitmap_avx512_8(bitmap, null_count, reinterpret_cast<uint8_t*>(data), size, 0x8080808080808080);
    }
    else if constexpr (std::is_same<TYPE,  uint8_t>::value) {
        return gen_bitmap_avx512_8(bitmap, null_count, reinterpret_cast<uint8_t*>(data), size, 0xFFFFFFFFFFFFFFFF);
    }
    else if constexpr (std::is_same<TYPE,  int32_t>::value) {
        return gen_bitmap_avx512_32(bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0x8000000080000000);
    }
    else if constexpr (std::is_same<TYPE,  uint32_t>::value) {
        return gen_bitmap_avx512_32(bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0xFFFFFFFFFFFFFFFF);
    }
    else if constexpr (std::is_same<TYPE,  int64_t>::value) {
            return gen_bitmap_avx512_64(bitmap, null_count, reinterpret_cast<uint64_t*>(data), size, avxbmp::helpers::null_builder<int64_t>());
    }
    else if constexpr (std::is_same<TYPE,  uint64_t>::value) {
            return gen_bitmap_avx512_64(bitmap, null_count, reinterpret_cast<uint64_t*>(data), size, avxbmp::helpers::null_builder<uint64_t>());
    }
    if constexpr (std::is_same<TYPE,float>::value) {
            return gen_bitmap_avx512_32(bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0x0080000000800000);
    }
    if constexpr (std::is_same<TYPE,double>::value) {
            return gen_bitmap_avx512_64(bitmap, null_count, reinterpret_cast<uint64_t*>(data), size, 0x0010000000000000);
    }
    else {
        throw std::runtime_error ("avxbm::gen_bitmap_avx512() -- Unsupported type: " + 
                        avxbmp::helpers::get_type_name<TYPE>() + ". Aborting.");
    }
}


template <typename TYPE>
void avxbmp::createBitmapAVX512(
                          std::vector<uint8_t>& bitmap_data, 
                          int64_t& null_count_out, 
                          const std::vector<TYPE>& vals)
{
    size_t null_count = 0;
    size_t size = vals.size();
    if (size_t raw_size = vals.size()*sizeof(TYPE); raw_size % 64 != 0) {
        throw std::runtime_error ("The size of the raw data (" + std::to_string(raw_size)
                + ") is not divisible by 64.  Change the number of entries in the input data"
                  " to satisfy this condition. Aborting");
    }

    gen_bitmap_avx512<TYPE>(bitmap_data.data(), &null_count, const_cast<TYPE*>(vals.data()), size);

    null_count_out = null_count;
}


template <typename TYPE>
void avxbmp::createBitmapParallelFor(
                          std::vector<uint8_t>& bitmap_data, 
                          int64_t& null_count_out, 
                          const std::vector<TYPE>& vals) 
{
    TYPE null_val = avxbmp::helpers::null_builder<TYPE>();
    size_t chunk_rows_count = vals.size();
    size_t unroll_count = chunk_rows_count & 0xFFFFFFFFFFFFFFF8ULL;

    std::atomic<int64_t> null_count = 0;
    // tbb::parallel_for(
    //   tbb::blocked_range<size_t>(static_cast<size_t>(0), unroll_count / 8),
    //     [&](auto r) {
    // for (auto i = r.begin() * 8; i < r.end() * 8; i += 8) {

    constexpr size_t block_size = 64*1024;

    tbb::parallel_for(static_cast<size_t>(0), unroll_count, block_size, 
        [&](size_t idx) {
            int64_t local_null_count = 0;

            for (auto i = idx; i < std::min(idx+block_size, unroll_count); i += 8) {
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
        }
    );

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
void avxbmp::createBitmapParallelForAVX512(
                        std::vector<uint8_t>& bitmap_data, 
                        int64_t& null_count_out, 
                        const std::vector<TYPE>& vals)
{
    throw std::runtime_error ("TODO: IMPLEMENT avxbmp::createBitmapParallelForAVX512()!");
}


template <typename TYPE>
void avxbmp::createBitmap(std::vector<uint8_t>& bitmap_data, 
                          int64_t& null_count_out, 
                          const std::vector<TYPE>& vals)
{
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

#endif // INCLUDED_AVXBMP_H