#include<atomic>
#include<bitset>
#include<cassert>
#include<cstdint>
#include<cstring>
#include<chrono>
#include<limits>
#include<iostream>
#include<vector>
#include<x86intrin.h>

//  TBB
#include<tbb/parallel_for.h>

// CC: 
//  g++ -O3 -std=c++17 bitmap.cpp -I /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/include -L /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/lib -ltbb -o bitmap 
size_t computeBitmapSize(size_t data_size) {
    return (data_size + 7) / 8;    
}


void printBitmap(const std::vector<uint8_t>& bitmap_data, bool reverse = true)
{
    for (auto & c: bitmap_data) {
        std::string s = std::bitset<8>(c).to_string();
        if (reverse) {
            std::reverse (std::begin(s), std::end(s));
        }
        std::cout << s << " ";
    }
    std::cout << std::endl;
}

template <typename TYPE = int32_t>
void createBitmapParallelFor(std::vector<uint8_t>& bitmap_data, int64_t& null_count_out, const std::vector<TYPE>& vals) 
{
    auto null_val = std::numeric_limits<TYPE>::max();
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

template <typename TYPE = int32_t>
void createBitmap(std::vector<uint8_t>& bitmap_data, int64_t& null_count_out, const std::vector<TYPE>& vals) 
{
    auto null_val = std::numeric_limits<TYPE>::max();
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


void test1() {
    std::vector<uint8_t> data = {0xFF, 0x0F, 0xF0, 0x00, 0x70, 0x07, 0x10, 0x01, 0x03};
    std::cout << "Bitmap size: " << computeBitmapSize (data.size()) << std::endl;
    printBitmap(data, false);
}

void profile(size_t size) {
    std::vector<int32_t> values(size, 0);
    values[0] = std::numeric_limits<int32_t>::max();
    size_t bitmap_size = computeBitmapSize(values.size());

    int64_t null_count = 0;
    std::vector<uint8_t> bitmap_data(bitmap_size, 0);

    auto start = std::chrono::high_resolution_clock::now();
    createBitmap(bitmap_data, null_count, values);
    size_t dur = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-start).count(); 
    std::cout << "[DEFAULT] Elapsed, usec: " << dur << std::endl;
}

void profile_tbb(size_t size) {
    std::vector<int32_t> values(size, 0);
    values[0] = std::numeric_limits<int32_t>::max();
    size_t bitmap_size = computeBitmapSize(values.size());

    int64_t null_count = 0;
    std::vector<uint8_t> bitmap_data(bitmap_size, 0);

    auto start = std::chrono::high_resolution_clock::now();
    createBitmapParallelFor(bitmap_data, null_count, values);
    size_t dur = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-start).count(); 
    std::cout << "[TBB_PARALLEL_FOR] Elapsed, usec: " << dur << std::endl;
}


size_t diffBitmap(std::vector<uint8_t>& bm1, std::vector<uint8_t>& bm2)
{
    if (bm1.size() != bm2.size()) {
        std::cout << "Bitmaps are of different size (" << bm1.size() << " vs " <<bm2.size() << ")\n";
        std::numeric_limits<size_t>::max();
    }
    size_t diff_count = 0;
    for (size_t i = 0; i<bm1.size(); i++) {
        if (bm1[i]!=bm2[i]) {
            std::cout 
                << "Bitmaps differ at: " << i << "; " 
                << std::bitset<8> (bm1[i]) << " vs " << std::bitset<8> (bm1[2]) << std::endl;
            diff_count++;
        }
    }
    if (diff_count) {
        std::cout << "Found " << diff_count << " differences" << std::endl;
    }
    return diff_count;
}

// It's assumed sample has 64 extra bytes to handle alignment.
__attribute__((target("avx512f"))) void spread_vec_sample(uint8_t* dst,
                                                          const size_t dst_size,
                                                          const uint8_t* sample_ptr,
                                                          const size_t sample_size) {
  assert((reinterpret_cast<uint64_t>(dst) & 0x3F) ==
         (reinterpret_cast<uint64_t>(sample_ptr) & 0x3F));
  // Align buffers.
  int64_t align_bytes = ((64ULL - reinterpret_cast<uint64_t>(dst)) & 0x3F);
  memcpy(dst, sample_ptr, align_bytes);

  uint8_t* align_dst = dst + align_bytes;
  const uint8_t* align_sample = sample_ptr + align_bytes;
  size_t rem = dst_size - align_bytes;
  size_t rem_scalar = rem % sample_size;
  size_t rem_vector = rem - rem_scalar;

  // Aligned vector part.
  auto vecs = sample_size / 64;
  auto vec_end = align_dst + rem_vector;
  while (align_dst < vec_end) {
    for (size_t i = 0; i < vecs; ++i) {
      __m512i vec_val =
          _mm512_load_si512(reinterpret_cast<const __m512i*>(align_sample) + i);
      _mm512_stream_si512(reinterpret_cast<__m512i*>(align_dst) + i, vec_val);
    }
    align_dst += sample_size;
  }

  // Scalar tail.
  memcpy(align_dst, align_sample, rem_scalar);
}

#include <immintrin.h>
#include <nmmintrin.h>
#include <xmmintrin.h>



extern "C" size_t gen_bitmap_8(uint8_t *bitmap, size_t *null_count, uint8_t *data, size_t size);
__v64qi v0_8 = {-1, 0, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, 0, -1};


int main()
{
    size_t null_count;
    std::vector<uint8_t> bm_data(512/8/8);
    gen_bitmap_8(bm_data.data(), &null_count, reinterpret_cast<uint8_t*>(&v0_8), 0);
    std::cout << "Null count: " << null_count << std::endl;
    printBitmap(bm_data, true);

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i<100000; i++) {
        gen_bitmap_8(bm_data.data(), &null_count, reinterpret_cast<uint8_t*>(&v0_8), 0);
    }
    size_t dur = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now()-start).count(); 
    std::cout << "[AVX512] Elapsed, ns: " << dur/100000.0 << std::endl;
}


int test2()
{
    size_t size = 16;
    std::vector<int32_t> values(size, 0);
    values[0] = std::numeric_limits<int32_t>::max();
    size_t bitmap_size = computeBitmapSize(values.size());
    std::vector<uint8_t> bitmap_data(bitmap_size, 0);
    std::cout << "Bitmap size: " << bitmap_data.size() << std::endl;
    int64_t null_count = 0;
    createBitmap(bitmap_data, null_count, values);
    std::cout << "Nulls count: " << null_count << std::endl;
    printBitmap(bitmap_data, true);

    std::cout << "Running diffBitmap on the same data\n";
    diffBitmap(bitmap_data, bitmap_data);

    std::cout << "Running diffBitmap on the different bitmap data\n";
    std::vector<uint8_t> test_bitmap_data (bitmap_data);
    test_bitmap_data [0] = 0;
    diffBitmap(bitmap_data, test_bitmap_data);


    for (int i = 0; i<10; i++) {
        profile(30'000'000);
    }

    for (int i = 0; i<10; i++) {
        profile_tbb(30'000'000);
    }    
    return 0;
}