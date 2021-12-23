#include<atomic>
#include<bitset>
#include<cassert>
#include<cfloat>
#include<cstdint>
#include<cstring>
#include<chrono>
#include<limits>
#include<iostream>
#include<vector>
#include<x86intrin.h>
#include<iomanip>


#include <immintrin.h>
#include <nmmintrin.h>
#include <xmmintrin.h>

//  tbb
#include<tbb/parallel_for.h>

//  boost
#include<boost/align/aligned_allocator.hpp>

//  External dependences
extern "C" size_t gen_bitmap_8(uint8_t *bitmap, size_t *null_count, uint8_t *data, size_t size, uint64_t null_val);
extern "C" size_t gen_bitmap_32(uint8_t *bitmap, size_t *null_count, uint32_t *data, size_t size, uint64_t null_val);
extern "C" size_t gen_bitmap_64(uint8_t *bitmap, size_t *null_count, uint64_t *data, size_t size, uint64_t null_val);

// CC: 
//  g++ -O3 -std=c++17 bitmap.cpp -I /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/include -L /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/lib -ltbb -o bitmap 
size_t computeBitmapSize(size_t data_size) {
    return (data_size + 7) / 8;    
}

template <class TYPE> constexpr inline TYPE inline_int_null_value() {
  return std::is_signed<TYPE>::value 
            ? std::numeric_limits<TYPE>::min()
            : std::numeric_limits<TYPE>::max();
}

template <class TYPE> constexpr inline TYPE inline_fp_null_value();

template <> constexpr inline float inline_fp_null_value<float>() {
  return FLT_MIN;
}

template <> constexpr inline double inline_fp_null_value<double>() {
  return DBL_MIN;
}

template <typename TYPE> constexpr TYPE null_builder() {
  static_assert(std::is_floating_point_v<TYPE> || std::is_integral_v<TYPE>,
                "Unsupported type");

  if constexpr (std::is_floating_point_v<TYPE>) {
    return inline_fp_null_value<TYPE>();
  } else if constexpr (std::is_integral_v<TYPE>) {
    return inline_int_null_value<TYPE>();
  }
}

template<typename TYPE>
std::string get_type_name() {
    if constexpr (std::is_same<TYPE,  char>::value)  return "char";
    else if constexpr (std::is_same<TYPE,  int8_t>::value)  return "int8_t";
    else if constexpr (std::is_same<TYPE,  uint8_t>::value) return "uint8_t";
    else if constexpr (std::is_same<TYPE,  int32_t>::value) return "int32_t";
    else if constexpr (std::is_same<TYPE, uint32_t>::value) return "uint32_t";
    else if constexpr (std::is_same<TYPE,  int64_t>::value) return "int64_t";
    else if constexpr (std::is_same<TYPE, uint64_t>::value) return "uint64_t";
    else if constexpr (std::is_same<TYPE,  double>::value) return "double";
    else if constexpr (std::is_same<TYPE,  float>::value)  return "float";
    else {
        throw std::runtime_error ("get_type_name() -- unsupported type");
    }
}

void printBitmap(const std::vector<uint8_t>& bitmap_data, bool reverse = true)
{
    for (auto & c: bitmap_data) {
        std::string s = std::bitset<8>(c).to_string();
        if (reverse) { std::reverse (std::begin(s), std::end(s)); }
        std::cout << s << " ";
    }
    std::cout << std::endl;
}

template <typename TYPE = int32_t>
void createBitmapParallelFor(std::vector<uint8_t>& bitmap_data, 
                             int64_t& null_count_out, 
                             const std::vector<TYPE>& vals,
                             TYPE null_val = null_builder<TYPE>() ) 
{
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
void createBitmap(std::vector<uint8_t>& bitmap_data, 
                  int64_t& null_count_out, 
                  const std::vector<TYPE>& vals,
                  TYPE null_val = null_builder<TYPE>()) 
{
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

// void profile(size_t size) {
//     std::vector<int32_t> values(size, 0);
//     values[0] = std::numeric_limits<int32_t>::max();
//     size_t bitmap_size = computeBitmapSize(values.size());

//     int64_t null_count = 0;
//     std::vector<uint8_t> bitmap_data(bitmap_size, 0);

//     auto start = std::chrono::high_resolution_clock::now();
//     createBitmap(bitmap_data, null_count, values);
//     size_t dur = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-start).count(); 
//     std::cout << "[DEFAULT] Elapsed, usec: " << dur << std::endl;
// }

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


template<typename TYPE>
size_t gen_bitmap(uint8_t *bitmap, size_t *null_count, TYPE *data, size_t size)
{
    if constexpr (std::is_same<TYPE,  int8_t>::value) {
        return gen_bitmap_8(bitmap, null_count, reinterpret_cast<uint8_t*>(data), size, 0x8080808080808080);
    }
    else if constexpr (std::is_same<TYPE,  uint8_t>::value) {
        return gen_bitmap_8(bitmap, null_count, reinterpret_cast<uint8_t*>(data), size, 0xFFFFFFFFFFFFFFFF);
    }
    else if constexpr (std::is_same<TYPE,  int32_t>::value) {
        return gen_bitmap_32(bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0x8000000080000000);
    }
    else if constexpr (std::is_same<TYPE,  uint32_t>::value) {
        return gen_bitmap_32(bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0xFFFFFFFFFFFFFFFF);
    }
    else if constexpr (std::is_same<TYPE,  int64_t>::value) {
            return gen_bitmap_64(bitmap, null_count, reinterpret_cast<uint64_t*>(data), size, null_builder<int64_t>());
    }
    else if constexpr (std::is_same<TYPE,  uint64_t>::value) {
            return gen_bitmap_64(bitmap, null_count, reinterpret_cast<uint64_t*>(data), size, null_builder<uint64_t>());
    }
    if constexpr (std::is_same<TYPE,float>::value) {
            return gen_bitmap_32(bitmap, null_count, reinterpret_cast<uint32_t*>(data), size, 0x0080000000800000);
    }
    if constexpr (std::is_same<TYPE,double>::value) {
            return gen_bitmap_64(bitmap, null_count, reinterpret_cast<uint64_t*>(data), size, 0x0010000000000000);
    }
    else {
        throw std::runtime_error ("gen_bitmap() -- Unsupported type: " + get_type_name<TYPE>() + ". Aborting.");
    }
}



template<typename TYPE>
void test(size_t size) {

    size_t items_per_iteration = 64/sizeof(TYPE);

    if (size % items_per_iteration != 0) {
        throw std::runtime_error ("Provided size ("+std::to_string(size)
                    +") is not a multiple of "+std::to_string(items_per_iteration)+".  Aborting.");
    }

    size_t actual_null_count;
    int64_t test_null_count;
    TYPE null_value = null_builder<TYPE>();

    std::vector<TYPE, boost::alignment::aligned_allocator<TYPE, 64>> 
        nulldata (size, 0);

    size_t bitmap_size = computeBitmapSize(nulldata.size());
    std::vector<uint8_t> actual_bitmap_data(bitmap_size, 0);
    std::vector<uint8_t> test_bitmap_data(bitmap_size, 0);

    //  populating nulldata
    for (size_t i=0; i<nulldata.size(); i++) {
        nulldata[i] = (i%2 == 0) ? 0 : null_value;
    }

    // std::cout << get_type_name<TYPE>() <<"; " << std::bitset<sizeof(TYPE)*8> (nulldata[0]) << std::dec << std::endl;
    // std::cout << get_type_name<TYPE>() <<"; " << std::bitset<sizeof(TYPE)*8> (reinterpret_cast<uint64_t&>(nulldata[1])) << std::dec << std::endl;
    // std::cout << get_type_name<TYPE>() <<"; " << std::hex << reinterpret_cast <uint64_t&>(nulldata[1]) << std::dec << std::endl;

    std::vector<TYPE> test_nulldata(nulldata.begin(), nulldata.end());

    createBitmap(test_bitmap_data, test_null_count, test_nulldata, null_value);
    gen_bitmap(actual_bitmap_data.data(), &actual_null_count, nulldata.data(), nulldata.size());

    std::cout <<get_type_name<TYPE>() <<"; Expected bitmap: "; printBitmap(test_bitmap_data, true);
    std::cout <<get_type_name<TYPE>() <<"; Actual bitmap:   "; printBitmap(actual_bitmap_data, true);

    std::cout <<get_type_name<TYPE>() <<"; NULL COUNT: expected: " << test_null_count << ", actual: " << actual_null_count << std::endl;
}




template<typename TYPE>
void profile_avx512(size_t size, size_t num_iter = 200) {
    size_t items_per_iteration = 64/sizeof(TYPE);

    if (size % items_per_iteration != 0) {
        throw std::runtime_error ("Provided size ("+std::to_string(size)
                    +") is not a multiple of "+std::to_string(items_per_iteration)+".  Aborting.");
    }

    std::vector<TYPE, boost::alignment::aligned_allocator<TYPE, 64>> 
        nulldata(size,0);

    //  populating nulldata
    for (size_t i=0; i<nulldata.size(); i++) {
        nulldata[i] = (i%2 == 0) ? 0 : null_builder<TYPE>();
    }


    size_t bitmap_size = computeBitmapSize(nulldata.size());
    std::vector<uint8_t> bitmap_data(bitmap_size, 0);
    size_t null_count = 0;


    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i<num_iter/sizeof(TYPE); i++) {
        gen_bitmap(bitmap_data.data(), &null_count, nulldata.data(), nulldata.size());
    }
    size_t dur = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now()-start).count(); 

    std::cout 
        << "[AVX512];\tSource size: " << std::setw(12) <<std::right << size 
        << ", type: " << std::setw(8) << get_type_name<TYPE>()
        << " (" << std::setw(0) << sizeof(TYPE) <<" byte)"
        << ", elapsed time, usec: " << std::setw(7) << std::setprecision(6) << std::left << dur/1000.0/num_iter << std::endl;
}

template<typename TYPE>
void profile_default(size_t size, size_t num_iter = 200) {
    size_t items_per_iteration = 64/sizeof(TYPE);

    if (size % items_per_iteration != 0) {
        throw std::runtime_error ("Provided size ("+std::to_string(size)
                    +") is not a multiple of "+std::to_string(items_per_iteration)+".  Aborting.");
    }

    std::vector<TYPE/*, boost::alignment::aligned_allocator<TYPE, 64>*/>
        nulldata(size,0);

    for (size_t i=0; i<nulldata.size(); i++) {
        nulldata[i] = (i%2 == 0) ? 0 : null_builder<TYPE>();
    }

    size_t bitmap_size = computeBitmapSize(nulldata.size());
    std::vector<uint8_t> bitmap_data(bitmap_size, 0);
    int64_t null_count = 0;


    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i<num_iter/sizeof(TYPE); i++) {
        createBitmap(bitmap_data, null_count, nulldata);
    }
    size_t dur = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now()-start).count(); 

    std::cout 
        << "[DEFAULT];\tSource size: " << std::setw(12) <<std::right << size 
        << ", type: " << std::setw(8) << get_type_name<TYPE>()
        << " (" << std::setw(0) << sizeof(TYPE) <<" byte)"
        << ", elapsed time, usec: " << std::setw(7) << std::setprecision(6) << std::left << dur/1000.0/num_iter << std::endl;
}

// void profile(size_t size) {
//     std::vector<int32_t> values(size, 0);
//     values[0] = std::numeric_limits<int32_t>::max();
//     size_t bitmap_size = computeBitmapSize(values.size());

//     int64_t null_count = 0;
//     std::vector<uint8_t> bitmap_data(bitmap_size, 0);

//     auto start = std::chrono::high_resolution_clock::now();
//     createBitmap(bitmap_data, null_count, values);
//     size_t dur = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-start).count(); 
//     std::cout << "[DEFAULT] Elapsed, usec: " << dur << std::endl;
// }



void major_test()
{
    test<int8_t>(64);
    test<uint8_t>(64);
    test<int32_t>(64);
    test<uint32_t>(64);
    test<int64_t>(64);
    test<uint64_t>(64);
    test<float>(64);
    test<double>(64);
}

void major_profile()
{
    std::cout << "\nAVX512 implementation profiling (single thread)\n";

    profile_avx512<int8_t>(30'000'000);
    profile_avx512<uint8_t>(30'000'000);

    profile_avx512<int32_t>(30'000'000);
    profile_avx512<uint32_t>(30'000'000);

    profile_avx512<int64_t>(30'000'000);
    profile_avx512<uint64_t>(30'000'000);

    profile_avx512<float>(30'000'000);
    profile_avx512<double>(30'000'000);

    std::cout << "\nThe usual C++ implementation profiling (single thread)\n";
    //  default (cpu) implementation profiling
    profile_default<int8_t>(30'000'000);
    profile_default<uint8_t>(30'000'000);

    profile_default<int32_t>(30'000'000);
    profile_default<uint32_t>(30'000'000);

    profile_default<int64_t>(30'000'000);
    profile_default<uint64_t>(30'000'000);

    profile_default<float>(30'000'000);
    profile_default<double>(30'000'000);
}

int main() try {
    major_test();
    major_profile();
    return 0;
}
catch (std::runtime_error & e) {
    std::cout << e.what() << std::endl;
    return 255;
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


    // for (int i = 0; i<10; i++) {
    //     profile(30'000'000);
    // }

    // for (int i = 0; i<10; i++) {
    //     profile_tbb(30'000'000);
    // }    
    return 0;
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
