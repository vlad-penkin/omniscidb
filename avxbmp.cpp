//  component headers
#include<avxbmp.h>


// #include<atomic>
#include<bitset>
// #include<cassert>
// #include<cfloat>
// #include<cstdint>
// #include<cstring>
// #include<chrono>
#include<limits>
#include<iostream>
#include<vector>
// #include<x86intrin.h>
// #include<iomanip>


// #include <immintrin.h>
// #include <nmmintrin.h>
// #include <xmmintrin.h>


//  boost
// #include<boost/align/aligned_allocator.hpp>

// CC: 
//  g++ -O3 -std=c++17 bitmap.cpp -I /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/include -L /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/lib -ltbb -o bitmap 
size_t avxbmp::computeBitmapSize(size_t data_size) {
    return (data_size + 7) / 8;    
}


void avxbmp::printBitmap(const std::vector<uint8_t>& bitmap_data, bool reverse)
{
    for (auto & c: bitmap_data) {
        std::string s = std::bitset<8>(c).to_string();
        if (reverse) { std::reverse (std::begin(s), std::end(s)); }
        std::cout << s << " ";
    }
    std::cout << std::endl;
}



size_t avxbmp::diffBitmap(std::vector<uint8_t>& bm1, std::vector<uint8_t>& bm2)
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




// void test1() {
//     std::vector<uint8_t> data = {0xFF, 0x0F, 0xF0, 0x00, 0x70, 0x07, 0x10, 0x01, 0x03};
//     std::cout << "Bitmap size: " << computeBitmapSize (data.size()) << std::endl;
//     printBitmap(data, false);
// }

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



// void major_test()
// {
//     test<uint8_t>(64);
//     test<int8_t>(64);
//     test<int32_t>(64);
//     test<uint32_t>(64);
//     test<int64_t>(64);
//     test<uint64_t>(64);
//     test<float>(64);
//     test<double>(64);
// }


// int main() try {
//     major_test();
//     major_profile();
//     major_profile(3'000'000);
//     major_profile(300'032);
//     major_profile(30'016);
//     major_profile(3'008);
//     major_profile(64);

//     return 0;
// }
// catch (std::runtime_error & e) {
//     std::cout << e.what() << std::endl;
//     return 255;
// }



// int test2()
// {
//     size_t size = 16;
//     std::vector<int32_t> values(size, 0);
//     values[0] = std::numeric_limits<int32_t>::max();
//     size_t bitmap_size = computeBitmapSize(values.size());
//     std::vector<uint8_t> bitmap_data(bitmap_size, 0);
//     std::cout << "Bitmap size: " << bitmap_data.size() << std::endl;
//     int64_t null_count = 0;
//     createBitmap(bitmap_data, null_count, values);
//     std::cout << "Nulls count: " << null_count << std::endl;
//     printBitmap(bitmap_data, true);

//     std::cout << "Running diffBitmap on the same data\n";
//     diffBitmap(bitmap_data, bitmap_data);

//     std::cout << "Running diffBitmap on the different bitmap data\n";
//     std::vector<uint8_t> test_bitmap_data (bitmap_data);
//     test_bitmap_data [0] = 0;
//     diffBitmap(bitmap_data, test_bitmap_data);

//     return 0;
// }














// // It's assumed sample has 64 extra bytes to handle alignment.
// __attribute__((target("avx512f"))) void spread_vec_sample(uint8_t* dst,
//                                                           const size_t dst_size,
//                                                           const uint8_t* sample_ptr,
//                                                           const size_t sample_size) {
//   assert((reinterpret_cast<uint64_t>(dst) & 0x3F) ==
//          (reinterpret_cast<uint64_t>(sample_ptr) & 0x3F));
//   // Align buffers.
//   int64_t align_bytes = ((64ULL - reinterpret_cast<uint64_t>(dst)) & 0x3F);
//   memcpy(dst, sample_ptr, align_bytes);

//   uint8_t* align_dst = dst + align_bytes;
//   const uint8_t* align_sample = sample_ptr + align_bytes;
//   size_t rem = dst_size - align_bytes;
//   size_t rem_scalar = rem % sample_size;
//   size_t rem_vector = rem - rem_scalar;

//   // Aligned vector part.
//   auto vecs = sample_size / 64;
//   auto vec_end = align_dst + rem_vector;
//   while (align_dst < vec_end) {
//     for (size_t i = 0; i < vecs; ++i) {
//       __m512i vec_val =
//           _mm512_load_si512(reinterpret_cast<const __m512i*>(align_sample) + i);
//       _mm512_stream_si512(reinterpret_cast<__m512i*>(align_dst) + i, vec_val);
//     }
//     align_dst += sample_size;
//   }

//   // Scalar tail.
//   memcpy(align_dst, align_sample, rem_scalar);
// }
