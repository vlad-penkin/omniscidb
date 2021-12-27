//  component headers
#include<avxbmp.h>

//  std headers
#include<bitset>
#include<limits>
#include<iostream>
#include<vector>

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



size_t avxbmp::diffBitmap(std::vector<uint8_t>& bm1, std::vector<uint8_t>& bm2, bool verbose)
{
    if (bm1.size() != bm2.size()) {
        if (verbose) {
            std::cout << "Bitmaps are of different size (" << bm1.size() << " vs " <<bm2.size() << ")\n";
        }
        return std::numeric_limits<size_t>::max();
    }

    size_t diff_count = 0;
    for (size_t i = 0; i<bm1.size(); i++) {
        if (bm1[i]!=bm2[i]) {
            if (verbose) {
                std::cout 
                    << "Bitmaps differ at: " << i << "; " 
                    << std::bitset<8> (bm1[i]) << " vs " << std::bitset<8> (bm1[2]) 
                    << std::endl;
            }
            diff_count++;
        }
    }

    if (verbose && diff_count) {
        std::cout << "Found " << diff_count << " differences" << std::endl;
    }
    return diff_count;
}



// It's assumed sample has 64 extra bytes to handle alignment.
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
