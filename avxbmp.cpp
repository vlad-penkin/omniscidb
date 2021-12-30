//  component headers
#include <avxbmp.h>

//  std headers
#include <bitset>
#include <iostream>
#include <limits>
#include <vector>

size_t avxbmp::computeBitmapSize(size_t data_size) {
  return (data_size + 7) / 8;
}

void avxbmp::printBitmap(const std::vector<uint8_t>& bitmap_data, bool reverse) {
  for (auto& c : bitmap_data) {
    std::string s = std::bitset<8>(c).to_string();
    if (reverse) {
      std::reverse(std::begin(s), std::end(s));
    }
    std::cout << s << " ";
  }
  std::cout << std::endl;
}

size_t avxbmp::diffBitmap(std::vector<uint8_t>& bm1,
                          std::vector<uint8_t>& bm2,
                          bool verbose) {
  if (bm1.size() != bm2.size()) {
    if (verbose) {
      std::cout << "Bitmaps are of different size (" << bm1.size() << " vs " << bm2.size()
                << ")\n";
    }
    return std::numeric_limits<size_t>::max();
  }

  size_t diff_count = 0;
  for (size_t i = 0; i < bm1.size(); i++) {
    if (bm1[i] != bm2[i]) {
      if (verbose) {
        std::cout << "Bitmaps differ at: " << i << "; " << std::bitset<8>(bm1[i])
                  << " vs " << std::bitset<8>(bm2[i]) << std::endl;
      }
      diff_count++;
    }
  }

  if (verbose && diff_count) {
    std::cout << "Found " << diff_count << " differences" << std::endl;
  }
  return diff_count;
}