// component headers
#include <avxbmp.h>

// std headers
#include <iomanip>
#include <iostream>
#include <vector>

// Google Test
#include <gtest/gtest.h>

//  =================
//  Testing functions
//  =================

template <typename TYPE, typename FUNCTION_BMCTOR, typename FUNCTION_NDPOPR>
static void test_bitmap_creator(const size_t size,
                                FUNCTION_BMCTOR&& bitmap_creator,
                                FUNCTION_NDPOPR&& nulldata_populator,
                                bool verbose = false) {
  int64_t actual_null_count;
  int64_t test_null_count;
  size_t bitmap_size = avxbmp::computeBitmapSize(size);

  std::vector<TYPE> nulldata(size, 0);
  std::vector<uint8_t> actual_bitmap_data(bitmap_size, 0);
  std::vector<uint8_t> expected_bitmap_data(bitmap_size, 0);

  std::invoke(nulldata_populator, nulldata);
  std::invoke(bitmap_creator, actual_bitmap_data, actual_null_count, nulldata);
  avxbmp::createBitmap<TYPE>(expected_bitmap_data, test_null_count, nulldata);

  if (verbose) {
    std::cout << avxbmp::helpers::get_type_name<TYPE>() << "; Expected bitmap: ";
    avxbmp::printBitmap(expected_bitmap_data, true);

    std::cout << avxbmp::helpers::get_type_name<TYPE>() << "; Actual bitmap:   ";
    avxbmp::printBitmap(actual_bitmap_data, true);

    std::cout << avxbmp::helpers::get_type_name<TYPE>()
              << "; NULL COUNT: expected: " << test_null_count
              << ", actual: " << actual_null_count << std::endl;
  }
  size_t diff_count =
      avxbmp::diffBitmap(actual_bitmap_data, expected_bitmap_data, verbose);
  ASSERT_EQ(test_null_count, actual_null_count);
  ASSERT_EQ(diff_count, 0);
}

template <typename TYPE>
static void testSimpleAVX512(const size_t size, bool verbose = false) {
  auto bitmap_creator = [](std::vector<uint8_t>& bitmap_data,
                           int64_t& null_count_out,
                           const std::vector<TYPE>& vals) {
    return avxbmp::createBitmapAVX512<TYPE>(bitmap_data, null_count_out, vals);
  };

  auto nulldata_populator = [](std::vector<TYPE>& nulldata) {
    for (size_t i = 0; i < nulldata.size(); i++) {
      nulldata[i] = (i % 2) == 0 ? 0 : avxbmp::helpers::null_builder<TYPE>();
    }
    // An alternative method:
    // for (size_t i=0, j=1; i<nulldata.size(); i++, j++) {
    //     nulldata[i] = (i%(2*j) == 0) ? 0 : avxbmp::helpers::null_builder<TYPE>();
    // }
  };

  test_bitmap_creator<TYPE>(
      size, std::move(bitmap_creator), std::move(nulldata_populator), verbose);
}

template <typename TYPE>
static void testParallelAVX512(const size_t size, bool verbose = false) {
  auto bitmap_creator = [](std::vector<uint8_t>& bitmap_data,
                           int64_t& null_count_out,
                           const std::vector<TYPE>& vals) {
    return avxbmp::createBitmapParallelForAVX512<TYPE>(bitmap_data, null_count_out, vals);
  };

  auto nulldata_populator = [](std::vector<TYPE>& nulldata) {
    for (size_t i = 0; i < nulldata.size(); i++) {
      nulldata[i] = (i % 2) == 0 ? 0 : avxbmp::helpers::null_builder<TYPE>();
    }
    // An alternative method:
    // for (size_t i=0, j=1; i<nulldata.size(); i++, j++) {
    //     nulldata[i] = (i%(2*j) == 0) ? 0 : avxbmp::helpers::null_builder<TYPE>();
    // }
  };

  test_bitmap_creator<TYPE>(
      size, std::move(bitmap_creator), std::move(nulldata_populator), verbose);
}

//  ==================
//  SimpleAVX512 tests
//  ==================

TEST(SimpleAVX512, uint8_t) {
  for (size_t size = 0; size < 255; size++) {
    testSimpleAVX512<uint8_t>(size);
  }
  testSimpleAVX512<uint8_t>(65539);
  testSimpleAVX512<uint8_t>(1024 * 1024 + 63);
}

TEST(SimpleAVX512, int8_t) {
  for (size_t size = 0; size < 255; size++) {
    testSimpleAVX512<int8_t>(size);
  }
  testSimpleAVX512<int8_t>(65539);
  testSimpleAVX512<int8_t>(1024 * 1024 + 63);
}

TEST(SimpleAVX512, int32_t) {
  for (size_t size = 0; size < 255; size++) {
    testSimpleAVX512<int32_t>(size);
  }
  testSimpleAVX512<int32_t>(65539);
  testSimpleAVX512<int32_t>(1024 * 1024 + 63);
}

TEST(SimpleAVX512, uint32_t) {
  for (size_t size = 0; size < 255; size++) {
    testSimpleAVX512<uint32_t>(size);
  }
  testSimpleAVX512<uint32_t>(65539);
  testSimpleAVX512<uint32_t>(1024 * 1024 + 63);
}
TEST(SimpleAVX512, int64_t) {
  for (size_t size = 0; size < 255; size++) {
    testSimpleAVX512<int64_t>(size);
  }
  testSimpleAVX512<int64_t>(65539);
  testSimpleAVX512<int64_t>(1024 * 1024 + 63);
}

TEST(SimpleAVX512, uint64_t) {
  for (size_t size = 0; size < 255; size++) {
    testSimpleAVX512<uint64_t>(size);
  }
  testSimpleAVX512<uint64_t>(65539);
  testSimpleAVX512<uint64_t>(1024 * 1024 + 63);
}

TEST(SimpleAVX512, float) {
  for (size_t size = 0; size < 255; size++) {
    testSimpleAVX512<float>(size);
  }
  testSimpleAVX512<float>(65539);
  testSimpleAVX512<float>(1024 * 1024 + 63);
}

TEST(SimpleAVX512, double) {
  for (size_t size = 0; size < 255; size++) {
    testSimpleAVX512<double>(size);
  }
  testSimpleAVX512<double>(65539);
  testSimpleAVX512<double>(1024 * 1024 + 63);
}

//  ====================
//  ParallelAXV512 tests
//  ====================

TEST(ParallelAVX512, uint8_t) {
  for (size_t size = 0; size < 255; size++) {
    testParallelAVX512<uint8_t>(size);
  }
  testParallelAVX512<uint8_t>(65539);
  testParallelAVX512<uint8_t>(1024 * 1024 + 63);
}

TEST(ParallelAVX512, int8_t) {
  for (size_t size = 0; size < 255; size++) {
    testParallelAVX512<int8_t>(size);
  }
  testParallelAVX512<int8_t>(65539);
  testParallelAVX512<int8_t>(1024 * 1024 + 63);
}

TEST(ParallelAVX512, int32_t) {
  for (size_t size = 0; size < 255; size++) {
    testParallelAVX512<int32_t>(size);
  }
  testParallelAVX512<int32_t>(65539);
  testParallelAVX512<int32_t>(1024 * 1024 + 63);
}

TEST(ParallelAVX512, uint32_t) {
  for (size_t size = 0; size < 255; size++) {
    testParallelAVX512<uint32_t>(size);
  }
  testParallelAVX512<uint32_t>(65539);
  testParallelAVX512<uint32_t>(1024 * 1024 + 63);
}
TEST(ParallelAVX512, int64_t) {
  for (size_t size = 0; size < 255; size++) {
    testParallelAVX512<int64_t>(size);
  }
  testParallelAVX512<int64_t>(65539);
  testParallelAVX512<int64_t>(1024 * 1024 + 63);
}

TEST(ParallelAVX512, uint64_t) {
  for (size_t size = 0; size < 255; size++) {
    testParallelAVX512<uint64_t>(size);
  }
  testParallelAVX512<uint64_t>(65539);
  testParallelAVX512<uint64_t>(1024 * 1024 + 63);
}

TEST(ParallelAVX512, float) {
  for (size_t size = 0; size < 255; size++) {
    testParallelAVX512<float>(size);
  }
  testParallelAVX512<float>(65539);
  testParallelAVX512<float>(1024 * 1024 + 63);
}

TEST(ParallelAVX512, double) {
  for (size_t size = 0; size < 255; size++) {
    testParallelAVX512<double>(size);
  }
  testParallelAVX512<double>(65539);
  testParallelAVX512<double>(1024 * 1024 + 63);
}

TEST(ParallelAVX512, various) {
  testParallelAVX512<int32_t>(155, false);
  testParallelAVX512<int8_t>(67, false);
  testParallelAVX512<uint8_t>(135, false);

  testParallelAVX512<uint32_t>(300, false);
  testParallelAVX512<uint8_t>(290, false);
  testParallelAVX512<uint64_t>(1200, false);

  testParallelAVX512<uint32_t>(3000031, false);
  testParallelAVX512<double>(3000023, false);
}

int main() try {
  testing::InitGoogleTest();
  testing::GTEST_FLAG(filter) = "ParallelAVX512.*";
  return RUN_ALL_TESTS();
} catch (std::runtime_error& e) {
  std::cout << e.what() << std::endl;
  return EXIT_FAILURE;
} catch (const std::exception& e) {
  std::cout << e.what();
  return EXIT_FAILURE;
}
