
// component headers
#include<avxbmp.h>

// std headers
#include<iomanip>
#include<iostream>

#include<vector>

template<typename TYPE>
void test(const size_t size) {
    int64_t actual_null_count;
    int64_t test_null_count;
    size_t  bitmap_size = avxbmp::computeBitmapSize(size);

    std::vector<TYPE>    nulldata(size, 0);
    std::vector<uint8_t> actual_bitmap_data(bitmap_size, 0);
    std::vector<uint8_t> expected_bitmap_data(bitmap_size, 0);

    TYPE null_value = avxbmp::helpers::null_builder<TYPE>();

    //  populating nulldata
    for (size_t i=0; i<nulldata.size(); i++) {
        nulldata[i] = (i%2 == 0) ? 0 : null_value;
    }

    std::cout << avxbmp::helpers::get_type_name<TYPE>() <<"; " << std::hex << reinterpret_cast <uint64_t&>(nulldata[1]) << std::dec << std::endl;

    avxbmp::createBitmap<TYPE>(expected_bitmap_data, test_null_count, nulldata);
    avxbmp::createBitmapAVX512<TYPE>(actual_bitmap_data, actual_null_count, nulldata);

    std::cout << avxbmp::helpers::get_type_name<TYPE>() 
              <<"; Expected bitmap: "; 
    avxbmp::printBitmap(expected_bitmap_data, true);

    std::cout << avxbmp::helpers::get_type_name<TYPE>()
              <<"; Actual bitmap:   "; 
    avxbmp::printBitmap(actual_bitmap_data, true);

    std::cout << avxbmp::helpers::get_type_name<TYPE>() 
              <<"; NULL COUNT: expected: " << test_null_count 
              << ", actual: " << actual_null_count 
              << std::endl;

    size_t diff_count = avxbmp::diffBitmap(actual_bitmap_data, expected_bitmap_data);

    std::cout 
        << (diff_count>0 ? "Bitmaps are different" : "Bitmaps are identical") 
        << std::endl;
}


void major_test()
{
    test<uint8_t>(64);
    test<int8_t>(64);
    test<int32_t>(64);
    test<uint32_t>(64);
    test<int64_t>(64);
    test<uint64_t>(64);
    test<float>(64);
    test<double>(64);
}

int main() try {
    major_test();
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
    size_t bitmap_size = avxbmp::computeBitmapSize(values.size());
    std::vector<uint8_t> bitmap_data(bitmap_size, 0);
    std::cout << "Bitmap size: " << bitmap_data.size() << std::endl;
    int64_t null_count = 0;
    avxbmp::createBitmap(bitmap_data, null_count, values);
    std::cout << "Nulls count: " << null_count << std::endl;
    avxbmp::printBitmap(bitmap_data, true);

    std::cout << "Running diffBitmap on the same data\n";
    avxbmp::diffBitmap(bitmap_data, bitmap_data);

    std::cout << "Running diffBitmap on the different bitmap data\n";
    std::vector<uint8_t> expected_bitmap_data (bitmap_data);
    expected_bitmap_data [0] = 0;
    avxbmp::diffBitmap(bitmap_data, expected_bitmap_data);

    return 0;
}


// void profile_tbb(size_t size) {
//     std::vector<int32_t> values(size, 0);
//     values[0] = std::numeric_limits<int32_t>::max();
//     size_t bitmap_size = avxbmp::computeBitmapSize(values.size());

//     int64_t null_count = 0;
//     std::vector<uint8_t> bitmap_data(bitmap_size, 0);

//     auto start = std::chrono::high_resolution_clock::now();
//     avxbmp::createBitmapParallelFor(bitmap_data, null_count, values);
//     size_t dur = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-start).count(); 
//     std::cout << "[TBB_PARALLEL_FOR] Elapsed, usec: " << dur << std::endl;
// }



