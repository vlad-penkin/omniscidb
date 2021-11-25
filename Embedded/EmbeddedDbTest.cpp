/*
 * Copyright 2020 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// project headers
#include "DBEngine.h"
#include "Shared/ArrowUtil.h"
#include "Shared/scope.h"

// boost headers
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

// std headers
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <tuple>

// arrow header
#include <arrow/api.h>
#include <arrow/csv/reader.h>
#include <arrow/io/file.h>

// Google Test
#include <gtest/gtest.h>

// Global Variables controlling execution
extern bool g_enable_columnar_output;
extern bool g_enable_lazy_fetch;

// DBEngine Instance
static std::shared_ptr<EmbeddedDatabase::DBEngine> g_dbe;

// =================================================================================
// TODO:
// главное проследить, чтобы достаточное количество путей проверялось.
//
// 1. должны быть столбцы с данными, которые мы можем конвертировать zero-copy 
// и которые не можем
// 2. должен быть случай с appended_storage и без
// 3. columnar и row wise форматы
// чтобы появились appended storage надо чтобы фрагмент был меньше количества строк, 
// задается в CREATE TABLE ... WITH (fragment_size=N)
//
// при создании таблицы задается размер фрагмента (в строках), при простых select 
// запросах каждый фрагмент породит свой storage. Если размер фрагмента больше 
// размера таблицы, то будет один фрагмент и соответственно не будет appended_storage 
// в результате
// 
// т.е. тебе надо будет создать как минимум две таблицы, каждый тест прогнать на 
// каждой из них с g_enable_columnar_output и без.
// =================================================================================

// TEST(DBEngine, SimpleArrowTable) {
//   auto cursor = g_dbe->executeDML("select b, c from test");
//   ASSERT_NE(cursor, nullptr);
//   auto table = cursor->getArrowTable();
//   ASSERT_NE(table, nullptr);
//   ASSERT_EQ(table->num_columns(), 2);
//   ASSERT_EQ(table->num_rows(), (int64_t)6);
// }

// TEST(DBEngine, EmptyArrowTable) {
//   auto cursor = g_dbe->executeDML("select b, c from test where b > 1000");
//   ASSERT_NE(cursor, nullptr);
//   auto table = cursor->getArrowTable();
//   ASSERT_NE(table, nullptr);
//   ASSERT_EQ(table->num_columns(), 2);
//   ASSERT_EQ(table->num_rows(), (int64_t)0);
// }

// TEST(DBEngine, SimpleArrowTableConversion) {
//   //  By default row-wise format is used, to use columnar format 
//   //  one has to set `g_enable_columnar_output=true`
//   //  Thus, saving previous values of `g_enable_columnar_output` and `g_enable_lazy_fetch`.
//   //  For execution setting them as:
//   //    g_enable_columnar_output = true;
//   //    g_enable_lazy_fetch = false;
//   bool prev_enable_columnar_output = g_enable_columnar_output ;
//   ScopeGuard reset = [prev_enable_columnar_output] { g_enable_columnar_output=prev_enable_columnar_output; };
//   g_enable_columnar_output = true;

//   bool prev_enable_lazy_fetch = g_enable_lazy_fetch;
//   ScopeGuard reset2 = [prev_enable_lazy_fetch] { g_enable_lazy_fetch=prev_enable_lazy_fetch; };
//   g_enable_lazy_fetch = false;

//   auto cursor = g_dbe->executeDML("select * from test");
//   ASSERT_NE(cursor, nullptr);
//   auto table = cursor->getArrowTable();
//   ASSERT_NE(table, nullptr);
//   ASSERT_EQ(table->num_columns(), 3);
//   ASSERT_EQ(table->num_rows(), (int64_t)6);
// }

// TEST(DBEngine, SimpleArrowTableSingleColumnConversion) {
//   //  Saving previous value of g_enable_columnar_output
//   //  For execution setting it as g_enable_columnar_output = true;
//   bool prev_enable_columnar_output = g_enable_columnar_output ;
//   ScopeGuard reset = [prev_enable_columnar_output] { g_enable_columnar_output=prev_enable_columnar_output; };
//   g_enable_columnar_output = true;

//   auto cursor = g_dbe->executeDML("select 2*b from test");
//   ASSERT_NE(cursor, nullptr);
//   auto table = cursor->getArrowTable();
//   ASSERT_NE(table, nullptr);
//   ASSERT_EQ(table->num_columns(), 1);
//   ASSERT_EQ(table->num_rows(), (int64_t)6);

//   auto column = table->column(0);
//   ASSERT_NE(column, nullptr);

//   size_t chunks_count = column->num_chunks();
//   std::cout <<"Chunks count: " << chunks_count << std::endl;
//   std::cout << column->ToString() << std::endl;
// }


// #define VERBOSE  //  Enable if you wish to see the content of the chunks
#ifdef VERBOSE
#define INFO(cmd) cmd;
#else
#define INFO(cmd) do { } while(0)
#endif

template <typename T> struct traits {};
template <> struct traits <double> { using type = arrow::DoubleArray; };
template <> struct traits <int32_t> { using type = arrow::Int32Array; };
template <> struct traits <int64_t> { using type = arrow::Int64Array; };

//  =====================================================================
//  Testing function for 6x4 table contained in `example_4.csv' file.
//  =====================================================================
void MainChunkedConversionTest(size_t chunk_size) {

  //  expected values
  std::vector <double>  col_d = {10.1, 20.2, 30.3, 40.4, 50.5, 60.6};
  std::vector <int32_t> col_i = {0,0,0,1,1,1};
  std::vector <int64_t> col_bi = {1,2,3,4,5,6};

  //  =====================================================================
  //  Testing lambda function
  //  ---
  //  Basically, at std::static_pointer_cast<> we cast chunks[i] to a the 
  //  type given to us by the traits<TYPE> so that we can use raw_values()
  //  call.  Here TYPE is the underlying type of the std::vector containing
  //  with testing values
  //  =====================================================================
  auto test_column = [&]<typename TYPE> (const std::vector<TYPE> & test_col, const std::shared_ptr<arrow::ChunkedArray> & column) 
  {
    using arrow_col_type = typename traits<TYPE>::type;
    const arrow::ArrayVector & chunks = column->chunks();
    
    INFO(std::cout << "\n===\n");
    for (int i = 0; i<column->num_chunks(); i++) {
      auto arrow_row_array = std::static_pointer_cast<arrow_col_type>(chunks[i]);

      const TYPE * chunk_data = arrow_row_array->raw_values();
      for (int64_t j = 0; j<arrow_row_array->length(); j++) {
        INFO(std::cout << chunk_data[j]<<", test: " << test_col[i*chunk_size+j] << '\n');
        ASSERT_EQ(chunk_data[j], test_col[i*chunk_size+j]);
      }
      INFO(std::cout << "==="<<std::endl);
    }
  };

  std::string  create_query = "CREATE TEMPORARY TABLE test_chunked (t TEXT, i INT, bi BIGINT, d DOUBLE) "
                              "WITH (storage_type='CSV:./example_4.csv', fragment_size="+std::to_string(chunk_size)+");";

  g_dbe->executeDDL(create_query); 
  auto cursor = g_dbe->executeDML("select * from test_chunked");

  ASSERT_NE(cursor, nullptr);
  auto table = cursor->getArrowTable(); 

  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->num_columns(), 4);
  ASSERT_EQ(table->num_rows(), (int64_t)6);

  auto column = table->column(1);
  size_t expected_chunk_count = (g_enable_lazy_fetch || !g_enable_columnar_output) ? 1 : (table->num_rows()+chunk_size-1)/chunk_size;

  size_t actual_chunks_count =  column->num_chunks();
  ASSERT_EQ(actual_chunks_count, expected_chunk_count);
  
  INFO(std::cout <<"Chunks count: " << actual_chunks_count << std::endl);
  INFO(std::cout << column->ToString() << std::endl);

  ASSERT_NE(table->column(0), nullptr);
  ASSERT_NE(table->column(1), nullptr);
  ASSERT_NE(table->column(2), nullptr);
  ASSERT_NE(table->column(3), nullptr);

  test_column(col_i,  table->column(1));
  test_column(col_bi, table->column(2));
  test_column(col_d,  table->column(3));

  g_dbe->executeDDL("DROP TABLE test_chunked;");
}

//  =====================================================================
//  columnar_output = false
//  lazy_fetch = false
//  =====================================================================
TEST(DBEngine, ArrowTable_ChunkedConversion1) {
  //  Saving previous value of g_enable_columnar_output, g_enable_lazy_fetch
  bool prev_enable_columnar_output = g_enable_columnar_output;
  bool prev_enable_lazy_fetch = g_enable_lazy_fetch;

  ScopeGuard reset = [prev_enable_columnar_output, prev_enable_lazy_fetch] { 
    g_enable_columnar_output = prev_enable_columnar_output; 
    g_enable_lazy_fetch = prev_enable_lazy_fetch;
  };

  g_enable_columnar_output = false;
  g_enable_lazy_fetch = false;

  MainChunkedConversionTest(/*chunk_size=*/4);
}

//  =====================================================================
//  columnar_output = true
//  lazy_fetch = false
//  =====================================================================
TEST(DBEngine, ArrowTable_ChunkedConversion2) {
  //  Saving previous value of g_enable_columnar_output, g_enable_lazy_fetch
  bool prev_enable_columnar_output = g_enable_columnar_output;
  bool prev_enable_lazy_fetch = g_enable_lazy_fetch;

  ScopeGuard reset = [prev_enable_columnar_output, prev_enable_lazy_fetch] { 
    g_enable_columnar_output = prev_enable_columnar_output; 
    g_enable_lazy_fetch = prev_enable_lazy_fetch;
  };

  g_enable_columnar_output = true;
  g_enable_lazy_fetch = false;

  MainChunkedConversionTest(/*chunk_size=*/4);
}

//  =====================================================================
//  columnar_output = false
//  lazy_fetch = true
//  =====================================================================
TEST(DBEngine, ArrowTable_ChunkedConversion3) {
  //  Saving previous value of g_enable_columnar_output, g_enable_lazy_fetch
  bool prev_enable_columnar_output = g_enable_columnar_output;
  bool prev_enable_lazy_fetch = g_enable_lazy_fetch;

  ScopeGuard reset = [prev_enable_columnar_output, prev_enable_lazy_fetch] { 
    g_enable_columnar_output = prev_enable_columnar_output; 
    g_enable_lazy_fetch = prev_enable_lazy_fetch;
  };

  g_enable_columnar_output = false;
  g_enable_lazy_fetch = true;

  MainChunkedConversionTest(/*chunk_size=*/4);
}

//  =====================================================================
//  columnar_output = true
//  lazy_fetch = true
//  =====================================================================
TEST(DBEngine, ArrowTable_ChunkedConversion4) {
  //  Saving previous value of g_enable_columnar_output, g_enable_lazy_fetch
  bool prev_enable_columnar_output = g_enable_columnar_output;
  bool prev_enable_lazy_fetch = g_enable_lazy_fetch;

  ScopeGuard reset = [prev_enable_columnar_output, prev_enable_lazy_fetch] { 
    g_enable_columnar_output = prev_enable_columnar_output; 
    g_enable_lazy_fetch = prev_enable_lazy_fetch;
  };

  g_enable_columnar_output = true;
  g_enable_lazy_fetch = true;

  MainChunkedConversionTest(/*chunk_size=*/4);
}

//  Forward Declarations
std::tuple <std::string, std::string>  parse_cli_args(int argc, char* argv[], std::string input_csv_file);
void import_file(const std::string& file_name, const std::string& table_name);

//  --- MAIN ---  //
int main(int argc, char* argv[]) try {
  namespace fs = std::filesystem;

  auto [options_str, input_file] = parse_cli_args(argc, argv, "./example_3.csv");
  std::cout << "OPTIONS:    " << options_str <<std::endl;
  std::cout << "INPUT FILE: " << input_file <<std::endl;

  if (!fs::exists(fs::path{input_file})) {
    throw std::runtime_error ("File not found: "+input_file+". Aborting\n");
  }

  testing::InitGoogleTest();
  //  If a single test is needed, uncomment (and modify, possibly) the following line:
  //  testing::GTEST_FLAG(filter) = "DBEngine.ArrowTable_ChunkedConversion1"

  g_dbe = EmbeddedDatabase::DBEngine::create(options_str);
  import_file(input_file, "test");

  int err = RUN_ALL_TESTS();

  g_dbe.reset();
  std::exit(err);

} catch (const std::exception& e) {
  std::cout <<  e.what();
  LOG(ERROR) << e.what();
  std::exit(EXIT_FAILURE); 
} catch (...) {
  std::cout << " Unknown exception caught\n";
}

// =============================================================
// Processes command line args, default name of input CSV file, 
// and returns a pair <options, input_csv_file>
// =============================================================
std::tuple <
  std::string,
  std::string
  >
parse_cli_args(int argc, char* argv[], std::string input_csv_file)
{
  namespace po = boost::program_options;

  int   calcite_port = 5555;
  bool  columnar_output = true;
  std::string   catalog_path = "tmp";
  std::string   opt_str;

  po::options_description desc("Options");

  desc.add_options()
    ("help,h",          "Print help messages ")
    ("catalog,c",        po::value<std::string>(&catalog_path)->default_value(catalog_path),     "Directory path to OmniSci catalogs")
    ("input-csv-file,i", po::value<std::string>(&input_csv_file)->default_value(input_csv_file), "Input CSV file")
    ("calcite-port,p",    po::value<int>(&calcite_port)->default_value(calcite_port),            "Calcite port")
    ("columnar-output,o", po::value<bool>(&columnar_output)->default_value(columnar_output),     "Enable columnar_output")
    ;

  po::variables_map  vm;

  try {
    po::store(po::command_line_parser(argc, argv)
                .options(desc)
                .run(), 
              vm);

    if (vm.count("help")) {
      std::cout << desc;
      std::exit(EXIT_SUCCESS);
    }
    po::notify(vm);

    opt_str = catalog_path + " --calcite-port " + std::to_string(calcite_port);
    if (columnar_output) {
      opt_str += " --enable-columnar-output";
    }
  } catch (boost::program_options::error& e) {
    std::cerr << "Usage Error: " << e.what() << std::endl;
    std::cout << desc;
    std::exit(EXIT_FAILURE);
  }

  return {opt_str, input_csv_file};
}

// =============================================================
// Loads CSV file, creates Arrow Table from it
// =============================================================
void import_file(const std::string& file_name, const std::string& table_name) 
{
  arrow::io::IOContext io_context = arrow::io::default_io_context();
  auto arrow_parse_options = arrow::csv::ParseOptions::Defaults();
  auto arrow_read_options = arrow::csv::ReadOptions::Defaults();
  auto arrow_convert_options = arrow::csv::ConvertOptions::Defaults();
  std::shared_ptr<arrow::io::ReadableFile> inp;
  auto file_result = arrow::io::ReadableFile::Open(file_name);
  ARROW_THROW_NOT_OK(file_result.status());
  inp = file_result.ValueOrDie();
  auto table_reader_result = arrow::csv::TableReader::Make(io_context,
                                                           inp, 
                                                           arrow_read_options, 
                                                           arrow_parse_options, 
                                                           arrow_convert_options);

  ARROW_THROW_NOT_OK(table_reader_result.status());
  auto table_reader = table_reader_result.ValueOrDie();
  std::shared_ptr<arrow::Table> arrowTable;
  auto arrow_table_result = table_reader->Read();
  ARROW_THROW_NOT_OK(arrow_table_result.status());
  arrowTable = arrow_table_result.ValueOrDie();
  g_dbe->importArrowTable(table_name, arrowTable);
}