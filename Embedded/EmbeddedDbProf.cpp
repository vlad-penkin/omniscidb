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
#include "Shared/InlineNullValues.h"
#include "Shared/scope.h"

// boost headers
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

// std headers
#include <array>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <limits>
#include <tuple>

// arrow headers
#include <arrow/api.h>
#include <arrow/csv/reader.h>
#include <arrow/io/file.h>

// Google Test
#include <gtest/gtest.h>

// Global variables controlling execution
extern bool g_enable_columnar_output;
extern bool g_enable_lazy_fetch;

// DBEngine Instance
static std::shared_ptr<EmbeddedDatabase::DBEngine> g_dbe;


//  HELPERS
namespace { namespace helpers {

// Processes command line args, return options string
std::tuple<std::string, size_t, size_t, size_t> parse_cli_args(int argc, char* argv[]) {
  namespace po = boost::program_options;

  int calcite_port = 5555;
  bool columnar_output = true;
  std::string catalog_path = "tmp";
  std::string opt_str;
  size_t  fragments_count = 1;
  size_t  column_size = 30'000'000;
  size_t  interations_count = 1;

  po::options_description desc("Options");

  desc.add_options()("help,h", "Print help messages ");

  desc.add_options()("data,d",
                     po::value<std::string>(&catalog_path)->default_value(catalog_path),
                     "Directory path to OmniSci catalogs");

  desc.add_options()("calcite-port,p",
                     po::value<int>(&calcite_port)->default_value(calcite_port),
                     "Calcite port");

  desc.add_options()("enable-columnar-output,o",
                     po::value<bool>(&columnar_output)->default_value(columnar_output),
                     "Enable columnar_output");


  desc.add_options()("column_size,c",
                     po::value<size_t>(&column_size)->default_value(column_size),
                     "Column size");

  desc.add_options()("fragments_count,f",
                     po::value<size_t>(&fragments_count)->default_value(fragments_count),
                     "Fragments count");

  desc.add_options()("interations_count,i",
                     po::value<size_t>(&interations_count)->default_value(interations_count),
                     "Iterations count");

  po::variables_map vm;

  try {
    po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);

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

  return {opt_str, column_size, fragments_count, interations_count};
}
}  // helpers namespace
}  // anonymous namespace


void buildTable(size_t N = 30'000'000, size_t fragments_count = 1, std::string table_name = "table1")
{
  if (!g_dbe) {
    throw std::runtime_error("DBEngine is not initialized.  Aborting.\n");
  }
  size_t fragment_size = fragments_count > 1 ? size_t((N+fragments_count-1)/fragments_count) : 0;

  std::string frag_size_param =
      fragment_size > 0 ? "WITH (fragment_size=" + std::to_string(fragment_size) + ");" : "";

  {
    // https://wesm.github.io/arrow-site-test/cpp/api/table.html#tables
    std::vector<int32_t> int32_values(N,2);
    arrow::NumericBuilder<arrow::Int32Type> int32_builder;
    int32_builder.Resize(N);

    int32_builder.AppendValues(int32_values);
    std::shared_ptr<arrow::Array> array;
    int32_builder.Finish(&array);

    auto schema = arrow::schema({arrow::field("i", arrow::int32())});
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {array});
    std::cout << "## buildTable() -- Importing arrow table"<<std::endl;
    g_dbe->importArrowTable(table_name, table, fragment_size);
  }

  //  testing for correctness
  std::cout << "## buildTable() -- testing for correctness: lauching query \"SELECT * FROM " << table_name<< ";\"" <<std::endl;
  auto cursor = g_dbe->executeDML("select * from " + table_name + ";");
  ASSERT_NE(cursor, nullptr);
  std::cout << "## buildTable() -- testing for correctness: invoking getArrowTable()"<<std::endl;
  auto table = cursor->getArrowTable();
  ASSERT_NE(table, nullptr);
  std::cout << "## buildTable() -- testing for correctness: checking table sizes."<<std::endl;
  ASSERT_EQ(table->num_columns(), 1);
  ASSERT_EQ(table->num_rows(), (int64_t)N);
  std::cout << "## buildTable() -- all checks passed"<<std::endl;
}

static void escape(void *p) {
  asm volatile("" : : "g"(p) : "memory");
}

static void clobber() {
  asm volatile("" : : : "memory");
}

void runProfiling(size_t iterations=1, std::string table_name = "table1") {
  if (!g_dbe) {
    throw std::runtime_error("DBEngine is not initialized.  Aborting.\n");
  }

  std::cout << "## runProfiling() -- Running profiling for " << iterations << " iterations."<<std::endl;
  for (size_t i = 0; i<iterations; i++) {
    auto cursor = g_dbe->executeDML("select * from " + table_name + ";");
    ASSERT_NE(cursor, nullptr);
    auto table = cursor->getArrowTable();
    escape (table.get());
    clobber();
  }
  std::cout << "## runProfiling() -- Profiling finished." << std::endl;
}

int main(int argc, char* argv[]) try {
  namespace fs = std::filesystem;

  bool prev_enable_columnar_output = g_enable_columnar_output;
  bool prev_enable_lazy_fetch = g_enable_lazy_fetch;

  ScopeGuard reset = [prev_enable_columnar_output, prev_enable_lazy_fetch] {
    g_enable_columnar_output = prev_enable_columnar_output;
    g_enable_lazy_fetch = prev_enable_lazy_fetch;
  };

  g_enable_columnar_output = true;
  g_enable_lazy_fetch = false;

  auto [options_str, column_size, fragments_count, interations_count]
     = helpers::parse_cli_args(argc, argv);

  std::cout 
    << "Parameters: \n" 
    << "  - options_str:       " << options_str << "\n"
    << "  - column_size:       " << column_size << "\n"
    << "  - fragments_count:   " << fragments_count << "\n"
    << "  - interations_count: " << interations_count << std::endl;


  g_dbe = EmbeddedDatabase::DBEngine::create(options_str);

  buildTable(column_size, fragments_count);
  runProfiling(interations_count);

  g_dbe.reset();
  return 0;
} catch (const boost::system::system_error& e) {
  std::cout << e.what();
  LOG(ERROR) << e.what();
  return EXIT_FAILURE;
} catch (const std::exception& e) {
  std::cout << e.what();
  LOG(ERROR) << e.what();
  return EXIT_FAILURE;
}
