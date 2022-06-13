/*
 * Copyright 2021 OmniSci, Inc.
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

#include "ArrowSQLRunner/ArrowSQLRunner.h"
#include "ArrowSQLRunner/SQLiteComparator.h"
#include "TestHelpers.h"

#include "QueryEngine/ArrowResultSet.h"
#include "QueryEngine/Execute.h"
#include "QueryEngine/ResultSetReductionJIT.h"
#include "Shared/Globals.h"
#include "Shared/scope.h"

#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>
#include <boost/program_options.hpp>

#include <cmath>
#include <cstdio>
#include <random>
#include <regex>

using namespace std;
using namespace TestHelpers;
using namespace TestHelpers::ArrowSQLRunner;

bool g_aggregator{false};

extern bool g_enable_left_join_filter_hoisting;

extern double g_gpu_mem_limit_percent;

extern bool g_enable_calcite_view_optimize;
extern bool g_enable_bump_allocator;

extern bool g_is_test_env;

namespace {

const size_t g_num_rows{10};

}  // namespace

class ExecuteTestBase {
 public:
  static bool hasGpu() { return gpusPresent(); }

  static void createEmptyTestTable() {
    createTable("empty_test_table",
                {{"id", SQLTypeInfo(kINT)},
                 {"x", SQLTypeInfo(kBIGINT)},
                 {"y", SQLTypeInfo(kINT)},
                 {"z", SQLTypeInfo(kSMALLINT)},
                 {"t", SQLTypeInfo(kTINYINT)},
                 {"f", SQLTypeInfo(kFLOAT)},
                 {"d", SQLTypeInfo(kDOUBLE)},
                 {"b", SQLTypeInfo(kBOOLEAN)}});
    run_sqlite_query("DROP TABLE IF EXISTS empty_test_table;");
    std::string create_statement(
        "CREATE TABLE empty_test_table (id int, x bigint, y int, z smallint, t "
        "tinyint, "
        "f float, d double, b boolean);");
    run_sqlite_query(create_statement);
  }

  static void createTestRangesTable() {
    createTable(
        "test_ranges", {{"i", SQLTypeInfo(kINT)}, {"b", SQLTypeInfo(kBIGINT)}}, {2});
    run_sqlite_query("DROP TABLE IF EXISTS test_ranges;");
    run_sqlite_query("CREATE TABLE test_ranges(i INT, b BIGINT);");
    {
      const std::string insert_query{
          "INSERT INTO test_ranges VALUES(2147483647, 9223372036854775806);"};
      run_sqlite_query(insert_query);
      insertCsvValues("test_ranges", "2147483647, 9223372036854775806");
    }
    {
      const std::string insert_query{
          "INSERT INTO test_ranges VALUES(-2147483647, -9223372036854775807);"};
      run_sqlite_query(insert_query);
      insertCsvValues("test_ranges", "-2147483647, -9223372036854775807");
    }
  }

  static void createTestInnerTable() {
    createTable("test_inner",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"y", SQLTypeInfo(kINT)},
                 {"xx", SQLTypeInfo(kSMALLINT)},
                 {"str", dictType()},
                 {"dt", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 0, kNULLT)},
                 {"dt32", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 32, kNULLT)},
                 {"dt16", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 16, kNULLT)},
                 {"ts", SQLTypeInfo(kTIMESTAMP)}},
                {2});
    run_sqlite_query("DROP TABLE IF EXISTS test_inner;");
    run_sqlite_query(
        "CREATE TABLE test_inner(x int not null, y int, xx smallint, str text, dt "
        "DATE, "
        "dt32 DATE, dt16 DATE, ts DATETIME);");
    {
      const std::string insert_query{
          "INSERT INTO test_inner VALUES(7, 43, 7, 'foo', '1999-09-09', '1999-09-09', "
          "'1999-09-09', '2014-12-13 22:23:15');"};
      insertCsvValues("test_inner",
                      "7,43,7,foo,1999-09-09,1999-09-09,1999-09-09,2014-12-13 22:23:15");
      run_sqlite_query(insert_query);
    }
    {
      const std::string insert_query{
          "INSERT INTO test_inner VALUES(-9, 72, -9, 'bars', '2014-12-13', '2014-12-13', "
          "'2014-12-13', '1999-09-09 14:15:16');"};
      insertCsvValues(
          "test_inner",
          "-9,72,-9,bars,2014-12-13,2014-12-13,2014-12-13,1999-09-09 14:15:16");
      run_sqlite_query(insert_query);
    }
  }

  static void createTestTable() {
    auto test_inner = getStorage()->getTableInfo(TEST_DB_ID, "test_inner");
    auto test_inner_str = getStorage()->getColumnInfo(*test_inner, "str");
    auto test_inner_str_type = test_inner_str->type;

    createTable("test",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"w", SQLTypeInfo(kTINYINT)},
                 {"y", SQLTypeInfo(kINT)},
                 {"z", SQLTypeInfo(kSMALLINT)},
                 {"t", SQLTypeInfo(kBIGINT)},
                 {"b", SQLTypeInfo(kBOOLEAN)},
                 {"f", SQLTypeInfo(kFLOAT)},
                 {"ff", SQLTypeInfo(kFLOAT)},
                 {"fn", SQLTypeInfo(kFLOAT)},
                 {"d", SQLTypeInfo(kDOUBLE)},
                 {"dn", SQLTypeInfo(kDOUBLE)},
                 {"str", test_inner_str_type},
                 {"null_str", dictType()},
                 {"fixed_str", dictType(2)},
                 {"fixed_null_str", dictType(2)},
                 {"real_str", SQLTypeInfo(kTEXT)},
                 {"shared_dict", test_inner_str_type},
                 {"m", SQLTypeInfo(kTIMESTAMP, 0, 0)},
                 {"m_3", SQLTypeInfo(kTIMESTAMP, 3, 0)},
                 {"m_6", SQLTypeInfo(kTIMESTAMP, 6, 0)},
                 {"m_9", SQLTypeInfo(kTIMESTAMP, 9, 0)},
                 {"n", SQLTypeInfo(kTIME)},
                 {"o", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 0, kNULLT)},
                 {"o1", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 16, kNULLT)},
                 {"o2", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 32, kNULLT)},
                 {"fx", SQLTypeInfo(kSMALLINT)},
                 {"dd", SQLTypeInfo(kDECIMAL, 10, 2, false)},
                 {"dd_notnull", SQLTypeInfo(kDECIMAL, 10, 2, true)},
                 {"ss", dictType()},
                 {"u", SQLTypeInfo(kINT)},
                 {"ofd", SQLTypeInfo(kINT)},
                 {"ufd", SQLTypeInfo(kINT, true)},
                 {"ofq", SQLTypeInfo(kBIGINT)},
                 {"ufq", SQLTypeInfo(kBIGINT, true)},
                 {"smallint_nulls", SQLTypeInfo(kSMALLINT)},
                 {"bn", SQLTypeInfo(kBOOLEAN, true)}},
                {2});
    run_sqlite_query("DROP TABLE IF EXISTS test;");
    run_sqlite_query(
        "CREATE TABLE test(x int not null, w tinyint, y int, z smallint, t bigint, b "
        "boolean, f "
        "float, ff float, fn float, d "
        "double, dn double, str varchar(10), null_str text, fixed_str text, "
        "fixed_null_str text, real_str text, "
        "shared_dict "
        "text, m timestamp(0), m_3 timestamp(3), m_6 timestamp(6), m_9 timestamp(9), n "
        "time(0), o date, o1 date, o2 date, "
        "fx int, dd decimal(10, 2), dd_notnull decimal(10, 2) not "
        "null, ss "
        "text, u int, ofd int, ufd int not null, ofq bigint, ufq bigint not null, "
        "smallint_nulls smallint, bn boolean not null);");

    CHECK_EQ(g_num_rows % 2, size_t(0));
    for (size_t i = 0; i < g_num_rows; ++i) {
      const std::string insert_query{
          "INSERT INTO test VALUES(7, -8, 42, 101, 1001, 't', 1.1, 1.1, null, 2.2, null, "
          "'foo', null, 'foo', null, "
          "'real_foo', 'foo',"
          "'2014-12-13 22:23:15', '2014-12-13 22:23:15.323', '1999-07-11 "
          "14:02:53.874533', "
          "'2006-04-26 "
          "03:49:04.607435125', "
          "'15:13:14', '1999-09-09', '1999-09-09', '1999-09-09', 9, 111.1, 111.1, "
          "'fish', "
          "null, "
          "2147483647, -2147483648, null, -1, 32767, 't');"};
      insertCsvValues(
          "test",
          "7,-8,42,101,1001,true,1.1,1.1,,2.2,,foo,,foo,,real_foo,foo,2014-12-13 "
          "22:23:15,2014-12-13 22:23:15.323,1999-07-11 14:02:53.874533,2006-04-26 "
          "03:49:04.607435125,15:13:14,1999-09-09,1999-09-09,1999-09-09,9,111.1,111.1,"
          "fish,,2147483647,-2147483648,,-1,32767,true");
      run_sqlite_query(insert_query);
    }
    for (size_t i = 0; i < g_num_rows / 2; ++i) {
      const std::string insert_query{
          "INSERT INTO test VALUES(8, -7, 43, -78, 1002, 'f', 1.2, 101.2, -101.2, 2.4, "
          "-2002.4, 'bar', null, 'bar', null, "
          "'real_bar', NULL, '2014-12-13 22:23:15', '2014-12-13 22:23:15.323', "
          "'2014-12-13 "
          "22:23:15.874533', "
          "'2014-12-13 22:23:15.607435763', '15:13:14', NULL, NULL, NULL, NULL, 222.2, "
          "222.2, "
          "null, null, null, "
          "-2147483647, "
          "9223372036854775807, -9223372036854775808, null, 'f');"};
      insertCsvValues(
          "test",
          "8,-7,43,-78,1002,false,1.2,101.2,-101.2,2.4,-2002.4,bar,,bar,,real_bar,,2014-"
          "12-13 22:23:15,2014-12-13 22:23:15.323,2014-12-13 22:23:15.874533,2014-12-13 "
          "22:23:15.607435763,15:13:14,,,,,222.2,222.2,,,,-2147483647,"
          "9223372036854775807,-9223372036854775808,,false");
      run_sqlite_query(insert_query);
    }
    for (size_t i = 0; i < g_num_rows / 2; ++i) {
      const std::string insert_query{
          "INSERT INTO test VALUES(7, -7, 43, 102, 1002, null, 1.3, 1000.3, -1000.3, "
          "2.6, "
          "-220.6, 'baz', null, null, null, "
          "'real_baz', 'baz', '2014-12-14 22:23:15', '2014-12-14 22:23:15.750', "
          "'2014-12-14 22:23:15.437321', "
          "'2014-12-14 22:23:15.934567401', '15:13:14', '1999-09-09', '1999-09-09', "
          "'1999-09-09', 11, "
          "333.3, 333.3, "
          "'boat', null, 1, "
          "-1, 1, -9223372036854775808, 1, 't');"};
      insertCsvValues(
          "test",
          "7,-7,43,102,1002,,1.3,1000.3,-1000.3,2.6,-220.6,baz,,,,real_baz,baz,2014-12-"
          "14 22:23:15,2014-12-14 22:23:15.750,2014-12-14 22:23:15.437321,2014-12-14 "
          "22:23:15.934567401,15:13:14,1999-09-09,1999-09-09,1999-09-09,11,333.3,333.3,"
          "boat,,1,-1,1,-9223372036854775808,1,true");
      run_sqlite_query(insert_query);
    }
  }

  static void createTestEmptyTable() {
    auto test_inner = getStorage()->getTableInfo(TEST_DB_ID, "test_inner");
    auto test_inner_str = getStorage()->getColumnInfo(*test_inner, "str");
    auto test_inner_str_type = test_inner_str->type;
    createTable("test_empty",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"w", SQLTypeInfo(kTINYINT)},
                 {"y", SQLTypeInfo(kINT)},
                 {"z", SQLTypeInfo(kSMALLINT)},
                 {"t", SQLTypeInfo(kBIGINT)},
                 {"b", SQLTypeInfo(kBOOLEAN)},
                 {"f", SQLTypeInfo(kFLOAT)},
                 {"ff", SQLTypeInfo(kFLOAT)},
                 {"fn", SQLTypeInfo(kFLOAT)},
                 {"d", SQLTypeInfo(kDOUBLE)},
                 {"dn", SQLTypeInfo(kDOUBLE)},
                 {"str", test_inner_str_type},
                 {"null_str", dictType()},
                 {"fixed_str", dictType(2)},
                 {"fixed_null_str", dictType(2)},
                 {"real_str", SQLTypeInfo(kTEXT)},
                 {"shared_dict", test_inner_str_type},
                 {"m", SQLTypeInfo(kTIMESTAMP, 0, 0)},
                 {"n", SQLTypeInfo(kTIME)},
                 {"o", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 0, kNULLT)},
                 {"o1", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 16, kNULLT)},
                 {"o2", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 32, kNULLT)},
                 {"fx", SQLTypeInfo(kSMALLINT)},
                 {"dd", SQLTypeInfo(kDECIMAL, 10, 2, false)},
                 {"dd_notnull", SQLTypeInfo(kDECIMAL, 10, 2, true)},
                 {"ss", dictType()},
                 {"u", SQLTypeInfo(kINT)},
                 {"ofd", SQLTypeInfo(kINT)},
                 {"ufd", SQLTypeInfo(kINT, true)},
                 {"ofq", SQLTypeInfo(kBIGINT)},
                 {"ufq", SQLTypeInfo(kBIGINT, true)}},
                {2});

    run_sqlite_query("DROP TABLE IF EXISTS test_empty;");
    run_sqlite_query(
        "CREATE TABLE test_empty(x int not null, w tinyint, y int, z smallint, t "
        "bigint, b boolean, f float, ff float, fn float, d "
        "double, dn double, str varchar(10), null_str text, fixed_str text, "
        "fixed_null_str text, real_str text, "
        "shared_dict "
        "text, m timestamp(0), n "
        "time(0), o date, o1 date, o2 date, "
        "fx int, dd decimal(10, 2), dd_notnull decimal(10, 2) not "
        "null, ss "
        "text, u int, ofd int, ufd int not null, ofq bigint, ufq bigint not null);");
  }

  static void createTestOneRowTable() {
    auto test_inner = getStorage()->getTableInfo(TEST_DB_ID, "test_inner");
    auto test_inner_str = getStorage()->getColumnInfo(*test_inner, "str");
    auto test_inner_str_type = test_inner_str->type;
    createTable("test_one_row",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"w", SQLTypeInfo(kTINYINT)},
                 {"y", SQLTypeInfo(kINT)},
                 {"z", SQLTypeInfo(kSMALLINT)},
                 {"t", SQLTypeInfo(kBIGINT)},
                 {"b", SQLTypeInfo(kBOOLEAN)},
                 {"f", SQLTypeInfo(kFLOAT)},
                 {"ff", SQLTypeInfo(kFLOAT)},
                 {"fn", SQLTypeInfo(kFLOAT)},
                 {"d", SQLTypeInfo(kDOUBLE)},
                 {"dn", SQLTypeInfo(kDOUBLE)},
                 {"str", test_inner_str_type},
                 {"null_str", dictType()},
                 {"fixed_str", dictType(2)},
                 {"fixed_null_str", dictType(2)},
                 {"real_str", SQLTypeInfo(kTEXT)},
                 {"shared_dict", test_inner_str_type},
                 {"m", SQLTypeInfo(kTIMESTAMP, 0, 0)},
                 {"n", SQLTypeInfo(kTIME)},
                 {"o", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 0, kNULLT)},
                 {"o1", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 16, kNULLT)},
                 {"o2", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 32, kNULLT)},
                 {"fx", SQLTypeInfo(kSMALLINT)},
                 {"dd", SQLTypeInfo(kDECIMAL, 10, 2, false)},
                 {"dd_notnull", SQLTypeInfo(kDECIMAL, 10, 2, true)},
                 {"ss", dictType()},
                 {"u", SQLTypeInfo(kINT)},
                 {"ofd", SQLTypeInfo(kINT)},
                 {"ufd", SQLTypeInfo(kINT, true)},
                 {"ofq", SQLTypeInfo(kBIGINT)},
                 {"ufq", SQLTypeInfo(kBIGINT, true)}},
                {2});

    run_sqlite_query("DROP TABLE IF EXISTS test_one_row;");
    run_sqlite_query(
        "CREATE TABLE test_one_row(x int not null, w tinyint, y int, z smallint, t "
        "bigint, b "
        "boolean, "
        "f "
        "float, ff float, fn float, d "
        "double, dn double, str varchar(10), null_str text, fixed_str text, "
        "fixed_null_str text, real_str text, "
        "shared_dict "
        "text, m timestamp(0), n "
        "time(0), o date, o1 date, o2 date, "
        "fx int, dd decimal(10, 2), dd_notnull decimal(10, 2) not "
        "null, ss "
        "text, u int, ofd int, ufd int not null, ofq bigint, ufq bigint not null);");

    {
      insertCsvValues("test_one_row",
                      "8,-8,43,-78,1002,false,1.2,101.2,-101.2,2.4,-2002.4,bar,,bar,,"
                      "real_bar,,2014-12-13 22:23:15,15:13:14,,,,, 222.2,"
                      "222.2,,,,2147483647,9223372036854775807,-9223372036854775808");
      const std::string insert_query{
          "INSERT INTO test_one_row VALUES(8, -8, 43, -78, 1002, 'f', 1.2, 101.2, "
          "-101.2, "
          "2.4, "
          "-2002.4, 'bar', null, 'bar', null, "
          "'real_bar', NULL, '2014-12-13 22:23:15', "
          "'15:13:14', NULL, NULL, NULL, NULL, 222.2, 222.2, "
          "null, null, null, "
          "-2147483647, "
          "9223372036854775807, -9223372036854775808);"};
      run_sqlite_query(insert_query);
    }
  }

  static constexpr size_t g_array_test_row_count{20};

  static void importArrayTest(const std::string& table_name) {
    CHECK_EQ(size_t(0), g_array_test_row_count % 4);
    auto tinfo = getStorage()->getTableInfo(TEST_DB_ID, table_name);
    ASSERT_TRUE(tinfo);
    auto col_infos = getStorage()->listColumns(*tinfo);
    std::stringstream json_ss;
    for (size_t row_idx = 0; row_idx < g_array_test_row_count; ++row_idx) {
      json_ss << "{";
      for (size_t col_idx = 0; col_idx < col_infos.size(); ++col_idx) {
        auto col_info = col_infos[col_idx];
        if (col_info->is_rowid) {
          continue;
        }
        if (col_idx) {
          json_ss << ", ";
        }
        json_ss << "\"" << col_info->name << "\" : ";
        const auto& ti = col_info->type;
        switch (ti.get_type()) {
          case kINT:
            json_ss << (7 + row_idx);
            break;
          case kARRAY: {
            const auto& elem_ti = ti.get_elem_type();
            std::vector<std::string> array_elems;
            switch (elem_ti.get_type()) {
              case kBOOLEAN: {
                for (size_t i = 0; i < 3; ++i) {
                  if (row_idx % 2) {
                    array_elems.emplace_back("true");
                    array_elems.emplace_back("false");
                  } else {
                    array_elems.emplace_back("false");
                    array_elems.emplace_back("true");
                  }
                }
                break;
              }
              case kTINYINT:
                for (size_t i = 0; i < 3; ++i) {
                  array_elems.push_back(std::to_string(row_idx + i + 1));
                }
                break;
              case kSMALLINT:
                for (size_t i = 0; i < 3; ++i) {
                  array_elems.push_back(std::to_string(row_idx + i + 1));
                }
                break;
              case kINT:
                for (size_t i = 0; i < 3; ++i) {
                  array_elems.push_back(std::to_string((row_idx + i + 1) * 10));
                }
                break;
              case kBIGINT:
                for (size_t i = 0; i < 3; ++i) {
                  array_elems.push_back(std::to_string((row_idx + i + 1) * 100));
                }
                break;
              case kTEXT:
                for (size_t i = 0; i < 3; ++i) {
                  std::string val(2, 'a' + row_idx + i);
                  array_elems.push_back("\""s + val + "\""s);
                }
                break;
              case kFLOAT:
                for (size_t i = 0; i < 3; ++i) {
                  array_elems.emplace_back(std::to_string(row_idx + i + 1) + "." +
                                           std::to_string(row_idx + i + 1));
                }
                break;
              case kDOUBLE:
                for (size_t i = 0; i < 3; ++i) {
                  array_elems.emplace_back(std::to_string(11 * (row_idx + i + 1)) + "." +
                                           std::to_string(row_idx + i + 1));
                }
                break;
              case kDECIMAL:
                for (size_t i = 0; i < 3; ++i) {
                  array_elems.emplace_back(std::to_string(11 * (row_idx + i + 1)) + "." +
                                           std::to_string(row_idx + i + 1));
                }
                break;
              default:
                CHECK(false);
            }
            json_ss << "[" << boost::algorithm::join(array_elems, ", ") << "]";
            break;
          }
          case kTEXT:
            json_ss << "\"real_str" << row_idx << "\"";
            break;
          default:
            CHECK(false);
        }
      }
      json_ss << "}\n";
    }
    auto json_data = json_ss.str();
    insertJsonValues(table_name, json_data);
  }

  static void createTestArrayTable() {
    createTable("array_test",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"arr_i16", arrayType(kSMALLINT)},
                 {"arr_i32", arrayType(kINT)},
                 {"arr_i64", arrayType(kBIGINT)},
                 {"arr_str", arrayType(kTEXT)},
                 {"arr_float", arrayType(kFLOAT)},
                 {"arr_double", arrayType(kDOUBLE)},
                 {"arr_bool", arrayType(kBOOLEAN)},
                 {"arr_decimal", decimalArrayType(18, 6)},
                 {"real_str", SQLTypeInfo(kTEXT)},
                 {"arr3_i8", arrayType(kTINYINT, 3)},
                 {"arr3_i16", arrayType(kSMALLINT, 3)},
                 {"arr3_i32", arrayType(kINT, 3)},
                 {"arr3_i64", arrayType(kBIGINT, 3)},
                 {"arr3_float", arrayType(kFLOAT, 3)},
                 {"arr3_double", arrayType(kDOUBLE, 3)},
                 {"arr6_bool", arrayType(kBOOLEAN, 6)},
                 {"arr3_decimal", decimalArrayType(18, 6, 3)}});
    importArrayTest("array_test");
  }

  static void importCoalesceColsTestTable(const int id) {
    const std::string table_name = "coalesce_cols_test_" + std::to_string(id);
    createTable(table_name,
                {{"x", SQLTypeInfo(kINT, true)},
                 {"y", SQLTypeInfo(kINT)},
                 {"str", dictType()},
                 {"dup_str", dictType()},
                 {"d", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 32, kNULLT)},
                 {"t", SQLTypeInfo(kTIME)},
                 {"tz", SQLTypeInfo(kTIMESTAMP)},
                 {"dn", SQLTypeInfo(kDECIMAL, 5, 0, false)}},
                {id == 2 ? 2ULL : 20ULL});
    run_sqlite_query("DROP TABLE IF EXISTS " + table_name + ";");
    run_sqlite_query("CREATE TABLE " + table_name +
                     "(x int not null, y int, str text, dup_str text, d date, t "
                     "time, tz timestamp, dn decimal(5));");
    TestHelpers::ValuesGenerator gen(table_name);
    std::stringstream ss;
    for (int i = 0; i < 5; i++) {
      const auto insert_query = gen(i,
                                    20 - i,
                                    "'test'",
                                    "'test'",
                                    "'2018-01-01'",
                                    "'12:34:56'",
                                    "'2018-01-01 12:34:56'",
                                    i * 1.1);
      run_sqlite_query(insert_query);
      ss << i << "," << (20 - i) << ",test,test,2018-01-01,12:34:56,2018-01-01 12:34:56,"
         << int(i * 1.1) << std::endl;
    }
    for (size_t i = 5; i < 10; i++) {
      const auto insert_query = gen(i,
                                    20 - i,
                                    "'test1'",
                                    "'test1'",
                                    "'2017-01-01'",
                                    "'12:34:00'",
                                    "'2017-01-01 12:34:56'",
                                    i * 1.1);
      run_sqlite_query(insert_query);
      ss << i << "," << (20 - i)
         << ",test1,test1,2017-01-01,12:34:00,2017-01-01 12:34:56," << int(i * 1.1)
         << std::endl;
    }
    if (id > 0) {
      for (size_t i = 10; i < 15; i++) {
        const auto insert_query = gen(i,
                                      20 - i,
                                      "'test2'",
                                      "'test2'",
                                      "'2016-01-01'",
                                      "'12:00:56'",
                                      "'2016-01-01 12:34:56'",
                                      i * 1.1);
        run_sqlite_query(insert_query);
        ss << i << "," << (20 - i)
           << ",test2,test2,2016-01-01,12:00:56,2016-01-01 12:34:56," << int(i * 1.1)
           << std::endl;
      }
    }
    if (id > 1) {
      for (size_t i = 15; i < 20; i++) {
        const auto insert_query = gen(i,
                                      20 - i,
                                      "'test3'",
                                      "'test3'",
                                      "'2015-01-01'",
                                      "'10:34:56'",
                                      "'2015-01-01 12:34:56'",
                                      i * 1.1);
        run_sqlite_query(insert_query);
        ss << i << "," << (20 - i)
           << ",test3,test3,2015-01-01,10:34:56,2015-01-01 12:34:56," << int(i * 1.1)
           << std::endl;
      }
    }
    insertCsvValues(table_name, ss.str());
  }

  static void createProjTopTable() {
    createTable("proj_top", {{"str", SQLTypeInfo(kTEXT)}, {"x", SQLTypeInfo(kINT)}});
    insertCsvValues("proj_top", "a,7\nb,6\nc,5");
    run_sqlite_query("DROP TABLE IF EXISTS proj_top;");
    run_sqlite_query("CREATE TABLE proj_top(str TEXT, x INT);");
    run_sqlite_query("INSERT INTO proj_top VALUES('a', 7);");
    run_sqlite_query("INSERT INTO proj_top VALUES('b', 6);");
    run_sqlite_query("INSERT INTO proj_top VALUES('c', 5);");
  }

  static void createJoinTestTable() {
    createTable("join_test",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"y", SQLTypeInfo(kINT)},
                 {"str", dictType()},
                 {"dup_str", dictType()}},
                {2});
    insertCsvValues("join_test", "7,43,foo,foo\n8,,bar,foo\n9,,baz,bar");
    run_sqlite_query("DROP TABLE IF EXISTS join_test;");
    run_sqlite_query(
        "CREATE TABLE join_test(x int not null, y int, str text, dup_str text);");
    run_sqlite_query("INSERT INTO join_test VALUES(7, 43, 'foo', 'foo');");
    run_sqlite_query("INSERT INTO join_test VALUES(8, null, 'bar', 'foo');");
    run_sqlite_query("INSERT INTO join_test VALUES(9, null, 'baz', 'bar');");
  }

  static void createQueryRewriteTestTable() {
    createTable(
        "query_rewrite_test", {{"x", SQLTypeInfo(kINT)}, {"str", dictType()}}, {2});
    run_sqlite_query("DROP TABLE IF EXISTS query_rewrite_test;");
    run_sqlite_query("CREATE TABLE query_rewrite_test(x int, str text);");
    std::stringstream ss;
    for (size_t i = 1; i <= 30; ++i) {
      for (size_t j = 1; j <= i % 2 + 1; ++j) {
        const std::string insert_query{"INSERT INTO query_rewrite_test VALUES(" +
                                       std::to_string(i) + ", 'str" + std::to_string(i) +
                                       "');"};
        run_sqlite_query(insert_query);
        ss << i << ",str" << i << std::endl;
      }
    }
    insertCsvValues("query_rewrite_test", ss.str());
  }

  static void createEmpTable() {
    createTable("emp",
                {{"empno", SQLTypeInfo(kINT)},
                 {"ename", dictType()},
                 {"deptno", SQLTypeInfo(kINT)}},
                {2});
    insertCsvValues("emp", "1,Brock,10\n2,Bill,20\n3,Julia,60\n4,David,10");
    run_sqlite_query("DROP TABLE IF EXISTS emp;");
    run_sqlite_query("CREATE TABLE emp(empno INT, ename TEXT NOT NULL, deptno INT);");
    run_sqlite_query("INSERT INTO emp VALUES(1, 'Brock', 10);");
    run_sqlite_query("INSERT INTO emp VALUES(2, 'Bill', 20);");
    run_sqlite_query("INSERT INTO emp VALUES(3, 'Julia', 60);");
    run_sqlite_query("INSERT INTO emp VALUES(4, 'David', 10);");
  }

  void createTestLotsColsTable() {
    const size_t num_columns = 50;
    const std::string table_name("test_lots_cols");
    const std::string drop_table("DROP TABLE IF EXISTS " + table_name + ";");
    run_sqlite_query(drop_table);
    std::string create_query("CREATE TABLE " + table_name + "(");
    std::string insert_query1("INSERT INTO " + table_name + " VALUES (");
    std::string insert_query2(insert_query1);
    std::vector<ArrowStorage::ColumnDescription> cols;
    std::string csv1;
    std::string csv2;

    for (size_t i = 0; i < num_columns - 1; i++) {
      create_query += ("x" + std::to_string(i) + " INTEGER, ");
      insert_query1 += (std::to_string(i) + ", ");
      insert_query2 += (std::to_string(10000 + i) + ", ");
      cols.push_back({"x"s + std::to_string(i), SQLTypeInfo(kINT)});
      csv1 += std::to_string(i) + ",";
      csv2 += std::to_string(10000 + i) + ",";
    }
    create_query += "real_str TEXT";
    insert_query1 += "'real_foo');";
    insert_query2 += "'real_bar');";
    cols.push_back({"real_str", SQLTypeInfo(kTEXT)});
    csv1 += "real_foo";
    csv2 += "real_bar";

    createTable(table_name, cols, {2});
    run_sqlite_query(create_query + ");");

    for (size_t i = 0; i < 10; i++) {
      insertCsvValues(table_name, i % 2 ? csv2 : csv1);
      run_sqlite_query(i % 2 ? insert_query2 : insert_query1);
    }
  }

  static void createTestDateTimeTable() {
    createTable("test_date_time",
                {{"dt", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 0, kNULLT)}},
                {2});
    insertCsvValues("test_date_time", "1963-05-07\n1968-04-22\n1970-01-01\n1980-11-28");
  }

  static void createBigDecimalRangeTestTable() {
    createTable("big_decimal_range_test",
                {{"d", SQLTypeInfo(kDECIMAL, 14, 2, false)},
                 {"d1", SQLTypeInfo(kDECIMAL, 17, 11)}},
                {2});
    insertCsvValues("big_decimal_range_test",
                    "-40840124.400000,1.3\n59016609.300000,1.3\n-999999999999.99,1.3");
    run_sqlite_query("DROP TABLE IF EXISTS big_decimal_range_test;");
    run_sqlite_query(
        "CREATE TABLE big_decimal_range_test(d DECIMAL(14, 2), d1 DECIMAL(17,11));");
    run_sqlite_query("INSERT INTO big_decimal_range_test VALUES(-40840124.400000, 1.3);");
    run_sqlite_query("INSERT INTO big_decimal_range_test VALUES(59016609.300000, 1.3);");
    run_sqlite_query("INSERT INTO big_decimal_range_test VALUES(-999999999999.99, 1.3);");
  }

  static void createGpuSortTestTable() {
    createTable("gpu_sort_test",
                {{"x", SQLTypeInfo(kBIGINT)},
                 {"y", SQLTypeInfo(kINT)},
                 {"z", SQLTypeInfo(kSMALLINT)},
                 {"t", SQLTypeInfo(kTINYINT)}},
                {2});
    run_sqlite_query("DROP TABLE IF EXISTS gpu_sort_test;");
    run_sqlite_query(
        "CREATE TABLE gpu_sort_test (x bigint, y int, z smallint, t tinyint);");
    TestHelpers::ValuesGenerator gen("gpu_sort_test");
    for (size_t i = 0; i < 4; ++i) {
      insertCsvValues("gpu_sort_test", "2,2,2,2");
      run_sqlite_query(gen(2, 2, 2, 2));
    }
    for (size_t i = 0; i < 6; ++i) {
      insertCsvValues("gpu_sort_test", "16000,16000,16000,127");
      run_sqlite_query(gen(16000, 16000, 16000, 127));
    }
  }

  static void createLogicalSizeTestTable() {
    createTable("logical_size_test",
                {
                    {"big_int", SQLTypeInfo(kBIGINT, true)},
                    {"big_int_null", SQLTypeInfo(kBIGINT)},
                    {"id", SQLTypeInfo(kINT, true)},
                    {"id_null", SQLTypeInfo(kINT)},
                    {"small_int", SQLTypeInfo(kSMALLINT, true)},
                    {"small_int_null", SQLTypeInfo(kSMALLINT)},
                    {"tiny_int", SQLTypeInfo(kTINYINT, true)},
                    {"tiny_int_null", SQLTypeInfo(kTINYINT)},
                    {"float_not_null", SQLTypeInfo(kFLOAT, true)},
                    {"float_null", SQLTypeInfo(kFLOAT)},
                    {"double_not_null", SQLTypeInfo(kDOUBLE, true)},
                    {"double_null", SQLTypeInfo(kDOUBLE)},
                },
                {4});
    run_sqlite_query("DROP TABLE IF EXISTS logical_size_test;");
    run_sqlite_query(
        "CREATE TABLE logical_size_test (big_int BIGINT NOT NULL, big_int_null BIGINT, "
        "id INT NOT NULL, id_null INT, small_int SMALLINT NOT NULL, small_int_null "
        "SMALLINT, tiny_int TINYINT NOT NULL, tiny_int_null TINYINT, float_not_null "
        "FLOAT NOT NULL, float_null FLOAT, double_not_null DOUBLE NOT NULL, double_null "
        "DOUBLE);");

    std::vector<std::string> insert_queries;
    std::string csv_data;
    auto query_maker = [&](std::string str) {
      insert_queries.push_back("INSERT INTO  logical_size_test VALUES (" + str + ");");
      csv_data += std::regex_replace(str, std::regex("NULL"), "") + "\n";
    };

    // fragment 0:
    query_maker("2002,-57,7,0,73,32767,22,127,1.5,NULL,11.5,-21.6");
    query_maker("1001,63,6,NULL,77,-32767,21,NULL,1.6,1.1,11.6,NULL");
    query_maker("3003,63,5,2,79,NULL,23,125,1.5,-1.3,11.5,22.3");
    query_maker("3003,NULL,4,6,78,0,20,126,1.7,-1.5,11.7,22.5");
    // fragment 1:
    query_maker("2002,NULL,4,NULL,75,-112,-13,-125,2.5,-2.3,22.5,-23.5");
    query_maker("1001,-57,6,2,77,NULL,-14,-126,2.6,NULL,22.6,23.7");
    query_maker("1001,63,7,0,78,-32767,-15,NULL,2.7,2.7,22.7,NULL");
    query_maker("1001,-57,5,6,79,32767,-12,-127,2.6,-2.4,22.6,-23.4");
    // fragment 2:
    query_maker("3003,63,5,2,79,-32767,4,NULL,3.6,3.3,32.6,-33.3");
    query_maker("2002,-57,7,4,76,32767,2,-1,3.5,-3.7,32.5,33.7");
    query_maker("3003,NULL,4,NULL,77,NULL,3,-2,3.7,NULL,32.7,-33.5");
    query_maker("1001,-57,6,0,73,2345,1,-3,3.4,32.4,32.5,NULL");
    // fragment 3:
    query_maker("1001,63,6,4,77,0,12,-3,4.5,4.3,11.6,NULL");
    query_maker("3003,-57,4,2,78,32767,16,-1,4.6,4.1,11.5,22.3");
    query_maker("2002,63,7,6,75,-32767,13,-2,4.7,-4.1,22.7,-33.3");
    query_maker("2002,NULL,5,NULL,76,NULL,15,NULL,4.4,NULL,22.5,-23.4");
    for (auto insert_query : insert_queries) {
      run_sqlite_query(insert_query);
    }
    insertCsvValues("logical_size_test", csv_data);
  }

  static void createRandomTestTable() {
    createTable("random_test",
                {{"x1", SQLTypeInfo(kINT)},
                 {"x2", SQLTypeInfo(kINT)},
                 {"x3", SQLTypeInfo(kINT)},
                 {"x4", SQLTypeInfo(kINT)},
                 {"x5", SQLTypeInfo(kINT)}},
                {256});
    run_sqlite_query("DROP TABLE IF EXISTS random_test;");
    run_sqlite_query(
        "CREATE TABLE random_test (x1 int, x2 int, x3 int, x4 int, x5 int);");

    TestHelpers::ValuesGenerator gen("random_test");
    std::string csv_data;
    constexpr double pi = 3.141592653589793;
    for (size_t i = 0; i < 512; i++) {
      int32_t x1 = static_cast<int32_t>((3 * i + 1) % 5);
      int32_t x2 = static_cast<int32_t>(std::floor(10 * std::sin(i * pi / 64.0)));
      int32_t x3 = static_cast<int32_t>(std::floor(10 * std::cos(i * pi / 45.0)));
      int32_t x4 =
          static_cast<int32_t>(100000000 * std::floor(10 * std::sin(i * pi / 32.0)));
      int32_t x5 = static_cast<int32_t>(std::floor(1000000000 * std::cos(i * pi / 32.0)));
      run_sqlite_query(gen(x1, x2, x3, x4, x5));
      csv_data += std::to_string(x1) + "," + std::to_string(x2) + "," +
                  std::to_string(x3) + "," + std::to_string(x4) + "," +
                  std::to_string(x5) + "\n";
    }
    insertCsvValues("random_test", csv_data);
  }

  static void createImportDecimalCompressionTestTable() {
    createTable("decimal_compression_test",
                {{"big_dec", SQLTypeInfo(kDECIMAL, 17, 2, false)},
                 {"med_dec", SQLTypeInfo(kDECIMAL, 9, 2, false)},
                 {"small_dec", SQLTypeInfo(kDECIMAL, 4, 2, false)}},
                {2});
    run_sqlite_query("DROP TABLE IF EXISTS decimal_compression_test;");
    run_sqlite_query(
        "CREATE TABLE decimal_compression_test(big_dec DECIMAL(17, 2), med_dec "
        "DECIMAL(9, "
        "2), small_dec DECIMAL(4, 2));");
    {
      insertCsvValues("decimal_compression_test", "999999999999999.99,9999999.99,99.99");
      const std::string insert_query{
          "INSERT INTO decimal_compression_test VALUES(999999999999999.99, 9999999.99, "
          "99.99);"};
      run_sqlite_query(insert_query);
    }
    {
      insertCsvValues("decimal_compression_test",
                      "-999999999999999.99,-9999999.99,-99.99");
      const std::string insert_query{
          "INSERT INTO decimal_compression_test VALUES(-999999999999999.99, -9999999.99, "
          "-99.99);"};
      run_sqlite_query(insert_query);
    }
    {
      insertCsvValues("decimal_compression_test", "12.24,12.24,12.24");
      const std::string sqlite_insert_query{
          "INSERT INTO decimal_compression_test VALUES(12.24, 12.24 , 12.24);"};
      run_sqlite_query(sqlite_insert_query);
    }
  }

  static void createEmptytabTable() {
    createTable("emptytab",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"y", SQLTypeInfo(kINT)},
                 {"t", SQLTypeInfo(kBIGINT, true)},
                 {"f", SQLTypeInfo(kFLOAT, true)},
                 {"d", SQLTypeInfo(kDOUBLE, true)},
                 {"dd", SQLTypeInfo(kDECIMAL, 10, 2, true)},
                 {"ts", SQLTypeInfo(kTIMESTAMP)}});
    run_sqlite_query("DROP TABLE IF EXISTS emptytab;");
    run_sqlite_query(
        "CREATE TABLE emptytab(x int not null, y int, t bigint not null, f float not "
        "null, d double not null, dd "
        "decimal(10, 2) not null, ts timestamp);");
  }

  static void createSubqueryTestTable() {
    createTable("subquery_test", {{"x", SQLTypeInfo(kINT)}}, {2});
    run_sqlite_query("DROP TABLE IF EXISTS subquery_test;");
    run_sqlite_query("CREATE TABLE subquery_test(x int);");
    CHECK_EQ(g_num_rows % 2, size_t(0));
    for (size_t i = 0; i < g_num_rows; ++i) {
      insertCsvValues("subquery_test", "7");
      run_sqlite_query("INSERT INTO subquery_test VALUES(7);");
    }
    for (size_t i = 0; i < g_num_rows / 2; ++i) {
      insertCsvValues("subquery_test", "8");
      run_sqlite_query("INSERT INTO subquery_test VALUES(8);");
    }
    for (size_t i = 0; i < g_num_rows / 2; ++i) {
      insertCsvValues("subquery_test", "9");
      run_sqlite_query("INSERT INTO subquery_test VALUES(9);");
    }
  }

  static void createTestInBitmaptable() {
    createTable("test_in_bitmap", {{"str", dictType()}});
    insertCsvValues("test_in_bitmap", "a\nb\nc");
    insertJsonValues("test_in_bitmap", "{\"str\": null}");
    run_sqlite_query("DROP TABLE IF EXISTS test_in_bitmap;");
    run_sqlite_query("CREATE TABLE test_in_bitmap(str TEXT);");
    run_sqlite_query("INSERT INTO test_in_bitmap VALUES('a');");
    run_sqlite_query("INSERT INTO test_in_bitmap VALUES('b');");
    run_sqlite_query("INSERT INTO test_in_bitmap VALUES('c');");
    run_sqlite_query("INSERT INTO test_in_bitmap VALUES(NULL);");
  }

  static void createDeptTable() {
    createTable("dept", {{"deptno", SQLTypeInfo(kINT)}, {"dname", dictType()}}, {2});
    insertCsvValues("dept", "10,Sales\n20,Dev\n30,Marketing\n40,HR\n50,QA");
    run_sqlite_query("DROP TABLE IF EXISTS dept;");
    run_sqlite_query("CREATE TABLE dept(deptno INT, dname TEXT ENCODING DICT);");
    run_sqlite_query("INSERT INTO dept VALUES(10, 'Sales');");
    run_sqlite_query("INSERT INTO dept VALUES(20, 'Dev');");
    run_sqlite_query("INSERT INTO dept VALUES(30, 'Marketing');");
    run_sqlite_query("INSERT INTO dept VALUES(40, 'HR');");
    run_sqlite_query("INSERT INTO dept VALUES(50, 'QA');");
  }

  static void createArrayTestInnerTable() {
    createTable("array_test_inner",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"arr_i16", arrayType(kSMALLINT)},
                 {"arr_i32", arrayType(kINT)},
                 {"arr_i64", arrayType(kBIGINT)},
                 {"arr_str", arrayType(kTEXT)},
                 {"arr_float", arrayType(kFLOAT)},
                 {"arr_double", arrayType(kDOUBLE)},
                 {"arr_bool", arrayType(kBOOLEAN)},
                 {"real_str", SQLTypeInfo(kTEXT)}});
    importArrayTest("array_test_inner");
  }

  static void createHashJoinTestTable() {
    createTable("hash_join_test",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"str", dictType()},
                 {"t", SQLTypeInfo(kBIGINT)}},
                {2});
    insertCsvValues("hash_join_test", "7,foo,1001\n8,bar,5000000000\n9,the,1002");
    run_sqlite_query("DROP TABLE IF EXISTS hash_join_test;");
    run_sqlite_query("CREATE TABLE hash_join_test(x int not null, str text, t BIGINT);");
    run_sqlite_query("INSERT INTO hash_join_test VALUES(7, 'foo', 1001);");
    run_sqlite_query("INSERT INTO hash_join_test VALUES(8, 'bar', 5000000000);");
    run_sqlite_query("INSERT INTO hash_join_test VALUES(9, 'the', 1002);");
  }

  static void createBarTable() {
    createTable("bar", {{"str", dictType()}}, {2});
    insertCsvValues("bar", "bar");
    run_sqlite_query("DROP TABLE IF EXISTS bar;");
    run_sqlite_query("CREATE TABLE bar(str text);");
    run_sqlite_query("INSERT INTO bar VALUES('bar');");
  }

  static void createSingleRowTestTable() {
    createTable("single_row_test", {{"x", SQLTypeInfo(kINT)}});
    insertJsonValues("single_row_test", "{\"x\": null}");
    run_sqlite_query("DROP TABLE IF EXISTS single_row_test;");
    run_sqlite_query("CREATE TABLE single_row_test(x int);");
    run_sqlite_query("INSERT INTO single_row_test VALUES(null);");
  }

  static void createTestInnerXTable() {
    createTable("test_inner_x",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"y", SQLTypeInfo(kINT, false)},
                 {"str", dictType()}},
                {2});
    insertCsvValues("test_inner_x", "7,43,foo");
    run_sqlite_query("DROP TABLE IF EXISTS test_inner_x;");
    run_sqlite_query("CREATE TABLE test_inner_x(x int not null, y int, str text);");
    run_sqlite_query("INSERT INTO test_inner_x VALUES(7, 43, 'foo');");
  }

  static void createTestXTable() {
    createTable("test_x",
                {{"x", SQLTypeInfo(kINT, true)},
                 {"y", SQLTypeInfo(kINT)},
                 {"z", SQLTypeInfo(kSMALLINT)},
                 {"t", SQLTypeInfo(kBIGINT)},
                 {"b", SQLTypeInfo(kBOOLEAN)},
                 {"f", SQLTypeInfo(kFLOAT)},
                 {"ff", SQLTypeInfo(kFLOAT)},
                 {"fn", SQLTypeInfo(kFLOAT)},
                 {"d", SQLTypeInfo(kDOUBLE)},
                 {"dn", SQLTypeInfo(kDOUBLE)},
                 {"str", dictType()},
                 {"null_str", dictType()},
                 {"fixed_str", dictType(2)},
                 {"real_str", SQLTypeInfo(kTEXT)},
                 {"m", SQLTypeInfo(kTIMESTAMP, 0, 0)},
                 {"n", SQLTypeInfo(kTIME)},
                 {"o", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 0, kNULLT)},
                 {"o1", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 16, kNULLT)},
                 {"o2", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 32, kNULLT)},
                 {"fx", SQLTypeInfo(kSMALLINT)},
                 {"dd", SQLTypeInfo(kDECIMAL, 10, 2, false)},
                 {"dd_notnull", SQLTypeInfo(kDECIMAL, 10, 2, true)},
                 {"ss", dictType()},
                 {"u", SQLTypeInfo(kINT)},
                 {"ofd", SQLTypeInfo(kINT)},
                 {"ufd", SQLTypeInfo(kINT, true)},
                 {"ofq", SQLTypeInfo(kBIGINT)},
                 {"ufq", SQLTypeInfo(kBIGINT, true)}},
                {2});
    run_sqlite_query("DROP TABLE IF EXISTS test_x;");
    run_sqlite_query(
        "CREATE TABLE test_x(x int not null, y int, z smallint, t bigint, b boolean, f "
        "float, ff float, fn float, d "
        "double, dn double, str "
        "text, null_str text,"
        "fixed_str text, real_str text, m timestamp(0), n time(0), o date, o1 date, "
        "o2 date, fx int, dd decimal(10, 2), "
        "dd_notnull decimal(10, 2) not null, ss text, u int, ofd int, ufd int not "
        "null, "
        "ofq bigint, ufq bigint not "
        "null);");
    CHECK_EQ(g_num_rows % 2, size_t(0));
    for (size_t i = 0; i < g_num_rows; ++i) {
      run_sqlite_query(
          "INSERT INTO test_x VALUES(7, 42, 101, 1001, 't', 1.1, 1.1, null, 2.2, null, "
          "'foo', null, 'foo', 'real_foo', '2014-12-13 22:23:15', "
          "'15:13:14', '1999-09-09', '1999-09-09', '1999-09-09', 9, 111.1, 111.1, "
          "'fish', null, 2147483647, -2147483648, null, -1);");
      insertCsvValues("test_x",
                      "7,42,101,1001,true,1.1,1.1,,2.2,,foo,,foo,real_foo,2014-12-13 "
                      "22:23:15,15:13:14,1999-09-09,1999-09-09,1999-09-09,9,111.1,111.1,"
                      "fish,,2147483647,-2147483648,,-1");
    }
    for (size_t i = 0; i < g_num_rows / 2; ++i) {
      run_sqlite_query(
          "INSERT INTO test_x VALUES(8, 43, 102, 1002, 'f', 1.2, 101.2, -101.2, 2.4, "
          "-2002.4, 'bar', null, 'bar', 'real_bar', '2014-12-13 22:23:15', "
          "'15:13:14', NULL, NULL, NULL, NULL, 222.2, 222.2, null, null, null, "
          "-2147483647, 9223372036854775807, -9223372036854775808);");
      insertCsvValues(
          "test_x",
          "8,43,102,1002,false,1.2,101.2,-101.2,2.4,-2002.4,bar,,bar,real_bar,2014-12-13 "
          "22:23:15,15:13:14,,,,,222.2,222.2,,,,-2147483647,9223372036854775807,-"
          "9223372036854775808");
    }
    for (size_t i = 0; i < g_num_rows / 2; ++i) {
      run_sqlite_query(
          "INSERT INTO test_x VALUES(7, 43, 102, 1002, 't', 1.3, 1000.3, -1000.3, 2.6, "
          "-220.6, 'baz', null, 'baz', 'real_baz', '2014-12-13 22:23:15', "
          "'15:13:14', '1999-09-09', '1999-09-09', '1999-09-09', 11, 333.3, 333.3, "
          "'boat', null, 1, -1, 1, -9223372036854775808);");
      insertCsvValues(
          "test_x",
          "7,43,102,1002,true,1.3,1000.3,-1000.3,2.6,-220.6,baz,,baz,real_baz,2014-12-13 "
          "22:23:15,15:13:14,1999-09-09,1999-09-09,1999-09-09,11,333.3,333.3,boat,,1,-1,"
          "1,-9223372036854775808");
    }
  }

  static void createBweqTestTable() {
    createTable("bweq_test", {{"x", SQLTypeInfo(kINT)}}, {2});
    run_sqlite_query("DROP TABLE IF EXISTS bweq_test");
    run_sqlite_query("create table bweq_test (x int);");
    for (auto i = 0; i < 15; i++) {
      insertCsvValues("bweq_test", "7");
      run_sqlite_query("insert into bweq_test values(7);");
    }
    for (auto i = 0; i < 5; i++) {
      insertJsonValues("bweq_test", "{\"x\": null}");
      run_sqlite_query("insert into bweq_test values(NULL);");
    }
  }

  static void createOuterJoinFooTable() {
    createTable(
        "outer_join_foo",
        {{"a", SQLTypeInfo(kINT)}, {"b", SQLTypeInfo(kINT)}, {"c", SQLTypeInfo(kINT)}});
    insertCsvValues("outer_join_foo", "1,3,2\n2,3,4\n,6,7\n7,,8\n,,10");
    run_sqlite_query("DROP TABLE IF EXISTS outer_join_foo;");
    run_sqlite_query("CREATE TABLE outer_join_foo (a int, b int, c int);");
    run_sqlite_query("INSERT INTO outer_join_foo VALUES (1,3,2)");
    run_sqlite_query("INSERT INTO outer_join_foo VALUES (2,3,4)");
    run_sqlite_query("INSERT INTO outer_join_foo VALUES (null,6,7)");
    run_sqlite_query("INSERT INTO outer_join_foo VALUES (7,null,8)");
    run_sqlite_query("INSERT INTO outer_join_foo VALUES (null,null,10)");
  }

  static void createOuterJoinBarTable() {
    createTable(
        "outer_join_bar",
        {{"d", SQLTypeInfo(kINT)}, {"e", SQLTypeInfo(kINT)}, {"f", SQLTypeInfo(kINT)}});
    insertCsvValues("outer_join_bar", "1,3,4\n4,3,5\n,9,7\n9,,8\n,,11");
    run_sqlite_query("DROP TABLE IF EXISTS outer_join_bar;");
    run_sqlite_query("CREATE TABLE outer_join_bar (d int, e int, f int);");
    run_sqlite_query("INSERT INTO outer_join_bar VALUES (1,3,4)");
    run_sqlite_query("INSERT INTO outer_join_bar VALUES (4,3,5)");
    run_sqlite_query("INSERT INTO outer_join_bar VALUES (null,9,7)");
    run_sqlite_query("INSERT INTO outer_join_bar VALUES (9,null,8)");
    run_sqlite_query("INSERT INTO outer_join_bar VALUES (null,null,11)");
  }

  static void createOuterJoinBar2Table() {
    createTable("outer_join_bar2",
                {{"d", SQLTypeInfo(kINT)},
                 {"e", SQLTypeInfo(kINT)},
                 {"f", SQLTypeInfo(kINT)},
                 {"g", SQLTypeInfo(kINT)},
                 {"h", SQLTypeInfo(kINT)},
                 {"i", SQLTypeInfo(kINT)},
                 {"j", SQLTypeInfo(kINT)}});
    insertCsvValues(
        "outer_join_bar2",
        "1,3,4,1,1,1,1\n4,3,5,2,2,2,2\n,9,7,2,2,2,2\n9,,8,2,2,2,2\n,,11,2,2,2,2");
    run_sqlite_query("DROP TABLE IF EXISTS outer_join_bar2;");
    run_sqlite_query(
        "CREATE TABLE outer_join_bar2 (d int, e int, f int, g int, h int, i int, j "
        "int)");
    run_sqlite_query("INSERT INTO outer_join_bar2 VALUES (1,3,4,1,1,1,1)");
    run_sqlite_query("INSERT INTO outer_join_bar2 VALUES (4,3,5,2,2,2,2)");
    run_sqlite_query("INSERT INTO outer_join_bar2 VALUES (null,9,7,2,2,2,2)");
    run_sqlite_query("INSERT INTO outer_join_bar2 VALUES (9,null,8,2,2,2,2)");
    run_sqlite_query("INSERT INTO outer_join_bar2 VALUES (null,null,11,2,2,2,2)");
  }

  static void createUnnestJoinTestTable() {
    createTable("unnest_join_test", {{"x", dictType()}});
    insertJsonValues("unnest_join_test", R"___({"x": "aaa"}
{"x": "bbb"}
{"x": "ccc"}
{"x": "ddd"}
{"x": null}
{"x": "aaa"}
{"x": "bbb"}
{"x": "ccc"}
{"x": "ddd"}
{"x": null})___");
  }

  static void cretaeTestInnerYTable() {
    createTable(
        "test_inner_y",
        {{"x", SQLTypeInfo(kINT, true)}, {"y", SQLTypeInfo(kINT)}, {"str", dictType()}},
        {2});
    insertCsvValues("test_inner_y", "8,43,bar\n7,43,foo");
    run_sqlite_query("DROP TABLE IF EXISTS test_inner_y;");
    run_sqlite_query("CREATE TABLE test_inner_y(x int not null, y int, str text);");
    run_sqlite_query("INSERT INTO test_inner_y VALUES(8, 43, 'bar');");
    run_sqlite_query("INSERT INTO test_inner_y VALUES(7, 43, 'foo');");
  }

  static void createHashJoinDecimalTest() {
    createTable("hash_join_decimal_test",
                {{"x", SQLTypeInfo(kDECIMAL, 18, 2, false)},
                 {"y", SQLTypeInfo(kDECIMAL, 18, 3, false)}},
                {2});
    insertCsvValues("hash_join_decimal_test",
                    "1.00,1.000\n2.00,2.000\n3.00,3.000\n4.00,4.001\n10.00,10.000");
    run_sqlite_query("DROP TABLE IF EXISTS hash_join_decimal_test;");
    run_sqlite_query(
        "CREATE TABLE hash_join_decimal_test(x DECIMAL(18,2), y DECIMAL(18,3));");
    run_sqlite_query("INSERT INTO hash_join_decimal_test VALUES(1.00, 1.000);");
    run_sqlite_query("INSERT INTO hash_join_decimal_test VALUES(2.00, 2.000);");
    run_sqlite_query("INSERT INTO hash_join_decimal_test VALUES(3.00, 3.000);");
    run_sqlite_query("INSERT INTO hash_join_decimal_test VALUES(4.00, 4.001);");
    run_sqlite_query("INSERT INTO hash_join_decimal_test VALUES(10.00, 10.000);");
  }

  static void createTextGroupByTestTable() {
    createTable(
        "text_group_by_test",
        {{"tdef", dictType()}, {"tdict", dictType()}, {"tnone", SQLTypeInfo(kTEXT)}},
        {200});
    insertCsvValues("text_group_by_test", "hello,world,:-)");
  }

  static void createDatetimeOverflowTable() {
    createTable("ts_overflow_underflow",
                {{"a", SQLTypeInfo(kTIMESTAMP, 0, 0)},
                 {"b", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 32, kNULLT)}});
    insertCsvValues(
        "ts_overflow_underflow",
        "2273-01-01 23:12:12,2273-01-01\n2263-01-01 00:00:00,2263-01-01\n1676-09-21 "
        "00:12:43,1676-09-21\n1677-09-21 00:00:43,1677-09-21\n,");
    run_sqlite_query("DROP TABLE IF EXISTS ts_overflow_underflow;");
    run_sqlite_query("CREATE TABLE ts_overflow_underflow (a TIMESTAMP(0), b DATE);");

    run_sqlite_query(
        "INSERT INTO ts_overflow_underflow VALUES('2273-01-01 23:12:12', "
        "'2273-01-01');");
    run_sqlite_query(
        "INSERT INTO ts_overflow_underflow VALUES('2263-01-01 00:00:00', "
        "'2263-01-01');");
    run_sqlite_query(
        "INSERT INTO ts_overflow_underflow VALUES('1676-09-21 00:12:43', "
        "'1676-09-21');");
    run_sqlite_query(
        "INSERT INTO ts_overflow_underflow VALUES('1677-09-21 00:00:43', "
        "'1677-09-21');");
    run_sqlite_query("INSERT INTO ts_overflow_underflow VALUES(null, null);");
  }

  static void createCurrentUserTable() {
    createTable("test_current_user", {{"u", dictType()}});
    insertCsvValues("test_current_user", "SESSIONLESS_USER\nsome_user\nsome_other_user");

    run_sqlite_query("DROP TABLE IF EXISTS test_current_user;");
    run_sqlite_query("CREATE TABLE test_current_user (u TEXT);");
    run_sqlite_query("INSERT INTO test_current_user VALUES('SESSIONLESS_USER');");
    run_sqlite_query("INSERT INTO test_current_user VALUES('some_user');");
    run_sqlite_query("INSERT INTO test_current_user VALUES('some_other_user');");
  }

  static void createCorrInFactsTable() {
    createTable("corr_in_facts", {{"id", SQLTypeInfo(kINT)}, {"val", SQLTypeInfo(kINT)}});
    insertCsvValues("corr_in_facts", "1,1\n1,2\n1,3\n1,4\n2,1\n2,2\n2,3\n2,4");

    run_sqlite_query("DROP TABLE IF EXISTS corr_in_facts;");
    run_sqlite_query("CREATE TABLE corr_in_facts (id INT, val INT);");
    run_sqlite_query("INSERT INTO corr_in_facts VALUES (1,1);");
    run_sqlite_query("INSERT INTO corr_in_facts VALUES (1,2)");
    run_sqlite_query("INSERT INTO corr_in_facts VALUES (1,3)");
    run_sqlite_query("INSERT INTO corr_in_facts VALUES (1,4)");
    run_sqlite_query("INSERT INTO corr_in_facts VALUES (2,1)");
    run_sqlite_query("INSERT INTO corr_in_facts VALUES (2,2)");
    run_sqlite_query("INSERT INTO corr_in_facts VALUES (2,3)");
    run_sqlite_query("INSERT INTO corr_in_facts VALUES (2,4)");
  }

  static void createCorrInLookup() {
    createTable("corr_in_lookup",
                {{"id", SQLTypeInfo(kINT)}, {"val", SQLTypeInfo(kINT)}});
    insertCsvValues("corr_in_lookup", "1,1\n2,2\n3,3\n4,4");

    run_sqlite_query("DROP TABLE IF EXISTS corr_in_lookup;");
    run_sqlite_query("CREATE TABLE corr_in_lookup (id INT, val INT);");
    run_sqlite_query("INSERT INTO corr_in_lookup VALUES (1,1);");
    run_sqlite_query("INSERT INTO corr_in_lookup VALUES (2,2)");
    run_sqlite_query("INSERT INTO corr_in_lookup VALUES (3,3)");
    run_sqlite_query("INSERT INTO corr_in_lookup VALUES (4,4)");
  }

  static void createRoundingTable() {
    createTable("test_rounding",
                {{"s16", SQLTypeInfo(kSMALLINT)},
                 {"s32", SQLTypeInfo(kINT)},
                 {"s64", SQLTypeInfo(kBIGINT)},
                 {"f32", SQLTypeInfo(kFLOAT)},
                 {"f64", SQLTypeInfo(kDOUBLE)},
                 {"n64", SQLTypeInfo(kDECIMAL, 10, 5, false)},
                 {"d64", SQLTypeInfo(kDECIMAL, 10, 5, false)}});
    insertCsvValues("test_rounding",
                    "3456,234567,3456789012,3456.3456,34567.23456,34567.23456,"
                    "34567.23456\n-3456,-234567,-3456789012,-3456.3456,-34567.23456,"
                    "-34567.23456,-34567.23456\n,,,,,,");

    run_sqlite_query("DROP TABLE IF EXISTS test_rounding;");
    run_sqlite_query(
        "CREATE TABLE test_rounding (s16 SMALLINT, s32 INTEGER, s64 BIGINT, f32 FLOAT, "
        "f64 DOUBLE, n64 NUMERIC(10,5), d64 DECIMAL(10,5));");
    run_sqlite_query(
        "INSERT INTO test_rounding VALUES(3456, 234567, 3456789012, 3456.3456, "
        "34567.23456, 34567.23456, 34567.23456);");
    run_sqlite_query(
        "INSERT INTO test_rounding VALUES(-3456, -234567, -3456789012, -3456.3456, "
        "-34567.23456, -34567.23456, -34567.23456);");
    run_sqlite_query(
        "INSERT INTO test_rounding VALUES(NULL, NULL, NULL, NULL, NULL, NULL, NULL);");
  }

  static void createWindowFuncTable(const bool multi_frag) {
    const std::string table_name =
        multi_frag ? "test_window_func_multi_frag" : "test_window_func";

    createTable(table_name,
                {{"x", SQLTypeInfo(kINT)},
                 {"y", dictType()},
                 {"t", SQLTypeInfo(kINT)},
                 {"d", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 32, kNULLT)},
                 {"f", SQLTypeInfo(kFLOAT)},
                 {"dd", SQLTypeInfo(kDOUBLE)}},
                {multi_frag ? 2ULL : 32000000ULL});

    run_sqlite_query("DROP TABLE IF EXISTS " + table_name + ";");
    run_sqlite_query("CREATE TABLE " + table_name +
                     " (x INTEGER, y TEXT, t INTEGER, d DATE, f FLOAT, dd DOUBLE);");
    insertCsvValues(table_name, "1,aaa,4,2019-03-02,1,1");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(1, 'aaa', 4, '2019-03-02', 1, 1);");
    insertCsvValues(table_name, "0,aaa,5,2019-03-01,0,0");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(0, 'aaa', 5, '2019-03-01', 0, 0);");
    insertCsvValues(table_name, "2,ccc,6,2019-03-03,2,2");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(2, 'ccc', 6, '2019-03-03', 2, 2);");
    insertCsvValues(table_name, "10,bbb,7,2019-03-11,10,10");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(10, 'bbb', 7, '2019-03-11', 10, 10);");
    insertCsvValues(table_name, "3,bbb,8,2019-03-04,3,3");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(3, 'bbb', 8, '2019-03-04', 3, 3);");
    insertCsvValues(table_name, "6,bbb,9,2019-03-07,6,6");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(6, 'bbb', 9, '2019-03-07', 6, 6);");
    insertCsvValues(table_name, "9,bbb,10,2019-03-10,9,9");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(9, 'bbb', 10, '2019-03-10', 9, 9);");
    insertCsvValues(table_name, "6,bbb,11,2019-03-07,6,6");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(6, 'bbb', 11, '2019-03-07', 6, 6);");
    insertCsvValues(table_name, "9,bbb,12,2019-03-10,9,9");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(9, 'bbb', 12, '2019-03-10', 9, 9);");
    insertCsvValues(table_name, "9,bbb,13,2019-03-10,9,9");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(9, 'bbb', 13, '2019-03-10', 9, 9);");
    insertCsvValues(table_name, ",,14,,,");
    run_sqlite_query("INSERT INTO " + table_name +
                     " VALUES(NULL, NULL, 14, NULL, NULL, NULL);");
  }

  static void createLargeWindowFuncTable(const bool multi_frag) {
    const std::string table_name =
        multi_frag ? "test_window_func_large_multi_frag" : "test_window_func_large";

    createTable(table_name,
                {{"i_unique", SQLTypeInfo(kBIGINT)},
                 {"i_1000", SQLTypeInfo(kINT)},
                 {"i_20", SQLTypeInfo(kSMALLINT)},
                 {"d", SQLTypeInfo(kDOUBLE)},
                 {"f", SQLTypeInfo(kFLOAT)},
                 {"t", dictType()},
                 {"t_unique", dictType()}},
                {multi_frag ? 100ULL : 32000000ULL});

    run_sqlite_query("DROP TABLE IF EXISTS " + table_name + ";");
    run_sqlite_query("CREATE TABLE " + table_name +
                     " (i_unique BIGINT, i_1000 INTEGER, i_20 SMALLINT, d DOUBLE, f "
                     "FLOAT, t TEXT, t_unique TEXT);");

    const size_t rand_seed = multi_frag ? 23 : 42;
    const size_t num_rows = 40000;

    std::mt19937 rand_gen(rand_seed);
    std::vector<std::string> text_values{"a", "b", "c", "d", "e"};
    std::uniform_int_distribution<> i_1000_dist(0, 999);
    std::uniform_int_distribution<> i_20_dist(0, 19);
    std::uniform_real_distribution<> d_dist(10.0, 10000.0);
    std::uniform_real_distribution<> f_dist(10.0, 10000.0);
    std::uniform_int_distribution<> t_dist(0, text_values.size() - 1);

    std::vector<std::vector<std::string>> insert_text_vals;
    std::string csv_file;

    for (size_t r = 0; r != num_rows; ++r) {
      const auto i_unique = r;
      const auto i_1000 = i_1000_dist(rand_gen);
      const auto i_20 = i_20_dist(rand_gen);
      const auto d = d_dist(rand_gen);
      const auto f = f_dist(rand_gen);
      const auto t = text_values[t_dist(rand_gen)];

      std::vector<std::string> row_values{std::to_string(i_unique),
                                          std::to_string(i_1000),
                                          std::to_string(i_20),
                                          std::to_string(d),
                                          std::to_string(f),
                                          t,
                                          std::to_string(i_unique)};
      if (r > 0) {
        csv_file += "\n";
      }
      csv_file += row_values[0] + "," + row_values[1] + "," + row_values[2] + "," +
                  row_values[3] + "," + row_values[4] + "," + row_values[5] + "," +
                  row_values[6];
      insert_text_vals.emplace_back(row_values);
    }

    insertCsvValues(table_name, csv_file);
    sqlite_batch_insert(table_name, insert_text_vals);
  }

  static void createUnionAllTestsTable() {
    std::string sql;

    createTable("union_all_a",
                {{"a0", SQLTypeInfo(kSMALLINT)},
                 {"a1", SQLTypeInfo(kINT)},
                 {"a2", SQLTypeInfo(kBIGINT)},
                 {"a3", SQLTypeInfo(kFLOAT)}},
                {2});
    createTable("union_all_b",
                {{"b0", SQLTypeInfo(kSMALLINT)},
                 {"b1", SQLTypeInfo(kINT)},
                 {"b2", SQLTypeInfo(kBIGINT)},
                 {"b3", SQLTypeInfo(kFLOAT)}},
                {3});

    run_sqlite_query("DROP TABLE IF EXISTS union_all_a;");
    run_sqlite_query("DROP TABLE IF EXISTS union_all_b;");
    run_sqlite_query(
        "CREATE TABLE union_all_a (a0 SMALLINT, a1 INT, a2 BIGINT, a3 FLOAT);");
    run_sqlite_query(
        "CREATE TABLE union_all_b (b0 SMALLINT, b1 INT, b2 BIGINT, b3 FLOAT);");

    for (int i = 0; i < 10; i++) {
      sql = cat("INSERT INTO union_all_a VALUES (",
                110 + i,
                ',',
                120 + i,
                ',',
                130 + i,
                ',',
                140 + i,
                ");");
      run_sqlite_query(sql);
      insertCsvValues("union_all_a",
                      std::to_string(110 + i) + "," + std::to_string(120 + i) + "," +
                          std::to_string(130 + i) + "," + std::to_string(140 + i));

      sql = cat("INSERT INTO union_all_b VALUES (",
                210 + i,
                ',',
                220 + i,
                ',',
                230 + i,
                ',',
                240 + i,
                ");");
      run_sqlite_query(sql);
      insertCsvValues("union_all_b",
                      std::to_string(210 + i) + "," + std::to_string(220 + i) + "," +
                          std::to_string(230 + i) + "," + std::to_string(240 + i));
    }
  }

  static void createVarlenLazyFetchTable() {
    createTable("varlen_table",
                {{"t", SQLTypeInfo(kTINYINT)},
                 {"real_str", SQLTypeInfo(kTEXT)},
                 {"array_i16", arrayType(kSMALLINT)}},
                {256});
    std::stringstream ss;
    for (int i = 0; i < 255; i++) {
      ss << "{\"t\": " << (i - 127) << ", \"real_str\": \"number" << i
         << "\", \"array_i16\": [" << (2 * i) << ", " << (2 * i + 1) << "]}" << std::endl;
    }
    insertJsonValues("varlen_table", ss.str());
  }

  static void createAndPopulateTestTables() {
    createTestInnerTable();
    createTestTable();
    createTestEmptyTable();
    createTestOneRowTable();
    createEmptyTestTable();
    createTestRangesTable();
    createTestArrayTable();
    importCoalesceColsTestTable(0);
    importCoalesceColsTestTable(1);
    importCoalesceColsTestTable(2);
    createProjTopTable();
    createJoinTestTable();
    createQueryRewriteTestTable();
    createEmpTable();
    createTestDateTimeTable();
    createBigDecimalRangeTestTable();
    createGpuSortTestTable();
    createLogicalSizeTestTable();
    createRandomTestTable();
    createImportDecimalCompressionTestTable();
    createEmptytabTable();
    createSubqueryTestTable();
    createDeptTable();
    createTestInBitmaptable();
    createArrayTestInnerTable();
    createHashJoinTestTable();
    createBarTable();
    createSingleRowTestTable();
    createTestInnerXTable();
    createTestXTable();
    createBweqTestTable();
    createOuterJoinFooTable();
    createOuterJoinBarTable();
    createOuterJoinBar2Table();
    createUnnestJoinTestTable();
    cretaeTestInnerYTable();
    createHashJoinDecimalTest();
    createTextGroupByTestTable();
    createDatetimeOverflowTable();
    createCurrentUserTable();
    createCorrInFactsTable();
    createCorrInLookup();
    createRoundingTable();
    createWindowFuncTable(true);
    createWindowFuncTable(false);
    createLargeWindowFuncTable(true);
    createLargeWindowFuncTable(false);
    createUnionAllTestsTable();
    createVarlenLazyFetchTable();
  }

  static void check_date_trunc_groups(const ResultSet& rows) {
    {
      const auto crt_row = rows.getNextRow(true, true);
      CHECK(!crt_row.empty());
      CHECK_EQ(size_t(3), crt_row.size());
      const auto sv0 = v<int64_t>(crt_row[0]);
      ASSERT_EQ(int64_t(936144000), sv0);
      const auto sv1 = boost::get<std::string>(v<NullableString>(crt_row[1]));
      ASSERT_EQ("foo", sv1);
      const auto sv2 = v<int64_t>(crt_row[2]);
      ASSERT_EQ(static_cast<int64_t>(g_num_rows), sv2);
    }
    {
      const auto crt_row = rows.getNextRow(true, true);
      CHECK(!crt_row.empty());
      CHECK_EQ(size_t(3), crt_row.size());
      const auto sv0 = v<int64_t>(crt_row[0]);
      ASSERT_EQ(inline_int_null_val(rows.getColType(0)), sv0);
      const auto sv1 = boost::get<std::string>(v<NullableString>(crt_row[1]));
      ASSERT_EQ("bar", sv1);
      const auto sv2 = v<int64_t>(crt_row[2]);
      ASSERT_EQ(static_cast<int64_t>(g_num_rows) / 2, sv2);
    }
    {
      const auto crt_row = rows.getNextRow(true, true);
      CHECK(!crt_row.empty());
      CHECK_EQ(size_t(3), crt_row.size());
      const auto sv0 = v<int64_t>(crt_row[0]);
      ASSERT_EQ(int64_t(936144000), sv0);
      const auto sv1 = boost::get<std::string>(v<NullableString>(crt_row[1]));
      ASSERT_EQ("baz", sv1);
      const auto sv2 = v<int64_t>(crt_row[2]);
      ASSERT_EQ(static_cast<int64_t>(g_num_rows) / 2, sv2);
    }
    const auto crt_row = rows.getNextRow(true, true);
    CHECK(crt_row.empty());
  }

  static void check_one_date_trunc_group(const ResultSet& rows, const int64_t ref_ts) {
    const auto crt_row = rows.getNextRow(true, true);
    ASSERT_EQ(size_t(1), crt_row.size());
    const auto actual_ts = v<int64_t>(crt_row[0]);
    ASSERT_EQ(ref_ts, actual_ts);
    const auto empty_row = rows.getNextRow(true, true);
    ASSERT_TRUE(empty_row.empty());
  }

  static void check_one_date_trunc_group_with_agg(const ResultSet& rows,
                                                  const int64_t ref_ts,
                                                  const int64_t ref_agg) {
    const auto crt_row = rows.getNextRow(true, true);
    ASSERT_EQ(size_t(2), crt_row.size());
    const auto actual_ts = v<int64_t>(crt_row[0]);
    ASSERT_EQ(ref_ts, actual_ts);
    const auto actual_agg = v<int64_t>(crt_row[1]);
    ASSERT_EQ(ref_agg, actual_agg);
    const auto empty_row = rows.getNextRow(true, true);
    ASSERT_TRUE(empty_row.empty());
  }

  // Example: "1969-12-31 23:59:59.999999" -> -1
  // The number of fractional digits must be 0, 3, 6, or 9.
  static int64_t timestampToInt64(char const* timestr, ExecutorDeviceType const dt) {
    constexpr int max = 128;
    char query[max];
    unsigned const dim = strlen(timestr) == 19 ? 0 : strlen(timestr) - 20;
    int const n = snprintf(query, max, "SELECT TIMESTAMP(%d) '%s';", dim, timestr);
    CHECK_LT(0, n);
    CHECK_LT(n, max);
    return v<int64_t>(run_simple_agg(query, dt));
  }

  static int64_t dateadd(char const* unit,
                         int const num,
                         char const* timestr,
                         ExecutorDeviceType const dt) {
    constexpr int max = 128;
    char query[max];
    unsigned const dim = strlen(timestr) == 19 ? 0 : strlen(timestr) - 20;
    int const n =
        snprintf(query,
                 max,
                 // Cast from TIMESTAMP(6) to TEXT not supported
                 // "SELECT CAST(DATEADD('%s', %d, TIMESTAMP(%d) '%s') AS TEXT);",
                 "SELECT DATEADD('%s', %d, TIMESTAMP(%d) '%s');",
                 unit,
                 num,
                 dim,
                 timestr);
    CHECK_LT(0, n);
    CHECK_LT(n, max);
    return v<int64_t>(run_simple_agg(query, dt));
  }

  static int64_t datediff(char const* unit,
                          char const* start,
                          char const* end,
                          ExecutorDeviceType const dt) {
    constexpr int max = 128;
    char query[max];
    unsigned const dim_start = strlen(start) == 19 ? 0 : strlen(start) - 20;
    unsigned const dim_end = strlen(end) == 19 ? 0 : strlen(end) - 20;
    int const n =
        snprintf(query,
                 max,
                 "SELECT DATEDIFF('%s', TIMESTAMP(%d) '%s', TIMESTAMP(%d) '%s');",
                 unit,
                 dim_start,
                 start,
                 dim_end,
                 end);
    CHECK_LT(0, n);
    CHECK_LT(n, max);
    return v<int64_t>(run_simple_agg(query, dt));
  }

  static std::string date_trunc(std::string const& unit,
                                char const* ts,
                                ExecutorDeviceType dt) {
    std::string const query =
        "SELECT CAST(DATE_TRUNC('" + unit + "', TIMESTAMP '" + ts + "') AS TEXT);";
    return boost::get<std::string>(v<NullableString>(run_simple_agg(query, dt)));
  }
};

bool skip_tests(const ExecutorDeviceType device_type) {
#ifdef HAVE_CUDA
  return device_type == ExecutorDeviceType::GPU && !gpusPresent();
#else
  return device_type == ExecutorDeviceType::GPU;
#endif
}

#define SKIP_NO_GPU()                                        \
  if (skip_tests(dt)) {                                      \
    CHECK(dt == ExecutorDeviceType::GPU);                    \
    LOG(WARNING) << "GPU not available, skipping GPU tests"; \
    continue;                                                \
  }

class Distributed50 : public ExecuteTestBase, public ::testing::Test {};

TEST_F(Distributed50, FailOver) {
  createTable("dist5", {{"col1", dictType()}});

  auto dt = ExecutorDeviceType::CPU;

  EXPECT_NO_THROW(insertCsvValues("dist5", "t1"));
  ASSERT_EQ(1, v<int64_t>(run_simple_agg("SELECT count(*) FROM dist5;", dt)));

  EXPECT_NO_THROW(insertCsvValues("dist5", "t2"));
  ASSERT_EQ(2, v<int64_t>(run_simple_agg("SELECT count(*) FROM dist5;", dt)));

  EXPECT_NO_THROW(insertCsvValues("dist5", "t3"));
  ASSERT_EQ(3, v<int64_t>(run_simple_agg("SELECT count(*) FROM dist5;", dt)));

  dropTable("dist5");
}

class Errors : public ExecuteTestBase, public ::testing::Test {};

TEST_F(Errors, InvalidQueries) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_ANY_THROW(run_multiple_agg(
        "SELECT * FROM test WHERE 1 = 2 AND ( 1 = 2 and 3 = 4 limit 100);", dt));
    EXPECT_ANY_THROW(run_multiple_agg("SET x = y;", dt));
  }
}

class Insert : public ExecuteTestBase, public ::testing::Test {};

TEST_F(Insert, NullArrayNullEmpty) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    createTable("table_array_empty", {{"val", arrayType(kINT)}});
    EXPECT_NO_THROW(insertJsonValues("table_array_empty", "{\"val\": []}"));
    EXPECT_NO_THROW(run_simple_agg("SELECT * from table_array_empty;", dt));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT CARDINALITY(val) from table_array_empty limit 1;", dt)));
    dropTable("table_array_empty");

    createTable("table_array_fixlen_text", {{"strings", arrayType(kTEXT, 2)}});
    EXPECT_NO_THROW(insertJsonValues("table_array_fixlen_text",
                                     R"___({"strings": null}
{"strings": []}
{"strings": [null, null]}
{"strings": ["a", "b"]})___"));
    ASSERT_EQ(
        4,
        v<int64_t>(run_simple_agg("SELECT count(*) FROM table_array_fixlen_text;", dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM table_array_fixlen_text WHERE strings IS NULL;", dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM table_array_fixlen_text WHERE strings IS NOT NULL;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM table_array_fixlen_text WHERE strings[1] IS NOT NULL;",
            dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM table_array_fixlen_text WHERE strings[2] IS NULL;",
            dt)));
    dropTable("table_array_fixlen_text");

    createTable("table_array_with_nulls",
                {{"i", SQLTypeInfo(kSMALLINT)},
                 {"sia", arrayType(kSMALLINT)},
                 {"fa2", arrayType(kFLOAT, 2)}});

    EXPECT_NO_THROW(insertJsonValues("table_array_with_nulls",
                                     R"___({"i": 1, "sia": [1, 1], "fa2": [1.0, 1.0]}
{"i": 2, "sia": [null, 2], "fa2": [null, 2.0]}
{"i": 3, "sia": [3, null], "fa2": [3.0, null]}
{"i": 4, "sia": [null, null], "fa2": [null, null]}
{"i": 5, "sia": null, "fa2": null}
{"i": 6, "sia": [], "fa2": null}
{"i": 7, "sia": [null, null], "fa2": [null, null]})___"));

    ASSERT_EQ(1,
              v<int64_t>(
                  run_simple_agg("SELECT MIN(sia[1]) FROM table_array_with_nulls;", dt)));
    ASSERT_EQ(3,
              v<int64_t>(
                  run_simple_agg("SELECT MAX(sia[1]) FROM table_array_with_nulls;", dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM table_array_with_nulls WHERE sia[2] IS NULL;", dt)));
    ASSERT_EQ(
        3.0,
        v<float>(run_simple_agg("SELECT MAX(fa2[1]) FROM table_array_with_nulls;", dt)));
    ASSERT_EQ(
        2.0,
        v<float>(run_simple_agg("SELECT MAX(fa2[2]) FROM table_array_with_nulls;", dt)));
    ASSERT_EQ(2,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM table_array_with_nulls WHERE fa2[1] IS NOT NULL;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM table_array_with_nulls WHERE sia IS NULL;", dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM table_array_with_nulls WHERE fa2 IS NOT NULL;", dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM table_array_with_nulls WHERE CARDINALITY(sia)=0;",
                  dt)));
    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM table_array_with_nulls WHERE CARDINALITY(sia)=2;",
                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM table_array_with_nulls WHERE CARDINALITY(sia) IS NULL;",
            dt)));

    // Simple lazy projection
    compare_array(
        run_simple_agg("SELECT sia FROM table_array_with_nulls WHERE i = 5;", dt),
        std::vector<int64_t>({}));

    // Simple non-lazy projection
    compare_array(
        run_simple_agg("SELECT sia FROM table_array_with_nulls WHERE sia IS NULL;", dt),
        std::vector<int64_t>({}));

    dropTable("table_array_with_nulls");
  }
}

TEST_F(Insert, IntArrayInsert) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    createTable("table_int_array", {{"bi", arrayType(kBIGINT)}});

    vector<std::string> vals = {"1", "33000", "650000", "1", "-7", "null", "5000000000"};
    string json;
    for (size_t ol = 0; ol < vals.size(); ol++) {
      string arr = "";
      for (size_t il = 0; il < vals.size(); il++) {
        size_t pos = (ol + il) % vals.size();
        arr.append(vals[pos]);
        if (il < (vals.size() - 1)) {
          arr.append(",");
        }
      }
      json += "{\"bi\": [" + arr + "]}\n";
    }
    EXPECT_NO_THROW(insertJsonValues("table_int_array", json));

    EXPECT_ANY_THROW(insertJsonValues("table_int_array", "{\"bi:\": [1,34,\"roof\"]}"));

    for (size_t ol = 0; ol < vals.size(); ol++) {
      string selString =
          "select sum(bi[" + std::to_string(ol + 1) + "]) from table_int_array;";
      ASSERT_EQ(5000682995, v<int64_t>(run_simple_agg(selString, dt)));
    }

    dropTable("table_int_array");
  }
}

TEST_F(Insert, DictBoundary) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    createTable("table_with_small_dict", {{"i", SQLTypeInfo(kINT)}, {"t", dictType(1)}});

    string csv;
    for (int cVal = 0; cVal < 280; cVal++) {
      csv += std::to_string(cVal) + ", \"" + std::to_string(cVal) + "\"\n";
    }
    EXPECT_NO_THROW(insertCsvValues("table_with_small_dict", csv));

    ASSERT_EQ(
        280,
        v<int64_t>(run_simple_agg("SELECT count(*) FROM table_with_small_dict;", dt)));
    ASSERT_EQ(255,
              v<int64_t>(run_simple_agg(
                  "SELECT count(distinct t) FROM table_with_small_dict;", dt)));
    ASSERT_EQ(25,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM table_with_small_dict WHERE t IS NULL;", dt)));

    dropTable("table_with_small_dict");
  }
}

class KeyForString : public ExecuteTestBase, public ::testing::Test {};

TEST_F(KeyForString, KeyForString) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_NO_THROW(createTable("kfs",
                                {{"ts", dictType(1)},
                                 {"ss", dictType(2)},
                                 {"ws", dictType(4)},
                                 {"ns", dictType(4, true)},
                                 {"sa", arrayType(kTEXT)}}));
    insertJsonValues("kfs",
                     R"___({"ts": "0", "ss": "0", "ws": "0", "ns": "0", "sa": ["0", "0"]}
{"ts": "1", "ss": "1", "ws": "1", "ns": "1", "sa": ["1", "1"]}
{"ts": null, "ss": null, "ws": null, "ns": "2", "sa": ["2", "2"]})___");

    ASSERT_EQ(3, v<int64_t>(run_simple_agg("select count(*) from kfs;", dt)));
    ASSERT_EQ(2,
              v<int64_t>(run_simple_agg(
                  "select count(*) from kfs where key_for_string(ts) is not null;", dt)));
    ASSERT_EQ(2,
              v<int64_t>(run_simple_agg(
                  "select count(*) from kfs where key_for_string(ss) is not null;", dt)));
    ASSERT_EQ(2,
              v<int64_t>(run_simple_agg(
                  "select count(*) from kfs where key_for_string(ws) is not null;", dt)));
    ASSERT_EQ(3,
              v<int64_t>(run_simple_agg(
                  "select count(*) from kfs where key_for_string(ns) is not null;", dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg(
            "select count(*) from kfs where key_for_string(sa[1]) is not null;", dt)));
    ASSERT_EQ(
        2,
        v<int64_t>(run_simple_agg(
            "select count(*) from kfs where key_for_string(ts) = key_for_string(ss);",
            dt)));
    ASSERT_EQ(
        2,
        v<int64_t>(run_simple_agg(
            "select count(*) from kfs where key_for_string(ss) = key_for_string(ws);",
            dt)));
    ASSERT_EQ(
        2,
        v<int64_t>(run_simple_agg(
            "select count(*) from kfs where key_for_string(ws) = key_for_string(ts);",
            dt)));
    ASSERT_EQ(
        2,
        v<int64_t>(run_simple_agg(
            "select count(*) from kfs where key_for_string(ws) = key_for_string(ns);",
            dt)));
    ASSERT_EQ(
        2,
        v<int64_t>(run_simple_agg(
            "select count(*) from kfs where key_for_string(ws) = key_for_string(sa[1]);",
            dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("select min(key_for_string(ts)) from kfs;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("select min(key_for_string(ss)) from kfs;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("select min(key_for_string(ws)) from kfs;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("select min(key_for_string(ns)) from kfs;", dt)));
    ASSERT_EQ(
        0, v<int64_t>(run_simple_agg("select min(key_for_string(sa[1])) from kfs;", dt)));
    ASSERT_EQ(
        0, v<int64_t>(run_simple_agg("select min(key_for_string(sa[2])) from kfs;", dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("select max(key_for_string(ts)) from kfs;", dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("select max(key_for_string(ss)) from kfs;", dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("select max(key_for_string(ws)) from kfs;", dt)));
    ASSERT_EQ(2,
              v<int64_t>(run_simple_agg("select max(key_for_string(ns)) from kfs;", dt)));
    ASSERT_EQ(
        2, v<int64_t>(run_simple_agg("select max(key_for_string(sa[1])) from kfs;", dt)));
    ASSERT_EQ(
        2, v<int64_t>(run_simple_agg("select max(key_for_string(sa[2])) from kfs;", dt)));
    ASSERT_EQ(
        2, v<int64_t>(run_simple_agg("select count(key_for_string(ts)) from kfs;", dt)));
    ASSERT_EQ(
        2, v<int64_t>(run_simple_agg("select count(key_for_string(ss)) from kfs;", dt)));
    ASSERT_EQ(
        2, v<int64_t>(run_simple_agg("select count(key_for_string(ws)) from kfs;", dt)));
    ASSERT_EQ(
        3, v<int64_t>(run_simple_agg("select count(key_for_string(ns)) from kfs;", dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg("select count(key_for_string(sa[1])) from kfs;", dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg("select count(key_for_string(sa[2])) from kfs;", dt)));

    EXPECT_NO_THROW(dropTable("kfs"));
  }
}

class Select : public ExecuteTestBase, public ::testing::Test {};

TEST_F(Select, NullWithAndOr) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    createTable("table_bool_test",
                {{"id", SQLTypeInfo(kINT)}, {"val", SQLTypeInfo(kBOOLEAN)}});
    insertCsvValues("table_bool_test", "1,true\n2,false\n3,");

    auto BOOLEAN_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kBOOLEAN, false));

    ASSERT_EQ(
        BOOLEAN_NULL_SENTINEL,
        v<int64_t>(run_simple_agg(
            "SELECT CAST(NULL AS BOOLEAN) AND val from table_bool_test WHERE id = 1;",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT CAST(NULL AS BOOLEAN) AND val from table_bool_test WHERE id = 2;",
            dt)));
    ASSERT_EQ(
        BOOLEAN_NULL_SENTINEL,
        v<int64_t>(run_simple_agg(
            "SELECT CAST(NULL AS BOOLEAN) AND val from table_bool_test WHERE id = 3;",
            dt)));
    ASSERT_EQ(
        BOOLEAN_NULL_SENTINEL,
        v<int64_t>(run_simple_agg(
            "SELECT val AND CAST(NULL AS BOOLEAN) from table_bool_test WHERE id = 1;",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT val AND CAST(NULL AS BOOLEAN) from table_bool_test WHERE id = 2;",
            dt)));
    ASSERT_EQ(
        BOOLEAN_NULL_SENTINEL,
        v<int64_t>(run_simple_agg(
            "SELECT val AND CAST(NULL AS BOOLEAN) from table_bool_test WHERE id = 3;",
            dt)));

    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT CAST(NULL AS BOOLEAN) OR val from table_bool_test WHERE id = 1;",
            dt)));
    ASSERT_EQ(
        BOOLEAN_NULL_SENTINEL,
        v<int64_t>(run_simple_agg(
            "SELECT CAST(NULL AS BOOLEAN) OR val from table_bool_test WHERE id = 2;",
            dt)));
    ASSERT_EQ(
        BOOLEAN_NULL_SENTINEL,
        v<int64_t>(run_simple_agg(
            "SELECT CAST(NULL AS BOOLEAN) OR val from table_bool_test WHERE id = 3;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT val OR CAST(NULL AS BOOLEAN) from table_bool_test WHERE id = 1;",
            dt)));
    ASSERT_EQ(
        BOOLEAN_NULL_SENTINEL,
        v<int64_t>(run_simple_agg(
            "SELECT val OR CAST(NULL AS BOOLEAN) from table_bool_test WHERE id = 2;",
            dt)));
    ASSERT_EQ(
        BOOLEAN_NULL_SENTINEL,
        v<int64_t>(run_simple_agg(
            "SELECT val OR CAST(NULL AS BOOLEAN) from table_bool_test WHERE id = 3;",
            dt)));

    dropTable("table_bool_test");
  }
}

TEST_F(Select, NullGroupBy) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    createTable("table_null_group_by", {{"val", dictType()}});
    insertJsonValues("table_null_group_by", "{\"val\": null}");
    run_simple_agg("SELECT val FROM table_null_group_by GROUP BY val;", dt);
    dropTable("table_null_group_by");

    createTable("table_null_group_by", {{"val", SQLTypeInfo(kDOUBLE)}});
    insertJsonValues("table_null_group_by", "{\"val\": null}");
    run_simple_agg("SELECT val FROM table_null_group_by GROUP BY val;", dt);
    dropTable("table_null_group_by");
  }
}

TEST_F(Select, FilterAndSimpleAggregation) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test;", dt);
    c("SELECT COUNT(f) FROM test;", dt);
    c("SELECT COUNT(smallint_nulls), COUNT(*), COUNT(fn) FROM test;", dt);
    c("SELECT MIN(x) FROM test;", dt);
    c("SELECT MAX(x) FROM test;", dt);
    c("SELECT MIN(z) FROM test;", dt);
    c("SELECT MAX(z) FROM test;", dt);
    c("SELECT MIN(t) FROM test;", dt);
    c("SELECT MAX(t) FROM test;", dt);
    c("SELECT MIN(ff) FROM test;", dt);
    c("SELECT MIN(fn) FROM test;", dt);
    c("SELECT SUM(ff) FROM test;", dt);
    c("SELECT SUM(fn) FROM test;", dt);
    c("SELECT SUM(x + y) FROM test;", dt);
    c("SELECT SUM(x + y + z) FROM test;", dt);
    c("SELECT SUM(x + y + z + t) FROM test;", dt);
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8;", dt);
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8 AND z > 100 AND z < 102;", dt);
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8 OR (z > 100 AND z < 103);", dt);
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8 AND z > 100 AND z < 102 AND t > "
      "1000 AND t < 1002;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8 OR (z > 100 AND z < 103);", dt);
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8 OR (z > 100 AND z < 102) OR (t > "
      "1000 AND t < 1003);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x <> 7;", dt);
    c("SELECT COUNT(*) FROM test WHERE z <> 102;", dt);
    c("SELECT COUNT(*) FROM test WHERE t <> 1002;", dt);
    c("SELECT COUNT(*) FROM test WHERE x + y = 49;", dt);
    c("SELECT COUNT(*) FROM test WHERE x + y + z = 150;", dt);
    c("SELECT COUNT(*) FROM test WHERE x + y + z + t = 1151;", dt);
    c("SELECT COUNT(*) FROM test WHERE CAST(x as TINYINT) + CAST(y as TINYINT) < CAST(z "
      "as TINYINT);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE CAST(y as TINYINT) / CAST(x as TINYINT) = 6", dt);
    c("SELECT SUM(x + y) FROM test WHERE x + y = 49;", dt);
    c("SELECT SUM(x + y + z) FROM test WHERE x + y = 49;", dt);
    c("SELECT SUM(x + y + z + t) FROM test WHERE x + y = 49;", dt);
    c("SELECT COUNT(*) FROM test WHERE x - y = -35;", dt);
    c("SELECT COUNT(*) FROM test WHERE x - y + z = 66;", dt);
    c("SELECT COUNT(*) FROM test WHERE x - y + z + t = 1067;", dt);
    c("SELECT COUNT(*) FROM test WHERE y - x = 35;", dt);
    c("SELECT 'Hello', 'World', 7 FROM test WHERE x <> 7;", dt);
    c("SELECT 'Total', COUNT(*) FROM test WHERE x <> 7;", dt);
    c("SELECT SUM(dd * x) FROM test;", dt);
    c("SELECT SUM(dd * y) FROM test;", dt);
    c("SELECT SUM(dd * w) FROM test;", dt);
    c("SELECT SUM(dd * z) FROM test;", dt);
    c("SELECT SUM(dd * t) FROM test;", dt);
    c("SELECT SUM(x * dd) FROM test;", dt);
    c("SELECT SUM(y * dd) FROM test;", dt);
    c("SELECT SUM(w * dd) FROM test;", dt);
    c("SELECT SUM(z * dd) FROM test;", dt);
    c("SELECT SUM(t * dd) FROM test;", dt);
    c("SELECT SUM(dd * ufd) FROM test;", dt);
    c("SELECT SUM(dd * d) FROM test;", dt);
    c("SELECT SUM(dd * dn) FROM test;", dt);
    c("SELECT SUM(x * dd_notnull) FROM test;", dt);
    c("SELECT SUM(2 * x) FROM test WHERE x = 7;", dt);
    c("SELECT SUM(2 * x + z) FROM test WHERE x = 7;", dt);
    c("SELECT SUM(x + y) FROM test WHERE x - y = -35;", dt);
    c("SELECT SUM(x + y) FROM test WHERE y - x = 35;", dt);
    c("SELECT SUM(x + y - z) FROM test WHERE y - x = 35;", dt);
    c("SELECT SUM(x * y + 15) FROM test WHERE x + y + 1 = 50;", dt);
    c("SELECT SUM(x * y + 15) FROM test WHERE x + y + z + 1 = 151;", dt);
    c("SELECT SUM(x * y + 15) FROM test WHERE x + y + z + t + 1 = 1152;", dt);
    c("SELECT SUM(z) FROM test WHERE z IS NOT NULL;", dt);
    c("SELECT MIN(x * y + 15) FROM test WHERE x + y + 1 = 50;", dt);
    c("SELECT MIN(x * y + 15) FROM test WHERE x + y + z + 1 = 151;", dt);
    c("SELECT MIN(x * y + 15) FROM test WHERE x + y + z + t + 1 = 1152;", dt);
    c("SELECT MAX(x * y + 15) FROM test WHERE x + y + 1 = 50;", dt);
    c("SELECT MAX(x * y + 15) FROM test WHERE x + y + z + 1 = 151;", dt);
    c("SELECT MAX(x * y + 15) FROM test WHERE x + y + z + t + 1 = 1152;", dt);
    c("SELECT MIN(x) FROM test WHERE x = 7;", dt);
    c("SELECT MIN(z) FROM test WHERE z = 101;", dt);
    c("SELECT MIN(t) FROM test WHERE t = 1001;", dt);
    c("SELECT AVG(x + y) FROM test;", dt);
    c("SELECT AVG(x + y + z) FROM test;", dt);
    c("SELECT AVG(x + y + z + t) FROM test;", dt);
    c("SELECT AVG(y) FROM test WHERE x > 6 AND x < 8;", dt);
    c("SELECT AVG(y) FROM test WHERE z > 100 AND z < 102;", dt);
    c("SELECT AVG(y) FROM test WHERE t > 1000 AND t < 1002;", dt);
    c("SELECT MIN(dd) FROM test;", dt);
    c("SELECT MAX(dd) FROM test;", dt);
    c("SELECT SUM(dd) FROM test;", dt);
    c("SELECT AVG(dd) FROM test;", dt);
    c("SELECT AVG(dd) FROM test WHERE x > 6 AND x < 8;", dt);
    c("SELECT COUNT(*) FROM test WHERE dd > 100;", dt);
    c("SELECT COUNT(*) FROM test WHERE dd > 200;", dt);
    c("SELECT COUNT(*) FROM test WHERE dd > 300;", dt);
    c("SELECT COUNT(*) FROM test WHERE dd > 111.0;", dt);
    c("SELECT COUNT(*) FROM test WHERE dd > 111.1;", dt);
    c("SELECT COUNT(*) FROM test WHERE dd > 222.2;", dt);
    c("SELECT MAX(x + dd) FROM test;", dt);
    c("SELECT MAX(x + 2 * dd), MIN(x + 2 * dd) FROM test;", dt);
    c("SELECT COUNT(*) FROM test WHERE dd > CAST(111.0 AS decimal(10, 2));", dt);
    c("SELECT COUNT(*) FROM test WHERE dd > CAST(222.0 AS decimal(10, 2));", dt);
    c("SELECT COUNT(*) FROM test WHERE dd > CAST(333.0 AS decimal(10, 2));", dt);
    c("SELECT MIN(dd * dd) FROM test;", dt);
    c("SELECT MAX(dd * dd) FROM test;", dt);
    c("SELECT COUNT(*) FROM test WHERE u IS NOT NULL;", dt);
    c("SELECT AVG(u * f) FROM test;", dt);
    c("SELECT AVG(u * d) FROM test;", dt);
    c("SELECT SUM(-y) FROM test;", dt);
    c("SELECT SUM(-z) FROM test;", dt);
    c("SELECT SUM(-t) FROM test;", dt);
    c("SELECT SUM(-dd) FROM test;", dt);
    c("SELECT SUM(-f) FROM test;", dt);
    c("SELECT SUM(-d) FROM test;", dt);
    c("SELECT SUM(dd * 0.99) FROM test;", dt);
    c("SELECT COUNT(*) FROM test WHERE 1<>2;", dt);
    c("SELECT COUNT(*) FROM test WHERE 1=1;", dt);
    c("SELECT COUNT(*) FROM test WHERE 22 > 33;", dt);
    c("SELECT COUNT(*) FROM test WHERE ff < 23.0/4.0 AND 22 < 33;", dt);
    c("SELECT COUNT(*) FROM test WHERE x + 3*8/2 < 35 + y - 20/5;", dt);
    c("SELECT x + 2 * 10/4 + 3 AS expr FROM test WHERE x + 3*8/2 < 35 + y - 20/5 ORDER "
      "BY expr ASC;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE ff + 3.0*8 < 20.0/5;", dt);
    c("SELECT COUNT(*) FROM test WHERE x < y AND 0=1;", dt);
    c("SELECT COUNT(*) FROM test WHERE x < y AND 1=1;", dt);
    c("SELECT COUNT(*) FROM test WHERE x < y OR 1<1;", dt);
    c("SELECT COUNT(*) FROM test WHERE x < y OR 1=1;", dt);
    c("SELECT COUNT(*) FROM test WHERE x < 35 AND x < y AND 1=1 AND 0=1;", dt);
    c("SELECT COUNT(*) FROM test WHERE 1>2 AND x < 35 AND x < y AND y < 10;", dt);
    c("SELECT COUNT(*) FROM test WHERE x < y GROUP BY x HAVING 0=1;", dt);
    c("SELECT COUNT(*) FROM test WHERE x < y GROUP BY x HAVING 1=1;", dt);
    c("SELECT COUNT(*) FROM test WHERE ofq >= 0 OR ofq IS NULL;", dt);
    c("SELECT COUNT(*) AS val FROM test WHERE (test.dd = 0.5 OR test.dd = 3);", dt);
    c("SELECT MAX(dd_notnull * 1) FROM test;", dt);
    c("SELECT x, COUNT(*) AS n FROM test GROUP BY x, ufd ORDER BY x, n;", dt);
    c("SELECT MIN(x), MAX(x) FROM test WHERE real_str LIKE '%nope%';", dt);
    c("SELECT COUNT(*) FROM test WHERE (x > 7 AND y / (x - 7) < 44);", dt);
    c("SELECT x, AVG(ff) AS val FROM test GROUP BY x ORDER BY val;", dt);
    c("SELECT x, MAX(fn) as val FROM test WHERE fn IS NOT NULL GROUP BY x ORDER BY val;",
      dt);
    c("SELECT MAX(dn) FROM test WHERE dn IS NOT NULL;", dt);
    c("SELECT x, MAX(dn) as val FROM test WHERE dn IS NOT NULL GROUP BY x ORDER BY val;",
      dt);
    c("SELECT COUNT(*) as val FROM test GROUP BY x, y, ufd ORDER BY val;", dt);
    ASSERT_NEAR(
        static_cast<double>(-1000.3),
        v<double>(run_simple_agg(
            "SELECT AVG(fn) AS val FROM test GROUP BY rowid ORDER BY val LIMIT 1;", dt)),
        static_cast<double>(0.2));
    c("SELECT COUNT(*) FROM test WHERE d = 2.2", dt);
    c("SELECT COUNT(*) FROM test WHERE fx + 1 IS NULL;", dt);
    c("SELECT COUNT(ss) FROM test;", dt);
    c("SELECT COUNT(*) FROM test WHERE null IS NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE null_str IS NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE null IS NOT NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE o1 > '1999-09-08';", dt);
    c("SELECT COUNT(*) FROM test WHERE o1 <= '1999-09-08';", dt);
    c("SELECT COUNT(*) FROM test WHERE o1 = '1999-09-08';", dt);
    c("SELECT COUNT(*) FROM test WHERE o1 <> '1999-09-08';", dt);
    c("SELECT COUNT(*) FROM test WHERE o >= CAST('1999-09-09' AS DATE);", dt);
    c("SELECT COUNT(*) FROM test WHERE o2 > '1999-09-08';", dt);
    c("SELECT COUNT(*) FROM test WHERE o2 <= '1999-09-08';", dt);
    c("SELECT COUNT(*) FROM test WHERE o2 = '1999-09-08';", dt);
    c("SELECT COUNT(*) FROM test WHERE o2 <> '1999-09-08';", dt);
    c("SELECT COUNT(*) FROM test WHERE o1 = o2;", dt);
    c("SELECT COUNT(*) FROM test WHERE o1 <> o2;", dt);
    c("SELECT COUNT(*) FROM test WHERE b = 'f';", dt);
    c("SELECT COUNT(*) FROM test WHERE bn = 'f';", dt);
    c("SELECT COUNT(*) FROM test WHERE b = null;", dt);
    c("SELECT COUNT(*) FROM test WHERE bn = null;", dt);
    c("SELECT COUNT(*) FROM test WHERE bn = b;", dt);
    ASSERT_EQ(19,
              v<int64_t>(run_simple_agg("SELECT rowid FROM test WHERE rowid = 19;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg("SELECT MAX(rowid) - MIN(rowid) + 1 FROM test;", dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE MOD(x, 7) = 0;", dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE MOD(x, 7) = 7;", dt)));
    ASSERT_EQ(5,
              v<int64_t>(
                  run_simple_agg("SELECT COUNT(*) FROM test WHERE MOD(x, 7) <> 0;", dt)));
    ASSERT_EQ(20,
              v<int64_t>(
                  run_simple_agg("SELECT COUNT(*) FROM test WHERE MOD(x, 7) <> 7;", dt)));
    c("SELECT MIN(x) FROM test WHERE x <> 7 AND x <> 8;", dt);
    c("SELECT MIN(x) FROM test WHERE z <> 101 AND z <> 102;", dt);
    c("SELECT MIN(x) FROM test WHERE t <> 1001 AND t <> 1002;", dt);
    ASSERT_NEAR(static_cast<double>(0.5),
                v<double>(run_simple_agg("SELECT STDDEV_POP(x) FROM test;", dt)),
                static_cast<double>(0.2));
    ASSERT_NEAR(static_cast<double>(0.5),
                v<double>(run_simple_agg("SELECT STDDEV_SAMP(x) FROM test;", dt)),
                static_cast<double>(0.2));
    ASSERT_NEAR(static_cast<double>(0.2),
                v<double>(run_simple_agg("SELECT VAR_POP(x) FROM test;", dt)),
                static_cast<double>(0.1));
    ASSERT_NEAR(static_cast<double>(0.2),
                v<double>(run_simple_agg("SELECT VAR_SAMP(x) FROM test;", dt)),
                static_cast<double>(0.1));
    ASSERT_NEAR(static_cast<double>(92.0),
                v<double>(run_simple_agg("SELECT STDDEV_POP(dd) FROM test;", dt)),
                static_cast<double>(2.0));
    ASSERT_NEAR(static_cast<double>(94.5),
                v<double>(run_simple_agg("SELECT STDDEV_SAMP(dd) FROM test;", dt)),
                static_cast<double>(1.0));
    ASSERT_NEAR(
        static_cast<double>(94.5),
        v<double>(run_simple_agg("SELECT POWER(((SUM(dd * dd) - SUM(dd) * SUM(dd) / "
                                 "COUNT(dd)) / (COUNT(dd) - 1)), 0.5) FROM test;",
                                 dt)),
        static_cast<double>(1.0));
    ASSERT_NEAR(static_cast<double>(8485.0),
                v<double>(run_simple_agg("SELECT VAR_POP(dd) FROM test;", dt)),
                static_cast<double>(10.0));
    ASSERT_NEAR(static_cast<double>(8932.0),
                v<double>(run_simple_agg("SELECT VAR_SAMP(dd) FROM test;", dt)),
                static_cast<double>(10.0));
    ASSERT_EQ(20,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test HAVING STDDEV_POP(x) < 1.0;", dt)));
    ASSERT_EQ(20,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test HAVING STDDEV_POP(x) * 5 < 3.0;", dt)));
    ASSERT_NEAR(
        static_cast<double>(0.65),
        v<double>(run_simple_agg("SELECT stddev(x) + VARIANCE(x) FROM test;", dt)),
        static_cast<double>(0.10));
    ASSERT_NEAR(static_cast<float>(0.5),
                v<float>(run_simple_agg("SELECT STDDEV_POP_FLOAT(x) FROM test;", dt)),
                static_cast<float>(0.2));
    ASSERT_NEAR(static_cast<float>(0.5),
                v<float>(run_simple_agg("SELECT STDDEV_SAMP_FLOAT(x) FROM test;", dt)),
                static_cast<float>(0.2));
    ASSERT_NEAR(static_cast<float>(0.2),
                v<float>(run_simple_agg("SELECT VAR_POP_FLOAT(x) FROM test;", dt)),
                static_cast<float>(0.1));
    ASSERT_NEAR(static_cast<float>(0.2),
                v<float>(run_simple_agg("SELECT VAR_SAMP_FLOAT(x) FROM test;", dt)),
                static_cast<float>(0.1));
    ASSERT_NEAR(static_cast<float>(92.0),
                v<float>(run_simple_agg("SELECT STDDEV_POP_FLOAT(dd) FROM test;", dt)),
                static_cast<float>(2.0));
    ASSERT_NEAR(static_cast<float>(94.5),
                v<float>(run_simple_agg("SELECT STDDEV_SAMP_FLOAT(dd) FROM test;", dt)),
                static_cast<float>(1.0));
    ASSERT_NEAR(
        static_cast<double>(94.5),
        v<double>(run_simple_agg("SELECT POWER(((SUM(dd * dd) - SUM(dd) * SUM(dd) / "
                                 "COUNT(dd)) / (COUNT(dd) - 1)), 0.5) FROM test;",
                                 dt)),
        static_cast<double>(1.0));
    ASSERT_NEAR(static_cast<float>(8485.0),
                v<float>(run_simple_agg("SELECT VAR_POP_FLOAT(dd) FROM test;", dt)),
                static_cast<float>(10.0));
    ASSERT_NEAR(static_cast<float>(8932.0),
                v<float>(run_simple_agg("SELECT VAR_SAMP_FLOAT(dd) FROM test;", dt)),
                static_cast<float>(10.0));
    ASSERT_EQ(20,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test HAVING STDDEV_POP_FLOAT(x) < 1.0;", dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test HAVING STDDEV_POP_FLOAT(x) * 5 < 3.0;", dt)));
    ASSERT_NEAR(static_cast<float>(0.65),
                v<float>(run_simple_agg(
                    "SELECT stddev_FLOAT(x) + VARIANCE_float(x) FROM test;", dt)),
                static_cast<float>(0.10));
    ASSERT_NEAR(static_cast<double>(0.125),
                v<double>(run_simple_agg("SELECT COVAR_POP(x, y) FROM test;", dt)),
                static_cast<double>(0.001));
    ASSERT_NEAR(static_cast<float>(0.125),
                v<float>(run_simple_agg("SELECT COVAR_POP_FLOAT(x, y) FROM test;", dt)),
                static_cast<float>(0.001));
    ASSERT_NEAR(
        static_cast<double>(0.125),  // covar_pop expansion
        v<double>(run_simple_agg("SELECT avg(x * y) - avg(x) * avg(y) FROM test;", dt)),
        static_cast<double>(0.001));
    ASSERT_NEAR(static_cast<double>(0.131),
                v<double>(run_simple_agg("SELECT COVAR_SAMP(x, y) FROM test;", dt)),
                static_cast<double>(0.001));
    ASSERT_NEAR(static_cast<double>(0.131),
                v<double>(run_simple_agg("SELECT COVAR_SAMP_FLOAT(x, y) FROM test;", dt)),
                static_cast<double>(0.001));
    ASSERT_NEAR(
        static_cast<double>(0.131),  // covar_samp expansion
        v<double>(run_simple_agg(
            "SELECT ((sum(x * y) - sum(x) * avg(y)) / (count(x) - 1)) FROM test;", dt)),
        static_cast<double>(0.001));
    ASSERT_NEAR(static_cast<double>(0.58),
                v<double>(run_simple_agg("SELECT CORRELATION(x, y) FROM test;", dt)),
                static_cast<double>(0.01));
    ASSERT_NEAR(static_cast<float>(0.58),
                v<float>(run_simple_agg("SELECT CORRELATION_FLOAT(x, y) FROM test;", dt)),
                static_cast<float>(0.01));
    ASSERT_NEAR(static_cast<double>(0.58),
                v<double>(run_simple_agg("SELECT CORR(x, y) FROM test;", dt)),
                static_cast<double>(0.01));
    ASSERT_NEAR(static_cast<float>(0.58),
                v<float>(run_simple_agg("SELECT CORR_FLOAT(x, y) FROM test;", dt)),
                static_cast<float>(0.01));
    ASSERT_NEAR(static_cast<double>(0.33),
                v<double>(run_simple_agg("SELECT POWER(CORR(x, y), 2) FROM test;", dt)),
                static_cast<double>(0.01));
    ASSERT_NEAR(static_cast<double>(0.58),  // corr expansion
                v<double>(run_simple_agg("SELECT (avg(x * y) - avg(x) * avg(y)) /"
                                         "(stddev_pop(x) * stddev_pop(y)) FROM test;",
                                         dt)),
                static_cast<double>(0.01));
  }
}

TEST_F(Select, AggregateOnEmptyDecimalColumn) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    for (int p = 1; p <= 18; ++p) {
      for (int s = 0; s <= p - 1; ++s) {
        std::string tbl_name = "D" + std::to_string(p) + "_" + std::to_string(s);

        createTable(tbl_name, {{"val", SQLTypeInfo(kDECIMAL, p, s)}});
        std::string decimal_prec =
            "val DECIMAL(" + std::to_string(p) + "," + std::to_string(s) + ")";
        run_sqlite_query("DROP TABLE IF EXISTS " + tbl_name + ";");
        run_sqlite_query("CREATE TABLE " + tbl_name + "( " + decimal_prec + ");");

        std::string query =
            "SELECT MIN(val), MAX(val), SUM(val), AVG(val) FROM " + tbl_name + ";";
        c(query, dt);

        run_sqlite_query("DROP TABLE IF EXISTS " + tbl_name + ";");
        dropTable(tbl_name);
      }
    }
  }
}

TEST_F(Select, AggregateConstantValueOnEmptyTable) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // tinyint: -126 / 126
    c("SELECT MIN(-126), MAX(-126), SUM(-126), AVG(-126), MIN(126), MAX(126), SUM(126), "
      "AVG(126) FROM "
      "empty_test_table;",
      dt);
    // smallint: -32766 / 32766
    c("SELECT MIN(-32766), MAX(-32766), SUM(-32766), AVG(-32766), MIN(32766), "
      "MAX(32766), SUM(32766), AVG(32766) "
      "FROM empty_test_table;",
      dt);
    // int: -2147483646 / 2147483646
    c("SELECT MIN(-2147483646), MAX(-2147483646), SUM(-2147483646), AVG(-2147483646), "
      "MIN(2147483646), "
      "MAX(2147483646), SUM(2147483646), AVG(2147483646) FROM empty_test_table;",
      dt);
    // bigint: -9223372036854775806 / 9223372036854775806
    c("SELECT MIN(-9223372036854775806), MAX(-9223372036854775806), "
      "AVG(-9223372036854775806),"
      "SUM(-9223372036854775806), MIN(9223372036854775806), MAX(9223372036854775806), "
      "SUM(9223372036854775806), AVG(9223372036854775806) FROM empty_test_table;",
      dt);
    // float: -1.5 / 1.5
    c("SELECT MIN(-1.5), MAX(-1.5), SUM(-1.5), AVG(-1.5), MIN(1.5), MAX(1.5), SUM(1.5), "
      "AVG(1.5) FROM "
      "empty_test_table;",
      dt);
    // double: -1.5055487897 / 1.5055487897
    c("SELECT MIN(-1.5055487897), MAX(-1.5055487897), SUM(-1.5055487897), "
      "AVG(-1.5055487897),"
      "MIN(1.5055487897), MAX(1.5055487897), SUM(1.5055487897), AVG(1.5055487897) FROM "
      "empty_test_table;",
      dt);
    // boolean: true / false
    c("SELECT MIN(true), MAX(true), MIN(false), MAX(false) FROM empty_test_table;", dt);
  }
}

TEST_F(Select, AggregateOnEmptyTable) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT AVG(x), AVG(y), AVG(z), AVG(t), AVG(f), AVG(d) FROM empty_test_table;", dt);
    c("SELECT MIN(x), MIN(y), MIN(z), MIN(t), MIN(f), MIN(d), MIN(b) FROM "
      "empty_test_table;",
      dt);
    c("SELECT MAX(x), MAX(y), MAX(z), MAX(t), MAX(f), MAX(d), MAX(b)  FROM "
      "empty_test_table;",
      dt);
    c("SELECT SUM(x), SUM(y), SUM(z), SUM(t), SUM(f), SUM(d) FROM empty_test_table;", dt);
    c("SELECT COUNT(x), COUNT(y), COUNT(z), COUNT(t), COUNT(f), COUNT(d), COUNT(b) FROM "
      "empty_test_table;",
      dt);
    // skipped fragment
    c("SELECT AVG(x), AVG(y), AVG(z), AVG(t), AVG(f), AVG(d) FROM empty_test_table "
      "WHERE id > 5;",
      dt);
    c("SELECT MIN(x), MIN(y), MIN(z), MIN(t), MIN(f), MIN(d), MIN(b) FROM "
      "empty_test_table WHERE "
      "id > 5;",
      dt);
    c("SELECT MAX(x), MAX(y), MAX(z), MAX(t), MAX(f), MAX(d), MAX(b) FROM "
      "empty_test_table WHERE "
      "id > 5;",
      dt);
    c("SELECT SUM(x), SUM(y), SUM(z), SUM(t), SUM(f), SUM(d) FROM empty_test_table WHERE "
      "id > 5;",
      dt);
    c("SELECT COUNT(x), COUNT(y), COUNT(z), COUNT(t), COUNT(f), COUNT(d), COUNT(b) FROM "
      "empty_test_table WHERE id > 5;",
      dt);
  }
}

TEST_F(Select, LimitAndOffset) {
  CHECK(g_num_rows >= 4);
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      const auto rows = run_multiple_agg("SELECT * FROM test LIMIT 5;", dt);
      ASSERT_EQ(size_t(5), rows->rowCount());
    }
    {
      const auto rows = run_multiple_agg("SELECT * FROM test LIMIT 5 OFFSET 3;", dt);
      ASSERT_EQ(size_t(5), rows->rowCount());
    }
    {
      const auto rows =
          run_multiple_agg("SELECT * FROM test WHERE x <> 8 LIMIT 3 OFFSET 1;", dt);
      ASSERT_EQ(size_t(3), rows->rowCount());
    }

    c("SELECT str FROM (SELECT str, SUM(y) as total_y FROM test GROUP BY str ORDER BY "
      "total_y DESC, "
      "str LIMIT 1);",
      dt);

    {
      const auto rows = run_multiple_agg("SELECT * FROM test LIMIT 0;", dt);
      ASSERT_EQ(size_t(0), rows->rowCount());
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT str FROM (SELECT str, SUM(y) as total_y FROM test GROUP BY str ORDER "
          "BY total_y DESC, str LIMIT 0);",
          dt);
      ASSERT_EQ(size_t(0), rows->rowCount());
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT * FROM ( SELECT * FROM test_inner LIMIT 3 ) t0 LIMIT 2", dt);
      ASSERT_EQ(size_t(2), rows->rowCount());
    }
  }
}

TEST_F(Select, FloatAndDoubleTests) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT MIN(f) FROM test;", dt);
    c("SELECT MAX(f) FROM test;", dt);
    c("SELECT AVG(f) FROM test;", dt);
    c("SELECT MIN(d) FROM test;", dt);
    c("SELECT MAX(d) FROM test;", dt);
    c("SELECT AVG(d) FROM test;", dt);
    c("SELECT SUM(f) FROM test;", dt);
    c("SELECT SUM(d) FROM test;", dt);
    c("SELECT SUM(f + d) FROM test;", dt);
    c("SELECT AVG(x * f) FROM test;", dt);
    c("SELECT AVG(z - 200) FROM test;", dt);
    c("SELECT SUM(CAST(x AS FLOAT)) FROM test;", dt);
    c("SELECT SUM(CAST(x AS FLOAT)) FROM test GROUP BY z;", dt);
    c("SELECT AVG(CAST(x AS FLOAT)) FROM test;", dt);
    c("SELECT AVG(CAST(x AS FLOAT)) FROM test GROUP BY y;", dt);
    c("SELECT COUNT(*) FROM test WHERE f > 1.0 AND f < 1.2;", dt);
    c("SELECT COUNT(*) FROM test WHERE f > 1.101 AND f < 1.299;", dt);
    c("SELECT COUNT(*) FROM test WHERE f > 1.201 AND f < 1.4;", dt);
    c("SELECT COUNT(*) FROM test WHERE f > 1.0 AND f < 1.2 AND d > 2.0 AND d < 2.4;", dt);
    c("SELECT COUNT(*) FROM test WHERE f > 1.0 AND f < 1.2 OR (d > 2.0 AND d < 3.0);",
      dt);
    c("SELECT SUM(x + y) FROM test WHERE f > 1.0 AND f < 1.2;", dt);
    c("SELECT SUM(x + y) FROM test WHERE d + f > 3.0 AND d + f < 4.0;", dt);
    c("SELECT SUM(f + d) FROM test WHERE x - y = -35;", dt);
    c("SELECT SUM(f + d) FROM test WHERE x + y + 1 = 50;", dt);
    c("SELECT SUM(f * d + 15) FROM test WHERE x + y + 1 = 50;", dt);
    c("SELECT MIN(x), AVG(x * y), MAX(y + 7), AVG(x * f + 15), COUNT(*) FROM test WHERE "
      "x + y > 47 AND x + y < 51;",
      dt);
    c("SELECT AVG(f), MAX(y) AS n FROM test WHERE x = 7 GROUP BY z HAVING AVG(y) > 42.0 "
      "ORDER BY n;",
      dt);
    c("SELECT AVG(f), MAX(y) AS n FROM test WHERE x = 7 GROUP BY z HAVING AVG(f) > 1.09 "
      "ORDER BY n;",
      dt);
    c("SELECT AVG(f), MAX(y) AS n FROM test WHERE x = 7 GROUP BY z HAVING AVG(f) > 1.09 "
      "AND AVG(y) > 42.0 ORDER BY n;",
      dt);
    c("SELECT AVG(d), MAX(y) AS n FROM test WHERE x = 7 GROUP BY z HAVING AVG(d) > 2.2 "
      "AND AVG(y) > 42.0 ORDER BY n;",
      dt);
    c("SELECT AVG(f), MAX(y) AS n FROM test WHERE x = 7 GROUP BY z HAVING AVG(d) > 2.2 "
      "AND AVG(y) > 42.0 ORDER BY n;",
      dt);
    c("SELECT AVG(f) + AVG(d), MAX(y) AS n FROM test WHERE x = 7 GROUP BY z HAVING "
      "AVG(f) + AVG(d) > 3.0 ORDER BY n;",
      dt);
    c("SELECT AVG(f) + AVG(d), MAX(y) AS n FROM test WHERE x = 7 GROUP BY z HAVING "
      "AVG(f) + AVG(d) > 3.5 ORDER BY n;",
      dt);
    c("SELECT f + d AS s, x * y FROM test ORDER by s DESC;", dt);
    c("SELECT COUNT(*) AS n FROM test GROUP BY f ORDER BY n;", dt);
    c("SELECT f, COUNT(*) FROM test GROUP BY f HAVING f > 1.25;", dt);
    c("SELECT COUNT(*) AS n FROM test GROUP BY d ORDER BY n;", dt);
    c("SELECT MIN(x + y) AS n FROM test WHERE x + y > 47 AND x + y < 53 GROUP BY f + 1, "
      "f + d ORDER BY n;",
      dt);
    c("SELECT f + d AS s FROM test GROUP BY s ORDER BY s DESC;", dt);
    c("SELECT f + 1 AS s, AVG(u * f) FROM test GROUP BY s ORDER BY s DESC;", dt);
    c("SELECT (CAST(dd AS float) * 0.5) AS key FROM test GROUP BY key ORDER BY key DESC;",
      dt);
    c("SELECT (CAST(dd AS double) * 0.5) AS key FROM test GROUP BY key ORDER BY key "
      "DESC;",
      dt);

    c("SELECT fn FROM test ORDER BY fn ASC NULLS FIRST;",
      "SELECT fn FROM test ORDER BY fn ASC;",
      dt);
    c("SELECT fn FROM test WHERE fn < 0 OR fn IS NULL ORDER BY fn ASC NULLS FIRST;",
      "SELECT fn FROM test WHERE fn < 0 OR fn IS NULL ORDER BY fn ASC;",
      dt);
    ASSERT_NEAR(static_cast<double>(1.3),
                v<double>(run_simple_agg("SELECT AVG(f) AS n FROM test WHERE x = 7 GROUP "
                                         "BY z HAVING AVG(y) + STDDEV(y) "
                                         "> 42.0 ORDER BY n + VARIANCE(y);",
                                         dt)),
                static_cast<double>(0.1));
    ASSERT_NEAR(
        static_cast<double>(92.0),
        v<double>(run_simple_agg("SELECT STDDEV_POP(dd) AS n FROM test ORDER BY n;", dt)),
        static_cast<double>(1.0));
  }
}

TEST_F(Select, FilterShortCircuit) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8 AND z > 100 AND z < 102 AND t > "
      "1000 AND UNLIKELY(t < 1002);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8 AND z > 100 AND z < 102 AND t > "
      "1000 AND t > 1000 AND t > 1001 "
      "AND t > 1002 AND t > 1003 AND t > 1004 AND UNLIKELY(t < 1002);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8 AND z > 100 AND z < 102 AND t > "
      "1000 AND t > 1000 AND t > 1001 "
      "AND t > 1002 AND t > 1003 AND t > 1004 AND t > 1005 AND UNLIKELY(t < 1002);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8 AND z > 100 AND z < 102 AND t > "
      "1000 AND t > 1000 AND t > 1001 "
      "AND t > 1002 AND t > 1003 AND UNLIKELY(t < 111) AND (str LIKE 'f__%%');",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x > 6 AND x < 8 AND UNLIKELY(z < 200) AND z > 100 "
      "AND z < 102 AND t > 1000 AND "
      "t > 1000 AND t > 1001  AND UNLIKELY(t < 1111 AND t > 1100) AND (str LIKE 'f__%%') "
      "AND t > 1002 AND t > 1003;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE UNLIKELY(x IN (7, 8, 9, 10)) AND y > 42;", dt);
    c("SELECT COUNT(*) FROM test WHERE (x / 2.0 > 3.500) AND (str LIKE 's__');", dt);
    {
      std::string query(
          "SELECT COUNT(*) FROM test WHERE (MOD(x, 2) = 0) AND (str LIKE 's__') AND (x "
          "in (7));");
      const auto result = run_multiple_agg(query, dt);
      const auto row = result->getNextRow(true, true);
      ASSERT_EQ(size_t(1), row.size());
      ASSERT_EQ(int64_t(0), v<int64_t>(row[0]));
    }
  }
}

TEST_F(Select, InValues) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c(R"(SELECT x FROM test WHERE x IN (8, 9, 10, 11, 12, 13, 14) GROUP BY x ORDER BY x;)",
      dt);
    c(R"(SELECT y FROM test WHERE y IN (43, 44, 45, 46, 47, 48, 49) GROUP BY y ORDER BY y;)",
      dt);
    c(R"(SELECT t FROM test WHERE t NOT IN (NULL) GROUP BY t ORDER BY t;)", dt);
    c(R"(SELECT t FROM test WHERE t NOT IN (1001, 1003, 1005, 1007, 1009, -10) GROUP BY t ORDER BY t;)",
      dt);
    c(R"(WITH dimensionValues AS (SELECT b FROM test GROUP BY b ORDER BY b) SELECT x FROM test WHERE b in (SELECT b FROM dimensionValues) GROUP BY x ORDER BY x;)",
      dt);
  }
}

TEST_F(Select, FilterAndMultipleAggregation) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT AVG(x), AVG(y) FROM test;", dt);
    c("SELECT MIN(x), AVG(x * y), MAX(y + 7), COUNT(*) FROM test WHERE x + y > 47 AND x "
      "+ y < 51;",
      dt);
    c("SELECT str, AVG(x), COUNT(*) as xx, COUNT(*) as countval FROM test GROUP BY str "
      "ORDER BY str;",
      dt);
  }
}

TEST_F(Select, GroupBy) {
  {  // generate dataset to test count distinct rewrite
    createTable("count_distinct_rewrite", {{"v1", SQLTypeInfo(kINT)}});
    std::string csv_data;
    for (int i = 0; i < 1000000; i++) {
      csv_data += std::to_string(i) + "\n";
    }
    insertCsvValues("count_distinct_rewrite", csv_data);
  }

  std::vector<std::string> runnable_column_names = {
      "x", "w", "y", "z", "fx", "f", "d", "dd", "dd_notnull", "u", "smallint_nulls"};
  std::set<std::string> str_col_names = {
      "str", "fixed_str", "shared_dict", "null_str", "fixed_null_str"};
  // some column type can execute a subset of agg ops without an exception
  using TypeAndAvaliableAggOps = std::pair<std::string, std::vector<SQLAgg>>;
  std::vector<TypeAndAvaliableAggOps> column_names_with_available_agg_ops = {
      {"str", std::vector<SQLAgg>{SQLAgg::kSAMPLE}},
      {"fixed_str", std::vector<SQLAgg>{SQLAgg::kSAMPLE}},
      {"shared_dict", std::vector<SQLAgg>{SQLAgg::kSAMPLE}},
      {"null_str", std::vector<SQLAgg>{SQLAgg::kSAMPLE}},
      {"fixed_null_str", std::vector<SQLAgg>{SQLAgg::kSAMPLE}},
      {"b", std::vector<SQLAgg>{SQLAgg::kMIN, SQLAgg::kMAX, SQLAgg::kSAMPLE}},
      {"m", std::vector<SQLAgg>{SQLAgg::kMIN, SQLAgg::kMAX, SQLAgg::kSAMPLE}},
      {"m_3", std::vector<SQLAgg>{SQLAgg::kMIN, SQLAgg::kMAX, SQLAgg::kSAMPLE}},
      {"m_6", std::vector<SQLAgg>{SQLAgg::kMIN, SQLAgg::kMAX, SQLAgg::kSAMPLE}},
      {"m_9", std::vector<SQLAgg>{SQLAgg::kMIN, SQLAgg::kMAX, SQLAgg::kSAMPLE}},
      {"n", std::vector<SQLAgg>{SQLAgg::kMIN, SQLAgg::kMAX, SQLAgg::kSAMPLE}},
      {"o", std::vector<SQLAgg>{SQLAgg::kMIN, SQLAgg::kMAX, SQLAgg::kSAMPLE}},
      {"o1", std::vector<SQLAgg>{SQLAgg::kMIN, SQLAgg::kMAX, SQLAgg::kSAMPLE}}};
  auto get_query_str = [](SQLAgg agg_op, const std::string& col_name) {
    std::ostringstream oss;
    oss << "SELECT " << col_name << " v1, ";
    switch (agg_op) {
      case SQLAgg::kMIN:
        oss << "MIN(" << col_name << ")";
        break;
      case SQLAgg::kMAX:
        oss << "MAX(" << col_name << ")";
        break;
      case SQLAgg::kAVG:
        oss << "AVG(" << col_name << ")";
        break;
      case SQLAgg::kSAMPLE:
        oss << "SAMPLE(" << col_name << ")";
        break;
      case SQLAgg::kAPPROX_QUANTILE:
        oss << "APPROX_PERCENTILE(" << col_name << ", 0.5) ";
        break;
      default:
        CHECK(false);
        break;
    }
    oss << " v2 FROM test GROUP BY " << col_name;
    return oss.str();
  };
  auto perform_test = [&get_query_str, &str_col_names](SQLAgg agg_op,
                                                       const std::string& col_name,
                                                       ExecutorDeviceType dt) {
    std::string omnisci_cnt_query, sqlite_cnt_query, omnisci_min_query, sqlite_min_query;
    if (agg_op == SQLAgg::kAPPROX_QUANTILE || agg_op == SQLAgg::kSAMPLE) {
      omnisci_cnt_query =
          "SELECT COUNT(*) FROM (" + get_query_str(agg_op, col_name) + ")";
      omnisci_min_query = "SELECT MIN(v2) FROM (" + get_query_str(agg_op, col_name) + ")";
      // since sqlite does not support sample and approx_quantile
      // we instead use max agg op; min and avg are also possible
      sqlite_cnt_query =
          "SELECT COUNT(*) FROM (" + get_query_str(SQLAgg::kMAX, col_name) + ")";
      sqlite_min_query =
          "SELECT MIN(v2) FROM (" + get_query_str(SQLAgg::kMAX, col_name) + ")";
      if (col_name.compare("d") != 0 && col_name.compare("f") != 0 &&
          col_name.compare("fx") != 0) {
        omnisci_cnt_query += " WHERE v1 = v2";
        sqlite_cnt_query += " WHERE v1 = v2";
      }
    } else {
      omnisci_cnt_query =
          "SELECT COUNT(*) FROM (" + get_query_str(agg_op, col_name) + ")";
      omnisci_min_query = "SELECT MIN(v2) FROM (" + get_query_str(agg_op, col_name) + ")";
      if (col_name.compare("d") != 0 && col_name.compare("f") != 0 &&
          col_name.compare("fx") != 0) {
        omnisci_cnt_query += " WHERE v1 = v2";
      }
      sqlite_cnt_query = omnisci_cnt_query;
      sqlite_min_query = omnisci_min_query;
    }
    c(omnisci_cnt_query, sqlite_cnt_query, dt);
    if (!str_col_names.count(col_name)) {
      c(omnisci_min_query, sqlite_min_query, dt);
    } else {
      LOG(WARNING) << "Skipping aggregation query on string column: "
                   << omnisci_min_query;
    }
  };

  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c("SELECT COUNT(*) FROM test_ranges GROUP BY i, b;", dt);
    c("SELECT i, b FROM test_ranges GROUP BY i, b;", dt);

    {
      const auto big_group_threshold = config().exec.group_by.big_group_threshold;
      ScopeGuard reset_big_group_threshold = [&big_group_threshold] {
        config().exec.group_by.big_group_threshold = big_group_threshold;
      };
      config().exec.group_by.big_group_threshold = 1;
      c("SELECT d, COUNT(*) FROM test GROUP BY d ORDER BY d DESC LIMIT 10;", dt);
    }

    if (g_enable_columnar_output) {
      // TODO: Fixup the tests below when running with columnar output enabled
      continue;
    }

    c("SELECT x, y, COUNT(*) FROM test GROUP BY x, y;", dt);
    c("SELECT x, y, APPROX_COUNT_DISTINCT(str) FROM test GROUP BY x, y;",
      "SELECT x, y, COUNT(distinct str) FROM test GROUP BY x, y;",
      dt);
    c("SELECT f, ff, APPROX_COUNT_DISTINCT(str) from test group by f, ff ORDER BY f, ff;",
      "SELECT f, ff, COUNT(distinct str) FROM test GROUP BY f, ff ORDER BY f, ff;",
      dt);

    // check rewriting agg on gby col to its equivalent case-when
    // 1. check count-distinct op runs successfully
    ASSERT_NO_THROW(run_multiple_agg(
        "SELECT v1, COUNT(DISTINCT v1) FROM count_distinct_rewrite GROUP BY v1 limit "
        "1;",
        dt));
    ASSERT_NO_THROW(run_multiple_agg(
        "SELECT v1, COUNT(DISTINCT v1), CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END "
        "FROM count_distinct_rewrite GROUP BY v1 limit 1;",
        dt));
    ASSERT_NO_THROW(
        run_multiple_agg("SELECT v1, COUNT(DISTINCT v1), APPROX_COUNT_DISTINCT(DISTINCT "
                         "v1) FROM count_distinct_rewrite GROUP BY v1 limit "
                         "1;",
                         dt));
    ASSERT_NO_THROW(run_multiple_agg(
        "SELECT v1, APPROX_COUNT_DISTINCT(v1) FROM count_distinct_rewrite GROUP BY v1 "
        "limit 1;",
        dt));
    ASSERT_NO_THROW(run_multiple_agg(
        "SELECT v1, APPROX_COUNT_DISTINCT(v1), CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 "
        "END, COUNT(DISTINCT v1) FROM count_distinct_rewrite GROUP BY v1 limit 1;",
        dt));

    // 2. remaining agg ops: avg / min / max / sample / approx_quantile
    // there are two exceptions when perform gby-agg: 1) gby fails and 2) agg fails
    // otherwise this rewriting should return the same result as the original query
    std::vector<SQLAgg> test_agg_ops = {SQLAgg::kMIN,
                                        SQLAgg::kMAX,
                                        SQLAgg::kSAMPLE,
                                        SQLAgg::kAVG,
                                        SQLAgg::kAPPROX_QUANTILE};
    for (auto& col_name : runnable_column_names) {
      for (auto& agg_op : test_agg_ops) {
        perform_test(agg_op, col_name, dt);
      }
    }
    for (TypeAndAvaliableAggOps& info : column_names_with_available_agg_ops) {
      const auto& col_name = info.first;
      const auto& agg_ops = info.second;
      for (auto& agg_op : agg_ops) {
        perform_test(agg_op, col_name, dt);
      }
    }

    // check whether we only apply case-when optimization towards count* distinct agg
    c("SELECT x, COUNT(x) FROM test GROUP BY x;", dt);
    c("SELECT x, COUNT(DISTINCT x) FROM test GROUP BY x;", dt);
    c("SELECT x, y, COUNT(x) FROM test GROUP BY x,y;", dt);
    c("SELECT x, y, COUNT(DISTINCT x) FROM test GROUP BY x,y;", dt);
  }
  dropTable("count_distinct_rewrite");
}

TEST_F(Select, ExecutePlanWithoutGroupBy) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // SQLite doesn't support NOW(), and timestamps may not be exactly equal,
    // so just test for no throw.
    EXPECT_NO_THROW(
        run_multiple_agg("SELECT COUNT(*), NOW(), CURRENT_TIME, CURRENT_DATE, "
                         "CURRENT_TIMESTAMP FROM test;",
                         dt));
    EXPECT_NO_THROW(
        run_multiple_agg("SELECT x, COUNT(*), NOW(), CURRENT_TIME, CURRENT_DATE, "
                         "CURRENT_TIMESTAMP FROM test GROUP BY x;",
                         dt));
  }
}

TEST_F(Select, FilterAndGroupBy) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT MIN(x + y) FROM test WHERE x + y > 47 AND x + y < 53 GROUP BY x, y;", dt);
    c("SELECT MIN(x + y) FROM test WHERE x + y > 47 AND x + y < 53 GROUP BY x + 1, x + "
      "y;",
      dt);
    c("SELECT x, y, COUNT(*) FROM test GROUP BY x, y;", dt);
    c("SELECT x, dd, COUNT(*) FROM test GROUP BY x, dd ORDER BY x, dd;", dt);
    c("SELECT dd AS key1, COUNT(*) AS value1 FROM test GROUP BY key1 HAVING key1 IS NOT "
      "NULL ORDER BY key1, value1 "
      "DESC "
      "LIMIT 12;",
      dt);
    c("SELECT 'literal_string' AS key0 FROM test GROUP BY key0;", dt);
    c("SELECT str, MIN(y) FROM test WHERE y IS NOT NULL GROUP BY str ORDER BY str DESC;",
      dt);
    c("SELECT x, MAX(z) FROM test WHERE z IS NOT NULL GROUP BY x HAVING x > 7;", dt);
    c("SELECT CAST((dd - 0.5) * 2.0 AS int) AS key0, COUNT(*) AS val FROM test WHERE (dd "
      ">= 100.0 AND dd < 400.0) "
      "GROUP "
      "BY key0 HAVING key0 >= 0 AND key0 < 400 ORDER BY val DESC LIMIT 50 OFFSET 0;",
      dt);
    c("SELECT y, AVG(CASE WHEN x BETWEEN 6 AND 7 THEN x END) FROM test GROUP BY y ORDER "
      "BY y;",
      dt);
    c("SELECT x, AVG(u), COUNT(*) AS n FROM test GROUP BY x ORDER BY n DESC;", dt);
    c("SELECT f, ss FROM test GROUP BY f, ss ORDER BY f DESC;", dt);
    c("SELECT fx, COUNT(*) FROM test GROUP BY fx HAVING COUNT(*) > 5;", dt);
    c("SELECT fx, COUNT(*) n FROM test GROUP BY fx ORDER BY n DESC, fx IS NULL DESC;",
      dt);
    c("SELECT CASE WHEN x > 8 THEN 100000000 ELSE 42 END AS c, COUNT(*) FROM test GROUP "
      "BY c;",
      dt);
    // SQLite floors instead of rounds when casting float to int.
    c("SELECT COUNT(*) FROM test WHERE CAST((CAST(x AS FLOAT) - 1) * 0.2 AS INT) = 1;",
      dt);
    c("SELECT CAST(CAST(d/2 AS FLOAT) AS INTEGER) AS key, COUNT(*) FROM test GROUP BY "
      "key;",
      dt);
    c("SELECT x * 2 AS x2, COUNT(DISTINCT y) AS n FROM test GROUP BY x2 ORDER BY n DESC;",
      dt);
    c("SELECT x, COUNT(real_str) FROM test GROUP BY x ORDER BY x DESC;", dt);
    c("SELECT str, SUM(y - y) FROM test GROUP BY str ORDER BY str ASC;", dt);
    c("SELECT str, SUM(y - y) FROM test WHERE y - y IS NOT NULL GROUP BY str ORDER BY "
      "str ASC;",
      dt);
    c("select shared_dict,m from test where (m >= CAST('2014-12-13 22:23:15' AS "
      "TIMESTAMP(0)) and m <= "
      "CAST('2014-12-14 22:23:15' AS TIMESTAMP(0)))  and CAST(m AS TIMESTAMP(0)) BETWEEN "
      "'2014-12-14 22:23:15' AND "
      "'2014-12-13 22:23:15' group by shared_dict,m;",
      dt);
    c("SELECT x, SUM(z) FROM test WHERE z IS NOT NULL GROUP BY x ORDER BY x;", dt);
    EXPECT_THROW(run_multiple_agg("SELECT MIN(str) FROM test GROUP BY x;", dt),
                 std::runtime_error);
  }
}

TEST_F(Select, GroupByBoundariesAndNull) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      std::string query(
          "SELECT CAST(CASE WHEN x = 7 THEN 2147483647 ELSE null END AS INTEGER) AS "
          "col0, COUNT(*) FROM test GROUP BY col0 ORDER BY col0 ASC");
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query(
          "SELECT smallint_nulls, COUNT(*) FROM test GROUP BY smallint_nulls ORDER BY "
          "smallint_nulls ASC");
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query(
          "SELECT CAST(CASE WHEN x = 7 THEN 127 ELSE null END AS TINYINT) AS col0, "
          "COUNT(*) FROM test GROUP BY col0 ORDER BY col0 ASC");
      c(query + " NULLS FIRST;", query + ";", dt);
    }
  }
}

TEST_F(Select, NestedGroupByWithFloat) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    char const* query =
        "SELECT c, x, f FROM ("
        "   SELECT x, COUNT(*) AS c, f"
        "   FROM test"
        "   GROUP BY x, f"
        " )"
        " GROUP BY c, x, f"
        " ORDER BY c, x, f;";
    c(query, dt);
  }
}

TEST_F(Select, Arrays) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    // Simple lazy projection
    compare_array(run_simple_agg("SELECT arr_i16 FROM array_test WHERE x = 8;", dt),
                  std::vector<int64_t>({2, 3, 4}));
    compare_array(run_simple_agg("SELECT arr_i32 FROM array_test WHERE x = 8;", dt),
                  std::vector<int64_t>({20, 30, 40}));
    compare_array(run_simple_agg("SELECT arr_i64 FROM array_test WHERE x = 8;", dt),
                  std::vector<int64_t>({200, 300, 400}));
    compare_array(run_simple_agg("SELECT arr_str FROM array_test WHERE x = 8;", dt),
                  std::vector<std::string>({"bb", "cc", "dd"}));
    compare_array(run_simple_agg("SELECT arr_float FROM array_test WHERE x = 8;", dt),
                  std::vector<float>({2.2, 3.3, 4.4}));
    compare_array(run_simple_agg("SELECT arr_double FROM array_test WHERE x = 8;", dt),
                  std::vector<double>({22.2, 33.3, 44.4}));
    compare_array(run_simple_agg("SELECT arr_bool FROM array_test WHERE x = 8;", dt),
                  std::vector<int64_t>({1, 0, 1, 0, 1, 0}));

    compare_array(run_simple_agg("SELECT arr3_i8 FROM array_test WHERE x = 8;", dt),
                  std::vector<int64_t>({2, 3, 4}));
    compare_array(run_simple_agg("SELECT arr3_i16 FROM array_test WHERE x = 8;", dt),
                  std::vector<int64_t>({2, 3, 4}));
    compare_array(run_simple_agg("SELECT arr3_i32 FROM array_test WHERE x = 8;", dt),
                  std::vector<int64_t>({20, 30, 40}));
    compare_array(run_simple_agg("SELECT arr3_i64 FROM array_test WHERE x = 8;", dt),
                  std::vector<int64_t>({200, 300, 400}));
    compare_array(run_simple_agg("SELECT arr3_float FROM array_test WHERE x = 8;", dt),
                  std::vector<float>({2.2, 3.3, 4.4}));
    compare_array(run_simple_agg("SELECT arr3_double FROM array_test WHERE x = 8;", dt),
                  std::vector<double>({22.2, 33.3, 44.4}));
    compare_array(run_simple_agg("SELECT arr6_bool FROM array_test WHERE x = 8;", dt),
                  std::vector<int64_t>({1, 0, 1, 0, 1, 0}));

    // requires punt to CPU
    compare_array(
        run_simple_agg("SELECT ARRAY[1,2,3,5] from array_test WHERE x = 8 limit 8675309;",
                       dt),
        std::vector<int64_t>({1, 2, 3, 5}));
    compare_array(
        run_simple_agg("SELECT ARRAY[2*arr3_i32[1],2*arr3_i32[2],2*arr3_i32[3]] FROM "
                       "array_test a WHERE x = 8 limit 31337;",
                       dt),
        std::vector<int64_t>({40, 60, 80}));

    // Simple non-lazy projection
    compare_array(
        run_simple_agg("SELECT arr_i16 FROM array_test WHERE arr_i16[1] = 2;", dt),
        std::vector<int64_t>({2, 3, 4}));
    compare_array(
        run_simple_agg("SELECT arr_i32 FROM array_test WHERE arr_i32[1] = 20;", dt),
        std::vector<int64_t>({20, 30, 40}));
    compare_array(
        run_simple_agg("SELECT arr_i64 FROM array_test WHERE arr_i64[1] = 200;", dt),
        std::vector<int64_t>({200, 300, 400}));
    compare_array(
        run_simple_agg("SELECT arr_str FROM array_test WHERE arr_str[1] = 'bb';", dt),
        std::vector<std::string>({"bb", "cc", "dd"}));
    // TODO(adb): Calcite is casting the column value to DOUBLE to do the comparison,
    // which results in the comparison failing. Is this desired behavior or a bug? Adding
    // the CAST below for now to test projection.
    compare_array(
        run_simple_agg(
            "SELECT arr_float FROM array_test WHERE arr_float[1] = CAST(2.2 as FLOAT);",
            dt),
        std::vector<float>({2.2, 3.3, 4.4}));
    compare_array(
        run_simple_agg("SELECT arr_double FROM array_test WHERE arr_double[1] = 22.2;",
                       dt),
        std::vector<double>({22.2, 33.3, 44.4}));
    compare_array(run_simple_agg(
                      "SELECT arr_bool FROM array_test WHERE x < 9 AND arr_bool[1];", dt),
                  std::vector<int64_t>({1, 0, 1, 0, 1, 0}));

    compare_array(
        run_simple_agg("SELECT arr3_i8 FROM array_test WHERE arr3_i8[1] = 2;", dt),
        std::vector<int64_t>({2, 3, 4}));
    compare_array(
        run_simple_agg("SELECT arr3_i16 FROM array_test WHERE arr3_i16[1] = 2;", dt),
        std::vector<int64_t>({2, 3, 4}));
    compare_array(
        run_simple_agg("SELECT arr3_i32 FROM array_test WHERE arr3_i32[1] = 20;", dt),
        std::vector<int64_t>({20, 30, 40}));
    compare_array(
        run_simple_agg("SELECT arr3_i64 FROM array_test WHERE arr3_i64[1] = 200;", dt),
        std::vector<int64_t>({200, 300, 400}));
    compare_array(
        run_simple_agg(
            "SELECT arr3_float FROM array_test WHERE arr3_float[1] = CAST(2.2 AS FLOAT);",
            dt),
        std::vector<float>({2.2, 3.3, 4.4}));
    compare_array(
        run_simple_agg("SELECT arr3_double FROM array_test WHERE arr3_double[1] = 22.2;",
                       dt),
        std::vector<double>({22.2, 33.3, 44.4}));
    compare_array(
        run_simple_agg("SELECT arr6_bool FROM array_test WHERE x < 9 AND arr6_bool[1];",
                       dt),
        std::vector<int64_t>({1, 0, 1, 0, 1, 0}));

    const auto watchdog_state = config().exec.watchdog.enable;
    ScopeGuard reset_Watchdog_state = [&watchdog_state] {
      config().exec.watchdog.enable = watchdog_state;
    };
    config().exec.watchdog.enable = true;
    // throw exception when comparing full array joins when watchdog is on
    EXPECT_THROW(run_simple_agg("SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                                "t1.arr_str[1] > t2.arr_str[1];",
                                dt),
                 std::runtime_error);

    EXPECT_THROW(run_simple_agg("SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                                "t1.arr_str[1] >= t2.arr_str[1];",
                                dt),
                 std::runtime_error);

    EXPECT_THROW(run_simple_agg("SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                                "t1.arr_str[1] < t2.arr_str[1];",
                                dt),
                 std::runtime_error);

    EXPECT_THROW(run_simple_agg("SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                                "t1.arr_str[1] <= t2.arr_str[1];",
                                dt),
                 std::runtime_error);

    // Even with watchdog on, we can do non-equality on dictionary string as dictionary is
    // shared since we are comparing a column with itself

    EXPECT_EQ(int64_t(20),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                  "t1.arr_str[1] = t2.arr_str[1];",
                  dt)));

    // New behavior introduced by [QE-261] allows translation to none-encoded strings for
    // comparison if watchdog is off for non-distributed deployments

    // The following tests throw "Cast from dictionary-encoded string to
    // none-encoded not supported for distributed queries" in distributed mode.
    // We will unlock these with planned work for sort permutations of dictionary
    // translation maps, as well as much faster support for this class of queries
    // with watchdog off (distributed and single-node).

    config().exec.watchdog.enable = false;

    EXPECT_EQ(int64_t(190),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                  "t1.arr_str[1] > t2.arr_str[1];",
                  dt)));  //

    EXPECT_EQ(int64_t(210),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                  "t1.arr_str[1] >= t2.arr_str[1];",
                  dt)));

    EXPECT_EQ(int64_t(190),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                  "t1.arr_str[1] < t2.arr_str[1];",
                  dt)));

    EXPECT_EQ(int64_t(210),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                  "t1.arr_str[1] <= t2.arr_str[1];",
                  dt)));

    // This query can run on distributed as it can leverage distributed
    // string translation

    EXPECT_EQ(int64_t(20),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                  "t1.arr_str[1] = t2.arr_str[1];",
                  dt)));
  }
}

TEST_F(Select, FilterCastToDecimal) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    ASSERT_EQ(static_cast<int64_t>(5),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE x > 7.1;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(10),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE y > 42.5;", dt)));
    ASSERT_EQ(static_cast<int64_t>(10),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE ufd > -2147483648.0;", dt)));
    ASSERT_EQ(static_cast<int64_t>(15),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE ofd > -2147483648;", dt)));
  }
}

TEST_F(Select, FilterAndGroupByMultipleAgg) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT MIN(x + y), COUNT(*), AVG(x + 1) FROM test WHERE x + y > 47 AND x + y < 53 "
      "GROUP BY x, y;",
      dt);
    c("SELECT MIN(x + y), COUNT(*), AVG(x + 1) FROM test WHERE x + y > 47 AND x + y < 53 "
      "GROUP BY x + 1, x + y;",
      dt);
  }
}

TEST_F(Select, GroupByKeylessAndNotKeyless) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT fixed_str FROM test WHERE fixed_str = 'fish' GROUP BY fixed_str;", dt);
    c("SELECT AVG(x), fixed_str FROM test WHERE fixed_str = 'fish' GROUP BY fixed_str;",
      dt);
    c("SELECT AVG(smallint_nulls), fixed_str FROM test WHERE fixed_str = 'foo' GROUP BY "
      "fixed_str;",
      dt);
    c("SELECT null_str, AVG(smallint_nulls) FROM test GROUP BY null_str;", dt);
  }
}

TEST_F(Select, Having) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT MAX(y) AS n FROM test WHERE x = 7 GROUP BY z HAVING MAX(x) > 5 ORDER BY n;",
      dt);
    c("SELECT MAX(y) AS n FROM test WHERE x = 7 GROUP BY z HAVING MAX(x) > 5 ORDER BY n "
      "LIMIT 1;",
      dt);
    c("SELECT MAX(y) AS n FROM test WHERE x > 7 GROUP BY z HAVING MAX(x) < 100 ORDER BY "
      "n;",
      dt);
    c("SELECT z, SUM(y) AS n FROM test WHERE x > 6 GROUP BY z HAVING MAX(x) < 100 ORDER "
      "BY n;",
      dt);
    c("SELECT z, SUM(y) AS n FROM test WHERE x > 6 GROUP BY z HAVING MAX(x) < 100 AND "
      "COUNT(*) > 5 ORDER BY n;",
      dt);
    c("SELECT z, SUM(y) AS n FROM test WHERE x > 6 GROUP BY z HAVING MAX(x) < 100 AND "
      "COUNT(*) > 9 ORDER BY n;",
      dt);
    c("SELECT str, COUNT(*) AS n FROM test GROUP BY str HAVING str IN ('bar', 'baz') "
      "ORDER BY str;",
      dt);
    c("SELECT str, COUNT(*) AS n FROM test GROUP BY str HAVING str LIKE 'ba_' ORDER BY "
      "str;",
      dt);
    c("SELECT ss, COUNT(*) AS n FROM test GROUP BY ss HAVING ss LIKE 'bo_' ORDER BY ss;",
      dt);
    c("SELECT x, COUNT(*) FROM test WHERE x > 9 GROUP BY x HAVING x > 15;", dt);
    c("SELECT x, AVG(y), AVG(y) FROM test GROUP BY x HAVING x >= 0 ORDER BY x;", dt);
    c("SELECT AVG(y), x, AVG(y) FROM test GROUP BY x HAVING x >= 0 ORDER BY x;", dt);
    c("SELECT x, y, COUNT(*) FROM test WHERE real_str LIKE 'nope%' GROUP BY x, y HAVING "
      "x >= 0 AND x < 12 AND y >= 0 "
      "AND y < 12 ORDER BY x, y;",
      dt);
  }
}

TEST_F(Select, ConstantWidthBucketExpr) {
  createTable("wb_test",
              {{"i1", SQLTypeInfo(kTINYINT)},
               {"i2", SQLTypeInfo(kSMALLINT)},
               {"i4", SQLTypeInfo(kINT)},
               {"i8", SQLTypeInfo(kBIGINT)},
               {"f", SQLTypeInfo(kFLOAT)},
               {"d", SQLTypeInfo(kDOUBLE)},
               {"dc", SQLTypeInfo(kDECIMAL, 15, 8, false)},
               {"n", SQLTypeInfo(kDECIMAL, 15, 8, false)}});
  insertCsvValues("wb_test", ",,,,,,,");
  auto drop = "DROP TABLE IF EXISTS wb_test;";
  auto create =
      "CREATE TABLE wb_test (i1 tinyint, i2 smallint, i4 int, i8 bigint, f float, d "
      "double, dc decimal(15,8), n numeric(15,8));";
  auto insert =
      "INSERT INTO wb_test VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);";
  run_sqlite_query(drop);
  run_sqlite_query(create);
  run_sqlite_query(insert);
  auto test_queries = [](const std::string col_name) {
    std::string omnisci_query =
        "SELECT WIDTH_BUCKET(" + col_name + ", 1, 2, 3) FROM wb_test;";
    std::string sqlite_query = "SELECT " + col_name + " FROM wb_test;";
    return std::make_pair(omnisci_query, sqlite_query);
  };
  std::vector<std::string> col_names{"i1", "i2", "i4", "i8", "d", "f", "dc", "n"};
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // in single-mode, we can see std::runtime_error exception for the below test queries
    // having unsupported or invalid arguments of the function
    // but in dist-mode, we detect Calcite SQL error instead of std::runtime_error
    // so we try to detect 'any' exception instead of the specific exception type
    EXPECT_ANY_THROW(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, 0);", dt));
    EXPECT_ANY_THROW(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, -1);", dt));
    EXPECT_ANY_THROW(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, NULL);", dt));
    EXPECT_ANY_THROW(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, 2147483649);", dt));
    EXPECT_ANY_THROW(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, -2147483649);", dt));
    EXPECT_ANY_THROW(
        run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, 9223372036854775800);", dt));
    EXPECT_ANY_THROW(
        run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, -9223372036854775800);", dt));
    EXPECT_ANY_THROW(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, 1.11112);", dt));
    EXPECT_ANY_THROW(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, 1.111121112);", dt));
    EXPECT_ANY_THROW(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, -1.11112);", dt));
    EXPECT_ANY_THROW(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, -1.111121112);", dt));
    EXPECT_ANY_THROW(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 2, 3);", dt));
    EXPECT_ANY_THROW(
        run_simple_agg("SELECT WIDTH_BUCKET(1, 2147483647, 2147483647, 3);", dt));
    EXPECT_ANY_THROW(
        run_simple_agg("SELECT WIDTH_BUCKET(1, 2147483649, 2147483649, 3);", dt));
    EXPECT_ANY_THROW(
        run_simple_agg("SELECT WIDTH_BUCKET(1, -2147483647, -2147483647, 3);", dt));
    EXPECT_ANY_THROW(
        run_simple_agg("SELECT WIDTH_BUCKET(1, -2147483649, -2147483649, 3);", dt));
    EXPECT_ANY_THROW(run_simple_agg(
        "SELECT WIDTH_BUCKET(1, 9223372036854775808, 9223372036854775808, 3);", dt));
    EXPECT_ANY_THROW(run_simple_agg(
        "SELECT WIDTH_BUCKET(1, -9223372036854775808, -9223372036854775808, 3);", dt));
    EXPECT_NO_THROW(run_simple_agg("SELECT WIDTH_BUCKET(NULL, 2, 3, 100);", dt));

    // check the correctness of the function based on postgres 12.7's result
    EXPECT_EQ(int64_t(0),
              v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, 3, 100);", dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(2, 2, 3, 100);", dt)));
    EXPECT_EQ(int64_t(101),
              v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(3, 2, 3, 100);", dt)));
    EXPECT_EQ(int64_t(11),
              v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(2.1, 2, 3, 100);", dt)));
    EXPECT_EQ(
        int64_t(11),
        v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(2.11, 2.1, 2.2, 100);", dt)));
    EXPECT_EQ(int64_t(91),
              v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(2.1, 3, 2, 100);", dt)));
    EXPECT_EQ(
        int64_t(95),
        v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(2.156789, 3, 2.11, 100);", dt)));
    EXPECT_EQ(int64_t(26),
              v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(1, 2, -2, 100);", dt)));
    EXPECT_EQ(int64_t(48),
              v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(0.1, 2, -2, 100);", dt)));
    EXPECT_EQ(int64_t(48),
              v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(-0.1, -2, 2, 100);", dt)));
    EXPECT_EQ(int64_t(53),
              v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(-0.1, 2, -2, 100);", dt)));

    for (auto& col : col_names) {
      auto queries = test_queries(col);
      c(queries.first, queries.second, dt);
    }
  }
  dropTable("wb_test");
  run_sqlite_query(drop);
}

TEST_F(Select, WidthBucketExpr) {
  createTable("wb_test",
              {{"i1", SQLTypeInfo(kTINYINT)},
               {"i2", SQLTypeInfo(kSMALLINT)},
               {"i4", SQLTypeInfo(kINT)},
               {"i8", SQLTypeInfo(kBIGINT)},
               {"f", SQLTypeInfo(kFLOAT)},
               {"d", SQLTypeInfo(kDOUBLE)},
               {"dc", SQLTypeInfo(kDECIMAL, 15, 8, false)},
               {"n", SQLTypeInfo(kDECIMAL, 15, 8, false)},
               {"i1n", SQLTypeInfo(kTINYINT)},
               {"i2n", SQLTypeInfo(kSMALLINT)},
               {"i4n", SQLTypeInfo(kINT)},
               {"i8n", SQLTypeInfo(kBIGINT)},
               {"fn", SQLTypeInfo(kFLOAT)},
               {"dn", SQLTypeInfo(kDOUBLE)},
               {"dcn", SQLTypeInfo(kDECIMAL, 15, 8, false)},
               {"nn", SQLTypeInfo(kDECIMAL, 15, 8, false)}});
  insertCsvValues("wb_test", "1, 1, 1, 1, 1.0, 1.0, 1.0, 1.0,,,,,,,,");
  auto drop = "DROP TABLE IF EXISTS wb_test;";
  auto create_sqlite =
      "CREATE TABLE wb_test (i1n tinyint, i2n smallint, i4n int, i8n bigint, fn float, "
      "dn "
      "double, dcn decimal(15,8), nn numeric(15,8));";
  auto insert_sqlite =
      "INSERT INTO wb_test VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);";
  run_sqlite_query(drop);
  run_sqlite_query(create_sqlite);
  run_sqlite_query(insert_sqlite);
  auto test_queries = [](const std::string col_name) {
    std::string omnisci_query =
        "SELECT WIDTH_BUCKET(" + col_name + ", i4, i4*10, i4*10) FROM wb_test;";
    std::string sqlite_query = "SELECT " + col_name + " FROM wb_test;";
    return std::make_pair(omnisci_query, sqlite_query);
  };
  std::vector<std::string> col_names{"i1", "i2", "i4", "i8", "d", "f", "dc", "n"};
  std::vector<std::string> wrong_partition_expr{"i1-1", "-1*i1", "d", "f", "dc", "n"};
  std::vector<std::string> null_col_names{
      "i1n", "i2n", "i4n", "i8n", "dn", "fn", "dcn", "nn"};
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    for (auto& col : col_names) {
      std::string q = "SELECT WIDTH_BUCKET(i1, i1, " + col + ", i1) FROM wb_test;";
      EXPECT_ANY_THROW(run_simple_agg(q, dt));
    }

    for (auto& col : col_names) {
      std::string q =
          "SELECT WIDTH_BUCKET(i1, " + col + ", " + col + ", i1) FROM wb_test;";
      EXPECT_ANY_THROW(run_simple_agg(q, dt));
    }

    for (auto& expr : wrong_partition_expr) {
      std::string q = "SELECT WIDTH_BUCKET(i1, i1*2, i1*3, " + expr + ") FROM wb_test;";
      EXPECT_ANY_THROW(run_simple_agg(q, dt)) << q;
    }

    for (size_t i = 0; i < col_names.size(); ++i) {
      auto col = col_names[i];
      auto ncol = null_col_names[i];
      std::string q1 =
          "SELECT WIDTH_BUCKET(i1, " + col + ", " + ncol + ", i1) FROM wb_test;";
      std::string q2 =
          "SELECT WIDTH_BUCKET(i1, " + ncol + ", " + col + ", i1) FROM wb_test;";
      EXPECT_ANY_THROW(run_simple_agg(q1, dt));
      EXPECT_ANY_THROW(run_simple_agg(q2, dt));
    }

    for (auto& ncol : null_col_names) {
      std::string q = "SELECT WIDTH_BUCKET(i1, i1*2, i1*3," + ncol + ") FROM wb_test;";
      EXPECT_ANY_THROW(run_simple_agg(q, dt));
    }

    EXPECT_EQ(int64_t(5),
              v<int64_t>(run_simple_agg(
                  "SELECT WIDTH_BUCKET(i1*5, i4, i4*10, i4*10) FROM wb_test;", dt)));
    EXPECT_EQ(int64_t(6),
              v<int64_t>(run_simple_agg(
                  "SELECT WIDTH_BUCKET(i1*5, i4*10, i4, i4*10) FROM wb_test;", dt)));
    EXPECT_EQ(int64_t(5),
              v<int64_t>(run_simple_agg(
                  "SELECT WIDTH_BUCKET(i1*5, i4*10, i4, i4*10) - 1 FROM wb_test;", dt)));
    EXPECT_EQ(
        int64_t(-1),
        v<int64_t>(run_simple_agg("SELECT WIDTH_BUCKET(i1*5, i4, i4*10, i4*10) - "
                                  "WIDTH_BUCKET(i1*5, i4*10, i4, i4*10) FROM wb_test;",
                                  dt)));
    EXPECT_EQ(int64_t(12),
              v<int64_t>(run_simple_agg(
                  "select width_bucket(i2+15, cast(i2*(d+1) as int), cast(i4*(n+25) as "
                  "int), cast(i8*20 as int)) from wb_test;",
                  dt)));
    EXPECT_EQ(
        int64_t(1),
        v<int64_t>(run_simple_agg(
            "SELECT WIDTH_BUCKET(i1, i4, i4*10, i4*10) b FROM wb_test GROUP BY b;", dt)));
    EXPECT_EQ(
        int64_t(0),
        v<int64_t>(run_simple_agg(
            "SELECT WIDTH_BUCKET(i1-1, i4, i4*10, i4*10) b FROM wb_test GROUP BY b;",
            dt)));
    EXPECT_EQ(
        int64_t(11),
        v<int64_t>(run_simple_agg(
            "SELECT WIDTH_BUCKET(i1+11, i4, i4*10, i4*10) b FROM wb_test GROUP BY b;",
            dt)));

    for (auto& col : null_col_names) {
      auto queries = test_queries(col);
      c(queries.first, queries.second, dt);
    }
  }
  dropTable("wb_test");
  run_sqlite_query(drop);
}

TEST_F(Select, WidthBucketWithGroupBy) {
  createTable("wb_test_nullable", {{"val", SQLTypeInfo(kINT)}});
  createTable("wb_test_non_nullable", {{"val", SQLTypeInfo(kINT, true)}});
  createTable("wb_test", {{"val", SQLTypeInfo(kINT)}});
  insertCsvValues("wb_test_nullable", "1\n2\n3");
  insertJsonValues("wb_test_nullable", "{\"val\": null}");
  insertCsvValues("wb_test_non_nullable", "1\n2\n3");
  insertCsvValues("wb_test", "1\n2\n3");
  std::vector<std::string> drop_tables;
  drop_tables.emplace_back("DROP TABLE IF EXISTS wb_test_nullable;");
  drop_tables.emplace_back("DROP TABLE IF EXISTS wb_test_non_nullable;");
  drop_tables.emplace_back("DROP TABLE IF EXISTS wb_test;");
  std::vector<std::string> create_tables;
  create_tables.emplace_back("CREATE TABLE wb_test_nullable (val int);");
  create_tables.emplace_back("CREATE TABLE wb_test_non_nullable (val int not null);");
  create_tables.emplace_back("CREATE TABLE wb_test (val int);");
  std::vector<std::string> populate_tables;
  for (int i = 1; i < 4; ++i) {
    populate_tables.push_back("INSERT INTO wb_test_nullable VALUES(" + std::to_string(i) +
                              ");");
    populate_tables.push_back("INSERT INTO wb_test_non_nullable VALUES(" +
                              std::to_string(i) + ");");
    populate_tables.push_back("INSERT INTO wb_test VALUES(" + std::to_string(i) + ");");
  }
  populate_tables.emplace_back("INSERT INTO wb_test_nullable VALUES(null);");
  for (const auto& stmt : drop_tables) {
    run_sqlite_query(stmt);
  }
  for (const auto& stmt : create_tables) {
    run_sqlite_query(stmt);
  }
  for (const auto& stmt : populate_tables) {
    run_sqlite_query(stmt);
  }

  auto query_gen = [&](const std::string& table_name, bool for_omnisci, bool has_filter) {
    std::ostringstream oss;
    oss << "SELECT SUM(cnt) FROM (SELECT ";
    if (for_omnisci) {
      oss << "WIDTH_BUCKET(val, 1, 3, 3) b";
    } else {
      oss << "val b";
    }
    oss << ", COUNT(1) cnt FROM ";
    oss << table_name;
    if (has_filter) {
      oss << " WHERE val < 3";
    }
    oss << " GROUP BY b);";
    return oss.str();
  };

  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c(query_gen("wb_test_nullable", true, false),
      query_gen("wb_test_nullable", false, false),
      dt);
    c(query_gen("wb_test_nullable", true, true),
      query_gen("wb_test_nullable", false, true),
      dt);
    c(query_gen("wb_test_non_nullable", true, false),
      query_gen("wb_test_non_nullable", false, false),
      dt);
    c(query_gen("wb_test_non_nullable", true, true),
      query_gen("wb_test_non_nullable", false, true),
      dt);
    c(query_gen("wb_test", true, false), query_gen("wb_test", false, false), dt);
    c(query_gen("wb_test", true, true), query_gen("wb_test", false, true), dt);
  }
  for (const auto& stmt : drop_tables) {
    run_sqlite_query(stmt);
  }
  dropTable("wb_test_nullable");
  dropTable("wb_test_non_nullable");
  dropTable("wb_test");
}

TEST_F(Select, WidthBucketNullability) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    {
      // no results
      auto result = run_multiple_agg(
          R"(SELECT WIDTH_BUCKET(x, 1, 20, 3) AS w, COUNT(*) AS n FROM test GROUP BY w HAVING (w IS null);)",
          dt);
      EXPECT_EQ(result->rowCount(), size_t(0));
    }

    {
      // one null row
      // no results
      auto result = run_multiple_agg(
          R"(SELECT WIDTH_BUCKET(ofd, 1, 20, 3) AS w, COUNT(*) AS n FROM test GROUP BY w HAVING (w IS null);)",
          dt);
      EXPECT_EQ(result->rowCount(), size_t(1));
    }
  }
}

TEST_F(Select, CountWithLimitAndOffset) {
  createTable("count_test", {{"val", SQLTypeInfo(kINT)}});
  insertCsvValues("count_test", "0\n1\n2\n3\n4\n5\n6\n7\n8\n9");

  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    EXPECT_EQ(int64_t(10),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test);", dt)));
    EXPECT_EQ(int64_t(9),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test OFFSET 1);", dt)));
    EXPECT_EQ(int64_t(8),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test OFFSET 2);", dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 1);", dt)));
    EXPECT_EQ(int64_t(2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 2);", dt)));
    EXPECT_EQ(
        int64_t(1),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 1 OFFSET 1);", dt)));
    EXPECT_EQ(
        int64_t(2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 2 OFFSET 1);", dt)));
    EXPECT_EQ(
        int64_t(1),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 2 OFFSET 9);", dt)));
    EXPECT_EQ(
        int64_t(2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 2 OFFSET 8);", dt)));
    EXPECT_EQ(
        int64_t(1),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 1 OFFSET 8);", dt)));

    EXPECT_EQ(int64_t(10),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test GROUP BY val);", dt)));
    EXPECT_EQ(
        int64_t(9),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test GROUP BY val OFFSET 1);",
            dt)));
    EXPECT_EQ(
        int64_t(8),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test GROUP BY val OFFSET 2);",
            dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test GROUP BY val LIMIT 1);",
                  dt)));
    EXPECT_EQ(int64_t(2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test GROUP BY val LIMIT 2);",
                  dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT * FROM count_test "
                                        "GROUP BY val LIMIT 1 OFFSET 1);",
                                        dt)));
    EXPECT_EQ(int64_t(2),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT * FROM count_test "
                                        "GROUP BY val LIMIT 2 OFFSET 1);",
                                        dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT * FROM count_test "
                                        "GROUP BY val LIMIT 2 OFFSET 9);",
                                        dt)));
    EXPECT_EQ(int64_t(2),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT * FROM count_test "
                                        "GROUP BY val LIMIT 2 OFFSET 8);",
                                        dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT * FROM count_test "
                                        "GROUP BY val LIMIT 1 OFFSET 8);",
                                        dt)));
  }

  // now increase the data
  {
    std::stringstream ss;
    // num_sets (-1) because data started out with 1 set of (10) items
    int64_t num_sets = static_cast<int64_t>(pow(2, 16)) - 1;
    for (int i = 0; i < num_sets; i++) {
      for (int j = 0; j < 10; j++) {
        ss << j << "\n";
      }
    }
    insertCsvValues("count_test", ss.str());
  }

  int64_t size = static_cast<int64_t>(pow(2, 16) * 10);
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    EXPECT_EQ(int64_t(size),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test);", dt)));
    EXPECT_EQ(int64_t(size - 1),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test OFFSET 1);", dt)));
    EXPECT_EQ(int64_t(size - 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test OFFSET 2);", dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 1);", dt)));
    EXPECT_EQ(int64_t(2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 2);", dt)));
    EXPECT_EQ(
        int64_t(1),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 1 OFFSET 1);", dt)));
    EXPECT_EQ(
        int64_t(2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 2 OFFSET 1);", dt)));
    EXPECT_EQ(
        int64_t(2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 2 OFFSET 9);", dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 2 OFFSET " +
                      std::to_string(size - 1) + ");",
                  dt)));
    EXPECT_EQ(
        int64_t(2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 2 OFFSET 8);", dt)));
    EXPECT_EQ(
        int64_t(1),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT * FROM count_test LIMIT 1 OFFSET 8);", dt)));

    EXPECT_EQ(
        int64_t(size),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT rowid FROM count_test GROUP BY rowid);", dt)));
    EXPECT_EQ(int64_t(size - 1),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT rowid FROM "
                                        "count_test GROUP BY rowid OFFSET 1);",
                                        dt)));
    EXPECT_EQ(int64_t(size - 2),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT rowid FROM "
                                        "count_test GROUP BY rowid OFFSET 2);",
                                        dt)));
    EXPECT_EQ(
        int64_t(1),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT rowid FROM count_test GROUP BY rowid LIMIT 1);",
            dt)));
    EXPECT_EQ(
        int64_t(2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM (SELECT rowid FROM count_test GROUP BY rowid LIMIT 2);",
            dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT rowid FROM "
                                        "count_test GROUP BY rowid LIMIT 1 OFFSET 1);",
                                        dt)));
    EXPECT_EQ(int64_t(2),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT rowid FROM "
                                        "count_test GROUP BY rowid LIMIT 2 OFFSET 1);",
                                        dt)));
    EXPECT_EQ(int64_t(2),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT rowid FROM "
                                        "count_test GROUP BY rowid LIMIT 2 OFFSET 9);",
                                        dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT rowid FROM "
                                        "count_test GROUP BY rowid LIMIT 2 OFFSET " +
                                            std::to_string(size - 1) + ");",
                                        dt)));
    EXPECT_EQ(int64_t(2),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT rowid FROM "
                                        "count_test GROUP BY rowid LIMIT 2 OFFSET 8);",
                                        dt)));
    EXPECT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM (SELECT rowid FROM "
                                        "count_test GROUP BY rowid LIMIT 1 OFFSET 8);",
                                        dt)));
  }
  dropTable("count_test");
}

TEST_F(Select, CountDistinct) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(distinct x) FROM test;", dt);
    c("SELECT COUNT(distinct b) FROM test;", dt);
    c("SELECT COUNT(distinct f) FROM test;", dt);
    c("SELECT COUNT(distinct d) FROM test;", dt);
    c("SELECT COUNT(distinct str) FROM test;", dt);
    c("SELECT COUNT(distinct ss) FROM test;", dt);
    c("SELECT COUNT(distinct x + 1) FROM test;", dt);
    c("SELECT COUNT(*), MIN(x), MAX(x), AVG(y), SUM(z) AS n, COUNT(distinct x) FROM test "
      "GROUP BY y ORDER BY n;",
      dt);
    c("SELECT COUNT(*), MIN(x), MAX(x), AVG(y), SUM(z) AS n, COUNT(distinct x + 1) FROM "
      "test GROUP BY y ORDER BY n;",
      dt);
    c("SELECT COUNT(distinct dd) AS n FROM test GROUP BY y ORDER BY n;", dt);
    c("SELECT z, str, AVG(z), COUNT(distinct z) FROM test GROUP BY z, str ORDER BY z, "
      "str;",
      dt);
    c("SELECT AVG(z), COUNT(distinct x) AS dx FROM test GROUP BY y HAVING dx > 1;", dt);
    c("SELECT z, str, COUNT(distinct f) FROM test GROUP BY z, str ORDER BY str DESC;",
      dt);
    c("SELECT COUNT(distinct x * (50000 - 1)) FROM test;", dt);
    EXPECT_THROW(run_multiple_agg("SELECT COUNT(distinct real_str) FROM test;", dt),
                 std::runtime_error);  // Strings must be dictionary-encoded
                                       // for COUNT(DISTINCT).
  }
}

TEST_F(Select, ApproxCountDistinct) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT APPROX_COUNT_DISTINCT(x) FROM test;",
      "SELECT COUNT(distinct x) FROM test;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(x) FROM test_empty;",
      "SELECT COUNT(distinct x) FROM test_empty;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(x) FROM test_one_row;",
      "SELECT COUNT(distinct x) FROM test_one_row;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(b) FROM test;",
      "SELECT COUNT(distinct b) FROM test;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(f) FROM test;",
      "SELECT COUNT(distinct f) FROM test;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(d) FROM test;",
      "SELECT COUNT(distinct d) FROM test;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(str) FROM test;",
      "SELECT COUNT(distinct str) FROM test;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(null_str) FROM test;",
      "SELECT COUNT(distinct null_str) FROM test;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(ss) FROM test WHERE ss IS NOT NULL;",
      "SELECT COUNT(distinct ss) FROM test;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(x + 1) FROM test;",
      "SELECT COUNT(distinct x + 1) FROM test;",
      dt);
    c("SELECT COUNT(*), MIN(x), MAX(x), AVG(y), SUM(z) AS n, APPROX_COUNT_DISTINCT(x) "
      "FROM test GROUP BY y ORDER "
      "BY n;",
      "SELECT COUNT(*), MIN(x), MAX(x), AVG(y), SUM(z) AS n, COUNT(distinct x) FROM test "
      "GROUP BY y ORDER BY n;",
      dt);
    c("SELECT COUNT(*), MIN(x), MAX(x), AVG(y), SUM(z) AS n, APPROX_COUNT_DISTINCT(x + "
      "1) FROM test GROUP BY y "
      "ORDER BY n;",
      "SELECT COUNT(*), MIN(x), MAX(x), AVG(y), SUM(z) AS n, COUNT(distinct x + 1) FROM "
      "test GROUP BY y ORDER BY n;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(dd) AS n FROM test GROUP BY y ORDER BY n;",
      "SELECT COUNT(distinct dd) AS n FROM test GROUP BY y ORDER BY n;",
      dt);
    c("SELECT z, str, AVG(z), APPROX_COUNT_DISTINCT(z) FROM test GROUP BY z, str ORDER "
      "BY z;",
      "SELECT z, str, AVG(z), COUNT(distinct z) FROM test GROUP BY z, str ORDER BY z;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(null_str) AS n FROM test GROUP BY x ORDER BY n;",
      "SELECT COUNT(distinct null_str) AS n FROM test GROUP BY x ORDER BY n;",
      dt);
    c("SELECT z, APPROX_COUNT_DISTINCT(null_str) AS n FROM test GROUP BY z ORDER BY z, "
      "n;",
      "SELECT z, COUNT(distinct null_str) AS n FROM test GROUP BY z ORDER BY z, n;",
      dt);
    c("SELECT AVG(z), APPROX_COUNT_DISTINCT(x) AS dx FROM test GROUP BY y HAVING dx > 1;",
      "SELECT AVG(z), COUNT(distinct x) AS dx FROM test GROUP BY y HAVING dx > 1;",
      dt);
    c("SELECT approx_value, exact_value FROM (SELECT APPROX_COUNT_DISTINCT(x) AS "
      "approx_value FROM test), (SELECT "
      "COUNT(distinct x) AS exact_value FROM test);",
      "SELECT approx_value, exact_value FROM (SELECT COUNT(distinct x) AS approx_value "
      "FROM test), (SELECT "
      "COUNT(distinct x) AS exact_value FROM test);",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(x, 1) FROM test;",
      "SELECT COUNT(distinct x) FROM test;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(b, 10) FROM test;",
      "SELECT COUNT(distinct b) FROM test;",
      dt);
    c("SELECT APPROX_COUNT_DISTINCT(f, 20) FROM test;",
      "SELECT COUNT(distinct f) FROM test;",
      dt);
    c("SELECT COUNT(*), MIN(x), MAX(x), AVG(y), SUM(z) AS n, APPROX_COUNT_DISTINCT(x, 1) "
      "FROM test GROUP BY y ORDER "
      "BY n;",
      "SELECT COUNT(*), MIN(x), MAX(x), AVG(y), SUM(z) AS n, COUNT(distinct x) FROM test "
      "GROUP BY y ORDER BY n;",
      dt);
    c("SELECT COUNT(*), MIN(x), MAX(x), AVG(y), SUM(z) AS n, APPROX_COUNT_DISTINCT(x + "
      "1, 1) FROM test GROUP BY y "
      "ORDER BY n;",
      "SELECT COUNT(*), MIN(x), MAX(x), AVG(y), SUM(z) AS n, COUNT(distinct x + 1) FROM "
      "test GROUP BY y ORDER BY n;",
      dt);
    // Test approx_count_distinct buffer allocation with multi-slot targets
    // sqlite does not support SAMPLE, grab the first row only
    c("SELECT SAMPLE(real_str), str, APPROX_COUNT_DISTINCT(x) FROM test WHERE real_str = "
      "'real_bar' GROUP BY str;",
      "SELECT real_str, str, COUNT( distinct x) FROM test WHERE real_str = "
      "'real_bar' GROUP BY str;",
      dt);
    c("SELECT SAMPLE(real_str), str, APPROX_COUNT_DISTINCT(x) FROM test WHERE real_str = "
      "'real_foo' GROUP BY str;",
      "SELECT real_str, str, COUNT(distinct x) FROM test WHERE real_str = "
      "'real_foo' GROUP BY str, real_str;",
      dt);

    EXPECT_NO_THROW(run_multiple_agg(
        "SELECT APPROX_COUNT_DISTINCT(x), SAMPLE(real_str) FROM test GROUP BY x;", dt));
    EXPECT_THROW(
        run_multiple_agg("SELECT APPROX_COUNT_DISTINCT(real_str) FROM test;", dt),
        std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT APPROX_COUNT_DISTINCT(x, 0) FROM test;", dt),
                 std::runtime_error);
  }
}

TEST_F(Select, ApproxMedianSanity) {
  auto dt = ExecutorDeviceType::CPU;
  auto approx_median = [dt](std::string const col) {
    std::string const query = "SELECT APPROX_MEDIAN(" + col + ") FROM test;";
    return v<double>(run_simple_agg(query, dt));
  };
  EXPECT_EQ(-7.5, approx_median("w"));
  EXPECT_EQ(7.0, approx_median("x"));
  EXPECT_EQ(42.5, approx_median("y"));
  EXPECT_EQ(101.0, approx_median("z"));
  EXPECT_EQ(1001.5, approx_median("t"));
  EXPECT_EQ((double(1.1f) + double(1.2f)) / 2, approx_median("f"));
  EXPECT_EQ((double(1.1f) + double(101.2f)) / 2, approx_median("ff"));
  EXPECT_EQ((double(-101.2f) + double(-1000.3f)) / 2, approx_median("fn"));
  EXPECT_EQ(2.3, approx_median("d"));
  EXPECT_EQ(-1111.5, approx_median("dn"));
  EXPECT_EQ((11110.0 / 100 + 22220.0 / 100) / 2, approx_median("dd"));
  EXPECT_EQ((11110.0 / 100 + 22220.0 / 100) / 2, approx_median("dd_notnull"));
  EXPECT_EQ(NULL_DOUBLE, approx_median("u"));
  EXPECT_EQ(2147483647.0, approx_median("ofd"));
  EXPECT_EQ(-2147483647.5, approx_median("ufd"));
  EXPECT_EQ(4611686018427387904.0, approx_median("ofq"));
  EXPECT_EQ(-4611686018427387904.5, approx_median("ufq"));
  EXPECT_EQ(32767.0, approx_median("smallint_nulls"));
}

TEST_F(Select, ApproxMedianLargeInts) {
  auto dt = ExecutorDeviceType::CPU;
  auto approx_median = [dt](std::string const col) {
    std::string const query =
        "SELECT APPROX_MEDIAN(" + col + ") FROM test_approx_median;";
    return v<double>(run_simple_agg(query, dt));
  };
  createTable("test_approx_median", {{"b", SQLTypeInfo(kBIGINT)}});
  insertCsvValues("test_approx_median", "-9223372036854775807\n9223372036854775807");
  EXPECT_EQ(0.0, approx_median("b"));
  dropTable("test_approx_median");
}

TEST_F(Select, ApproxMedianSort) {
  auto const dt = ExecutorDeviceType::CPU;
  char const* const prefix =
      "SELECT t2.x, APPROX_MEDIAN(t0.x) am FROM coalesce_cols_test_2 t2 LEFT JOIN "
      "coalesce_cols_test_0 t0 ON t2.x=t0.x GROUP BY t2.x ORDER BY am ";
  std::vector<std::string> const tests{
      "ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST"};
  constexpr size_t NROWS = 20;
  for (size_t t = 0; t < tests.size(); ++t) {
    std::string const query = prefix + tests[t] + ", x;";
    auto rows = run_multiple_agg(query, dt);
    EXPECT_EQ(rows->colCount(), 2u) << query;
    EXPECT_EQ(rows->rowCount(), NROWS) << query;
    for (size_t i = 0; i < NROWS; ++i) {
      switch (t) {
        case 0:
          if (i < 10) {
            EXPECT_EQ(v<int64_t>(rows->getRowAt(i, 0, true)), int64_t(i) + 10)
                << query << "i=" << i;
            EXPECT_EQ(v<double>(rows->getRowAt(i, 1, true)), NULL_DOUBLE)
                << query << "i=" << i;
          } else {
            EXPECT_EQ(v<int64_t>(rows->getRowAt(i, 0, true)), int64_t(i) - 10)
                << query << "i=" << i;
            EXPECT_EQ(v<double>(rows->getRowAt(i, 1, true)), double(i) - 10)
                << query << "i=" << i;
          }
          break;
        case 1:
          EXPECT_EQ(v<int64_t>(rows->getRowAt(i, 0, true)), int64_t(i))
              << query << "i=" << i;
          if (i < 10) {
            EXPECT_EQ(v<double>(rows->getRowAt(i, 1, true)), double(i))
                << query << "i=" << i;
          } else {
            EXPECT_EQ(v<double>(rows->getRowAt(i, 1, true)), NULL_DOUBLE)
                << query << "i=" << i;
          }
          break;
        case 2:
          if (i < 10) {
            EXPECT_EQ(v<int64_t>(rows->getRowAt(i, 0, true)), int64_t(i) + 10)
                << query << "i=" << i;
            EXPECT_EQ(v<double>(rows->getRowAt(i, 1, true)), NULL_DOUBLE)
                << query << "i=" << i;
          } else {
            EXPECT_EQ(v<int64_t>(rows->getRowAt(i, 0, true)), 19 - int64_t(i))
                << query << "i=" << i;
            EXPECT_EQ(v<double>(rows->getRowAt(i, 1, true)), 19 - double(i))
                << query << "i=" << i;
          }
          break;
        case 3:
          if (i < 10) {
            EXPECT_EQ(v<int64_t>(rows->getRowAt(i, 0, true)), 9 - int64_t(i))
                << query << "i=" << i;
            EXPECT_EQ(v<double>(rows->getRowAt(i, 1, true)), 9 - double(i))
                << query << "i=" << i;
          } else {
            EXPECT_EQ(v<int64_t>(rows->getRowAt(i, 0, true)), int64_t(i))
                << query << "i=" << i;
            EXPECT_EQ(v<double>(rows->getRowAt(i, 1, true)), NULL_DOUBLE)
                << query << "i=" << i;
          }
          break;
        default:
          EXPECT_TRUE(false) << t;
      }
    }
  }
}

// APPROX_PERCENTILE is exact when the number of rows is low.
TEST_F(Select, ApproxPercentileExactValues) {
  auto const dt = ExecutorDeviceType::CPU;
  // clang-format off
  double tests[][2]{{0.0, 2.2}, {0.25, 2.2}, {0.45, 2.2}, {0.5, 2.3}, {0.55, 2.4},
                    {0.7, 2.4}, {0.75, 2.5}, {0.8, 2.6}, {1.0, 2.6}};
  // clang-format on
  for (auto test : tests) {
    std::stringstream query;
    query << "SELECT APPROX_PERCENTILE(d," << test[0] << ") FROM test;";
    EXPECT_EQ(test[1], v<double>(run_simple_agg(query.str(), dt)));
  }
}

// APPROX_QUANTILE is exact when the number of rows is low.
TEST_F(Select, ApproxQuantileExactValues) {
  auto const dt = ExecutorDeviceType::CPU;
  // clang-format off
  double tests[][2]{{0.0, 2.2}, {0.25, 2.2}, {0.45, 2.2}, {0.5, 2.3}, {0.55, 2.4},
                    {0.7, 2.4}, {0.75, 2.5}, {0.8, 2.6}, {1.0, 2.6}};
  // clang-format on
  for (auto test : tests) {
    std::stringstream query;
    query << "SELECT APPROX_QUANTILE(d," << test[0] << ") FROM test;";
    EXPECT_EQ(test[1], v<double>(run_simple_agg(query.str(), dt)));
  }
}

TEST_F(Select, ApproxPercentileMinMax) {
  auto const dt = ExecutorDeviceType::CPU;
  // clang-format off
  char const* cols[]{"w", "x", "y", "z", "t", "f", "ff", "fn", "d", "dn", "dd",
                     "dd_notnull", "u", "ofd", "ufd", "ofq", "ufq", "smallint_nulls"};
  // clang-format on
  for (std::string col : cols) {
    c("SELECT APPROX_PERCENTILE(" + col + ",0) FROM test;",
      // MIN(ofq) = -1 but MIN(CAST(ofq AS DOUBLE)) = -2^63 due to null sentinel logic
      //"SELECT CAST(MIN(" + col + ") AS DOUBLE) FROM test;",
      "SELECT MIN(CAST(" + col + " AS DOUBLE)) FROM test;",
      dt);
    c("SELECT APPROX_PERCENTILE(" + col + ",1) FROM test;",
      "SELECT CAST(MAX(" + col + ") AS DOUBLE) FROM test;",
      dt);
  }
}

TEST_F(Select, ApproxPercentileSubqueries) {
  auto const dt = ExecutorDeviceType::CPU;
  const char* query =
      "SELECT MIN(am) FROM (SELECT x, APPROX_MEDIAN(w) AS am FROM test GROUP BY x);";
  EXPECT_EQ(-8.0, v<double>(run_simple_agg(query, dt)));
  query =
      "SELECT MIN(am) FROM (SELECT x, APPROX_PERCENTILE(w,0.5) AS am FROM test GROUP "
      "BY x);";
  EXPECT_EQ(-8.0, v<double>(run_simple_agg(query, dt)));
  query = "SELECT MAX(am) FROM (SELECT x, APPROX_MEDIAN(w) AS am FROM test GROUP BY x);";
  EXPECT_EQ(-7.0, v<double>(run_simple_agg(query, dt)));
  query =
      "SELECT MAX(am) FROM (SELECT x, APPROX_PERCENTILE(w,0.5) AS am FROM test GROUP "
      "BY x);";
  EXPECT_EQ(-7.0, v<double>(run_simple_agg(query, dt)));
}

// Immerse invokes sql_validate which requires testing.
TEST_F(Select, ApproxPercentileValidate) {
  auto const dt = ExecutorDeviceType::CPU;
  auto eo = getExecutionOptions(true);
  eo.just_validate = true;
  // APPROX_MEDIAN
  char const* query = "SELECT APPROX_MEDIAN(x) FROM test;";
  std::shared_ptr<ResultSet> rows = runSqlQuery(query, dt, std::move(eo)).getRows();
  auto crt_row = rows->getNextRow(true, true);
  CHECK_EQ(1u, crt_row.size()) << query;
  EXPECT_EQ(NULL_DOUBLE, v<double>(crt_row[0]));
  // APPROX_PERCENTILE
  query = "SELECT APPROX_PERCENTILE(x,0.1) FROM test;";
  rows = runSqlQuery(query, dt, std::move(eo)).getRows();
  crt_row = rows->getNextRow(true, true);
  CHECK_EQ(1u, crt_row.size()) << query;
  EXPECT_EQ(NULL_DOUBLE, v<double>(crt_row[0]));
}

TEST_F(Select, ScanNoAggregation) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT * FROM test ORDER BY x ASC, y ASC;", dt);
    c("SELECT t.* FROM test t ORDER BY x ASC, y ASC;", dt);
    c("SELECT x, z, t FROM test ORDER BY x ASC, y ASC;", dt);
    c("SELECT x, y, x + 1 FROM test ORDER BY x ASC, y ASC;", dt);
    c("SELECT x + z, t FROM test WHERE x <> 7 AND y > 42;", dt);
    c("SELECT * FROM test WHERE x > 8;", dt);
    c("SELECT fx FROM test WHERE fx IS NULL;", dt);
    c("SELECT z,t,f,m,d,x,real_str,u,z,y FROM test WHERE z = -78 AND t = "
      "1002 AND x >= 8 AND y = 43 AND d > 1.0 AND f > 1.0 AND real_str = 'real_bar' "
      "ORDER BY f ASC;",
      dt);
    c("SELECT * FROM test WHERE d > 2.4 AND real_str IS NOT NULL AND fixed_str IS NULL "
      "AND z = 102 AND fn < 0 AND y = 43 AND t >= 0 AND x <> 8;",
      dt);
    c("SELECT * FROM test WHERE d > 2.4 AND real_str IS NOT NULL AND fixed_str IS NULL "
      "AND z = 102 AND fn < 0 AND y = 43 AND t >= 0 AND x = 8;",
      dt);
    c("SELECT real_str,f,fn,y,d,x,z,str,fixed_str,t,dn FROM test WHERE f IS NOT NULL AND "
      "y IS NOT NULL AND str = 'bar' AND x >= 7 AND t < 1003 AND z < 0;",
      dt);
    c("SELECT t,y,str,real_str,d,fixed_str,dn,fn,z,f,x FROM test WHERE f IS NOT NULL AND "
      "y IS NOT NULL AND str = 'baz' AND x >= 7 AND t < 1003 AND f > 1.2 LIMIT 1;",
      dt);
    c("SELECT fn,real_str,str,z,d,x,fixed_str,dn,y,t,f FROM test WHERE f < 1.4 AND "
      "real_str IS NOT NULL AND fixed_str IS NULL AND z = 102 AND dn < 0 AND y = 43;",
      dt);
    c("SELECT dn,str,y,z,fixed_str,fn,d,real_str,t,f,x FROM test WHERE z < 0 AND f < 2 "
      "AND d > 2.0 AND fn IS NOT NULL AND dn < 2000 AND str IS NOT NULL AND fixed_str = "
      "'bar' AND real_str = 'real_bar' AND t >= 1001 AND y >= 42 AND x > 7 ORDER BY z, "
      "x;",
      dt);
    c("SELECT z,f,d,str,real_str,x,dn,y,t,fn,fixed_str FROM test WHERE fn IS NULL AND dn "
      "IS NULL AND x >= 0 AND real_str = 'real_foo' ORDER BY y;",
      dt);
  }
}

TEST_F(Select, OrderBy) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    const auto rows = run_multiple_agg(
        "SELECT x, y, z + t, x * y AS m FROM test ORDER BY 3 desc LIMIT 5;", dt);
    CHECK_EQ(rows->rowCount(), std::min(size_t(5), static_cast<size_t>(g_num_rows)) + 0);
    CHECK_EQ(rows->colCount(), size_t(4));
    for (size_t row_idx = 0; row_idx < rows->rowCount(); ++row_idx) {
      ASSERT_TRUE(v<int64_t>(rows->getRowAt(row_idx, 0, true)) == 8 ||
                  v<int64_t>(rows->getRowAt(row_idx, 0, true)) == 7);
      ASSERT_EQ(v<int64_t>(rows->getRowAt(row_idx, 1, true)), 43);
      ASSERT_EQ(v<int64_t>(rows->getRowAt(row_idx, 2, true)), 1104);
      ASSERT_TRUE(v<int64_t>(rows->getRowAt(row_idx, 3, true)) == 344 ||
                  v<int64_t>(rows->getRowAt(row_idx, 3, true)) == 301);
    }
    c("SELECT x, COUNT(distinct y) AS n FROM test GROUP BY x ORDER BY n DESC;", dt);
    c("SELECT x x1, x, COUNT(*) AS val FROM test GROUP BY x HAVING val > 5 ORDER BY val "
      "DESC LIMIT 5;",
      dt);
    c("SELECT ufd, COUNT(*) n FROM test GROUP BY ufd, str ORDER BY ufd, n;", dt);
    c("SELECT -x, COUNT(*) FROM test GROUP BY x ORDER BY x DESC;", dt);
    c("SELECT real_str FROM test WHERE real_str LIKE '%real%' ORDER BY real_str ASC;",
      dt);
    c("SELECT ss FROM test GROUP by ss ORDER BY ss ASC NULLS FIRST;",
      "SELECT ss FROM test GROUP by ss ORDER BY ss ASC;",
      dt);
    c("SELECT str, COUNT(*) n FROM test WHERE x < 0 GROUP BY str ORDER BY n DESC LIMIT "
      "5;",
      dt);
    c("SELECT x FROM test ORDER BY x LIMIT 50;", dt);
    c("SELECT x FROM test ORDER BY x LIMIT 5;", dt);
    c("SELECT x FROM test ORDER BY x ASC LIMIT 20;", dt);
    c("SELECT dd FROM test ORDER BY dd ASC LIMIT 20;", dt);
    c("SELECT f FROM test ORDER BY f ASC LIMIT 5;", dt);
    c("SELECT f FROM test ORDER BY f ASC LIMIT 20;", dt);
    c("SELECT fn as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 5;",
      "SELECT fn as k FROM test ORDER BY k ASC LIMIT 5;",
      dt);
    c("SELECT fn as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT fn as k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    c("SELECT dn as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 5;",
      "SELECT dn as k FROM test ORDER BY k ASC LIMIT 5;",
      dt);
    c("SELECT dn as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT dn as k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    c("SELECT ff as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 5;",
      "SELECT ff as k FROM test ORDER BY k ASC LIMIT 5;",
      dt);
    c("SELECT ff as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT ff as k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    c("SELECT d as k FROM test ORDER BY k ASC LIMIT 5;", dt);
    c("SELECT d as k FROM test ORDER BY k ASC LIMIT 20;", dt);
    c("SELECT dn as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 5;",
      "SELECT dn as k FROM test ORDER BY k ASC LIMIT 5;",
      dt);
    c("SELECT dn as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT dn as k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    c("SELECT ofq AS k FROM test ORDER BY k ASC NULLS FIRST LIMIT 5;",
      "SELECT ofq as k FROM test ORDER BY k ASC LIMIT 5;",
      dt);
    c("SELECT ofq AS k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT ofq as k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    c("SELECT ufq as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 5;",
      "SELECT ufq as k FROM test ORDER BY k ASC LIMIT 5;",
      dt);
    c("SELECT ufq as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT ufq as k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    c("SELECT CAST(ofd AS FLOAT) as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 5;",
      "SELECT CAST(ofd AS FLOAT) as k FROM test ORDER BY k ASC LIMIT 5;",
      dt);
    c("SELECT CAST(ofd AS FLOAT) as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT CAST(ofd AS FLOAT) as k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    c("SELECT CAST(ufd AS FLOAT) as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 5;",
      "SELECT CAST(ufd AS FLOAT) as k FROM test ORDER BY k ASC LIMIT 5;",
      dt);
    c("SELECT CAST(ufd AS FLOAT) as k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT CAST(ufd AS FLOAT) as k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    c("SELECT m AS k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT m AS k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    c("SELECT n AS k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT n AS k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    c("SELECT o AS k FROM test ORDER BY k ASC NULLS FIRST LIMIT 20;",
      "SELECT o AS k FROM test ORDER BY k ASC LIMIT 20;",
      dt);
    for (std::string order : {"ASC", "DESC"}) {
      c("SELECT d, MAX(f) FROM test WHERE f IS NOT NULL GROUP BY d ORDER BY 2 " + order +
            " LIMIT "
            "1;",
        dt);
      c("SELECT d, AVG(f) FROM test WHERE f IS NOT NULL GROUP BY d ORDER BY 2 " + order +
            " LIMIT "
            "1;",
        dt);
      c("SELECT d, SUM(f) FROM test WHERE f IS NOT NULL GROUP BY d ORDER BY 2 " + order +
            " LIMIT "
            "1;",
        dt);
      c("SELECT d, MAX(f) FROM test GROUP BY d ORDER BY 2 " + order + " LIMIT 1;", dt);
      c("SELECT x, y, MAX(f) FROM test GROUP BY x, y ORDER BY 3 " + order + " LIMIT 1;",
        dt);
      c("SELECT x, y, SUM(f) FROM test WHERE f IS NOT NULL GROUP BY x, y ORDER BY 3 " +
            order + " LIMIT 1;",
        dt);
      for (std::string nulls : {" NULLS LAST", " NULLS FIRST"}) {
        char const* const prefix =
            "SELECT t2.x, t0.x FROM coalesce_cols_test_2 t2 LEFT JOIN "
            "coalesce_cols_test_0 t0 ON t2.x=t0.x ORDER BY t0.x ";
        std::string query = prefix + order + nulls + ", t2.x ASC NULLS LAST;";
        c(query, dt);
      }
    }
    c("SELECT * FROM ( SELECT x, y FROM test ORDER BY x, y ASC NULLS FIRST LIMIT 10 ) t0 "
      "LIMIT 5;",
      "SELECT * FROM ( SELECT x, y FROM test ORDER BY x, y ASC LIMIT 10 ) t0 LIMIT 5;",
      dt);
    c(R"(SELECT str, COUNT(*) FROM test GROUP BY str ORDER BY 2 DESC NULLS FIRST LIMIT 50 OFFSET 10;)",
      R"(SELECT str, COUNT(*) FROM test GROUP BY str ORDER BY 2 DESC LIMIT 50 OFFSET 10;)",
      dt);
  }
}

TEST_F(Select, VariableLengthOrderBy) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT real_str FROM test ORDER BY real_str;", dt);
    EXPECT_THROW(
        run_multiple_agg("SELECT arr_float FROM array_test ORDER BY arr_float;", dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg("SELECT arr3_i16 FROM array_test ORDER BY arr3_i16 DESC;", dt),
        std::runtime_error);
  }
}

TEST_F(Select, TopKHeap) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT str, x FROM proj_top ORDER BY x DESC LIMIT 1;", dt);
  }
}

TEST_F(Select, ComplexQueries) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) * MAX(y) - SUM(z) FROM test;", dt);
    c("SELECT x + y AS a, COUNT(*) * MAX(y) - SUM(z) AS b FROM test WHERE z BETWEEN 100 "
      "AND 200 GROUP BY x, y;",
      dt);
    c("SELECT x + y AS a, COUNT(*) * MAX(y) - SUM(z) AS b FROM test WHERE z BETWEEN 100 "
      "AND 200 "
      "GROUP BY x, y HAVING y > 2 * x AND MIN(y) > MAX(x);",
      dt);
    c("SELECT x + y AS a, COUNT(*) * MAX(y) - SUM(z) AS b FROM test WHERE z BETWEEN 100 "
      "AND 200 "
      "GROUP BY x, y HAVING y > 2 * x AND MIN(y) > MAX(x) + 35;",
      dt);
    c("SELECT x + y AS a, COUNT(*) * MAX(y) - SUM(z) AS b FROM test WHERE z BETWEEN 100 "
      "AND 200 "
      "GROUP BY x, y HAVING y > 2 * x AND MIN(y) > MAX(x) + 36;",
      dt);
    c("SELECT x + y AS a, COUNT(*) * MAX(y) - SUM(z) AS b FROM test "
      "WHERE z BETWEEN 100 AND 200 GROUP BY a, y;",
      dt);
    c("SELECT x, y FROM (SELECT a.str AS str, b.x AS x, a.y AS y FROM test a, join_test "
      "b WHERE a.x = b.x) WHERE str = "
      "'foo' ORDER BY x LIMIT 1;",
      dt);
    const auto rows = run_multiple_agg(
        "SELECT x + y AS a, COUNT(*) * MAX(y) - SUM(z) AS b FROM test "
        "WHERE z BETWEEN 100 AND 200 GROUP BY x, y ORDER BY a DESC LIMIT 2;",
        dt);
    ASSERT_EQ(rows->rowCount(), size_t(2));
    {
      auto crt_row = rows->getNextRow(true, true);
      CHECK_EQ(size_t(2), crt_row.size());
      ASSERT_EQ(v<int64_t>(crt_row[0]), 50);
      ASSERT_EQ(v<int64_t>(crt_row[1]), -295);
    }
    {
      auto crt_row = rows->getNextRow(true, true);
      CHECK_EQ(size_t(2), crt_row.size());
      ASSERT_EQ(v<int64_t>(crt_row[0]), 49);
      ASSERT_EQ(v<int64_t>(crt_row[1]), -590);
    }
    auto empty_row = rows->getNextRow(true, true);
    CHECK(empty_row.empty());
  }
}

TEST_F(Select, MultiStepQueries) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    const auto skip_intermediate_count = config().opts.skip_intermediate_count;
    ScopeGuard reset_skip_intermediate_count = [&skip_intermediate_count] {
      config().opts.skip_intermediate_count = skip_intermediate_count;
    };

    c("SELECT z, (z * SUM(x)) / SUM(y) + 1 FROM test GROUP BY z ORDER BY z;", dt);
    c("SELECT z,COUNT(*), AVG(x) / SUM(y) + 1 FROM test GROUP BY z ORDER BY z;", dt);
  }
}

TEST_F(Select, GroupByPushDownFilterIntoExprRange) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    const auto rows = run_multiple_agg(
        "SELECT b, COUNT(*) AS n FROM test WHERE b GROUP BY b ORDER BY b", dt);
    ASSERT_EQ(
        size_t(1),
        rows->rowCount());  // Sqlite does not have a boolean type, so do this for now
    c("SELECT x, COUNT(*) AS n FROM test WHERE x > 7 GROUP BY x ORDER BY x", dt);
    c("SELECT y, COUNT(*) AS n FROM test WHERE y < 43 GROUP BY y ORDER BY n DESC", dt);
    c("SELECT z, COUNT(*) AS n FROM test WHERE z <= 43 AND y > 10 GROUP BY z ORDER BY n "
      "DESC",
      dt);
    c("SELECT t, SUM(y) AS sum_y FROM test WHERE t < 2000 GROUP BY t ORDER BY t DESC",
      dt);
    c("SELECT t, SUM(y) AS sum_y FROM test WHERE t < 2000 GROUP BY t ORDER BY sum_y", dt);
    c("SELECT o, COUNT(*) as n FROM test WHERE o <= '1999-09-09' GROUP BY o ORDER BY n",
      dt);
    c("SELECT t + x, AVG(x) AS avg_x FROM test WHERE z <= 50 and t < 2000 GROUP BY t + x "
      "ORDER BY avg_x DESC",
      dt);
  }
}

TEST_F(Select, GroupByExprNoFilterNoAggregate) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT x + y AS a FROM test GROUP BY a ORDER BY a;", dt);
    ASSERT_EQ(8,
              v<int64_t>(run_simple_agg("SELECT TRUNCATE(x, 0) AS foo FROM test GROUP BY "
                                        "TRUNCATE(x, 0) ORDER BY foo DESC LIMIT 1;",
                                        dt)));
  }
}

TEST_F(Select, DistinctProjection) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT DISTINCT str FROM test ORDER BY str;", dt);
    c("SELECT DISTINCT(str), SUM(x) FROM test WHERE x > 7 GROUP BY str LIMIT 2;", dt);
  }
}

TEST_F(Select, ProjectionCountOptimization) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c(R"(select count(*) from (select cast(x * 1 as int) as x1 from test) s, (select cast(x * 2 as int) as x2 from test WHERE x = 8) t;)",
      dt);
  }
}

TEST_F(Select, Case) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c("SELECT SUM(CASE WHEN x BETWEEN 6 AND 7 THEN 1 WHEN x BETWEEN 8 AND 9 THEN 2 ELSE "
      "3 END) FROM test;",
      dt);
    c("SELECT SUM(CASE WHEN x BETWEEN 6 AND 7 THEN 1 END) FROM test;", dt);
    c("SELECT SUM(CASE WHEN x BETWEEN 6 AND 7 THEN 1 WHEN x BETWEEN 8 AND 9 THEN 2 ELSE "
      "3 END) "
      "FROM test WHERE CASE WHEN y BETWEEN 42 AND 43 THEN 5 ELSE 4 END > 4;",
      dt);
    ASSERT_EQ(std::numeric_limits<int64_t>::min(),
              v<int64_t>(run_simple_agg(
                  "SELECT SUM(CASE WHEN x BETWEEN 6 AND 7 THEN 1 WHEN x BETWEEN 8 AND 9 "
                  "THEN 2 ELSE 3 END) FROM test "
                  "WHERE CASE WHEN y BETWEEN 44 AND 45 THEN 5 ELSE 4 END > 4;",
                  dt)));
    c("SELECT CASE WHEN x + y > 50 THEN 77 ELSE 88 END AS foo, COUNT(*) FROM test GROUP "
      "BY foo ORDER BY foo;",
      dt);
    ASSERT_EQ(std::numeric_limits<double>::min(),
              v<double>(run_simple_agg(
                  "SELECT SUM(CASE WHEN x BETWEEN 6 AND 7 THEN 1.1 WHEN x BETWEEN 8 AND "
                  "9 THEN 2.2 ELSE 3.3 END) FROM "
                  "test WHERE CASE WHEN y BETWEEN 44 AND 45 THEN 5.1 ELSE 3.9 END > 4;",
                  dt)));
    c("SELECT CASE WHEN x BETWEEN 1 AND 3 THEN 'oops 1' WHEN x BETWEEN 4 AND 6 THEN "
      "'oops 2' ELSE real_str END c "
      "FROM "
      "test ORDER BY c ASC;",
      dt);

    c("SELECT CASE WHEN x BETWEEN 1 AND 3 THEN 'oops 1' WHEN x BETWEEN 4 AND 6 THEN "
      "'oops 2' ELSE str END c FROM "
      "test "
      "ORDER BY c ASC;",
      dt);
    c("SELECT CASE WHEN x BETWEEN 1 AND 7 THEN 'seven' WHEN x BETWEEN 7 AND 10 THEN "
      "'eight' ELSE 'ooops' END c FROM "
      "test ORDER BY c ASC;",
      dt);
    c("SELECT CASE WHEN x BETWEEN 1 AND 7 THEN 'seven' WHEN x BETWEEN 7 AND 10 THEN "
      "real_str ELSE 'ooops' END AS g "
      "FROM test ORDER BY g ASC;",
      dt);
    c("SELECT CASE WHEN x BETWEEN 1 AND 7 THEN 'seven' WHEN x BETWEEN 7 AND 10 THEN str "
      "ELSE 'ooops' END c FROM test "
      "ORDER BY c ASC;",
      dt);
    c("SELECT CASE WHEN x BETWEEN 1 AND 7 THEN 'seven' WHEN x BETWEEN 7 AND 10 THEN "
      "'eight' ELSE 'ooops' END c FROM "
      "test ORDER BY c ASC;",
      dt);
    c("SELECT CASE WHEN x BETWEEN 1 AND 7 THEN str WHEN x BETWEEN 7 AND 10 THEN 'eight' "
      "ELSE 'ooops' END AS g, "
      "COUNT(*) FROM test GROUP BY g ORDER BY g;",
      dt);
    c(R"(SELECT CASE WHEN x = 7 THEN 'a' WHEN x = 8 then 'b' ELSE shared_dict END FROM test GROUP BY 1 ORDER BY 1 ASC;)",
      dt);
    c(R"(SELECT CASE WHEN x = 7 THEN 'a' WHEN x = 8 then str ELSE str END FROM test GROUP BY 1 ORDER BY 1 ASC;)",
      dt);
    c(R"(SELECT CASE WHEN x = 7 THEN 'a' WHEN x = 8 then str ELSE shared_dict END FROM test GROUP BY 1 ORDER BY 1 ASC;)",
      dt);
    c(R"(SELECT COUNT(*) FROM (SELECT CASE WHEN x = 7 THEN 1 WHEN x = 8 then str ELSE shared_dict END FROM test GROUP BY 1);)",
      dt);
    c(R"(SELECT COUNT(*) FROM test WHERE (CASE WHEN x = 7 THEN str ELSE 'b' END) = shared_dict;)",
      dt);
    c(R"(SELECT COUNT(*) FROM test WHERE (CASE WHEN str = 'foo' THEN 'a' WHEN str = 'bar' THEN 'b' ELSE str END) = 'b';)",
      dt);
    c(R"(SELECT str, count(*) FROM test WHERE (CASE WHEN str = 'foo' THEN 'a' WHEN str = 'bar' THEN 'b' ELSE str END) = 'b' GROUP BY str;)",
      dt);
    c(R"(SELECT COUNT(*) FROM test WHERE (CASE WHEN fixed_str = 'foo' THEN 'a' WHEN fixed_str is NULL THEN 'b' ELSE str END) = 'z';)",
      dt);

    c(R"(SELECT CASE WHEN x = 7 THEN 'a' WHEN x = 8 then str ELSE fixed_str END FROM test ORDER BY x, str, fixed_str ASC;)",
      dt);
    c(R"(SELECT CASE WHEN x = 7 THEN 'a' WHEN str <> fixed_str then str ELSE fixed_str END FROM test ORDER BY x, str, fixed_str ASC;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN 'a' WHEN str <> fixed_str then str ELSE fixed_str END AS case_group, COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN 'a' WHEN str <> fixed_str then 'b' ELSE fixed_str END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN 'a' WHEN str <> fixed_str THEN 'b' ELSE NULL END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN 'a' WHEN str <> fixed_str THEN NULL ELSE NULL END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    EXPECT_ANY_THROW(c(
        R"(SELECT CASE WHEN x = 8 THEN NULL WHEN str <> fixed_str THEN NULL ELSE NULL END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
        dt));  // Untyped NULL values are not supported. Please CAST any NULL constants to
               // a type.
    c(R"(SELECT CASE WHEN x = 8 THEN NULL WHEN str <> fixed_str THEN str ELSE fixed_str END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN str WHEN x = 7 THEN 'b' END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN str WHEN x = 7 THEN NULL END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN 'b' WHEN x = 7 THEN str END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN NULL WHEN x = 7 THEN str END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN str ELSE 'b' END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN str ELSE NULL END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN 'b' ELSE str END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN NULL ELSE str END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN 'b' ELSE str END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    // Note that Sqlite does not support TRUE/FALSE boolean literals, use 1/0 instead
    c(R"(SELECT CASE WHEN x = 8 THEN FALSE ELSE (str = fixed_str) END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      R"(SELECT CASE WHEN x = 8 THEN 0 ELSE (str = fixed_str) END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT COUNT(*) FROM test WHERE CASE WHEN x = 7 THEN str WHEN x = 8 THEN fixed_str ELSE 'bar' END = 'foo';)",
      dt);
    c(R"(SELECT COUNT(*) FROM test WHERE CASE WHEN x = 7 THEN str WHEN x = 8 THEN fixed_str ELSE 'bar' END = str;)",
      dt);
    c(R"(SELECT COUNT(*) FROM test WHERE CASE WHEN x = 7 THEN str WHEN x = 8 THEN fixed_str ELSE 'bar' END = fixed_str;)",
      dt);

    c(R"(SELECT CASE WHEN x = 8 THEN 'b' WHEN x = 7 THEN str END AS case_group, COUNT(*) AS n FROM test WHERE CASE WHEN x = 7 THEN str WHEN x = 8 THEN fixed_str ELSE 'bar' END = str GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
      dt);

    // Ensure that transients added during case-statement string dictionary column casts
    // are propogated to aggregator in distributed mode

    c(R"(SELECT CASE WHEN x = 8 THEN str WHEN x = 7 THEN ss END AS case_expr FROM test ORDER BY case_expr ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN str WHEN x = 7 THEN ss END AS case_group, COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN str WHEN x = 7 AND fixed_str IS NOT NULL THEN fixed_str ELSE ss END AS case_group, COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN str WHEN x = 7 AND fixed_str IS NOT NULL THEN 'a' ELSE ss END AS case_group, COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN ss WHEN x = 7 AND fixed_str IS NOT NULL THEN ss ELSE fixed_str END AS case_group, COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST;)",
      dt);
    c(R"(SELECT CASE WHEN x = 8 THEN ss WHEN x = 7 AND fixed_str IS NOT NULL THEN ss ELSE fixed_str END AS case_group, COUNT(*) AS n FROM test WHERE CASE WHEN x = 7 THEN ss ELSE str END = 'fish' GROUP BY case_group ORDER BY case_group ASC NULLS FIRST;)",
      dt);

    EXPECT_ANY_THROW(
        c(R"(SELECT CASE WHEN x = 8 THEN str ELSE (str = fixed_str) END AS case_group,
     COUNT(*) AS n FROM test GROUP BY case_group ORDER BY case_group ASC NULLS FIRST, n ASC NULLS FIRST;)",
          dt));  // Cast from BOOLEAN to TEXT not supported

    {
      const auto watchdog_state = config().exec.watchdog.enable;
      config().exec.watchdog.enable = true;
      ScopeGuard reset_Watchdog_state = [&watchdog_state] {
        config().exec.watchdog.enable = watchdog_state;
      };

      // casts not yet supported in distributed mode
      config().exec.watchdog.enable = false;
      c(R"(SELECT CASE WHEN x = 7 THEN 'a' WHEN x = 8 then str ELSE fixed_str END FROM test ORDER BY 1;)",
        dt);
      c(R"(SELECT CASE WHEN str = 'foo' THEN real_str WHEN str = 'bar' THEN 'b' ELSE null_str END FROM test ORDER BY 1)",
        dt);
      EXPECT_ANY_THROW(run_multiple_agg(
          R"(SELECT CASE WHEN str = 'foo' THEN real_str WHEN str = 'bar' THEN 'b' ELSE null_str END case_col, sum(x) FROM test GROUP BY case_col;)",
          dt));  // cannot group by none encoded string columns
    }
    c("SELECT y AS key0, SUM(CASE WHEN x > 7 THEN x / (x - 7) ELSE 99 END) FROM test "
      "GROUP BY key0 ORDER BY key0;",
      dt);
    ASSERT_NO_THROW(run_multiple_agg(
        "SELECT y AS key0, CASE WHEN y > 7 THEN STDDEV(x) ELSE 99 END FROM test "
        "GROUP BY y ORDER BY y;",
        dt,
        false));
    ASSERT_NO_THROW(run_multiple_agg(
        "SELECT y AS key0, CASE WHEN y > 7 THEN 1 ELSE STDDEV(x) END FROM test "
        "GROUP BY y ORDER BY y;",
        dt,
        false));
    c("SELECT CASE WHEN str IN ('str1', 'str3', 'str8') THEN 'foo' WHEN str IN ('str2', "
      "'str4', 'str9') THEN 'bar' "
      "ELSE 'baz' END AS bucketed_str, COUNT(*) AS n FROM query_rewrite_test GROUP BY "
      "bucketed_str ORDER BY n "
      "DESC;",
      dt);
    c("SELECT CASE WHEN y > 40 THEN x END c, x FROM test ORDER BY c ASC;", dt);
    c("SELECT COUNT(CASE WHEN str = 'foo' THEN 1 END) FROM test;", dt);
    c("SELECT COUNT(CASE WHEN str = 'foo' THEN 1 ELSE NULL END) FROM test;", dt);
    c("SELECT CASE WHEN x BETWEEN 1 AND 3 THEN y ELSE y END AS foobar FROM test ORDER BY "
      "foobar DESC;",
      dt);
    c("SELECT x, AVG(CASE WHEN y BETWEEN 41 AND 42 THEN y END) FROM test GROUP BY x "
      "ORDER BY x;",
      dt);
    c("SELECT x, SUM(CASE WHEN y BETWEEN 41 AND 42 THEN y END) FROM test GROUP BY x "
      "ORDER BY x;",
      dt);
    c("SELECT x, COUNT(CASE WHEN y BETWEEN 41 AND 42 THEN y END) FROM test GROUP BY x "
      "ORDER BY x;",
      dt);
    c("SELECT CASE WHEN x > 8 THEN 'oops' ELSE 'ok' END FROM test LIMIT 1;", dt);
    c("SELECT CASE WHEN x < 9 THEN 'ok' ELSE 'oops' END FROM test LIMIT 1;", dt);
    c("SELECT CASE WHEN str IN ('foo', 'bar') THEN str END key1, COUNT(*) FROM test "
      "GROUP BY str HAVING key1 IS NOT "
      "NULL ORDER BY key1;",
      dt);

    c("SELECT CASE WHEN str IN ('foo') THEN 'FOO' WHEN str IN ('bar') THEN 'BAR' ELSE "
      "'BAZ' END AS g, COUNT(*) "
      "FROM test GROUP BY g ORDER BY g DESC;",
      dt);
    c("SELECT x, COUNT(case when y = 42 then 1 else 0 end) AS n1, COUNT(*) AS n2 FROM "
      "test GROUP BY x ORDER BY n2 "
      "DESC;",
      dt);
    c("SELECT CASE WHEN test.str = 'foo' THEN 'foo' ELSE test.str END AS g FROM test "
      "GROUP BY g ORDER BY g ASC;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE CASE WHEN x > 8 THEN 'oops' END = 'oops' OR CASE "
      "WHEN x > 8 THEN 'oops' END = 'oops';",
      dt);
    ASSERT_EQ(
        int64_t(1418428800),
        v<int64_t>(run_simple_agg(
            "SELECT CASE WHEN 1 > 0 THEN DATE_TRUNC(day, m) ELSE DATE_TRUNC(year, m) END "
            "AS date_bin FROM test GROUP BY date_bin;",
            dt)));
    ASSERT_EQ(
        int64_t(1388534400),
        v<int64_t>(run_simple_agg(
            "SELECT CASE WHEN 1 < 0 THEN DATE_TRUNC(day, m) ELSE DATE_TRUNC(year, m) END "
            "AS date_bin FROM test GROUP BY date_bin;",
            dt)));
    c("SELECT COUNT(CASE WHEN str IN ('foo', 'bar') THEN 'foo_bar' END) from test;", dt);
    ASSERT_EQ(
        int64_t(1),
        v<int64_t>(run_simple_agg(
            "SELECT MIN(CASE WHEN x BETWEEN 7 AND 8 THEN true ELSE false END) FROM test;",
            dt)));
    ASSERT_EQ(
        int64_t(0),
        v<int64_t>(run_simple_agg(
            "SELECT MIN(CASE WHEN x BETWEEN 6 AND 7 THEN true ELSE false END) FROM test;",
            dt)));
    c("SELECT CASE WHEN test.str in ('boo', 'simple', 'case', 'not', 'much', 'to', "
      "'see', 'foo_in_case', 'foo', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', "
      "'k', 'l') THEN 'foo_in_case' ELSE test.str END AS g FROM test GROUP BY g ORDER BY "
      "g ASC;",
      dt);
    c("SELECT CASE WHEN shared_dict is null THEN 'hello' ELSE 'world' END key0, count(*) "
      "val FROM test GROUP BY key0 ORDER BY val;",
      dt);
    c("WITH distinct_x AS (SELECT x FROM test GROUP BY x) SELECT SUM(CASE WHEN x = 7 "
      "THEN -32767 ELSE -1 END) FROM distinct_x",
      dt);
    ASSERT_NO_THROW(run_multiple_agg(
        "WITH distinct_x AS (SELECT x FROM test GROUP BY x) SELECT CASE WHEN x = 7 "
        "THEN STDDEV(x) ELSE -1 END FROM distinct_x GROUP BY x;",
        dt,
        false));
    ASSERT_NO_THROW(run_multiple_agg(
        "WITH distinct_x AS (SELECT x FROM test GROUP BY x) SELECT CASE WHEN x = 7 "
        "THEN -32767 ELSE STDDEV(x) END FROM distinct_x GROUP BY x;",
        dt,
        false));
    ASSERT_NO_THROW(run_multiple_agg(
        "WITH distinct_x AS (SELECT x FROM test GROUP BY x) SELECT CASE WHEN x = 7 "
        "THEN -32767 ELSE STDDEV(x) END as V FROM distinct_x GROUP BY x ORDER BY V;",
        dt,
        false));
    ASSERT_NO_THROW(run_multiple_agg(
        "WITH distinct_x AS (SELECT x FROM test GROUP BY x) SELECT CASE WHEN x = 7 "
        "THEN STDDEV(x) ELSE -1 END as V FROM distinct_x GROUP BY x ORDER BY V;",
        dt,
        false));
    c("WITH distinct_x AS (SELECT x FROM test GROUP BY x) SELECT AVG(CASE WHEN x = 7 "
      "THEN -32767 ELSE -1 END) FROM distinct_x",
      dt);
    c("SELECT CASE WHEN x BETWEEN 1 AND 7 THEN '1' WHEN x BETWEEN 8 AND 10 THEN '2' ELSE "
      "real_str END AS c FROM test WHERE y IN (43) ORDER BY c ASC;",
      dt);
    c("SELECT ROUND(a.numerator / a.denominator, 2) FROM (SELECT SUM(CASE WHEN y > 42 "
      "THEN 1.0 ELSE 0.0 END) as numerator, SUM(CASE WHEN dd > 0 THEN 1 ELSE -1 END) as "
      "denominator, y FROM test GROUP BY y ORDER BY y) a",
      dt);
    c("SELECT ROUND((numerator / denominator) * 100, 2) FROM (SELECT "
      "SUM(CASE WHEN a.x > 0 THEN "
      "1 ELSE 0 END) as numerator, SUM(CASE WHEN a.dd < 0 "
      "THEN 0.5 ELSE -0.5 END) as denominator "
      "FROM test a, test_inner b where a.x = b.x) test_sub",
      dt);
    EXPECT_EQ(
        int64_t(-1),
        v<int64_t>(run_simple_agg("SELECT ROUND(numerator / denominator, 2) FROM (SELECT "
                                  "SUM(CASE WHEN a.x > 0 THEN "
                                  "1 ELSE 0 END) as numerator, SUM(CASE WHEN a.dd < 0 "
                                  "THEN 1 ELSE -1 END) as denominator "
                                  "FROM test a, test_inner b where a.x = b.x) test_sub",
                                  dt)));
    EXPECT_EQ(
        double(100),
        v<double>(run_simple_agg(
            "SELECT CEIL((a.numerator / a.denominator) * 100) as c FROM (SELECT SUM(CASE "
            "WHEN "
            "y > 42 "
            "THEN 1.0 ELSE 0.0 END) as numerator, SUM(CASE WHEN dd > 0 THEN 1 ELSE "
            "-1 END) as "
            "denominator, y FROM test GROUP BY y ORDER BY y) a GROUP BY c HAVING c > 0",
            dt)));

    const auto constrained_by_in_threshold_state =
        config().opts.constrained_by_in_threshold;
    config().opts.constrained_by_in_threshold = 0;
    ScopeGuard reset_constrained_by_in_threshold = [&constrained_by_in_threshold_state] {
      config().opts.constrained_by_in_threshold = constrained_by_in_threshold_state;
    };
    c("SELECT fixed_str AS key0, str as key1, count(*) as val FROM test WHERE "
      "((fixed_str IN (SELECT fixed_str FROM test GROUP BY fixed_str))) GROUP BY key0, "
      "key1 ORDER BY val desc;",
      dt);
    c("SELECT CASE str WHEN 'foo' THEN 'truncated' ELSE 'bar' END trunc"
      " FROM test ORDER BY trunc;",
      dt);
    c("SELECT CASE str WHEN 'foo' THEN 'bar' ELSE 'truncated' END trunc"
      " FROM test ORDER BY trunc;",
      dt);
  }
}

TEST_F(Select, CaseSubQuery) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT CASE WHEN (SELECT COUNT(*) FROM test) < 10"
      "       THEN (SELECT 2*COUNT(*) FROM test) + 5"
      "       ELSE (SELECT 3*COUNT(*) FROM test) - 5"
      "       END;",
      dt);
    c("SELECT CASE WHEN (SELECT COUNT(*) FROM test) >= 10"
      "       THEN (SELECT 2*COUNT(*) FROM test) - 4"
      "       ELSE (SELECT 3*COUNT(*) FROM test) + 4"
      "       END;",
      dt);
  }
}

TEST_F(Select, Strings) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c("SELECT str, COUNT(*) FROM test GROUP BY str HAVING COUNT(*) > 5 ORDER BY str;",
      dt);
    c("SELECT str, COUNT(*) FROM test WHERE str = 'bar' GROUP BY str HAVING COUNT(*) > 4 "
      "ORDER BY str;",
      dt);
    c("SELECT str, COUNT(*) FROM test WHERE str = 'bar' GROUP BY str HAVING COUNT(*) > 5 "
      "ORDER BY str;",
      dt);
    c("SELECT str, COUNT(*) FROM test where str IS NOT NULL GROUP BY str ORDER BY str;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IS NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE str IS NOT NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE ss IS NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE ss IS NOT NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE str LIKE '%%%';", dt);
    c("SELECT COUNT(*) FROM test WHERE str LIKE 'ba%';", dt);
    c("SELECT COUNT(*) FROM test WHERE str LIKE '%eal_bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE str LIKE '%ba%';", dt);
    c("SELECT * FROM test WHERE str LIKE '%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE str LIKE 'f%%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE str LIKE 'f%\%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE ss LIKE 'f%\%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE str LIKE '@f%%' ESCAPE '@' ORDER BY x ASC, y ASC;", dt);
    c(R"(SELECT COUNT(*) FROM test WHERE real_str LIKE '%foo' OR real_str LIKE '%"bar"';)",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str LIKE 'ba_' or str LIKE 'fo_';", dt);
    c("SELECT COUNT(*) FROM test WHERE str IS NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE str IS NOT NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE str > 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE str > 'fo';", dt);
    c("SELECT COUNT(*) FROM test WHERE str >= 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE 'bar' < str;", dt);
    c("SELECT COUNT(*) FROM test WHERE 'fo' < str;", dt);
    c("SELECT COUNT(*) FROM test WHERE 'bar' <= str;", dt);
    c("SELECT COUNT(*) FROM test WHERE str = 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE 'bar' = str;", dt);
    c("SELECT COUNT(*) FROM test WHERE str <> 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE 'bar' <> str;", dt);
    c("SELECT COUNT(*) FROM test WHERE str = 'foo' OR str = 'bar';", dt);
    // The following tests throw Cast from dictionary-encoded string to none-encoded not
    // supported for distributed queries in distributed mode
    c("SELECT COUNT(*) FROM test WHERE str = real_str;", dt);
    c("SELECT COUNT(*) FROM test WHERE str <> str;", dt);
    c("SELECT COUNT(*) FROM test WHERE ss <> str;", dt);
    c("SELECT COUNT(*) FROM test WHERE ss = str;", dt);
    c("SELECT COUNT(*) FROM test WHERE LENGTH(str) = 3;", dt);
    c("SELECT fixed_str, COUNT(*) FROM test GROUP BY fixed_str HAVING COUNT(*) > 5 ORDER "
      "BY fixed_str;",
      dt);
    c("SELECT fixed_str, COUNT(*) FROM test WHERE fixed_str = 'bar' GROUP BY fixed_str "
      "HAVING COUNT(*) > 4 ORDER BY "
      "fixed_str;",
      dt);
    c("SELECT COUNT(*) FROM emp WHERE ename LIKE 'D%%' OR ename = 'Julia';", dt);
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CHAR_LENGTH(str) = 3;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE str ILIKE 'f%%';", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE (str ILIKE 'f%%');", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE ( str ILIKE 'f%%' );", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE str ILIKE 'McDonald''s';", dt)));
    ASSERT_EQ("foo",
              boost::get<std::string>(v<NullableString>(run_simple_agg(
                  "SELECT str FROM test WHERE REGEXP_LIKE(str, '^f.?.+');", dt))));
    ASSERT_EQ("bar",
              boost::get<std::string>(v<NullableString>(run_simple_agg(
                  "SELECT str FROM test WHERE REGEXP_LIKE(str, '^[a-z]+r$');", dt))));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE str REGEXP '.*';", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE str REGEXP '...';", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE str REGEXP '.+.+.+';", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE str REGEXP '.?.?.?';", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE str REGEXP 'ba.' or str REGEXP 'fo.';",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE "
                                        "REGEXP_LIKE(str, 'ba.') or str REGEXP 'fo.?';",
                                        dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE str REGEXP "
                                        "'ba.' or REGEXP_LIKE(str, 'fo.+');",
                                        dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE str REGEXP 'ba.+';", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE REGEXP_LIKE(str, '.?ba.*');", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE "
                                  "REGEXP_LIKE(str,'ba.') or REGEXP_LIKE(str, 'fo.+');",
                                  dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE str REGEXP "
                                        "'ba.' or REGEXP_LIKE(str, 'fo.+');",
                                        dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE "
                                        "REGEXP_LIKE(str, 'ba.') or str REGEXP 'fo.?';",
                                        dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE str REGEXP 'ba.' or str REGEXP 'fo.';",
                  dt)));
    EXPECT_ANY_THROW(run_simple_agg("SELECT LENGTH(NULL) FROM test;", dt));
  }
}

TEST_F(Select, SharedDictionary) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c("SELECT shared_dict, COUNT(*) FROM test GROUP BY shared_dict HAVING COUNT(*) > 5 "
      "ORDER BY shared_dict;",
      dt);
    c("SELECT shared_dict, COUNT(*) FROM test WHERE shared_dict = 'bar' GROUP BY "
      "shared_dict HAVING COUNT(*) > 4 ORDER "
      "BY shared_dict;",
      dt);
    c("SELECT shared_dict, COUNT(*) FROM test WHERE shared_dict = 'bar' GROUP BY "
      "shared_dict HAVING COUNT(*) > 5 ORDER "
      "BY shared_dict;",
      dt);
    c("SELECT shared_dict, COUNT(*) FROM test where shared_dict IS NOT NULL GROUP BY "
      "shared_dict ORDER BY shared_dict;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict IS NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict IS NOT NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE ss IS NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE ss IS NOT NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict LIKE '%%%';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict LIKE 'ba%';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict LIKE '%eal_bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict LIKE '%ba%';", dt);
    c("SELECT * FROM test WHERE shared_dict LIKE '%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE shared_dict LIKE 'f%%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE shared_dict LIKE 'f%\%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE ss LIKE 'f%\%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE shared_dict LIKE '@f%%' ESCAPE '@' ORDER BY x ASC, y "
      "ASC;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict LIKE 'ba_' or shared_dict LIKE 'fo_';",
      dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict IS NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict IS NOT NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict = 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE 'bar' = shared_dict;", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict <> 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE 'bar' <> shared_dict;", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict = 'foo' OR shared_dict = 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict = real_str;", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict <> shared_dict;", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict > 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict > 'fo';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict >= 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE 'bar' < shared_dict;", dt);
    c("SELECT COUNT(*) FROM test WHERE 'fo' < shared_dict;", dt);
    c("SELECT COUNT(*) FROM test WHERE 'bar' <= shared_dict;", dt);
    c("SELECT COUNT(*) FROM test WHERE LENGTH(shared_dict) = 3;", dt);

    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CHAR_LENGTH(shared_dict) = 3;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE shared_dict ILIKE 'f%%';", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE (shared_dict ILIKE 'f%%');", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE ( shared_dict ILIKE 'f%%' );", dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE shared_dict ILIKE 'McDonald''s';", dt)));

    ASSERT_EQ(
        "foo",
        boost::get<std::string>(v<NullableString>(run_simple_agg(
            "SELECT shared_dict FROM test WHERE REGEXP_LIKE(shared_dict, '^f.?.+');",
            dt))));
    ASSERT_EQ(
        "baz",
        boost::get<std::string>(v<NullableString>(run_simple_agg(
            "SELECT shared_dict FROM test WHERE REGEXP_LIKE(shared_dict, '^[a-z]+z$');",
            dt))));

    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE shared_dict REGEXP '.*';", dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE shared_dict REGEXP '...';", dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE shared_dict REGEXP '.+.+.+';", dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE shared_dict REGEXP '.?.?.?';", dt)));

    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE shared_dict "
                                        "REGEXP 'ba.' or shared_dict REGEXP 'fo.';",
                                        dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE REGEXP_LIKE(shared_dict, 'ba.') or "
                  "shared_dict REGEXP 'fo.?';",
                  dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE shared_dict REGEXP "
                                  "'ba.' or REGEXP_LIKE(shared_dict, 'fo.+');",
                                  dt)));
    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE shared_dict REGEXP 'ba.+';", dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE REGEXP_LIKE(shared_dict, '.?ba.*');", dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE REGEXP_LIKE(shared_dict,'ba.') or "
                  "REGEXP_LIKE(shared_dict, 'fo.+');",
                  dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE shared_dict REGEXP "
                                  "'ba.' or REGEXP_LIKE(shared_dict, 'fo.+');",
                                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE REGEXP_LIKE(shared_dict, 'ba.') or "
                  "shared_dict REGEXP 'fo.?';",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE shared_dict "
                                        "REGEXP 'ba.' or shared_dict REGEXP 'fo.';",
                                        dt)));
  }
}

TEST_F(Select, StringCompare) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test WHERE str = 'ba';", dt);
    c("SELECT COUNT(*) FROM test WHERE str <> 'ba';", dt);

    c("SELECT COUNT(*) FROM test WHERE shared_dict < 'ba';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict < 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict < 'baf';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict < 'baz';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict < 'bbz';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict < 'foo';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict < 'foon';", dt);

    c("SELECT COUNT(*) FROM test WHERE shared_dict > 'ba';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict > 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict > 'baf';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict > 'baz';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict > 'bbz';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict > 'foo';", dt);
    c("SELECT COUNT(*) FROM test WHERE shared_dict > 'foon';", dt);

    c("SELECT COUNT(*) FROM test WHERE real_str <= 'ba';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str <= 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str <= 'baf';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str <= 'baz';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str <= 'bbz';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str <= 'foo';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str <= 'foon';", dt);

    c("SELECT COUNT(*) FROM test WHERE real_str >= 'ba';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str >= 'bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str >= 'baf';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str >= 'baz';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str >= 'bbz';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str >= 'foo';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str >= 'foon';", dt);

    c("SELECT COUNT(*) FROM test WHERE real_str <= 'äâ';", dt);

    c("SELECT COUNT(*) FROM test WHERE 'ba' < shared_dict;", dt);
    c("SELECT COUNT(*) FROM test WHERE 'bar' < shared_dict;", dt);
    c("SELECT COUNT(*) FROM test WHERE 'ba' > shared_dict;", dt);
    c("SELECT COUNT(*) FROM test WHERE 'bar' > shared_dict;", dt);

    const auto watchdog_state = config().exec.watchdog.enable;
    ScopeGuard reset_Watchdog_state = [&watchdog_state] {
      config().exec.watchdog.enable = watchdog_state;
    };

    config().exec.watchdog.enable = true;

    EXPECT_THROW(run_simple_agg("SELECT COUNT(*) FROM test, test_inner WHERE "
                                "test.shared_dict < test_inner.str",
                                dt),
                 std::runtime_error);

    config().exec.watchdog.enable = false;

    c("SELECT COUNT(*) FROM test, test_inner WHERE "
      "test.shared_dict < test_inner.str",
      dt);
  }
}

TEST_F(Select, DictionaryStringEquality) {
  // Introduces by QE-261, ensure that = and <> comparisons can
  // execute between two text columns even when they do not share
  // dictionaries, with watchdog both on and off and without punting
  // to CPU
  const auto watchdog_state = config().exec.watchdog.enable;
  const auto cpu_retry_state = config().exec.heterogeneous.allow_cpu_retry;
  const auto cpu_step_retry_state =
      config().exec.heterogeneous.allow_query_step_cpu_retry;

  ScopeGuard reset_global_state =
      [&watchdog_state, &cpu_retry_state, &cpu_step_retry_state] {
        config().exec.watchdog.enable = watchdog_state;
        config().exec.heterogeneous.allow_cpu_retry = cpu_retry_state;
        config().exec.heterogeneous.allow_query_step_cpu_retry = cpu_step_retry_state;
      };

  config().exec.heterogeneous.allow_cpu_retry = false;
  config().exec.heterogeneous.allow_query_step_cpu_retry = false;

  for (auto enable_watchdog : {true, false}) {
    config().exec.watchdog.enable = enable_watchdog;
    for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
      SKIP_NO_GPU();
      c("SELECT COUNT(*) FROM test WHERE str = fixed_str", dt);
      c("SELECT COUNT(*) FROM test WHERE str <> fixed_str", dt);
      c("SELECT COUNT(*) FROM test WHERE fixed_str = str", dt);
      c("SELECT COUNT(*) FROM test WHERE fixed_str <> str", dt);
      c("SELECT COUNT(*) FROM test WHERE str = null_str", dt);
      c("SELECT COUNT(*) FROM test WHERE str <> null_str", dt);
      c("SELECT COUNT(*) FROM test WHERE null_str = str", dt);
      c("SELECT COUNT(*) FROM test WHERE null_str <> str", dt);
    }
  }
}

TEST_F(Select, StringsNoneEncoding) {
  createTestLotsColsTable();
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c("SELECT COUNT(*) FROM test WHERE real_str LIKE 'real_%%%';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str LIKE 'real_ba%';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str LIKE '%eal_bar';", dt);
    c("SELECT * FROM test_lots_cols WHERE real_str LIKE '%' ORDER BY x0 ASC;", dt);
    c("SELECT * FROM test WHERE real_str LIKE '%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE real_str LIKE 'real_f%%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE real_str LIKE 'real_f%\%' ORDER BY x ASC, y ASC;", dt);
    c("SELECT * FROM test WHERE real_str LIKE 'real_@f%%' ESCAPE '@' ORDER BY x ASC, y "
      "ASC;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE real_str LIKE 'real_ba_' or real_str LIKE "
      "'real_fo_';",
      dt);
    c("SELECT COUNT(*) FROM test WHERE real_str IS NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str IS NOT NULL;", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str > 'real_bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str > 'real_fo';", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str >= 'real_bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE 'real_bar' < real_str;", dt);
    c("SELECT COUNT(*) FROM test WHERE 'real_fo' < real_str;", dt);
    c("SELECT COUNT(*) FROM test WHERE 'real_bar' <= real_str;", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str = 'real_bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE 'real_bar' = real_str;", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str <> 'real_bar';", dt);
    c("SELECT COUNT(*) FROM test WHERE 'real_bar' <> real_str;", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str = 'real_foo' OR real_str = 'real_bar';",
      dt);
    c("SELECT COUNT(*) FROM test WHERE real_str = real_str;", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str <> real_str;", dt);
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE real_str ILIKE 'rEaL_f%%';", dt)));
    c("SELECT COUNT(*) FROM test WHERE LENGTH(real_str) = 8;", dt);
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CHAR_LENGTH(real_str) = 8;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE REGEXP_LIKE(real_str,'real_.*.*.*');", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE real_str REGEXP 'real_ba.*';", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE real_str REGEXP '.*';", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE real_str REGEXP 'real_f.*.*';", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE real_str REGEXP 'real_f.+\%';", dt)));
    EXPECT_THROW(
        run_multiple_agg("SELECT COUNT(*) FROM test WHERE real_str LIKE str;", dt),
        std::runtime_error);
    EXPECT_THROW(run_multiple_agg(
                     "SELECT COUNT(*) FROM test WHERE REGEXP_LIKE(real_str, str);", dt),
                 std::runtime_error);
  }
  dropTable("test_lots_cols");
}

TEST_F(Select, TimeSyntaxCheck) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    ASSERT_EQ(1325376000LL,
              v<int64_t>(run_simple_agg("SELECT DATE_TRUNC(year, CAST('2012-05-08 "
                                        "20:15:12' AS TIMESTAMP)) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(1325376000LL,
              v<int64_t>(run_simple_agg("SELECT DATE_TRUNC('year', CAST('2012-05-08 "
                                        "20:15:12' AS TIMESTAMP)) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(1325376000LL,
              v<int64_t>(run_simple_agg("SELECT PG_DATE_TRUNC(year, CAST('2012-05-08 "
                                        "20:15:12' AS TIMESTAMP)) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(1325376000LL,
              v<int64_t>(run_simple_agg("SELECT PG_DATE_TRUNC('year', CAST('2012-05-08 "
                                        "20:15:12' AS TIMESTAMP)) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(2007,
              v<int64_t>(run_simple_agg("SELECT PG_EXTRACT('year', CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(2007,
              v<int64_t>(run_simple_agg("SELECT PG_EXTRACT(YEAR, CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(2007,
              v<int64_t>(run_simple_agg("SELECT extract('year' from CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(2007,
              v<int64_t>(run_simple_agg("SELECT extract(year from CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(2007,
              v<int64_t>(run_simple_agg("SELECT DATEPART('year', CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(2007,
              v<int64_t>(run_simple_agg("SELECT DATEPART(YEAR, CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg("SELECT DATEDIFF('year', CAST('2006-01-07 00:00:00' as "
                                  "TIMESTAMP), CAST('2009-01-07 00:00:00' AS "
                                  "TIMESTAMP)) FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg("SELECT DATEDIFF(YEAR, CAST('2006-01-07 00:00:00' as "
                                  "TIMESTAMP), CAST('2009-01-07 00:00:00' AS "
                                  "TIMESTAMP)) FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT DATEADD('day', 1, CAST('2017-05-31' AS DATE)) "
                                  "= TIMESTAMP '2017-06-01 0:00:00' from test limit 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT DATEADD(DAY, 1, CAST('2017-05-31' AS DATE)) "
                                  "= TIMESTAMP '2017-06-01 0:00:00' from test limit 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPADD('year', 1, TIMESTAMP '2008-02-29 1:11:11') = TIMESTAMP "
            "'2009-02-28 1:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPADD(YEAR, 1, TIMESTAMP '2008-02-29 1:11:11') = TIMESTAMP "
            "'2009-02-28 1:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        128885,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPDIFF('minute', TIMESTAMP '2003-02-01 0:00:00', TIMESTAMP "
            "'2003-05-01 12:05:55') FROM test LIMIT 1;",
            dt)));
    ASSERT_EQ(
        128885,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPDIFF(minute, TIMESTAMP '2003-02-01 0:00:00', TIMESTAMP "
            "'2003-05-01 12:05:55') FROM test LIMIT 1;",
            dt)));
    ASSERT_EQ(128885,
              v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF('sql_tsi_minute', "
                                        "TIMESTAMP '2003-02-01 0:00:00', TIMESTAMP "
                                        "'2003-05-01 12:05:55') FROM test LIMIT 1;",
                                        dt)));
    ASSERT_EQ(128885,
              v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF(sql_tsi_minute, TIMESTAMP "
                                        "'2003-02-01 0:00:00', TIMESTAMP "
                                        "'2003-05-01 12:05:55') FROM test LIMIT 1;",
                                        dt)));
  }
}

TEST_F(Select, Time) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // check DATE Formats
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE CAST('1999-09-10' AS DATE) > o;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE CAST('10/09/1999' AS DATE) > o;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CAST('10/09/99' AS DATE) > o;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CAST('10-Sep-99' AS DATE) > o;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CAST('9/10/99' AS DATE) > o;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE CAST('31/Oct/2013' AS DATE) > o;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CAST('10/31/13' AS DATE) > o;", dt)));
    // check TIME FORMATS
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CAST('15:13:15' AS TIME) > n;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CAST('151315' AS TIME) > n;", dt)));

    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE CAST('1999-09-10' AS DATE) > o;", dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE CAST('1999-09-10' AS DATE) <= o;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CAST('15:13:15' AS TIME) > n;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CAST('15:13:15' AS TIME) <= n;", dt)));
    cta("SELECT DATETIME('NOW') FROM test limit 1;", dt);
    EXPECT_ANY_THROW(run_simple_agg("SELECT DATETIME(NULL) FROM test LIMIT 1;", dt));
    // these next tests work because all dates are before now 2015-12-8 17:00:00
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m < NOW();", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE o IS NULL OR o < CURRENT_DATE;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE o IS NULL OR o < CURRENT_DATE();", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE m < CURRENT_TIMESTAMP;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE m < CURRENT_TIMESTAMP();", dt)));
    ASSERT_TRUE(v<int64_t>(
        run_simple_agg("SELECT CURRENT_DATE = CAST(CURRENT_TIMESTAMP AS DATE);", dt)));
    ASSERT_TRUE(
        v<int64_t>(run_simple_agg("SELECT DATEADD('day', -1, CURRENT_TIMESTAMP) < "
                                  "CURRENT_DATE AND CURRENT_DATE <= CURRENT_TIMESTAMP;",
                                  dt)));
    ASSERT_TRUE(v<int64_t>(run_simple_agg(
        "SELECT CAST(CURRENT_DATE AS TIMESTAMP) <= CURRENT_TIMESTAMP;", dt)));
    ASSERT_TRUE(v<int64_t>(run_simple_agg(
        "SELECT EXTRACT(YEAR FROM CURRENT_DATE) = EXTRACT(YEAR FROM CURRENT_TIMESTAMP)"
        " AND EXTRACT(MONTH FROM CURRENT_DATE) = EXTRACT(MONTH FROM CURRENT_TIMESTAMP)"
        " AND EXTRACT(DAY FROM CURRENT_DATE) = EXTRACT(DAY FROM CURRENT_TIMESTAMP)"
        " AND EXTRACT(HOUR FROM CURRENT_DATE) = 0"
        " AND EXTRACT(MINUTE FROM CURRENT_DATE) = 0"
        " AND EXTRACT(SECOND FROM CURRENT_DATE) = 0;",
        dt)));
    ASSERT_TRUE(v<int64_t>(run_simple_agg(
        "SELECT EXTRACT(HOUR FROM CURRENT_TIME()) = EXTRACT(HOUR FROM CURRENT_TIMESTAMP)"
        " AND EXTRACT(MINUTE FROM CURRENT_TIME) = EXTRACT(MINUTE FROM CURRENT_TIMESTAMP)"
        " AND EXTRACT(SECOND FROM CURRENT_TIME) = EXTRACT(SECOND FROM CURRENT_TIMESTAMP)"
        ";",
        dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE m > timestamp(0) '2014-12-13T000000';",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE CAST(o AS "
                                        "TIMESTAMP) > timestamp(0) '1999-09-08T160000';",
                                        dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE CAST(o AS "
                                        "TIMESTAMP) > timestamp(0) '1999-09-10T160000';",
                                        dt)));
    ASSERT_EQ(14185957950LL,
              v<int64_t>(run_simple_agg(
                  "SELECT MAX(EXTRACT(EPOCH FROM m) * 10) FROM test;", dt)));
    ASSERT_EQ(14185152000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT MAX(EXTRACT(DATEEPOCH FROM m) * 10) FROM test;", dt)));
    ASSERT_EQ(20140,
              v<int64_t>(run_simple_agg(
                  "SELECT MAX(EXTRACT(YEAR FROM m) * 10) FROM test;", dt)));
    ASSERT_EQ(120,
              v<int64_t>(run_simple_agg(
                  "SELECT MAX(EXTRACT(MONTH FROM m) * 10) FROM test;", dt)));
    ASSERT_EQ(140,
              v<int64_t>(
                  run_simple_agg("SELECT MAX(EXTRACT(DAY FROM m) * 10) FROM test;", dt)));
    ASSERT_EQ(
        22,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(HOUR FROM m)) FROM test;", dt)));
    ASSERT_EQ(
        23,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(MINUTE FROM m)) FROM test;", dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(SECOND FROM m)) FROM test;", dt)));
    ASSERT_EQ(
        6, v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(DOW FROM m)) FROM test;", dt)));
    ASSERT_EQ(
        348,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(DOY FROM m)) FROM test;", dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(HOUR FROM n)) FROM test;", dt)));
    ASSERT_EQ(
        13,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(MINUTE FROM n)) FROM test;", dt)));
    ASSERT_EQ(
        14,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(SECOND FROM n)) FROM test;", dt)));
    ASSERT_EQ(
        1999,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(YEAR FROM o)) FROM test;", dt)));
    ASSERT_EQ(
        9,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(MONTH FROM o)) FROM test;", dt)));
    ASSERT_EQ(
        9, v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(DAY FROM o)) FROM test;", dt)));
    ASSERT_EQ(4,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM o) FROM test WHERE o IS NOT NULL;", dt)));
    ASSERT_EQ(252,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM o) FROM test WHERE o IS NOT NULL;", dt)));
    ASSERT_EQ(
        936835200LL,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(EPOCH FROM o)) FROM test;", dt)));
    ASSERT_EQ(936835200LL,
              v<int64_t>(run_simple_agg(
                  "SELECT MAX(EXTRACT(DATEEPOCH FROM o)) FROM test;", dt)));
    ASSERT_EQ(52LL,
              v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(WEEK FROM CAST('2012-01-01 "
                                        "20:15:12' AS TIMESTAMP))) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(
        1LL,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(WEEK_SUNDAY FROM CAST('2012-01-01 "
                                  "20:15:12' AS TIMESTAMP))) FROM test limit 1;",
                                  dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(
                  run_simple_agg("SELECT MAX(EXTRACT(WEEK_SATURDAY FROM CAST('2012-01-01 "
                                 "20:15:12' AS TIMESTAMP))) FROM test limit 1;",
                                 dt)));
    ASSERT_EQ(10LL,
              v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(WEEK FROM CAST('2008-03-03 "
                                        "20:15:12' AS TIMESTAMP))) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(
        10LL,
        v<int64_t>(run_simple_agg("SELECT MAX(EXTRACT(WEEK_SUNDAY FROM CAST('2008-03-03 "
                                  "20:15:12' AS TIMESTAMP))) FROM test limit 1;",
                                  dt)));
    ASSERT_EQ(10LL,
              v<int64_t>(
                  run_simple_agg("SELECT MAX(EXTRACT(WEEK_SATURDAY FROM CAST('2008-03-03 "
                                 "20:15:12' AS TIMESTAMP))) FROM test limit 1;",
                                 dt)));
    // Monday
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg("SELECT EXTRACT(DOW FROM CAST('2008-03-03 "
                                        "20:15:12' AS TIMESTAMP)) FROM test limit 1;",
                                        dt)));
    // Monday
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg("SELECT EXTRACT(ISODOW FROM CAST('2008-03-03 "
                                        "20:15:12' AS TIMESTAMP)) FROM test limit 1;",
                                        dt)));
    // Sunday
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg("SELECT EXTRACT(DOW FROM CAST('2008-03-02 "
                                        "20:15:12' AS TIMESTAMP)) FROM test limit 1;",
                                        dt)));
    // Sunday
    ASSERT_EQ(7LL,
              v<int64_t>(run_simple_agg("SELECT EXTRACT(ISODOW FROM CAST('2008-03-02 "
                                        "20:15:12' AS TIMESTAMP)) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(15000000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(nanosecond from m) FROM test limit 1;", dt)));
    ASSERT_EQ(15000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(microsecond from m) FROM test limit 1;", dt)));
    ASSERT_EQ(15000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(millisecond from m) FROM test limit 1;", dt)));
    ASSERT_EQ(56000000000LL,
              v<int64_t>(run_simple_agg("SELECT EXTRACT(nanosecond from TIMESTAMP(0) "
                                        "'1999-03-14 23:34:56') FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(56000000LL,
              v<int64_t>(run_simple_agg("SELECT EXTRACT(microsecond from TIMESTAMP(0) "
                                        "'1999-03-14 23:34:56') FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(56000LL,
              v<int64_t>(run_simple_agg("SELECT EXTRACT(millisecond from TIMESTAMP(0) "
                                        "'1999-03-14 23:34:56') FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(2005,
              v<int64_t>(run_simple_agg("select EXTRACT(year from TIMESTAMP '2005-12-31 "
                                        "23:59:59') from test limit 1;",
                                        dt)));
    ASSERT_EQ(1997,
              v<int64_t>(run_simple_agg("select EXTRACT(year from TIMESTAMP '1997-01-01 "
                                        "23:59:59') from test limit 1;",
                                        dt)));
    ASSERT_EQ(2006,
              v<int64_t>(run_simple_agg("select EXTRACT(year from TIMESTAMP '2006-01-01 "
                                        "00:0:00') from test limit 1;",
                                        dt)));
    ASSERT_EQ(2014,
              v<int64_t>(run_simple_agg("select EXTRACT(year from TIMESTAMP '2014-01-01 "
                                        "00:00:00') from test limit 1;",
                                        dt)));

    // test DATE format processing
    ASSERT_EQ(1434844800LL,
              v<int64_t>(run_simple_agg(
                  "select CAST('2015-06-21' AS DATE) FROM test limit 1;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE o < CAST('06/21/2015' AS DATE);", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE o < CAST('21-Jun-15' AS DATE);", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE o < CAST('21/Jun/2015' AS DATE);", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE o < CAST('1434844800' AS DATE);", dt)));

    // test different input formats
    // added new format for customer
    ASSERT_EQ(
        1434896116LL,
        v<int64_t>(run_simple_agg(
            "select CAST('2015-06-21 14:15:16' AS timestamp) FROM test limit 1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m <= "
                                        "CAST('2015-06-21:141516' AS TIMESTAMP);",
                                        dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m <= CAST('21-JUN-15 "
                                  "2.15.16.12345 PM' AS TIMESTAMP);",
                                  dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m <= CAST('21-JUN-15 "
                                  "2.15.16.12345 AM' AS TIMESTAMP);",
                                  dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m <= "
                                        "CAST('21-JUN-15 2:15:16 AM' AS TIMESTAMP);",
                                        dt)));

    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m <= "
                                        "CAST('06/21/2015 14:15:16' AS TIMESTAMP);",
                                        dt)));

    // Support ISO date offset format
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m <= "
                                  "CAST('21/Aug/2015:12:13:14 -0600' AS TIMESTAMP);",
                                  dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m <= "
                                        "CAST('2015-08-21T12:13:14 -0600' AS TIMESTAMP);",
                                        dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m <= "
                                        "CAST('21-Aug-15 12:13:14 -0600' AS TIMESTAMP);",
                                        dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m <= "
                                  "CAST('21/Aug/2015:13:13:14 -0500' AS TIMESTAMP);",
                                  dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE m <= "
                                        "CAST('2015-08-21T18:13:14' AS TIMESTAMP);",
                                        dt)));
    // add test for quarterday behaviour
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "select EXTRACT (QUARTERDAY FROM CAST('2015-08-21T04:23:11' AS "
                  "timestamp)) FROM test limit 1;",
                  dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "select EXTRACT (QUARTERDAY FROM CAST('2015-08-21T00:00:00' AS "
                  "timestamp)) FROM test limit 1;",
                  dt)));
    ASSERT_EQ(2LL,
              v<int64_t>(run_simple_agg(
                  "select EXTRACT (QUARTERDAY FROM CAST('2015-08-21T08:23:11' AS "
                  "timestamp)) FROM test limit 1;",
                  dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "select EXTRACT (QUARTERDAY FROM CAST('2015-08-21T14:23:11' AS "
                  "timestamp)) FROM test limit 1;",
                  dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "select EXTRACT (QUARTERDAY FROM CAST('2015-08-21T23:23:11' AS "
                  "timestamp)) FROM test limit 1;",
                  dt)));
    ASSERT_EQ(1440115200LL,
              v<int64_t>(run_simple_agg(
                  "select DATE_TRUNC (QUARTERDAY, CAST('2015-08-21T04:23:11' AS "
                  "timestamp)) FROM test limit 1;",
                  dt)));
    ASSERT_EQ(1440136800LL,
              v<int64_t>(run_simple_agg(
                  "select DATE_TRUNC (QUARTERDAY, CAST('2015-08-21T08:23:11' AS "
                  "timestamp)) FROM test limit 1;",
                  dt)));
    ASSERT_EQ(1440158400LL,
              v<int64_t>(run_simple_agg(
                  "select DATE_TRUNC (QUARTERDAY, CAST('2015-08-21T13:23:11' AS "
                  "timestamp)) FROM test limit 1;",
                  dt)));
    ASSERT_EQ(1440180000LL,
              v<int64_t>(run_simple_agg(
                  "select DATE_TRUNC (QUARTERDAY, CAST('2015-08-21T23:59:59' AS "
                  "timestamp)) FROM test limit 1;",
                  dt)));
    ASSERT_EQ(2007,
              v<int64_t>(run_simple_agg("SELECT DATEPART('year', CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(2007,
              v<int64_t>(run_simple_agg("SELECT DATEPART('yyyy', CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(
        2007,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('yy', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(4,
              v<int64_t>(run_simple_agg("SELECT DATEPART('quarter', CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(
        4,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('qq', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(
        4,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('q', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("SELECT DATEPART('month', CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('mm', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('m', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(303,
              v<int64_t>(run_simple_agg("SELECT DATEPART('dayofyear', CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(
        303,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('dy', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(
        303,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('y', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(
        30,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('day', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(
        30,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('dd', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(
        30,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('d', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(12,
              v<int64_t>(run_simple_agg("SELECT DATEPART('hour', CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(
        12,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('hh', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg("SELECT DATEPART('minute', CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('mi', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('n', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(32,
              v<int64_t>(run_simple_agg("SELECT DATEPART('second', CAST('2007-10-30 "
                                        "12:15:32' AS TIMESTAMP)) FROM test;",
                                        dt)));
    ASSERT_EQ(
        32,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('ss', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(
        32,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('s', CAST('2007-10-30 12:15:32' AS TIMESTAMP)) FROM test;",
            dt)));
    ASSERT_EQ(
        32,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('s', TIMESTAMP '2007-10-30 12:15:32') FROM test;", dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg("SELECT DATEDIFF('year', CAST('2006-01-07 00:00:00' as "
                                  "TIMESTAMP), CAST('2009-01-07 00:00:00' AS "
                                  "TIMESTAMP)) FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        36,
        v<int64_t>(run_simple_agg("SELECT DATEDIFF('month', CAST('2006-01-07 00:00:00' "
                                  "as TIMESTAMP), CAST('2009-01-07 00:00:00' AS "
                                  "TIMESTAMP)) FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        1096,
        v<int64_t>(run_simple_agg("SELECT DATEDIFF('day', CAST('2006-01-07 00:00:00' as "
                                  "TIMESTAMP), CAST('2009-01-07 00:00:00' AS "
                                  "TIMESTAMP)) FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        12,
        v<int64_t>(run_simple_agg("SELECT DATEDIFF('quarter', CAST('2006-01-07 00:00:00' "
                                  "as TIMESTAMP), CAST('2009-01-07 00:00:00' AS "
                                  "TIMESTAMP)) FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEDIFF('day', DATE '2009-2-28', DATE "
                                        "'2009-03-01') FROM test LIMIT 1;",
                                        dt)));
    ASSERT_EQ(2,
              v<int64_t>(run_simple_agg("SELECT DATEDIFF('day', DATE '2008-2-28', DATE "
                                        "'2008-03-01') FROM test LIMIT 1;",
                                        dt)));
    ASSERT_EQ(-425,
              v<int64_t>(run_simple_agg("select DATEDIFF('day', DATE '1971-03-02', DATE "
                                        "'1970-01-01') from test limit 1;",
                                        dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('day', o, o + INTERVAL '1' DAY) FROM test LIMIT 1;", dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg("SELECT count(*) from test where DATEDIFF('day', "
                                        "CAST (m AS DATE), o) < -5570;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEDIFF('second', m, TIMESTAMP(0) "
                                        "'2014-12-13 22:23:16') FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(1000,
              v<int64_t>(run_simple_agg("SELECT DATEDIFF('millisecond', m, TIMESTAMP(0) "
                                        "'2014-12-13 22:23:16') FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(44000000,
              v<int64_t>(run_simple_agg("SELECT DATEDIFF('microsecond', m, TIMESTAMP(0) "
                                        "'2014-12-13 22:23:59') FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(34000000000,
              v<int64_t>(run_simple_agg("SELECT DATEDIFF('nanosecond', m, TIMESTAMP(0) "
                                        "'2014-12-13 22:23:49') FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(-1000,
              v<int64_t>(run_simple_agg("SELECT DATEDIFF('millisecond', TIMESTAMP(0) "
                                        "'2014-12-13 22:23:16', m) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(-44000000,
              v<int64_t>(run_simple_agg("SELECT DATEDIFF('microsecond', TIMESTAMP(0) "
                                        "'2014-12-13 22:23:59', m) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(-34000000000,
              v<int64_t>(run_simple_agg("SELECT DATEDIFF('nanosecond', TIMESTAMP(0) "
                                        "'2014-12-13 22:23:49', m) FROM test limit 1;",
                                        dt)));
    // DATEADD tests
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT DATEADD('day', 1, CAST('2017-05-31' AS DATE)) "
                                  "= TIMESTAMP '2017-06-01 0:00:00' from test limit 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT DATEADD('day', 2, DATE '2017-05-31') = "
                                  "TIMESTAMP '2017-06-02 0:00:00' from test limit 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT DATEADD('day', -1, CAST('2017-05-31' AS DATE)) "
                                  "= TIMESTAMP '2017-05-30 0:00:00' from test limit 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT DATEADD('day', -2, DATE '2017-05-31') = "
                                  "TIMESTAMP '2017-05-29 0:00:00' from test limit 1;",
                                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('hour', 1, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
                  "'2017-05-31 2:11:11' from test limit 1;",
                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('hour', 10, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-31 11:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('hour', -1, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-31 0:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('hour', -10, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-30 15:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('minute', 1, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-31 1:12:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('minute', 10, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-31 1:21:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('minute', -1, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-31 1:10:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('minute', -10, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-31 1:01:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('second', 1, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-31 1:11:12' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('second', 10, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-31 1:11:21' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('second', -1, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-31 1:11:10' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('second', -10, TIMESTAMP '2017-05-31 1:11:11') = TIMESTAMP "
            "'2017-05-31 1:11:01' from test limit 1;",
            dt)));

    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('month', 1, DATE '2017-01-10') = TIMESTAMP "
                  "'2017-02-10 0:00:00' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('month', 10, DATE '2017-01-10') = TIMESTAMP "
                  "'2017-11-10 0:00:00' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('month', 1, DATE '2009-01-30') = TIMESTAMP "
                  "'2009-02-28 0:00:00' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('month', 1, DATE '2008-01-30') = TIMESTAMP "
                  "'2008-02-29 0:00:00' from test limit 1;",
                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('month', 1, TIMESTAMP '2009-01-30 1:11:11') = TIMESTAMP "
            "'2009-02-28 1:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('month', -1, TIMESTAMP '2009-03-30 1:11:11') = TIMESTAMP "
            "'2009-02-28 1:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('month', -4, TIMESTAMP '2009-03-30 1:11:11') = TIMESTAMP "
            "'2008-11-30 1:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('month', 5, TIMESTAMP '2009-01-31 1:11:11') = TIMESTAMP "
            "'2009-6-30 1:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('year', 1, TIMESTAMP '2008-02-29 1:11:11') = TIMESTAMP "
                  "'2009-02-28 1:11:11' from test limit 1;",
                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPADD(YEAR, 1, TIMESTAMP '2008-02-29 1:11:11') = TIMESTAMP "
            "'2009-02-28 1:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPADD(YEAR, -8, TIMESTAMP '2008-02-29 1:11:11') = TIMESTAMP "
            "'2000-02-29 1:11:11' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPADD(YEAR, -8, TIMESTAMP '2008-02-29 1:11:11') = TIMESTAMP "
            "'2000-02-29 1:11:11' from test limit 1;",
            dt)));

    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT m = TIMESTAMP '2014-12-13 22:23:15' from test limit 1;", dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('day', 1, m) = TIMESTAMP "
                                        "'2014-12-14 22:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('day', -1, m) = TIMESTAMP "
                                        "'2014-12-12 22:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('day', 1, m) = TIMESTAMP "
                                        "'2014-12-14 22:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('day', -1, m) = TIMESTAMP "
                                        "'2014-12-12 22:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(
                  run_simple_agg("SELECT o = DATE '1999-09-09' from test limit 1;", dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('day', 1, o) = TIMESTAMP "
                                        "'1999-09-10 0:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('day', -3, o) = TIMESTAMP "
                                        "'1999-09-06 0:00:00' from test limit 1;",
                                        dt)));
    /* DATE ADD subseconds to default timestamp(0) */
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT DATEADD('millisecond', 1000, m) = TIMESTAMP "
                                  "'2014-12-13 22:23:16' from test limit 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT DATEADD('microsecond', 1000000, m) = TIMESTAMP "
                                  "'2014-12-13 22:23:16' from test limit 1;",
                                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('nanosecond', 1000000000, m) = TIMESTAMP "
                  "'2014-12-13 22:23:16' from test limit 1;",
                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT DATEADD('millisecond', 5123, m) = TIMESTAMP "
                                  "'2014-12-13 22:23:20' from test limit 1;",
                                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('microsecond', 86400000000, m) = TIMESTAMP "
                  "'2014-12-14 22:23:15' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('nanosecond', 86400000000123, m) = TIMESTAMP "
                  "'2014-12-14 22:23:15' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('weekday', -3, o) = TIMESTAMP "
                                        "'1999-09-06 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('decade', 3, o) = TIMESTAMP "
                                        "'2029-09-09 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('week', 1, o) = TIMESTAMP "
                                        "'1999-09-16 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(DAY, 1, TIMESTAMP '2009-03-02 "
                                  "1:23:45') = TIMESTAMP '2009-03-03 1:23:45' "
                                  "FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(DAY, -1, TIMESTAMP '2009-03-02 "
                                  "1:23:45') = TIMESTAMP '2009-03-01 1:23:45' "
                                  "FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(DAY, 15, TIMESTAMP '2009-03-02 "
                                  "1:23:45') = TIMESTAMP '2009-03-17 1:23:45' "
                                  "FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(DAY, -15, TIMESTAMP '2009-03-02 "
                                  "1:23:45') = TIMESTAMP '2009-02-15 1:23:45' "
                                  "FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(HOUR, 1, TIMESTAMP '2009-03-02 "
                                  "1:23:45') = TIMESTAMP '2009-03-02 2:23:45' "
                                  "FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(HOUR, -1, TIMESTAMP '2009-03-02 "
                                  "1:23:45') = TIMESTAMP '2009-03-02 0:23:45' "
                                  "FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(HOUR, 15, TIMESTAMP '2009-03-02 "
                                  "1:23:45') = TIMESTAMP '2009-03-02 16:23:45' "
                                  "FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(HOUR, -15, TIMESTAMP '2009-03-02 "
                                  "1:23:45') = TIMESTAMP '2009-03-01 10:23:45' "
                                  "FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(MINUTE, 15, TIMESTAMP '2009-03-02 "
                                  "1:23:45') = TIMESTAMP '2009-03-02 1:38:45' "
                                  "FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(
                  run_simple_agg("SELECT TIMESTAMPADD(MINUTE, -15, TIMESTAMP '2009-03-02 "
                                 "1:23:45') = TIMESTAMP '2009-03-02 1:08:45' "
                                 "FROM test LIMIT 1;",
                                 dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(SECOND, 15, TIMESTAMP '2009-03-02 "
                                  "1:23:45') = TIMESTAMP '2009-03-02 1:24:00' "
                                  "FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(
                  run_simple_agg("SELECT TIMESTAMPADD(SECOND, -15, TIMESTAMP '2009-03-02 "
                                 "1:23:45') = TIMESTAMP '2009-03-02 1:23:30' "
                                 "FROM test LIMIT 1;",
                                 dt)));

    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(DAY, 1, m) = TIMESTAMP '2014-12-14 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(DAY, -1, m) = TIMESTAMP '2014-12-12 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(DAY, 15, m) = TIMESTAMP '2014-12-28 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(DAY, -15, m) = TIMESTAMP '2014-11-28 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(HOUR, 1, m) = TIMESTAMP '2014-12-13 23:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(HOUR, -1, m) = TIMESTAMP '2014-12-13 21:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(HOUR, 15, m) = TIMESTAMP '2014-12-14 13:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(HOUR, -15, m) = TIMESTAMP '2014-12-13 7:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MINUTE, 15, m) = TIMESTAMP '2014-12-13 22:38:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MINUTE, -15, m) = TIMESTAMP '2014-12-13 22:08:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(SECOND, 15, m) = TIMESTAMP '2014-12-13 22:23:30' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(SECOND, -15, m) = TIMESTAMP '2014-12-13 22:23:00' "
                  "FROM test LIMIT 1;",
                  dt)));

    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MONTH, 1, m) = TIMESTAMP '2015-01-13 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MONTH, -1, m) = TIMESTAMP '2014-11-13 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MONTH, 5, m) = TIMESTAMP '2015-05-13 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(DAY, -5, m) = TIMESTAMP '2014-12-08 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(YEAR, 1, m) = TIMESTAMP '2015-12-13 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(YEAR, -1, m) = TIMESTAMP '2013-12-13 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(YEAR, 5, m) = TIMESTAMP '2019-12-13 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(YEAR, -5, m) = TIMESTAMP '2009-12-13 22:23:15' "
                  "FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg("select count(*) from test where TIMESTAMPADD(YEAR, "
                                  "15, CAST(o AS TIMESTAMP)) > m;",
                                  dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg("select count(*) from test where TIMESTAMPADD(YEAR, "
                                  "16, CAST(o AS TIMESTAMP)) > m;",
                                  dt)));

    ASSERT_EQ(
        128885,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPDIFF(minute, TIMESTAMP '2003-02-01 0:00:00', TIMESTAMP "
            "'2003-05-01 12:05:55') FROM test LIMIT 1;",
            dt)));
    ASSERT_EQ(2148,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPDIFF(hour, TIMESTAMP '2003-02-01 0:00:00', TIMESTAMP "
                  "'2003-05-01 12:05:55') FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(89,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPDIFF(day, TIMESTAMP '2003-02-01 0:00:00', TIMESTAMP "
                  "'2003-05-01 12:05:55') FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(3,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPDIFF(month, TIMESTAMP '2003-02-01 0:00:00', TIMESTAMP "
                  "'2003-05-01 12:05:55') FROM test LIMIT 1;",
                  dt)));
    ASSERT_EQ(
        -3,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPDIFF(month, TIMESTAMP '2003-05-01 12:05:55', TIMESTAMP "
            "'2003-02-01 0:00:00') FROM test LIMIT 1;",
            dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPDIFF(month, m, m + INTERVAL '5' MONTH) FROM test LIMIT 1;",
            dt)));
    ASSERT_EQ(
        -5,
        v<int64_t>(run_simple_agg(
            "SELECT TIMESTAMPDIFF(month, m, m - INTERVAL '5' MONTH) FROM test LIMIT 1;",
            dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg("select count(*) from test where TIMESTAMPDIFF(YEAR, "
                                  "m, CAST(o AS TIMESTAMP)) < 0;",
                                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF(year, DATE '2018-01-02', "
                                        "DATE '2019-03-04') FROM test LIMIT 1;",
                                        dt)));
    ASSERT_EQ(14,
              v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF(month, DATE '2018-01-02', "
                                        "DATE '2019-03-04') FROM test LIMIT 1;",
                                        dt)));
    ASSERT_EQ(426,
              v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF(day, DATE '2018-01-02', "
                                        "DATE '2019-03-04') FROM test LIMIT 1;",
                                        dt)));
    ASSERT_EQ(60,
              v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF(week, DATE '2018-01-02', "
                                        "DATE '2019-03-04') FROM test LIMIT 1;",
                                        dt)));
    ASSERT_EQ(
        60,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF(week_sunday, DATE '2018-01-02', "
                                  "DATE '2019-03-04') FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(
        60,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF(week_saturday, DATE "
                                  "'2018-01-02', DATE '2019-03-04') FROM test LIMIT 1;",
                                  dt)));
    ASSERT_EQ(613440,
              v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF(minute, DATE '2018-01-02', "
                                        "DATE '2019-03-04') FROM test LIMIT 1;",
                                        dt)));
    ASSERT_EQ(10224,
              v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF(hour, DATE '2018-01-02', "
                                        "DATE '2019-03-04') FROM test LIMIT 1;",
                                        dt)));
    ASSERT_EQ(36806400,
              v<int64_t>(run_simple_agg("SELECT TIMESTAMPDIFF(second, DATE '2018-01-02', "
                                        "DATE '2019-03-04') FROM test LIMIT 1;",
                                        dt)));

    ASSERT_EQ(
        1418428800LL,
        v<int64_t>(run_simple_agg("SELECT CAST(m AS date) FROM test LIMIT 1;", dt)));
    ASSERT_EQ(1336435200LL,
              v<int64_t>(run_simple_agg("SELECT CAST(CAST('2012-05-08 20:15:12' AS "
                                        "TIMESTAMP) AS DATE) FROM test LIMIT 1;",
                                        dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test GROUP BY CAST(m AS date);", dt)));
    const auto rows = run_multiple_agg(
        "SELECT DATE_TRUNC(month, CAST(o AS TIMESTAMP(0))) AS key0, str AS key1, "
        "COUNT(*) AS val FROM test GROUP BY "
        "key0, key1 ORDER BY val DESC, key1;",
        dt);
    check_date_trunc_groups(*rows);
    const auto one_row = run_multiple_agg(
        "SELECT DATE_TRUNC(year, CASE WHEN str = 'foo' THEN m END) d FROM test GROUP BY "
        "d "
        "HAVING d IS NOT NULL;",
        dt);
    check_one_date_trunc_group(*one_row, 1388534400);
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test where "
                                        "DATE '2017-05-30' = DATE '2017-05-31' OR "
                                        "DATE '2017-05-31' = DATE '2017-05-30';",
                                        dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test where "
                                        "EXTRACT(DOW from TIMESTAMPADD(HOUR, -5, "
                                        "TIMESTAMP '2017-05-31 1:11:11')) = 1 OR "
                                        "EXTRACT(DOW from TIMESTAMPADD(HOUR, -5, "
                                        "TIMESTAMP '2017-05-31 1:11:11')) = 2;",
                                        dt)));

    std::vector<std::tuple<std::string, int64_t, int64_t>> date_trunc_queries{
        /*TIMESTAMP(0) */
        std::make_tuple("year, m", 1388534400LL, 20),
        std::make_tuple("month, m", 1417392000LL, 20),
        std::make_tuple("day, m", 1418428800LL, 15),
        std::make_tuple("hour, m", 1418508000LL, 15),
        std::make_tuple("minute, m", 1418509380LL, 15),
        std::make_tuple("second, m", 1418509395LL, 15),
        std::make_tuple("millennium, m", 978307200LL, 20),
        std::make_tuple("century, m", 978307200LL, 20),
        std::make_tuple("decade, m", 1262304000LL, 20),
        std::make_tuple("week, m", 1417996800LL, 20),
        std::make_tuple("week_sunday, m", 1417910400LL, 15),
        std::make_tuple("week_saturday, m", 1418428800LL, 20),
        std::make_tuple("nanosecond, m", 1418509395LL, 15),
        std::make_tuple("microsecond, m", 1418509395LL, 15),
        std::make_tuple("millisecond, m", 1418509395LL, 15),
        /* TIMESTAMP(3) */
        std::make_tuple("year, m_3", 1388534400000LL, 20),
        std::make_tuple("month, m_3", 1417392000000LL, 20),
        std::make_tuple("day, m_3", 1418428800000LL, 15),
        std::make_tuple("hour, m_3", 1418508000000LL, 15),
        std::make_tuple("minute, m_3", 1418509380000LL, 15),
        std::make_tuple("second, m_3", 1418509395000LL, 15),
        std::make_tuple("millennium, m_3", 978307200000LL, 20),
        std::make_tuple("century, m_3", 978307200000LL, 20),
        std::make_tuple("decade, m_3", 1262304000000LL, 20),
        std::make_tuple("week, m_3", 1417996800000LL, 20),
        std::make_tuple("week_sunday, m_3", 1417910400000LL, 15),
        std::make_tuple("week_saturday, m_3", 1418428800000LL, 20),
        std::make_tuple("nanosecond, m_3", 1418509395323LL, 15),
        std::make_tuple("microsecond, m_3", 1418509395323LL, 15),
        std::make_tuple("millisecond, m_3", 1418509395323LL, 15),
        /* TIMESTAMP(6) */
        std::make_tuple("year, m_6", 915148800000000LL, 10),
        std::make_tuple("month, m_6", 930787200000000LL, 10),
        std::make_tuple("day, m_6", 931651200000000LL, 10),
        std::make_tuple("hour, m_6", 931701600000000LL, 10),
        /* std::make_tuple("minute, m_6", 931701720000000LL, 10), // Exception with sort
           watchdog */
        std::make_tuple("second, m_6", 931701773000000LL, 10),
        std::make_tuple("millennium, m_6", -30578688000000000LL, 10),
        std::make_tuple("century, m_6", -2177452800000000LL, 10),
        std::make_tuple("decade, m_6", 631152000000000LL, 10),
        std::make_tuple("week, m_6", 931132800000000LL, 10),
        std::make_tuple("week_sunday, m_6", 931651200000000LL, 10),
        std::make_tuple("week_saturday, m_6", 931564800000000LL, 10),
        std::make_tuple("nanosecond, m_6", 931701773874533LL, 10),
        std::make_tuple("microsecond, m_6", 931701773874533LL, 10),
        std::make_tuple("millisecond, m_6", 931701773874000LL, 10),
        /* TIMESTAMP(9) */
        std::make_tuple("year, m_9", 1136073600000000000LL, 10),
        std::make_tuple("month, m_9", 1143849600000000000LL, 10),
        std::make_tuple("day, m_9", 1146009600000000000LL, 10),
        std::make_tuple("hour, m_9", 1146020400000000000LL, 10),
        /* std::make_tuple("minute, m_9", 1146023340000000000LL, 10), // Exception with
           sort watchdog */
        std::make_tuple("second, m_9", 1146023344000000000LL, 10),
        std::make_tuple("millennium, m_9", 978307200000000000LL, 20),
        std::make_tuple("century, m_9", 978307200000000000LL, 20),
        std::make_tuple("decade, m_9", 946684800000000000LL, 10),
        std::make_tuple("week, m_9", 1145836800000000000LL, 10),
        std::make_tuple("week_sunday, m_9", 1145750400000000000LL, 10),
        std::make_tuple("week_saturday, m_9", 1145664000000000000LL, 10),
        std::make_tuple("nanosecond, m_9", 1146023344607435125LL, 10),
        std::make_tuple("microsecond, m_9", 1146023344607435000LL, 10),
        std::make_tuple("millisecond, m_9", 1146023344607000000LL, 10)};
    for (auto& query : date_trunc_queries) {
      const auto one_row = run_multiple_agg(
          "SELECT date_trunc(" + std::get<0>(query) +
              ") as key0,COUNT(*) AS val FROM test group by key0 order by key0 "
              "limit 1;",
          dt);
      check_one_date_trunc_group_with_agg(
          *one_row, std::get<1>(query), std::get<2>(query));
    }
    // Compressed DATE - limits test
    ASSERT_EQ(4708022400LL,
              v<int64_t>(run_simple_agg(
                  "select CAST('2119-03-12' AS DATE) FROM test limit 1;", dt)));
    ASSERT_EQ(7998912000LL,
              v<int64_t>(run_simple_agg("select CAST(CAST('2223-06-24 23:13:57' AS "
                                        "TIMESTAMP) AS DATE) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('year', 411, o) = TIMESTAMP "
                                        "'2410-09-09 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('year', -399, o) = TIMESTAMP "
                                        "'1600-09-09 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('month', 6132, o) = TIMESTAMP "
                                        "'2510-09-09 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('month', -1100, o) = TIMESTAMP "
                                        "'1908-01-09 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('day', 312456, o) = TIMESTAMP "
                                        "'2855-03-01 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('day', -23674, o) = TIMESTAMP "
                                        "'1934-11-15 00:00:00' from test limit 1 ;",
                                        dt)));
    ASSERT_EQ(
        -302,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('year', DATE '2302-04-21', o) from test limit 1;", dt)));
    ASSERT_EQ(
        501,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('year', o, DATE '2501-04-21') from test limit 1;", dt)));
    ASSERT_EQ(
        -4895,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('month', DATE '2407-09-01', o) from test limit 1;", dt)));
    ASSERT_EQ(
        3817,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('month', o, DATE '2317-11-01') from test limit 1;", dt)));
    ASSERT_EQ(
        -86972,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('day', DATE '2237-10-23', o) from test limit 1;", dt)));
    ASSERT_EQ(
        86972,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('day', o, DATE '2237-10-23') from test limit 1;", dt)));
    ASSERT_EQ(
        2617,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('year', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        12,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('month', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        23,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('day', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('hour', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('minute', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('second', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        6,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('weekday', CAST ('2011-12-31' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(365,
              v<int64_t>(run_simple_agg("SELECT DATEPART('dayofyear', CAST ('2011-12-31' "
                                        "as DATE)) from test limit 1;",
                                        dt)));
    // Compressed DATE - limits test
    ASSERT_EQ(4708022400LL,
              v<int64_t>(run_simple_agg(
                  "select CAST('2119-03-12' AS DATE) FROM test limit 1;", dt)));
    ASSERT_EQ(7998912000LL,
              v<int64_t>(run_simple_agg("select CAST(CAST('2223-06-24 23:13:57' AS "
                                        "TIMESTAMP) AS DATE) FROM test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('year', 411, o) = TIMESTAMP "
                                        "'2410-09-09 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('year', -399, o) = TIMESTAMP "
                                        "'1600-09-09 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('month', 6132, o) = TIMESTAMP "
                                        "'2510-09-09 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('month', -1100, o) = TIMESTAMP "
                                        "'1908-01-09 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('day', 312456, o) = TIMESTAMP "
                                        "'2855-03-01 00:00:00' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT DATEADD('day', -23674, o) = TIMESTAMP "
                                        "'1934-11-15 00:00:00' from test limit 1 ;",
                                        dt)));
    ASSERT_EQ(
        -302,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('year', DATE '2302-04-21', o) from test limit 1;", dt)));
    ASSERT_EQ(
        501,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('year', o, DATE '2501-04-21') from test limit 1;", dt)));
    ASSERT_EQ(
        -4895,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('month', DATE '2407-09-01', o) from test limit 1;", dt)));
    ASSERT_EQ(
        3817,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('month', o, DATE '2317-11-01') from test limit 1;", dt)));
    ASSERT_EQ(
        -86972,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('day', DATE '2237-10-23', o) from test limit 1;", dt)));
    ASSERT_EQ(
        86972,
        v<int64_t>(run_simple_agg(
            "SELECT DATEDIFF('day', o, DATE '2237-10-23') from test limit 1;", dt)));
    ASSERT_EQ(
        2617,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('year', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        12,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('month', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        23,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('day', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('hour', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('minute', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT DATEPART('second', CAST ('2617-12-23' as DATE)) from test limit 1;",
            dt)));
    /* Compressed Date ColumnarResults fetch tests*/
    ASSERT_EQ(1999,
              v<int64_t>(run_simple_agg("select yr from (SELECT EXTRACT(year from o) as "
                                        "yr, o from test order by x) limit 1;",
                                        dt)));
    ASSERT_EQ(936835200,
              v<int64_t>(run_simple_agg("select dy from (SELECT DATE_TRUNC(day, o) as "
                                        "dy, o from test order by x) limit 1;",
                                        dt)));
    ASSERT_EQ(936921600,
              v<int64_t>(run_simple_agg("select dy from (SELECT DATEADD('day', 1, o) as "
                                        "dy, o from test order by x) limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "select dy from (SELECT DATEDIFF('day', o, DATE '1999-09-10') as dy, o "
                  "from test order by x) limit 1;",
                  dt)));

    // range tests
    ASSERT_EQ(
        1417392000,
        v<int64_t>(run_simple_agg("SELECT date_trunc(month, m) as key0 FROM "
                                  "test WHERE (m >= TIMESTAMP(3) '1970-01-01 "
                                  "00:00:00.000') GROUP BY key0 ORDER BY key0 LIMIT 1;",
                                  dt)));
  }
}

TEST_F(Select, DateTruncate) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    ASSERT_EQ(1325376000LL,
              v<int64_t>(run_simple_agg(
                  R"(SELECT DATE_TRUNC(year, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
                  dt)));
    ASSERT_EQ(
        1335830400LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(month, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        1336435200LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(day, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)", dt)));
    ASSERT_EQ(1336507200LL,
              v<int64_t>(run_simple_agg(
                  R"(SELECT DATE_TRUNC(hour, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
                  dt)));
    ASSERT_EQ(
        1336508112LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(second, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        978307200LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(millennium, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        978307200LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(century, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        1262304000LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(decade, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        1336508112LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(millisecond, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        1336508112LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(microsecond, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(1336348800LL,
              v<int64_t>(run_simple_agg(
                  R"(SELECT DATE_TRUNC(week, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
                  dt)));
    ASSERT_EQ(
        1336348800LL - 24 * 3600,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(week_sunday, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        1336348800LL - 48 * 3600,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(week_saturday, CAST('2012-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(-2114380800LL,
              v<int64_t>(run_simple_agg(
                  R"(SELECT DATE_TRUNC(year, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
                  dt)));
    ASSERT_EQ(
        -2104012800LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(month, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        -2103408000LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(day, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)", dt)));
    ASSERT_EQ(-2103336000LL,
              v<int64_t>(run_simple_agg(
                  R"(SELECT DATE_TRUNC(hour, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
                  dt)));
    ASSERT_EQ(
        -2103335088LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(second, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        -30578688000LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(millennium, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        -2177452800LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(century, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        -2208988800LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(decade, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        -2103335088LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(millisecond, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        -2103335088LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(microsecond, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(-2103753600LL,
              v<int64_t>(run_simple_agg(
                  R"(SELECT DATE_TRUNC(week, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
                  dt)));
    ASSERT_EQ(
        -2103753600L - 24 * 3600,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(week_sunday, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        -2103753600L - 48 * 3600,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(week_saturday, CAST('1903-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(decade, CAST('1972-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    ASSERT_EQ(
        946684800LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(decade, CAST('2000-05-08 20:15:12' AS TIMESTAMP));)",
            dt)));
    // test QUARTER
    ASSERT_EQ(
        4,
        v<int64_t>(run_simple_agg(
            R"(SELECT EXTRACT(quarter FROM CAST('2008-11-27 12:12:12' AS timestamp));)",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            R"(SELECT EXTRACT(quarter FROM CAST('2008-03-21 12:12:12' AS timestamp));)",
            dt)));
    ASSERT_EQ(
        1199145600LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(quarter, CAST('2008-03-21 12:12:12' AS timestamp));)",
            dt)));
    ASSERT_EQ(
        1230768000LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(quarter, CAST('2009-03-21 12:12:12' AS timestamp));)",
            dt)));
    ASSERT_EQ(
        1254355200LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(quarter, CAST('2009-11-21 12:12:12' AS timestamp));)",
            dt)));
    ASSERT_EQ(
        946684800LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(quarter, CAST('2000-03-21 12:12:12' AS timestamp));)",
            dt)));
    ASSERT_EQ(
        -2208988800LL,
        v<int64_t>(run_simple_agg(
            R"(SELECT DATE_TRUNC(quarter, CAST('1900-03-21 12:12:12' AS timestamp));)",
            dt)));

    // Correctness tests for pre-epoch, epoch, and post-epoch dates
    auto check_epoch_result = [](const auto& result,
                                 const std::vector<int64_t>& expected) {
      EXPECT_EQ(result->rowCount(), expected.size());
      for (size_t i = 0; i < expected.size(); i++) {
        auto row = result->getNextRow(false, false);
        EXPECT_EQ(row.size(), size_t(1));
        EXPECT_EQ(expected[i], v<int64_t>(row[0]));
      }
    };

    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM dt) FROM test_date_time ORDER BY dt;)", dt),
        {-210038400, -53481600, 0, 344217600});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('year', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-220924800, -63158400, 0, 315532800});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('quarter', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-213148800, -55296000, 0, 339206400});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('month', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-210556800, -55296000, 0, 341884800});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('day', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-210038400, -53481600, 0, 344217600});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('hour', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-210038400, -53481600, 0, 344217600});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('minute', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-210038400, -53481600, 0, 344217600});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('second', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-210038400, -53481600, 0, 344217600});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('millennium', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-30578688000, -30578688000, -30578688000, -30578688000});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('century', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-2177452800, -2177452800, -2177452800, -2177452800});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('decade', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-315619200, -315619200, 0, 315532800});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('week', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-210124800, -53481600, -259200, 343872000});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('week_sunday', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-210211200, -53568000, -345600, 343785600});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('week_saturday', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-210297600, -53654400, -432000, 343699200});
    check_epoch_result(
        run_multiple_agg(
            R"(SELECT EXTRACT('epoch' FROM date_trunc('quarter', dt)) FROM test_date_time ORDER BY dt;)",
            dt),
        {-213148800, -55296000, 0, 339206400});
  }
}

TEST_F(Select, ExtractEpoch) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    // Test EXTRACT(epoch) for high-precision timestamps when read from a table.
    ASSERT_TRUE(v<int64_t>(run_simple_agg(
        "SELECT MIN(DATEDIFF('second', DATE '1970-01-01', dt) = EXTRACT('epoch' FROM "
        "CAST(dt AS TIMESTAMP(0)))) FROM test_date_time;",
        dt)));
    ASSERT_TRUE(v<int64_t>(run_simple_agg(
        "SELECT MIN(DATEDIFF('second', DATE '1970-01-01', dt) = EXTRACT('epoch' FROM "
        "CAST(dt AS TIMESTAMP(3)))) FROM test_date_time;",
        dt)));
    ASSERT_TRUE(v<int64_t>(run_simple_agg(
        "SELECT MIN(DATEDIFF('second', DATE '1970-01-01', dt) = EXTRACT('epoch' FROM "
        "CAST(dt AS TIMESTAMP(6)))) FROM test_date_time;",
        dt)));
    ASSERT_TRUE(v<int64_t>(run_simple_agg(
        "SELECT MIN(DATEDIFF('second', DATE '1970-01-01', dt) = EXTRACT('epoch' FROM "
        "CAST(dt AS TIMESTAMP(9)))) FROM test_date_time;",
        dt)));

    // Test EXTRACT(epoch) for constant high-precision timestamps.
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT('epoch' FROM TIMESTAMP(0) '1970-01-01 00:00:03');", dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT('epoch' FROM TIMESTAMP(3) '1970-01-01 00:00:03.123');", dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT('epoch' FROM TIMESTAMP(6) '1970-01-01 00:00:03.123456');",
            dt)));
    ASSERT_EQ(
        3,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT('epoch' FROM TIMESTAMP(9) '1970-01-01 00:00:03.123456789');",
            dt)));

    ASSERT_EQ(
        -3,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT('epoch' FROM TIMESTAMP(0) '1969-12-31 23:59:57');", dt)));
    ASSERT_EQ(
        -3,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT('epoch' FROM TIMESTAMP(3) '1969-12-31 23:59:57.123');", dt)));
    ASSERT_EQ(
        -3,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT('epoch' FROM TIMESTAMP(6) '1969-12-31 23:59:57.123456');",
            dt)));
    ASSERT_EQ(
        -3,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT('epoch' FROM TIMESTAMP(9) '1969-12-31 23:59:57.123456789');",
            dt)));
  }
}

TEST_F(Select, DateTruncate2) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    ASSERT_EQ("1900-01-01 12:34:59", date_trunc("SECOND", "1900-01-01 12:34:59", dt));
    ASSERT_EQ("1900-01-01 12:35:00", date_trunc("SECOND", "1900-01-01 12:35:00", dt));
    ASSERT_EQ("3900-01-01 12:34:59", date_trunc("SECOND", "3900-01-01 12:34:59", dt));
    ASSERT_EQ("3900-01-01 12:35:00", date_trunc("SECOND", "3900-01-01 12:35:00", dt));

    ASSERT_EQ("1900-01-01 12:34:00", date_trunc("MINUTE", "1900-01-01 12:34:59", dt));
    ASSERT_EQ("1900-01-01 12:35:00", date_trunc("MINUTE", "1900-01-01 12:35:00", dt));
    ASSERT_EQ("3900-01-01 12:34:00", date_trunc("MINUTE", "3900-01-01 12:34:59", dt));
    ASSERT_EQ("3900-01-01 12:35:00", date_trunc("MINUTE", "3900-01-01 12:35:00", dt));

    ASSERT_EQ("1900-01-01 12:00:00", date_trunc("HOUR", "1900-01-01 12:59:59", dt));
    ASSERT_EQ("1900-01-01 13:00:00", date_trunc("HOUR", "1900-01-01 13:00:00", dt));
    ASSERT_EQ("3900-01-01 12:00:00", date_trunc("HOUR", "3900-01-01 12:59:59", dt));
    ASSERT_EQ("3900-01-01 13:00:00", date_trunc("HOUR", "3900-01-01 13:00:00", dt));

    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("QUARTERDAY", "1900-01-01 00:00:00", dt));
    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("QUARTERDAY", "1900-01-01 05:59:59", dt));
    ASSERT_EQ("1900-01-01 06:00:00", date_trunc("QUARTERDAY", "1900-01-01 06:00:00", dt));
    ASSERT_EQ("1900-01-01 06:00:00", date_trunc("QUARTERDAY", "1900-01-01 11:59:59", dt));
    ASSERT_EQ("1900-01-01 12:00:00", date_trunc("QUARTERDAY", "1900-01-01 12:00:00", dt));
    ASSERT_EQ("1900-01-01 12:00:00", date_trunc("QUARTERDAY", "1900-01-01 17:59:59", dt));
    ASSERT_EQ("1900-01-01 18:00:00", date_trunc("QUARTERDAY", "1900-01-01 18:00:00", dt));
    ASSERT_EQ("1900-01-01 18:00:00", date_trunc("QUARTERDAY", "1900-01-01 23:59:59", dt));
    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("QUARTERDAY", "3900-01-01 00:00:00", dt));
    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("QUARTERDAY", "3900-01-01 05:59:59", dt));
    ASSERT_EQ("3900-01-01 06:00:00", date_trunc("QUARTERDAY", "3900-01-01 06:00:00", dt));
    ASSERT_EQ("3900-01-01 06:00:00", date_trunc("QUARTERDAY", "3900-01-01 11:59:59", dt));
    ASSERT_EQ("3900-01-01 12:00:00", date_trunc("QUARTERDAY", "3900-01-01 12:00:00", dt));
    ASSERT_EQ("3900-01-01 12:00:00", date_trunc("QUARTERDAY", "3900-01-01 17:59:59", dt));
    ASSERT_EQ("3900-01-01 18:00:00", date_trunc("QUARTERDAY", "3900-01-01 18:00:00", dt));
    ASSERT_EQ("3900-01-01 18:00:00", date_trunc("QUARTERDAY", "3900-01-01 23:59:59", dt));

    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("DAY", "1900-01-01 00:00:00", dt));
    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("DAY", "1900-01-01 23:59:59", dt));
    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("DAY", "3900-01-01 00:00:00", dt));
    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("DAY", "3900-01-01 23:59:59", dt));

    // 1900-01-01 is a Monday (= start of "WEEK").
    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("WEEK", "1900-01-01 00:00:00", dt));
    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("WEEK", "1900-01-07 23:59:59", dt));
    ASSERT_EQ("1900-01-08 00:00:00", date_trunc("WEEK", "1900-01-08 00:00:00", dt));
    ASSERT_EQ("1900-01-08 00:00:00", date_trunc("WEEK", "1900-01-14 23:59:59", dt));
    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("WEEK", "3900-01-01 00:00:00", dt));
    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("WEEK", "3900-01-07 23:59:59", dt));
    ASSERT_EQ("3900-01-08 00:00:00", date_trunc("WEEK", "3900-01-08 00:00:00", dt));
    ASSERT_EQ("3900-01-08 00:00:00", date_trunc("WEEK", "3900-01-14 23:59:59", dt));

    // 1899-12-31 is a Sunday (= start of "WEEK_SUNDAY").
    ASSERT_EQ("1899-12-31 00:00:00",
              date_trunc("WEEK_SUNDAY", "1899-12-31 00:00:00", dt));
    ASSERT_EQ("1899-12-31 00:00:00",
              date_trunc("WEEK_SUNDAY", "1900-01-06 23:59:59", dt));
    ASSERT_EQ("1900-01-07 00:00:00",
              date_trunc("WEEK_SUNDAY", "1900-01-07 00:00:00", dt));
    ASSERT_EQ("1900-01-07 00:00:00",
              date_trunc("WEEK_SUNDAY", "1900-01-13 23:59:59", dt));
    ASSERT_EQ("3899-12-31 00:00:00",
              date_trunc("WEEK_SUNDAY", "3899-12-31 00:00:00", dt));
    ASSERT_EQ("3899-12-31 00:00:00",
              date_trunc("WEEK_SUNDAY", "3900-01-06 23:59:59", dt));
    ASSERT_EQ("3900-01-07 00:00:00",
              date_trunc("WEEK_SUNDAY", "3900-01-07 00:00:00", dt));
    ASSERT_EQ("3900-01-07 00:00:00",
              date_trunc("WEEK_SUNDAY", "3900-01-13 23:59:59", dt));

    // 1899-12-30 is a Saturday (= start of "WEEK_SATURDAY").
    ASSERT_EQ("1899-12-30 00:00:00",
              date_trunc("WEEK_SATURDAY", "1899-12-30 00:00:00", dt));
    ASSERT_EQ("1899-12-30 00:00:00",
              date_trunc("WEEK_SATURDAY", "1900-01-05 23:59:59", dt));
    ASSERT_EQ("1900-01-06 00:00:00",
              date_trunc("WEEK_SATURDAY", "1900-01-06 00:00:00", dt));
    ASSERT_EQ("1900-01-06 00:00:00",
              date_trunc("WEEK_SATURDAY", "1900-01-12 23:59:59", dt));
    ASSERT_EQ("3899-12-30 00:00:00",
              date_trunc("WEEK_SATURDAY", "3899-12-30 00:00:00", dt));
    ASSERT_EQ("3899-12-30 00:00:00",
              date_trunc("WEEK_SATURDAY", "3900-01-05 23:59:59", dt));
    ASSERT_EQ("3900-01-06 00:00:00",
              date_trunc("WEEK_SATURDAY", "3900-01-06 00:00:00", dt));
    ASSERT_EQ("3900-01-06 00:00:00",
              date_trunc("WEEK_SATURDAY", "3900-01-12 23:59:59", dt));

    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("MONTH", "1900-01-01 00:00:00", dt));
    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("MONTH", "1900-01-31 23:59:59", dt));
    ASSERT_EQ("1900-02-01 00:00:00", date_trunc("MONTH", "1900-02-01 00:00:00", dt));
    ASSERT_EQ("1900-02-01 00:00:00", date_trunc("MONTH", "1900-02-28 23:59:59", dt));
    ASSERT_EQ("1900-03-01 00:00:00", date_trunc("MONTH", "1900-03-01 00:00:00", dt));
    ASSERT_EQ("1900-03-01 00:00:00", date_trunc("MONTH", "1900-03-31 23:59:59", dt));
    ASSERT_EQ("1900-04-01 00:00:00", date_trunc("MONTH", "1900-04-01 00:00:00", dt));
    ASSERT_EQ("1900-04-01 00:00:00", date_trunc("MONTH", "1900-04-30 23:59:59", dt));
    ASSERT_EQ("1900-05-01 00:00:00", date_trunc("MONTH", "1900-05-01 00:00:00", dt));
    ASSERT_EQ("1900-05-01 00:00:00", date_trunc("MONTH", "1900-05-31 23:59:59", dt));
    ASSERT_EQ("1900-06-01 00:00:00", date_trunc("MONTH", "1900-06-01 00:00:00", dt));
    ASSERT_EQ("1900-06-01 00:00:00", date_trunc("MONTH", "1900-06-30 23:59:59", dt));
    ASSERT_EQ("1900-07-01 00:00:00", date_trunc("MONTH", "1900-07-01 00:00:00", dt));
    ASSERT_EQ("1900-07-01 00:00:00", date_trunc("MONTH", "1900-07-31 23:59:59", dt));
    ASSERT_EQ("1900-08-01 00:00:00", date_trunc("MONTH", "1900-08-01 00:00:00", dt));
    ASSERT_EQ("1900-08-01 00:00:00", date_trunc("MONTH", "1900-08-31 23:59:59", dt));
    ASSERT_EQ("1900-09-01 00:00:00", date_trunc("MONTH", "1900-09-01 00:00:00", dt));
    ASSERT_EQ("1900-09-01 00:00:00", date_trunc("MONTH", "1900-09-30 23:59:59", dt));
    ASSERT_EQ("1900-10-01 00:00:00", date_trunc("MONTH", "1900-10-01 00:00:00", dt));
    ASSERT_EQ("1900-10-01 00:00:00", date_trunc("MONTH", "1900-10-31 23:59:59", dt));
    ASSERT_EQ("1900-11-01 00:00:00", date_trunc("MONTH", "1900-11-01 00:00:00", dt));
    ASSERT_EQ("1900-11-01 00:00:00", date_trunc("MONTH", "1900-11-30 23:59:59", dt));
    ASSERT_EQ("1900-12-01 00:00:00", date_trunc("MONTH", "1900-12-01 00:00:00", dt));
    ASSERT_EQ("1900-12-01 00:00:00", date_trunc("MONTH", "1900-12-31 23:59:59", dt));

    ASSERT_EQ("2000-01-01 00:00:00", date_trunc("MONTH", "2000-01-01 00:00:00", dt));
    ASSERT_EQ("2000-01-01 00:00:00", date_trunc("MONTH", "2000-01-31 23:59:59", dt));
    ASSERT_EQ("2000-02-01 00:00:00", date_trunc("MONTH", "2000-02-01 00:00:00", dt));
    ASSERT_EQ("2000-02-01 00:00:00", date_trunc("MONTH", "2000-02-29 23:59:59", dt));
    ASSERT_EQ("2000-03-01 00:00:00", date_trunc("MONTH", "2000-03-01 00:00:00", dt));
    ASSERT_EQ("2000-03-01 00:00:00", date_trunc("MONTH", "2000-03-31 23:59:59", dt));
    ASSERT_EQ("2000-04-01 00:00:00", date_trunc("MONTH", "2000-04-01 00:00:00", dt));
    ASSERT_EQ("2000-04-01 00:00:00", date_trunc("MONTH", "2000-04-30 23:59:59", dt));
    ASSERT_EQ("2000-05-01 00:00:00", date_trunc("MONTH", "2000-05-01 00:00:00", dt));
    ASSERT_EQ("2000-05-01 00:00:00", date_trunc("MONTH", "2000-05-31 23:59:59", dt));
    ASSERT_EQ("2000-06-01 00:00:00", date_trunc("MONTH", "2000-06-01 00:00:00", dt));
    ASSERT_EQ("2000-06-01 00:00:00", date_trunc("MONTH", "2000-06-30 23:59:59", dt));
    ASSERT_EQ("2000-07-01 00:00:00", date_trunc("MONTH", "2000-07-01 00:00:00", dt));
    ASSERT_EQ("2000-07-01 00:00:00", date_trunc("MONTH", "2000-07-31 23:59:59", dt));
    ASSERT_EQ("2000-08-01 00:00:00", date_trunc("MONTH", "2000-08-01 00:00:00", dt));
    ASSERT_EQ("2000-08-01 00:00:00", date_trunc("MONTH", "2000-08-31 23:59:59", dt));
    ASSERT_EQ("2000-09-01 00:00:00", date_trunc("MONTH", "2000-09-01 00:00:00", dt));
    ASSERT_EQ("2000-09-01 00:00:00", date_trunc("MONTH", "2000-09-30 23:59:59", dt));
    ASSERT_EQ("2000-10-01 00:00:00", date_trunc("MONTH", "2000-10-01 00:00:00", dt));
    ASSERT_EQ("2000-10-01 00:00:00", date_trunc("MONTH", "2000-10-31 23:59:59", dt));
    ASSERT_EQ("2000-11-01 00:00:00", date_trunc("MONTH", "2000-11-01 00:00:00", dt));
    ASSERT_EQ("2000-11-01 00:00:00", date_trunc("MONTH", "2000-11-30 23:59:59", dt));
    ASSERT_EQ("2000-12-01 00:00:00", date_trunc("MONTH", "2000-12-01 00:00:00", dt));
    ASSERT_EQ("2000-12-01 00:00:00", date_trunc("MONTH", "2000-12-31 23:59:59", dt));

    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("MONTH", "3900-01-01 00:00:00", dt));
    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("MONTH", "3900-01-31 23:59:59", dt));
    ASSERT_EQ("3900-02-01 00:00:00", date_trunc("MONTH", "3900-02-01 00:00:00", dt));
    ASSERT_EQ("3900-02-01 00:00:00", date_trunc("MONTH", "3900-02-28 23:59:59", dt));
    ASSERT_EQ("3900-03-01 00:00:00", date_trunc("MONTH", "3900-03-01 00:00:00", dt));
    ASSERT_EQ("3900-03-01 00:00:00", date_trunc("MONTH", "3900-03-31 23:59:59", dt));
    ASSERT_EQ("3900-04-01 00:00:00", date_trunc("MONTH", "3900-04-01 00:00:00", dt));
    ASSERT_EQ("3900-04-01 00:00:00", date_trunc("MONTH", "3900-04-30 23:59:59", dt));
    ASSERT_EQ("3900-05-01 00:00:00", date_trunc("MONTH", "3900-05-01 00:00:00", dt));
    ASSERT_EQ("3900-05-01 00:00:00", date_trunc("MONTH", "3900-05-31 23:59:59", dt));
    ASSERT_EQ("3900-06-01 00:00:00", date_trunc("MONTH", "3900-06-01 00:00:00", dt));
    ASSERT_EQ("3900-06-01 00:00:00", date_trunc("MONTH", "3900-06-30 23:59:59", dt));
    ASSERT_EQ("3900-07-01 00:00:00", date_trunc("MONTH", "3900-07-01 00:00:00", dt));
    ASSERT_EQ("3900-07-01 00:00:00", date_trunc("MONTH", "3900-07-31 23:59:59", dt));
    ASSERT_EQ("3900-08-01 00:00:00", date_trunc("MONTH", "3900-08-01 00:00:00", dt));
    ASSERT_EQ("3900-08-01 00:00:00", date_trunc("MONTH", "3900-08-31 23:59:59", dt));
    ASSERT_EQ("3900-09-01 00:00:00", date_trunc("MONTH", "3900-09-01 00:00:00", dt));
    ASSERT_EQ("3900-09-01 00:00:00", date_trunc("MONTH", "3900-09-30 23:59:59", dt));
    ASSERT_EQ("3900-10-01 00:00:00", date_trunc("MONTH", "3900-10-01 00:00:00", dt));
    ASSERT_EQ("3900-10-01 00:00:00", date_trunc("MONTH", "3900-10-31 23:59:59", dt));
    ASSERT_EQ("3900-11-01 00:00:00", date_trunc("MONTH", "3900-11-01 00:00:00", dt));
    ASSERT_EQ("3900-11-01 00:00:00", date_trunc("MONTH", "3900-11-30 23:59:59", dt));
    ASSERT_EQ("3900-12-01 00:00:00", date_trunc("MONTH", "3900-12-01 00:00:00", dt));
    ASSERT_EQ("3900-12-01 00:00:00", date_trunc("MONTH", "3900-12-31 23:59:59", dt));

    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("QUARTER", "1900-01-01 00:00:00", dt));
    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("QUARTER", "1900-03-31 23:59:59", dt));
    ASSERT_EQ("1900-04-01 00:00:00", date_trunc("QUARTER", "1900-04-01 00:00:00", dt));
    ASSERT_EQ("1900-04-01 00:00:00", date_trunc("QUARTER", "1900-06-30 23:59:59", dt));
    ASSERT_EQ("1900-07-01 00:00:00", date_trunc("QUARTER", "1900-07-01 00:00:00", dt));
    ASSERT_EQ("1900-07-01 00:00:00", date_trunc("QUARTER", "1900-09-30 23:59:59", dt));
    ASSERT_EQ("1900-10-01 00:00:00", date_trunc("QUARTER", "1900-10-01 00:00:00", dt));
    ASSERT_EQ("1900-10-01 00:00:00", date_trunc("QUARTER", "1900-12-31 23:59:59", dt));

    ASSERT_EQ("2000-01-01 00:00:00", date_trunc("QUARTER", "2000-01-01 00:00:00", dt));
    ASSERT_EQ("2000-01-01 00:00:00", date_trunc("QUARTER", "2000-03-31 23:59:59", dt));
    ASSERT_EQ("2000-04-01 00:00:00", date_trunc("QUARTER", "2000-04-01 00:00:00", dt));
    ASSERT_EQ("2000-04-01 00:00:00", date_trunc("QUARTER", "2000-06-30 23:59:59", dt));
    ASSERT_EQ("2000-07-01 00:00:00", date_trunc("QUARTER", "2000-07-01 00:00:00", dt));
    ASSERT_EQ("2000-07-01 00:00:00", date_trunc("QUARTER", "2000-09-30 23:59:59", dt));
    ASSERT_EQ("2000-10-01 00:00:00", date_trunc("QUARTER", "2000-10-01 00:00:00", dt));
    ASSERT_EQ("2000-10-01 00:00:00", date_trunc("QUARTER", "2000-12-31 23:59:59", dt));

    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("QUARTER", "3900-01-01 00:00:00", dt));
    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("QUARTER", "3900-03-31 23:59:59", dt));
    ASSERT_EQ("3900-04-01 00:00:00", date_trunc("QUARTER", "3900-04-01 00:00:00", dt));
    ASSERT_EQ("3900-04-01 00:00:00", date_trunc("QUARTER", "3900-06-30 23:59:59", dt));
    ASSERT_EQ("3900-07-01 00:00:00", date_trunc("QUARTER", "3900-07-01 00:00:00", dt));
    ASSERT_EQ("3900-07-01 00:00:00", date_trunc("QUARTER", "3900-09-30 23:59:59", dt));
    ASSERT_EQ("3900-10-01 00:00:00", date_trunc("QUARTER", "3900-10-01 00:00:00", dt));
    ASSERT_EQ("3900-10-01 00:00:00", date_trunc("QUARTER", "3900-12-31 23:59:59", dt));

    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("YEAR", "1900-01-01 00:00:00", dt));
    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("YEAR", "1900-12-31 23:59:59", dt));
    ASSERT_EQ("1901-01-01 00:00:00", date_trunc("YEAR", "1901-01-01 00:00:00", dt));
    ASSERT_EQ("1901-01-01 00:00:00", date_trunc("YEAR", "1901-12-31 23:59:59", dt));

    ASSERT_EQ("2000-01-01 00:00:00", date_trunc("YEAR", "2000-01-01 00:00:00", dt));
    ASSERT_EQ("2000-01-01 00:00:00", date_trunc("YEAR", "2000-12-31 23:59:59", dt));
    ASSERT_EQ("2001-01-01 00:00:00", date_trunc("YEAR", "2001-01-01 00:00:00", dt));
    ASSERT_EQ("2001-01-01 00:00:00", date_trunc("YEAR", "2001-12-31 23:59:59", dt));

    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("YEAR", "3900-01-01 00:00:00", dt));
    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("YEAR", "3900-12-31 23:59:59", dt));
    ASSERT_EQ("3901-01-01 00:00:00", date_trunc("YEAR", "3901-01-01 00:00:00", dt));
    ASSERT_EQ("3901-01-01 00:00:00", date_trunc("YEAR", "3901-12-31 23:59:59", dt));

    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("DECADE", "1900-01-01 00:00:00", dt));
    ASSERT_EQ("1900-01-01 00:00:00", date_trunc("DECADE", "1909-12-31 23:59:59", dt));
    ASSERT_EQ("1910-01-01 00:00:00", date_trunc("DECADE", "1910-01-01 00:00:00", dt));
    ASSERT_EQ("1910-01-01 00:00:00", date_trunc("DECADE", "1919-12-31 23:59:59", dt));

    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("DECADE", "3900-01-01 00:00:00", dt));
    ASSERT_EQ("3900-01-01 00:00:00", date_trunc("DECADE", "3909-12-31 23:59:59", dt));
    ASSERT_EQ("3910-01-01 00:00:00", date_trunc("DECADE", "3910-01-01 00:00:00", dt));
    ASSERT_EQ("3910-01-01 00:00:00", date_trunc("DECADE", "3919-12-31 23:59:59", dt));

    ASSERT_EQ("1801-01-01 00:00:00", date_trunc("CENTURY", "1801-01-01 00:00:00", dt));
    ASSERT_EQ("1801-01-01 00:00:00", date_trunc("CENTURY", "1900-12-31 23:59:59", dt));
    ASSERT_EQ("1901-01-01 00:00:00", date_trunc("CENTURY", "1901-01-01 00:00:00", dt));
    ASSERT_EQ("1901-01-01 00:00:00", date_trunc("CENTURY", "2000-12-31 23:59:59", dt));
    ASSERT_EQ("2001-01-01 00:00:00", date_trunc("CENTURY", "2001-01-01 00:00:00", dt));
    ASSERT_EQ("2001-01-01 00:00:00", date_trunc("CENTURY", "2100-12-31 23:59:59", dt));
    ASSERT_EQ("3901-01-01 00:00:00", date_trunc("CENTURY", "3901-01-01 00:00:00", dt));
    ASSERT_EQ("3901-01-01 00:00:00", date_trunc("CENTURY", "4000-12-31 23:59:59", dt));

    ASSERT_EQ("0001-01-01 00:00:00", date_trunc("MILLENNIUM", "0001-01-01 00:00:00", dt));
    ASSERT_EQ("0001-01-01 00:00:00", date_trunc("MILLENNIUM", "1000-12-31 23:59:59", dt));
    ASSERT_EQ("1001-01-01 00:00:00", date_trunc("MILLENNIUM", "1001-01-01 00:00:00", dt));
    ASSERT_EQ("1001-01-01 00:00:00", date_trunc("MILLENNIUM", "1900-12-31 23:59:59", dt));
    ASSERT_EQ("1001-01-01 00:00:00", date_trunc("MILLENNIUM", "1901-01-01 00:00:00", dt));
    ASSERT_EQ("1001-01-01 00:00:00", date_trunc("MILLENNIUM", "2000-12-31 23:59:59", dt));
    ASSERT_EQ("2001-01-01 00:00:00", date_trunc("MILLENNIUM", "2001-01-01 00:00:00", dt));
    ASSERT_EQ("2001-01-01 00:00:00", date_trunc("MILLENNIUM", "3000-12-31 23:59:59", dt));
    ASSERT_EQ("3001-01-01 00:00:00", date_trunc("MILLENNIUM", "3001-01-01 00:00:00", dt));
    ASSERT_EQ("3001-01-01 00:00:00", date_trunc("MILLENNIUM", "4000-12-31 23:59:59", dt));
    ASSERT_EQ("4001-01-01 00:00:00", date_trunc("MILLENNIUM", "4001-01-01 00:00:00", dt));
    ASSERT_EQ("4001-01-01 00:00:00", date_trunc("MILLENNIUM", "5000-12-31 23:59:59", dt));
  }
}

TEST_F(Select, TimeRedux) {
  // The time tests need a general cleanup. Collect tests found from specific bugs here so
  // we don't accidentally remove them
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    EXPECT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            R"(SELECT COUNT(*) FROM test WHERE o = (DATE '1999-09-01') OR CAST(o AS TIMESTAMP) = (TIMESTAMP '1999-09-09 00:00:00.000');)",
            dt)));
    EXPECT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            R"(SELECT COUNT(*) FROM test WHERE CAST(m AS DATE) = (DATE '2014-12-13');)",
            dt)));
    EXPECT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            R"(SELECT COUNT(*) FROM test WHERE CAST(m_3 AS DATE) = (DATE '2014-12-13');)",
            dt)));
    EXPECT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            R"(SELECT COUNT(*) FROM test WHERE CAST(m_6 AS DATE) = (DATE '1999-07-11');)",
            dt)));
    EXPECT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            R"(SELECT COUNT(*) FROM test WHERE CAST(m_9 AS DATE) = (DATE '2006-04-26');)",
            dt)));
  }
}

TEST_F(Select, In) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test WHERE x IN (7, 8);", dt);
    c("SELECT COUNT(*) FROM test WHERE x IN (9, 10);", dt);
    c("SELECT COUNT(*) FROM test WHERE z IN (101, 102);", dt);
    c("SELECT COUNT(*) FROM test WHERE z IN (201, 202);", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str IN ('real_foo', 'real_bar');", dt);
    c("SELECT COUNT(*) FROM test WHERE real_str IN ('real_foo', 'real_bar', 'real_baz', "
      "'foo');",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IN ('foo', 'bar', 'real_foo');", dt);
    c("SELECT COUNT(*) FROM test WHERE x IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, "
      "14, 15, 16, 17, 18, 19, 20);",
      dt);
  }
}

TEST_F(Select, DivByZero) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_THROW(run_multiple_agg("SELECT x / 0 FROM test;", dt), std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT 1 / 0 FROM test;", dt), std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT COUNT(distinct x / 0) FROM test;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT f / 0. FROM test;", dt), std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT d / 0. FROM test;", dt), std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT f / (f - f) FROM test;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT COUNT(*) FROM test GROUP BY y / (x - x);", dt),
                 std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg("SELECT COUNT(*) FROM test GROUP BY z, y / (x - x);", dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg("SELECT COUNT(*) FROM test GROUP BY MOD(y , (x - x));", dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT SUM(x) / SUM(CASE WHEN str = 'none' THEN y ELSE 0 END) FROM test;",
            dt),
        std::runtime_error);
    EXPECT_THROW(run_simple_agg("SELECT COUNT(*) FROM test WHERE y / (x - x) = 0;", dt),
                 std::runtime_error);
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE x = x OR  y / (x - x) = y;", dt)));
  }
}

TEST_F(Select, ReturnNullFromDivByZero) {
  config().exec.codegen.null_div_by_zero = true;
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT x / 0 FROM test;", dt);
    c("SELECT 1 / 0 FROM test;", dt);
    c("SELECT f / 0. FROM test;", dt);
    c("SELECT d / 0. FROM test;", dt);
    c("SELECT f / (f - f) FROM test;", dt);
    c("SELECT COUNT(*) FROM test GROUP BY y / (x - x);", dt);
    c("SELECT COUNT(*) n FROM test GROUP BY z, y / (x - x) ORDER BY n ASC;", dt);
    c("SELECT SUM(x) / SUM(CASE WHEN str = 'none' THEN y ELSE 0 END) FROM test;", dt);
    c("SELECT COUNT(*) FROM test WHERE y / (x - x) = 0;", dt);
    c("SELECT COUNT(*) FROM test WHERE x = x OR  y / (x - x) = y;", dt);
  }
}

TEST_F(Select, ReturnInfFromDivByZero) {
  config().exec.codegen.inf_div_by_zero = true;
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT f / 0. FROM test;", "SELECT 2e308 FROM test;", dt);
    c("SELECT d / 0. FROM test;", "SELECT 2e308 FROM test;", dt);
    c("SELECT -f / 0. FROM test;", "SELECT -2e308 FROM test;", dt);
    c("SELECT -d / 0. FROM test;", "SELECT -2e308 FROM test;", dt);
    c("SELECT f / (f - f) FROM test;", "SELECT 2e308 FROM test;", dt);
    c("SELECT (f - f) / 0. FROM test;", dt);
  }
}

TEST_F(Select, ConstantFolding) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT 1 + 2 FROM test limit 1;", dt);
    c("SELECT 1 + 2.3 FROM test limit 1;", dt);
    c("SELECT 2.3 + 1 FROM test limit 1;", dt);
    c("SELECT 2 * 3 FROM test limit 1;", dt);
    c("SELECT 604 * 575 FROM test limit 1;", dt);
    c("SELECT 604 * (75 + 500) FROM test limit 1;", dt);
    c("SELECT 604 * (5 * 115) FROM test limit 1;", dt);
    c("SELECT 100000 + (1 - 604 * 575) FROM test limit 1;", dt);
    c("SELECT 1 + 604 * 575 FROM test limit 1;", dt);
    c("SELECT 2 + (1 - 604 * 575) FROM test limit 1;", dt);
    c("SELECT t + 604 * 575 FROM test limit 1;", dt);  // mul is folded in BIGINT
    c("SELECT z + 604 * 575 FROM test limit 1;", dt);
    c("SELECT 9.1 + 2.9999999999 FROM test limit 1;", dt);
    c("SELECT -9.1 - 2.9999999999 FROM test limit 1;", dt);
    c("SELECT -(9.1 + 99.22) FROM test limit 1;", dt);
    c("SELECT 3/2 FROM test limit 1;", dt);
    c("SELECT 3/2.0 FROM test limit 1;", dt);
    c("SELECT 11.1 * 2.22 FROM test limit 1;", dt);
    c("SELECT 1.01 * 1.00001 FROM test limit 1;", dt);
    c("SELECT 11.1 * 2.222222222 FROM test limit 1;", dt);
    c("SELECT 9.99 * 9999.9 FROM test limit 1;", dt);
    c("SELECT 9.22337203685477 * 9.223 FROM test limit 1;", dt);
    c("SELECT 3.0+8 from test limit 1;", dt);
    c("SELECT 3.0*8 from test limit 1;", dt);
    c("SELECT 1.79769e+308 * 0.1 FROM test limit 1;", dt);
    c("SELECT COUNT(*) FROM test WHERE 3.0+8 < 30;", dt);
    c("SELECT COUNT(*) FROM test WHERE 3.0*8 > 30.01;", dt);
    c("SELECT COUNT(*) FROM test WHERE 3.0*8 > 30.0001;", dt);
    c("SELECT COUNT(*) FROM test WHERE ff + 3.0*8 < 60.0/2;", dt);
    c("SELECT COUNT(*) FROM test WHERE t > 0 AND t = t;", dt);
    c("SELECT COUNT(*) FROM test WHERE t > 0 AND t <> t;", dt);
    c("SELECT COUNT(*) FROM test WHERE t > 0 OR t = t;", dt);
    c("SELECT COUNT(*) FROM test WHERE t > 0 OR t <> t;", dt);
    c("SELECT COUNT(*) FROM test where (604=575) OR (33.0<>12 AND 2.0001e+4>20000.9) "
      "OR (NOT t>=t OR f<>f OR (x=x AND x-x=0));",
      dt);
  }
}

TEST_F(Select, OverflowAndUnderFlow) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test WHERE z + 32600 > 0;", dt);
    c("SELECT COUNT(*) FROM test WHERE z + 32666 > 0;", dt);
    c("SELECT COUNT(*) FROM test WHERE -32670 - z < 0;", dt);
    c("SELECT COUNT(*) FROM test WHERE (z + 16333) * 2 > 0;", dt);
    EXPECT_THROW(
        run_multiple_agg("SELECT COUNT(*) FROM test WHERE x + 2147483640 > 0;", dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg("SELECT COUNT(*) FROM test WHERE -x - 2147483642 < 0;", dt),
        std::runtime_error);
    c("SELECT COUNT(*) FROM test WHERE t + 9223372036854774000 > 0;", dt);
    EXPECT_THROW(run_multiple_agg(
                     "SELECT COUNT(*) FROM test WHERE t + 9223372036854775000 > 0;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg(
                     "SELECT COUNT(*) FROM test WHERE -t - 9223372036854775000 < 0;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT COUNT(*) FROM test WHERE ofd + x - 2 > 0;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg(
                     "SELECT COUNT(*) FROM test WHERE ufd * 3 - ofd * 1024 < -2;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT COUNT(*) FROM test WHERE ofd * 2 > 0;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT COUNT(*) FROM test WHERE ofq + 1 > 0;", dt),
                 std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT COUNT(*) FROM test WHERE -ufq - 9223372036854775000 > 0;", dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg("SELECT COUNT(*) FROM test WHERE -92233720368547758 - ofq <= 0;",
                         dt),
        std::runtime_error);
    c("SELECT cast((z - -32666) * 0.000190 as int) as key0, COUNT(*) AS val FROM test "
      "WHERE (z >= -32666 AND z < 31496) GROUP BY key0 HAVING key0 >= 0 AND key0 < 12 "
      "ORDER BY val DESC LIMIT 50 OFFSET 0;",
      dt);
    EXPECT_THROW(run_multiple_agg("SELECT dd * 2000000000000000 FROM test LIMIT 5;", dt),
                 std::runtime_error);
    c("SELECT dd * 200000000000000 FROM test ORDER BY dd ASC LIMIT 5;",
      dt);  // overflow avoided through decimal mul optimization
    c("SELECT COUNT(*) FROM test WHERE dd + 2.0000000000000009 > 110.0;",
      dt);  // no overflow in the cast
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT COUNT(*) FROM test WHERE dd + 2.00000000000000099 > 110.0;", dt),
        std::runtime_error);  // overflow in the cast due to higher precision
    c("SELECT dd / 2.00000009 FROM test ORDER BY dd ASC LIMIT 1;",
      dt);  // dividend still fits after cast and division upscaling
    EXPECT_THROW(run_multiple_agg("SELECT dd / 2.000000099 FROM test LIMIT 1;", dt),
                 std::runtime_error);  // dividend overflows after cast and division
                                       // upscaling due to higher precision
    c("SELECT (dd - 40.6364668888) / 2 FROM test ORDER BY dd ASC LIMIT 1;",
      dt);  // decimal div by const optimization avoids overflow
    c("SELECT (dd - 40.6364668888) / x FROM test ORDER BY dd ASC LIMIT 1;",
      dt);  // decimal div by int cast optimization avoids overflow
    c("SELECT (dd - 40.63646688) / dd FROM test ORDER BY dd ASC LIMIT 1;",
      dt);  // dividend still fits after upscaling from cast and division
    EXPECT_THROW(run_multiple_agg("select (dd-40.6364668888)/dd from test limit 1;", dt),
                 std::runtime_error);  // dividend overflows on upscaling on a slightly
                                       // higher precision, test detection
    EXPECT_THROW(run_multiple_agg("SELECT CAST(x * 10000 AS SMALLINT) FROM test;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT CAST(y * 1000 AS SMALLINT) FROM test;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT CAST(x * -10000 AS SMALLINT) FROM test;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT CAST(y * -1000 AS SMALLINT) FROM test;", dt),
                 std::runtime_error);
    c("SELECT cast((cast(z as int) - -32666) *0.000190 as int) as key0, "
      "COUNT(*) AS val FROM test WHERE (z >= -32666 AND z < 31496) "
      "GROUP BY key0 HAVING key0 >= 0 AND key0 < 12 ORDER BY val "
      "DESC LIMIT 50 OFFSET 0;",
      dt);
    c("select -1 * dd as expr from test order by expr asc;", dt);
    c("select dd * -1 as expr from test order by expr asc;", dt);
    c("select (dd - 1000000111.10) * dd as expr from test order by expr asc;", dt);
    c("select dd * (dd - 1000000111.10) as expr from test order by expr asc;", dt);
    // avoiding overflows in decimal compares against higher precision literals:
    // truncate literals based on the other side's precision, e.g. for d which is
    // DECIMAL(14,2)
    c("select count(*) from big_decimal_range_test where (d >  4.955357142857142);",
      dt);  // compare with 4.955
    c("select count(*) from big_decimal_range_test where (d >= 4.955357142857142);",
      dt);  // compare with 4.955
    c("select count(*) from big_decimal_range_test where (d <  4.955357142857142);",
      dt);  // compare with 4.955
    c("select count(*) from big_decimal_range_test where (d <= 4.955357142857142);",
      dt);  // compare with 4.955
    c("select count(*) from big_decimal_range_test where (d >= 4.950357142857142);",
      dt);  // compare with 4.951
    c("select count(*) from big_decimal_range_test where (d <  4.950357142857142);",
      dt);  // compare with 4.951
    c("select count(*) from big_decimal_range_test where (d < 59016609.300000056);",
      dt);  // compare with 59016609.301
    c("select count(*) from test where (t*123456 > 9681668.33071388567);",
      dt);  // compare with 9681668.3
    c("select count(*) from test where (x*12345678 < 9681668.33071388567);",
      dt);  // compare with 9681668.3
    c("select count(*) from test where (z*12345678 < 9681668.33071388567);",
      dt);  // compare with 9681668.3
    c("select count(*) from test where dd <= 111.222;", dt);
    c("select count(*) from test where dd >= -15264923.533545015;", dt);
    // avoiding overflows with constant folding and pushed down casts
    c("select count(*) + (604*575) from test;", dt);
    c("select count(*) - (604*575) from test;", dt);
    c("select count(*) * (604*575) from test;", dt);
    c("select (604*575) / count(*) from test;", dt);
    c("select (400604+575) / count(*) from test;", dt);
    c("select cast(count(*) as DOUBLE) + (604*575) from test;", dt);
    c("select cast(count(*) as DOUBLE) - (604*575) from test;", dt);
    c("select cast(count(*) as DOUBLE) * (604*575) from test;", dt);
    c("select (604*575) / cast(count(*) as DOUBLE) from test;", dt);
    c("select (12345-123456789012345) / cast(count(*) as DOUBLE) from test;", dt);
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg("SELECT COUNT(CAST(EXTRACT(QUARTER FROM CAST(NULL AS "
                                  "TIMESTAMP)) AS BIGINT) - 1) FROM test;",
                                  dt)));
  }
}

TEST_F(Select, DetectOverflowedLiteralBuf) {
  // constructing literal buf to trigger overflow takes too much time
  // so we mimic the literal buffer collection during codegen
  std::vector<CgenState::LiteralValue> literals;
  size_t literal_bytes{0};
  auto getOrAddLiteral = [&literals, &literal_bytes](const std::string& val) {
    const CgenState::LiteralValue var_val(val);
    literals.emplace_back(val);
    const auto lit_bytes = CgenState::literalBytes(var_val);
    literal_bytes = CgenState::addAligned(literal_bytes, lit_bytes);
    return literal_bytes - lit_bytes;
  };

  // add unique string literals until we detect the overflow
  // note that we only consider unique literals so we don't need to
  // lookup the existing literal buffer offset when adding the literal
  auto perform_test = [getOrAddLiteral]() {
    checked_int16_t checked_lit_off{-1};
    int added_literals = 0;
    try {
      for (; added_literals < 100000; ++added_literals) {
        checked_lit_off = getOrAddLiteral(std::to_string(added_literals));
      }
    } catch (const std::range_error& e) {
      throw TooManyLiterals();
    }
  };
  EXPECT_THROW(perform_test(), TooManyLiterals);
}

TEST_F(Select, BooleanColumn) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE bn;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE b;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows / 2),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE NOT bn;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE x < 8 AND bn;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE x < 8 AND NOT bn;", dt)));
    ASSERT_EQ(5,
              v<int64_t>(
                  run_simple_agg("SELECT COUNT(*) FROM test WHERE x > 7 OR false;", dt)));
    ASSERT_EQ(7,
              v<int64_t>(run_simple_agg(
                  "SELECT MAX(x) FROM test WHERE b = CAST('t' AS boolean);", dt)));
    ASSERT_EQ(static_cast<int64_t>(3 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  " SELECT SUM(2 *(CASE when x = 7 then 1 else 0 END)) FROM test;", dt)));
    c("SELECT COUNT(*) AS n FROM test GROUP BY x = 7, b ORDER BY n;", dt);
  }
}

TEST_F(Select, UnsupportedCast) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_THROW(run_multiple_agg("SELECT CAST(x AS VARCHAR) FROM test;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT CAST(f AS VARCHAR) FROM test;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT CAST(d AS VARCHAR) FROM test;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT CAST(f AS DECIMAL) FROM test;", dt),
                 std::runtime_error);
  }
}

TEST_F(Select, CastFromLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT CAST(2.3 AS TINYINT) FROM test;", dt);
    c("SELECT CAST(2.3 AS SMALLINT) FROM test;", dt);
    c("SELECT CAST(2.3 AS INT) FROM test;", dt);
    c("SELECT CAST(2.3 AS BIGINT) FROM test;", dt);
    c("SELECT CAST(2.3 AS FLOAT) FROM test;", dt);
    c("SELECT CAST(2.3 AS DOUBLE) FROM test;", dt);
    c("SELECT CAST(2.3 AS DECIMAL(2, 1)) FROM test;", dt);
    c("SELECT CAST(2.3 AS NUMERIC(2, 1)) FROM test;", dt);
    c("SELECT CAST(CAST(10 AS float) / CAST(3600 as float) AS float) FROM test LIMIT 1;",
      dt);
    c("SELECT CAST(CAST(10 AS double) / CAST(3600 as double) AS double) FROM test LIMIT "
      "1;",
      dt);
    c("SELECT z from test where z = -78;", dt);
  }
}

TEST_F(Select, CastFromNull) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT CAST(NULL AS TINYINT) FROM test;", dt);
    c("SELECT CAST(NULL AS SMALLINT) FROM test;", dt);
    c("SELECT CAST(NULL AS INT) FROM test;", dt);
    c("SELECT CAST(NULL AS BIGINT) FROM test;", dt);
    c("SELECT CAST(NULL AS FLOAT) FROM test;", dt);
    c("SELECT CAST(NULL AS DOUBLE) FROM test;", dt);
    c("SELECT CAST(NULL AS DECIMAL) FROM test;", dt);
    c("SELECT CAST(NULL AS NUMERIC) FROM test;", dt);
  }
}

TEST_F(Select, CastFromNull2) {
  createTable("cast_from_null2",
              {{"d", SQLTypeInfo(kDOUBLE)}, {"dd", SQLTypeInfo(kDECIMAL, 8, 2, false)}});
  insertCsvValues("cast_from_null2", "1.0,");
  run_sqlite_query("DROP TABLE IF EXISTS cast_from_null2;");
  run_sqlite_query("CREATE TABLE cast_from_null2 (d DOUBLE, dd DECIMAL(8,2));");
  run_sqlite_query("INSERT INTO cast_from_null2 VALUES (1.0, NULL);");
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT d * dd FROM cast_from_null2;", dt);
  }
  dropTable("cast_from_null2");
}

TEST_F(Select, CastRound) {
  auto const run = [](char const* n, char const* type, ExecutorDeviceType const dt) {
    return run_simple_agg(std::string("SELECT CAST(") + n + " AS " + type + ");", dt);
  };
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_EQ(127, v<int64_t>(run("127.4999999999999999", "TINYINT", dt)));
    EXPECT_ANY_THROW(run("127.5", "TINYINT", dt));  // overflow
    EXPECT_EQ(-128, v<int64_t>(run("-128.4999999999999999", "TINYINT", dt)));
    EXPECT_ANY_THROW(run("-128.5", "TINYINT", dt));  // overflow

    EXPECT_EQ(32767, v<int64_t>(run("32767.49999999999999", "SMALLINT", dt)));
    EXPECT_ANY_THROW(run("32767.5", "SMALLINT", dt));  // overflow
    EXPECT_EQ(-32768, v<int64_t>(run("-32768.49999999999999", "SMALLINT", dt)));
    EXPECT_ANY_THROW(run("-32768.5", "SMALLINT", dt));  // overflow

    EXPECT_EQ(2147483647, v<int64_t>(run("2147483647.499999999", "INT", dt)));
    EXPECT_ANY_THROW(run("2147483647.5", "INT", dt));  // overflow
    EXPECT_EQ(-2147483648, v<int64_t>(run("-2147483648.499999999", "INT", dt)));
    EXPECT_ANY_THROW(run("-2147483648.5", "INT", dt));  // overflow

    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              v<int64_t>(run("9223372036854775807.", "BIGINT", dt)));
    EXPECT_ANY_THROW(run("9223372036854775807.0", "BIGINT", dt));  // out of range
    EXPECT_ANY_THROW(run("9223372036854775807.5", "BIGINT", dt));  // out of range
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              v<int64_t>(run("-9223372036854775808.", "BIGINT", dt)));
    EXPECT_ANY_THROW(run("-9223372036854775808.0", "BIGINT", dt));  // out of range
    EXPECT_ANY_THROW(run("-9223372036854775808.5", "BIGINT", dt));  // out of range

    EXPECT_EQ(1e18f, v<float>(run("999999999999999999", "FLOAT", dt)));
    EXPECT_EQ(1e10f, v<float>(run("9999999999.99999999", "FLOAT", dt)));
    EXPECT_EQ(-1e18f, v<float>(run("-999999999999999999", "FLOAT", dt)));
    EXPECT_EQ(-1e10f, v<float>(run("-9999999999.99999999", "FLOAT", dt)));

    EXPECT_EQ(1e18, v<double>(run("999999999999999999", "DOUBLE", dt)));
    EXPECT_EQ(1e10, v<double>(run("9999999999.99999999", "DOUBLE", dt)));
    EXPECT_EQ(-1e18, v<double>(run("-999999999999999999", "DOUBLE", dt)));
    EXPECT_EQ(-1e10, v<double>(run("-9999999999.99999999", "DOUBLE", dt)));

    EXPECT_ANY_THROW(run("9223372036854775808e0", "BIGINT", dt));  // overflow
    EXPECT_ANY_THROW(run("9223372036854775807e0", "BIGINT", dt));  // overflow
    EXPECT_ANY_THROW(run("9223372036854775296e0", "BIGINT", dt));  // overflow
    // RHS = Largest integer that doesn't overflow when cast to DOUBLE to BIGINT.
    // LHS = Largest double value less than std::numeric_limits<int64_t>::max().
    EXPECT_EQ(9223372036854774784ll,
              v<int64_t>(run("9223372036854775295e0", "BIGINT", dt)));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              v<int64_t>(run("-9223372036854775808e0", "BIGINT", dt)));
    /* These results may be platform-dependent so are not included in tests.
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              v<int64_t>(run("-9223372036854776959e0", "BIGINT", dt)));
    EXPECT_ANY_THROW(run("-9223372036854776960e0", "BIGINT", dt));  // overflow
    */

    // Apply BIGINT tests to DECIMAL
    EXPECT_ANY_THROW(run("9223372036854775808e0", "DECIMAL", dt));  // overflow
    EXPECT_ANY_THROW(run("9223372036854775807e0", "DECIMAL", dt));  // overflow
    EXPECT_ANY_THROW(run("9223372036854775296e0", "DECIMAL", dt));  // overflow
    EXPECT_EQ(9223372036854774784.0,
              v<double>(run("9223372036854775295e0", "DECIMAL", dt)));
    EXPECT_EQ(static_cast<double>(std::numeric_limits<int64_t>::min()),
              v<double>(run("-9223372036854775808e0", "DECIMAL", dt)));

    EXPECT_ANY_THROW(run("2147483647.5e0", "INT", dt));  // overflow
    EXPECT_EQ(2147483647, v<int64_t>(run("2147483647.4999e0", "BIGINT", dt)));
    EXPECT_EQ(std::numeric_limits<int32_t>::min(),
              v<int64_t>(run("-2147483648.4999e0", "INT", dt)));
    EXPECT_ANY_THROW(run("-2147483648.5e0", "INT", dt));  // overflow

    EXPECT_ANY_THROW(run("32767.5e0", "SMALLINT", dt));  // overflow
    EXPECT_EQ(32767, v<int64_t>(run("32767.4999e0", "SMALLINT", dt)));
    EXPECT_EQ(-32768, v<int64_t>(run("-32768.4999e0", "SMALLINT", dt)));
    EXPECT_ANY_THROW(run("-32768.5e0", "SMALLINT", dt));  // overflow

    EXPECT_ANY_THROW(run("127.5e0", "TINYINT", dt));  // overflow
    EXPECT_EQ(127, v<int64_t>(run("127.4999e0", "TINYINT", dt)));
    EXPECT_EQ(-128, v<int64_t>(run("-128.4999e0", "TINYINT", dt)));
    EXPECT_ANY_THROW(run("-128.5e0", "TINYINT", dt));  // overflow

    EXPECT_TRUE(
        v<int64_t>(run_simple_agg("SELECT '292277026596-12-04 15:30:07' = "
                                  "CAST(9223372036854775807 AS TIMESTAMP(0));",
                                  dt)));
    EXPECT_TRUE(
        v<int64_t>(run_simple_agg("SELECT '292278994-08-17 07:12:55.807' = "
                                  "CAST(9223372036854775807 AS TIMESTAMP(3));",
                                  dt)));
    EXPECT_TRUE(v<int64_t>(
        run_simple_agg("SELECT CAST('294247-01-10 04:00:54.775807' AS TIMESTAMP(6)) = "
                       "CAST(9223372036854775807 AS TIMESTAMP(6));",
                       dt)));
    EXPECT_TRUE(v<int64_t>(
        run_simple_agg("SELECT CAST('2262-04-11 23:47:16.854775807' AS TIMESTAMP(9)) = "
                       "CAST(9223372036854775807 AS TIMESTAMP(9));",
                       dt)));
  }
}

TEST_F(Select, CastRoundNullable) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_EQ(
        20,
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE ROUND(f+0.2) = CAST(f+0.2 AS INT);", dt)));
    EXPECT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE ROUND(fn-0.2) = CAST(fn-0.2 AS INT);", dt)));
    EXPECT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE CAST(fn AS INT) IS NULL;", dt)));
    EXPECT_EQ(11,
              v<int64_t>(run_simple_agg("SELECT CAST(CAST(x AS FLOAT) * 1.6 AS INT) AS "
                                        "key0 FROM test GROUP BY key0 ORDER BY key0;",
                                        dt)));
  }
}

TEST_F(Select, ExtensionFunctionsTypeMatching) {
  createTable("extension_func_type_match_test",
              {
                  {"tinyint_type", SQLTypeInfo(kTINYINT)},
                  {"smallint_type", SQLTypeInfo(kSMALLINT)},
                  {"int_type", SQLTypeInfo(kTINYINT)},
                  {"bigint_type", SQLTypeInfo(kBIGINT)},
                  {"float_type", SQLTypeInfo(kFLOAT)},
                  {"double_type", SQLTypeInfo(kDOUBLE)},
                  {"decimal_7_type", SQLTypeInfo(kDECIMAL, 7, 1, false)},
                  {"decimal_8_type", SQLTypeInfo(kDECIMAL, 8, 1, false)},
              });
  insertCsvValues("extension_func_type_match_test", "10,10,10,10,10.0,10.0,10.0,10.0");
  const double float_result = 2.302585124969482;  // log(10) result using the fp32 version
                                                  // of the log extension function
  const double double_result =
      2.302585092994046;  // log(10) result using the fp64 version of the log extension
                          // function
  constexpr double RESULT_EPS =
      1.0e-8;  // Sufficient to differentiate fp32 and fp64 results
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      ASSERT_NEAR(
          double_result, v<double>(run_simple_agg("SELECT log(10);", dt)), RESULT_EPS);
    }

    {
      ASSERT_NEAR(
          double_result, v<double>(run_simple_agg("SELECT log(10.0);", dt)), RESULT_EPS);
    }

    {
      ASSERT_NEAR(double_result,
                  v<double>(run_simple_agg("SELECT log(CAST(10.0 AS FLOAT));", dt)),
                  RESULT_EPS);
    }

    {
      ASSERT_NEAR(
          double_result,
          v<double>(run_simple_agg(
              "SELECT log(tinyint_type) FROM extension_func_type_match_test;", dt)),
          RESULT_EPS);
    }

    {
      ASSERT_NEAR(
          double_result,
          v<double>(run_simple_agg(
              "SELECT log(smallint_type) FROM extension_func_type_match_test;", dt)),
          RESULT_EPS);
    }

    {
      ASSERT_NEAR(double_result,
                  v<double>(run_simple_agg(
                      "SELECT log(int_type) FROM extension_func_type_match_test;", dt)),
                  RESULT_EPS);
    }

    {
      ASSERT_NEAR(
          double_result,
          v<double>(run_simple_agg(
              "SELECT log(bigint_type) FROM extension_func_type_match_test;", dt)),
          RESULT_EPS);
    }

    {
      ASSERT_NEAR(float_result,
                  v<double>(run_simple_agg(
                      "SELECT log(float_type) FROM extension_func_type_match_test;", dt)),
                  RESULT_EPS);
    }

    {
      ASSERT_NEAR(
          double_result,
          v<double>(run_simple_agg(
              "SELECT log(double_type) FROM extension_func_type_match_test;", dt)),
          RESULT_EPS);
    }

    {
      ASSERT_NEAR(
          float_result,
          v<double>(run_simple_agg(
              "SELECT log(decimal_7_type) FROM extension_func_type_match_test;", dt)),
          RESULT_EPS);
    }

    {
      ASSERT_NEAR(
          double_result,
          v<double>(run_simple_agg(
              "SELECT log(decimal_8_type) FROM extension_func_type_match_test;", dt)),
          RESULT_EPS);
    }
  }
  dropTable("extension_func_type_match_test");
}

TEST_F(Select, CastDecimalToDecimal) {
  createTable("decimal_to_decimal_test",
              {{"id", SQLTypeInfo(kINT)}, {"val", SQLTypeInfo(kDECIMAL, 10, 5, false)}});
  insertCsvValues("decimal_to_decimal_test",
                  "1,456.78956\n2,456.12345\n-1,-456.78956\n-2,-456.12345");

  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    ASSERT_NEAR(456.78956,
                v<double>(run_simple_agg(
                    "SELECT val FROM decimal_to_decimal_test WHERE id = 1;", dt)),
                456.78956 * EPS);
    ASSERT_NEAR(-456.78956,
                v<double>(run_simple_agg(
                    "SELECT val FROM decimal_to_decimal_test WHERE id = -1;", dt)),
                456.78956 * EPS);
    ASSERT_NEAR(456.12345,
                v<double>(run_simple_agg(
                    "SELECT val FROM decimal_to_decimal_test WHERE id = 2;", dt)),
                EPS);
    ASSERT_NEAR(-456.12345,
                v<double>(run_simple_agg(
                    "SELECT val FROM decimal_to_decimal_test WHERE id = -2;", dt)),
                456.12345 * EPS);

    ASSERT_NEAR(456.7896,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,4)) FROM "
                                         "decimal_to_decimal_test WHERE id = 1;",
                                         dt)),
                456.7896 * EPS);
    ASSERT_NEAR(-456.7896,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,4)) FROM "
                                         "decimal_to_decimal_test WHERE id = -1;",
                                         dt)),
                456.7896 * EPS);
    ASSERT_NEAR(456.123,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,4)) FROM "
                                         "decimal_to_decimal_test WHERE id = 2;",
                                         dt)),
                456.123 * EPS);
    ASSERT_NEAR(-456.123,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,4)) FROM "
                                         "decimal_to_decimal_test WHERE id = -2;",
                                         dt)),
                456.123 * EPS);

    ASSERT_NEAR(456.790,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,3)) FROM "
                                         "decimal_to_decimal_test WHERE id = 1;",
                                         dt)),
                456.790 * EPS);
    ASSERT_NEAR(-456.790,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,3)) FROM "
                                         "decimal_to_decimal_test WHERE id = -1;",
                                         dt)),
                456.790 * EPS);
    ASSERT_NEAR(456.1234,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,3)) FROM "
                                         "decimal_to_decimal_test WHERE id = 2;",
                                         dt)),
                456.1234 * EPS);
    ASSERT_NEAR(-456.1234,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,3)) FROM "
                                         "decimal_to_decimal_test WHERE id = -2;",
                                         dt)),
                456.1234 * EPS);

    ASSERT_NEAR(456.79,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,2)) FROM "
                                         "decimal_to_decimal_test WHERE id = 1;",
                                         dt)),
                456.79 * EPS);
    ASSERT_NEAR(-456.79,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,2)) FROM "
                                         "decimal_to_decimal_test WHERE id = -1;",
                                         dt)),
                456.79 * EPS);
    ASSERT_NEAR(456.12,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,2)) FROM "
                                         "decimal_to_decimal_test WHERE id = 2;",
                                         dt)),
                456.12 * EPS);
    ASSERT_NEAR(-456.12,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,2)) FROM "
                                         "decimal_to_decimal_test WHERE id = -2;",
                                         dt)),
                456.12 * EPS);

    ASSERT_NEAR(456.8,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,1)) FROM "
                                         "decimal_to_decimal_test WHERE id = 1;",
                                         dt)),
                456.8 * EPS);
    ASSERT_NEAR(-456.8,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,1)) FROM "
                                         "decimal_to_decimal_test WHERE id = -1;",
                                         dt)),
                456.8 * EPS);
    ASSERT_NEAR(456.1,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,1)) FROM "
                                         "decimal_to_decimal_test WHERE id = 2;",
                                         dt)),
                456.1 * EPS);
    ASSERT_NEAR(-456.1,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,1)) FROM "
                                         "decimal_to_decimal_test WHERE id = -2;",
                                         dt)),
                456.1 * EPS);
    ASSERT_NEAR(457,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,0)) FROM "
                                         "decimal_to_decimal_test WHERE id = 1;",
                                         dt)),
                457 * EPS);
    ASSERT_NEAR(-457,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,0)) FROM "
                                         "decimal_to_decimal_test WHERE id = -1;",
                                         dt)),
                457 * EPS);
    ASSERT_NEAR(456,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,0)) FROM "
                                         "decimal_to_decimal_test WHERE id = 2;",
                                         dt)),
                456 * EPS);
    ASSERT_NEAR(-456,
                v<double>(run_simple_agg("SELECT CAST(val AS DECIMAL(10,0)) FROM "
                                         "decimal_to_decimal_test WHERE id = -2;",
                                         dt)),
                456 * EPS);

    ASSERT_EQ(457,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(val AS BIGINT) FROM decimal_to_decimal_test WHERE id = 1;",
                  dt)));
    ASSERT_EQ(
        -457,
        v<int64_t>(run_simple_agg(
            "SELECT CAST(val AS BIGINT) FROM decimal_to_decimal_test WHERE id = -1;",
            dt)));
    ASSERT_EQ(456,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(val AS BIGINT) FROM decimal_to_decimal_test WHERE id = 2;",
                  dt)));
    ASSERT_EQ(
        -456,
        v<int64_t>(run_simple_agg(
            "SELECT CAST(val AS BIGINT) FROM decimal_to_decimal_test WHERE id = -2;",
            dt)));
  }
  dropTable("decimal_to_decimal_test");
}

TEST_F(Select, ColumnWidths) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT DISTINCT x FROM test_inner ORDER BY x;", dt);
    c("SELECT DISTINCT y FROM test_inner ORDER BY y;", dt);
    c("SELECT DISTINCT xx FROM test_inner ORDER BY xx;", dt);
    c("SELECT x, xx, y FROM test_inner GROUP BY x, xx, y ORDER BY x, xx, y;", dt);
    c("SELECT x, xx, y FROM test_inner GROUP BY x, xx, y ORDER BY x, xx, y;", dt);
    c("SELECT DISTINCT str from test_inner ORDER BY str;", dt);
    c("SELECT DISTINCT t FROM test ORDER BY t;", dt);
    c("SELECT DISTINCT t, z FROM test GROUP BY t, z ORDER BY t, z;", dt);
    c("SELECT fn from test where fn < -100.7 ORDER BY fn;", dt);
    c("SELECT fixed_str, SUM(f)/SUM(t)  FROM test WHERE fixed_str IN ('foo','bar') GROUP "
      "BY fixed_str ORDER BY "
      "fixed_str;",
      dt);
  }
}

TEST_F(Select, TimeInterval) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(
        60 * 60 * 1000LL,
        v<int64_t>(run_simple_agg("SELECT INTERVAL '1' HOUR FROM test LIMIT 1;", dt)));
    ASSERT_EQ(
        24 * 60 * 60 * 1000LL,
        v<int64_t>(run_simple_agg("SELECT INTERVAL '1' DAY FROM test LIMIT 1;", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (INTERVAL '1' YEAR)/12 FROM test order by o LIMIT 1;", dt)));
    ASSERT_EQ(
        1LL,
        v<int64_t>(run_simple_agg(
            "SELECT INTERVAL '1' MONTH FROM test group by m order by m LIMIT 1;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE INTERVAL '1' MONTH < INTERVAL '2' MONTH;",
            dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE INTERVAL '1' DAY < INTERVAL '2' DAY;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test GROUP BY INTERVAL '1' DAY;", dt)));
    ASSERT_EQ(3 * 60 * 60 * 1000LL,
              v<int64_t>(
                  run_simple_agg("SELECT 3 * INTERVAL '1' HOUR FROM test LIMIT 1;", dt)));
    ASSERT_EQ(3 * 60 * 60 * 1000LL,
              v<int64_t>(
                  run_simple_agg("SELECT INTERVAL '1' HOUR * 3 FROM test LIMIT 1;", dt)));
    ASSERT_EQ(7LL,
              v<int64_t>(run_simple_agg(
                  "SELECT INTERVAL '1' MONTH * x FROM test WHERE x <> 8 LIMIT 1;", dt)));
    ASSERT_EQ(7LL,
              v<int64_t>(run_simple_agg(
                  "SELECT x * INTERVAL '1' MONTH FROM test WHERE x <> 8 LIMIT 1;", dt)));
    ASSERT_EQ(42LL,
              v<int64_t>(run_simple_agg(
                  "SELECT INTERVAL '1' MONTH * y FROM test WHERE y <> 43 LIMIT 1;", dt)));
    ASSERT_EQ(42LL,
              v<int64_t>(run_simple_agg(
                  "SELECT y * INTERVAL '1' MONTH FROM test WHERE y <> 43 LIMIT 1;", dt)));
    ASSERT_EQ(
        1002LL,
        v<int64_t>(run_simple_agg(
            "SELECT INTERVAL '1' MONTH * t FROM test WHERE t <> 1001 LIMIT 1;", dt)));
    ASSERT_EQ(
        1002LL,
        v<int64_t>(run_simple_agg(
            "SELECT t * INTERVAL '1' MONTH FROM test WHERE t <> 1001 LIMIT 1;", dt)));
    ASSERT_EQ(
        3LL,
        v<int64_t>(run_simple_agg(
            "SELECT INTERVAL '1' MONTH + INTERVAL '2' MONTH FROM test LIMIT 1;", dt)));
    ASSERT_EQ(
        1388534400LL,
        v<int64_t>(run_simple_agg("SELECT CAST(m AS date) + CAST(TRUNCATE(-1 * "
                                  "(EXTRACT(DOY FROM m) - 1), 0) AS INTEGER) * INTERVAL "
                                  "'1' DAY AS g FROM test GROUP BY g;",
                                  dt)));
    ASSERT_EQ(
        1417392000LL,
        v<int64_t>(run_simple_agg("SELECT CAST(m AS date) + CAST(TRUNCATE(-1 * "
                                  "(EXTRACT(DAY FROM m) - 1), 0) AS INTEGER) * INTERVAL "
                                  "'1' DAY AS g FROM test GROUP BY g;",
                                  dt)));
    ASSERT_EQ(1418508000LL,
              v<int64_t>(run_simple_agg("SELECT CAST(m AS date) + EXTRACT(HOUR FROM m) * "
                                        "INTERVAL '1' HOUR AS g FROM test GROUP BY g;",
                                        dt)));
    ASSERT_EQ(
        1388534400LL,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(SQL_TSI_DAY, CAST(TRUNCATE(-1 * "
                                  "(EXTRACT(DOY from m) - 1), 0) AS INTEGER), "
                                  "CAST(m AS DATE)) AS g FROM test GROUP BY g;",
                                  dt)));
    ASSERT_EQ(
        1417392000LL,
        v<int64_t>(run_simple_agg("SELECT TIMESTAMPADD(SQL_TSI_DAY, CAST(TRUNCATE(-1 * "
                                  "(EXTRACT(DAY from m) - 1), 0) AS INTEGER), "
                                  "CAST(m AS DATE)) AS g FROM test GROUP BY g;",
                                  dt)));
    ASSERT_EQ(1418508000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(SQL_TSI_HOUR, EXTRACT(HOUR from "
                  "m), CAST(m AS DATE)) AS g FROM test GROUP BY g order by g;",
                  dt)));

    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-1-31' + INTERVAL '1' YEAR) = "
                                        "DATE '2009-01-31' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-1-31' + INTERVAL '5' YEAR) = "
                                        "DATE '2013-01-31' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-1-31' - INTERVAL '1' YEAR) = "
                                        "DATE '2007-01-31' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-1-31' - INTERVAL '4' YEAR) = "
                                        "DATE '2004-01-31' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-1-31' + INTERVAL '1' MONTH) "
                                        "= DATE '2008-02-29' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-1-31' + INTERVAL '5' MONTH) "
                                        "= DATE '2008-06-30' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-1-31' - INTERVAL '1' MONTH) "
                                        "= DATE '2007-12-31' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-1-31' - INTERVAL '4' MONTH) "
                                        "= DATE '2007-09-30' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-2-28' + INTERVAL '1' DAY) = "
                                        "DATE '2008-02-29' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2009-2-28' + INTERVAL '1' DAY) = "
                                        "DATE '2009-03-01' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-2-28' + INTERVAL '4' DAY) = "
                                        "DATE '2008-03-03' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2009-2-28' + INTERVAL '4' DAY) = "
                                        "DATE '2009-03-04' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-03-01' - INTERVAL '1' DAY) = "
                                        "DATE '2008-02-29' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2009-03-01' - INTERVAL '1' DAY) = "
                                        "DATE '2009-02-28' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2008-03-03' - INTERVAL '4' DAY) = "
                                        "DATE '2008-02-28' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (DATE '2009-03-04' - INTERVAL '4' DAY) = "
                                        "DATE '2009-02-28' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT m = TIMESTAMP '2014-12-13 22:23:15' from test limit 1;", dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (m + INTERVAL '1' SECOND) = TIMESTAMP "
                                        "'2014-12-13 22:23:16' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (m + INTERVAL '1' MINUTE) = TIMESTAMP "
                                        "'2014-12-13 22:24:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (m + INTERVAL '1' HOUR) = TIMESTAMP "
                                        "'2014-12-13 23:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (m + INTERVAL '2' DAY) = TIMESTAMP "
                                        "'2014-12-15 22:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (m + INTERVAL '1' MONTH) = TIMESTAMP "
                                        "'2015-01-13 22:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (m + INTERVAL '1' YEAR) = TIMESTAMP "
                                        "'2015-12-13 22:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT (m - 5 * INTERVAL '1' SECOND) = TIMESTAMP "
                                  "'2014-12-13 22:23:10' from test limit 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT (m - x * INTERVAL '1' MINUTE) = TIMESTAMP "
                                  "'2014-12-13 22:16:15' from test limit 1;",
                                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT (m - 2 * x * INTERVAL '1' HOUR) = TIMESTAMP "
                                  "'2014-12-13 8:23:15' from test limit 1;",
                                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (m - x * INTERVAL '1' DAY) = TIMESTAMP "
                                        "'2014-12-06 22:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (m - x * INTERVAL '1' MONTH) = TIMESTAMP "
                                        "'2014-05-13 22:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg("SELECT (m - x * INTERVAL '1' YEAR) = TIMESTAMP "
                                        "'2007-12-13 22:23:15' from test limit 1;",
                                        dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT (m - INTERVAL '5' DAY + INTERVAL '2' HOUR - x * INTERVAL '2' "
                  "SECOND) +"
                  "(x - 1) * INTERVAL '1' MONTH - x * INTERVAL '10' YEAR = "
                  "TIMESTAMP '1945-06-09 00:23:01' from test limit 1;",
                  dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "select count(*) from test where m < CAST (o AS TIMESTAMP) + INTERVAL '10' "
            "YEAR AND m > CAST(o AS TIMESTAMP) - INTERVAL '10' YEAR;",
            dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            "select count(*) from test where m < CAST (o AS TIMESTAMP) + INTERVAL '16' "
            "YEAR AND m > CAST(o AS TIMESTAMP) - INTERVAL '16' YEAR;",
            dt)));

    ASSERT_EQ(1,
              v<int64_t>(
                  run_simple_agg("SELECT o = DATE '1999-09-09' from test limit 1;", dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT (o + INTERVAL '10' DAY) = DATE '1999-09-19' from test limit 1;",
                  dt)));
  }
}

TEST_F(Select, LogicalValues) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    // empty logical values
    c("SELECT 1 + 2;", dt);
    c("SELECT 1 * 2.1;", dt);
    c("SELECT 'alex', 'omnisci';", dt);
    c("SELECT COALESCE(5, NULL, 4);", dt);
    c("SELECT abs(-5) AS tmp;", dt);

    EXPECT_EQ(6, v<double>(run_simple_agg("SELECT ceil(5.556) AS tmp;", dt)));
    EXPECT_EQ(5, v<double>(run_simple_agg("SELECT floor(5.556) AS tmp;", dt)));

    // values
    c("SELECT * FROM (VALUES(1,2,3));", dt);
    c("SELECT * FROM (VALUES(1, NULL, 3));", dt);
    c("SELECT * FROM (VALUES(1, 2), (3, NULL));", dt);
    c("SELECT * FROM (SELECT * FROM (VALUES (1,2) , (3,4)) t1) t0 LIMIT 5;", dt);

    {
      auto rows = run_multiple_agg("SELECT * FROM (VALUES(1, 2, 3)) as t(x, y, z);", dt);
      EXPECT_EQ(rows->rowCount(), size_t(1));
      const auto row = rows->getNextRow(false, false);
      EXPECT_EQ(1, v<int64_t>(row[0]));
      EXPECT_EQ(2, v<int64_t>(row[1]));
      EXPECT_EQ(3, v<int64_t>(row[2]));
    }
    {
      auto rows = run_multiple_agg(
          "SELECT x, COUNT(y) FROM (VALUES(1, 1), (2, 2), (NULL, NULL), (3, 3)) as t(x, "
          "y) GROUP BY x;",
          dt);
      EXPECT_EQ(rows->rowCount(), size_t(4));
      {
        const auto row = rows->getNextRow(false, false);
        EXPECT_EQ(1, v<int64_t>(row[0]));
        EXPECT_EQ(1, v<int64_t>(row[1]));
      }
      {
        const auto row = rows->getNextRow(false, false);
        EXPECT_EQ(2, v<int64_t>(row[0]));
        EXPECT_EQ(1, v<int64_t>(row[1]));
      }
      {
        const auto row = rows->getNextRow(false, false);
        EXPECT_EQ(3, v<int64_t>(row[0]));
        EXPECT_EQ(1, v<int64_t>(row[1]));
      }
      {
        const auto row = rows->getNextRow(false, false);
        EXPECT_EQ(inline_int_null_val(SQLTypeInfo(kINT, false)), v<int64_t>(row[0]));
        EXPECT_EQ(0, v<int64_t>(row[1]));
      }
    }
    {
      auto rows = run_multiple_agg(
          "SELECT SUM(x), AVG(y), MIN(z) FROM (VALUES(1, 2, 3)) as t(x, y, z);", dt);
      EXPECT_EQ(rows->rowCount(), size_t(1));
      const auto row = rows->getNextRow(false, false);
      EXPECT_EQ(1, v<int64_t>(row[0]));
      EXPECT_EQ(2, v<double>(row[1]));
      EXPECT_EQ(3, v<int64_t>(row[2]));
    }
    {
      auto rows = run_multiple_agg("SELECT * FROM (VALUES(1, 2, 3),(4, 5, 6));", dt);
      EXPECT_EQ(rows->rowCount(), size_t(2));
      {
        const auto row = rows->getNextRow(false, false);
        EXPECT_EQ(1, v<int64_t>(row[0]));
        EXPECT_EQ(2, v<int64_t>(row[1]));
        EXPECT_EQ(3, v<int64_t>(row[2]));
      }
      {
        const auto row = rows->getNextRow(false, false);
        EXPECT_EQ(4, v<int64_t>(row[0]));
        EXPECT_EQ(5, v<int64_t>(row[1]));
        EXPECT_EQ(6, v<int64_t>(row[2]));
      }
    }
    {
      auto rows = run_multiple_agg(
          "SELECT SUM(x), AVG(y), MIN(z) FROM (VALUES(1, 2, 3),(4, 5, 6)) as t(x, y, z);",
          dt);
      EXPECT_EQ(rows->rowCount(), size_t(1));
      const auto row = rows->getNextRow(false, false);
      EXPECT_EQ(5, v<int64_t>(row[0]));
      ASSERT_NEAR(3.5, v<double>(row[1]), double(0.01));
      EXPECT_EQ(3, v<int64_t>(row[2]));
    }
    EXPECT_ANY_THROW(run_simple_agg("SELECT * FROM (VALUES(1, 'test'));", dt));

    EXPECT_ANY_THROW(run_simple_agg("SELECT (1,2);", dt));

    {
      auto eo = getExecutionOptions(false, true);
      auto co = getCompilationOptions(dt);
      co.hoist_literals = true;
      const auto query_explain_result = runSqlQuery("SELECT 1+2;", co, eo);
      const auto explain_result = query_explain_result.getRows();
      EXPECT_EQ(size_t(1), explain_result->rowCount());
      const auto crt_row = explain_result->getNextRow(true, true);
      EXPECT_EQ(size_t(1), crt_row.size());
      const auto explain_str = boost::get<std::string>(v<NullableString>(crt_row[0]));
      EXPECT_TRUE(explain_str.find("IR for the ") == 0);
    }
  }
}

TEST_F(Select, UnsupportedNodes) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // MAT No longer throws a logicalValues gets a regular parse error'
    // EXPECT_THROW(run_multiple_agg("SELECT *;", dt), std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT x, COUNT(*) FROM test GROUP BY ROLLUP(x);", dt),
                 std::runtime_error);
  }
}

TEST_F(Select, UnsupportedMultipleArgAggregate) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_THROW(run_multiple_agg("SELECT COUNT(distinct x, y) FROM test;", dt),
                 std::runtime_error);
  }
}

TEST_F(Select, ArrayUnnest) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    unsigned power10 = 1;
    for (const unsigned int_width : {16, 32, 64}) {
      auto result_rows =
          run_multiple_agg("SELECT COUNT(*), UNNEST(arr_i" + std::to_string(int_width) +
                               ") AS a FROM array_test GROUP BY a ORDER BY a DESC;",
                           dt);
      ASSERT_EQ(g_array_test_row_count + 2, result_rows->rowCount());
      ASSERT_EQ(int64_t(g_array_test_row_count + 2) * power10,
                v<int64_t>(result_rows->getRowAt(0, 1, true)));
      ASSERT_EQ(1,
                v<int64_t>(result_rows->getRowAt(g_array_test_row_count + 1, 0, true)));
      ASSERT_EQ(1, v<int64_t>(result_rows->getRowAt(0, 0, true)));
      ASSERT_EQ(power10,
                v<int64_t>(result_rows->getRowAt(g_array_test_row_count + 1, 1, true)));

      auto fixed_result_rows =
          run_multiple_agg("SELECT COUNT(*), UNNEST(arr3_i" + std::to_string(int_width) +
                               ") AS a FROM array_test GROUP BY a ORDER BY a DESC;",
                           dt);
      ASSERT_EQ(g_array_test_row_count + 2, fixed_result_rows->rowCount());
      ASSERT_EQ(int64_t(g_array_test_row_count + 2) * power10,
                v<int64_t>(fixed_result_rows->getRowAt(0, 1, true)));
      ASSERT_EQ(
          1,
          v<int64_t>(fixed_result_rows->getRowAt(g_array_test_row_count + 1, 0, true)));
      ASSERT_EQ(1, v<int64_t>(fixed_result_rows->getRowAt(0, 0, true)));
      ASSERT_EQ(
          power10,
          v<int64_t>(fixed_result_rows->getRowAt(g_array_test_row_count + 1, 1, true)));

      power10 *= 10;
    }
    for (const std::string float_type : {"float", "double"}) {
      auto result_rows =
          run_multiple_agg("SELECT COUNT(*), UNNEST(arr_" + float_type +
                               ") AS a FROM array_test GROUP BY a ORDER BY a DESC;",
                           dt);
      ASSERT_EQ(g_array_test_row_count + 2, result_rows->rowCount());
      ASSERT_EQ(1,
                v<int64_t>(result_rows->getRowAt(g_array_test_row_count + 1, 0, true)));
      ASSERT_EQ(1, v<int64_t>(result_rows->getRowAt(0, 0, true)));

      auto fixed_result_rows =
          run_multiple_agg("SELECT COUNT(*), UNNEST(arr3_" + float_type +
                               ") AS a FROM array_test GROUP BY a ORDER BY a DESC;",
                           dt);
      ASSERT_EQ(g_array_test_row_count + 2, fixed_result_rows->rowCount());
      ASSERT_EQ(
          1,
          v<int64_t>(fixed_result_rows->getRowAt(g_array_test_row_count + 1, 0, true)));
      ASSERT_EQ(1, v<int64_t>(fixed_result_rows->getRowAt(0, 0, true)));
    }
    {
      auto result_rows = run_multiple_agg(
          "SELECT COUNT(*), UNNEST(arr_str) AS a FROM array_test GROUP BY a ORDER BY a "
          "DESC;",
          dt);
      ASSERT_EQ(g_array_test_row_count + 2, result_rows->rowCount());
      ASSERT_EQ(1,
                v<int64_t>(result_rows->getRowAt(g_array_test_row_count + 1, 0, true)));
      ASSERT_EQ(1, v<int64_t>(result_rows->getRowAt(0, 0, true)));
    }
    {
      auto result_rows = run_multiple_agg(
          "SELECT COUNT(*), UNNEST(arr_bool) AS a FROM array_test GROUP BY a ORDER BY a "
          "DESC;",
          dt);
      ASSERT_EQ(size_t(2), result_rows->rowCount());
      ASSERT_EQ(int64_t(g_array_test_row_count * 3),
                v<int64_t>(result_rows->getRowAt(0, 0, true)));
      ASSERT_EQ(int64_t(g_array_test_row_count * 3),
                v<int64_t>(result_rows->getRowAt(1, 0, true)));
      ASSERT_EQ(1, v<int64_t>(result_rows->getRowAt(0, 1, true)));
      ASSERT_EQ(0, v<int64_t>(result_rows->getRowAt(1, 1, true)));

      auto fixed_result_rows = run_multiple_agg(
          "SELECT COUNT(*), UNNEST(arr6_bool) AS a FROM array_test GROUP BY a ORDER BY a "
          "DESC;",
          dt);
      ASSERT_EQ(size_t(2), fixed_result_rows->rowCount());
      ASSERT_EQ(int64_t(g_array_test_row_count * 3),
                v<int64_t>(fixed_result_rows->getRowAt(0, 0, true)));
      ASSERT_EQ(int64_t(g_array_test_row_count * 3),
                v<int64_t>(fixed_result_rows->getRowAt(1, 0, true)));
      ASSERT_EQ(1, v<int64_t>(fixed_result_rows->getRowAt(0, 1, true)));
      ASSERT_EQ(0, v<int64_t>(fixed_result_rows->getRowAt(1, 1, true)));
    }

    // unnest groupby, force estimator run
    const auto big_group_threshold = config().exec.group_by.big_group_threshold;
    ScopeGuard reset_big_group_threshold = [&big_group_threshold] {
      // this sets the "has estimation" parameter to false for baseline hash groupby of
      // small tables, forcing the estimator to run
      config().exec.group_by.big_group_threshold = big_group_threshold;
    };
    config().exec.group_by.big_group_threshold = 1;

    EXPECT_EQ(
        v<int64_t>(run_simple_agg(
            R"(SELECT count(*) FROM (SELECT  unnest(arr_str), unnest(arr_float) FROM array_test GROUP BY 1, 2);)",
            dt)),
        int64_t(104));
  }
}

TEST_F(Select, ArrayIndex) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    for (size_t row_idx = 0; row_idx < g_array_test_row_count; ++row_idx) {
      ASSERT_EQ(1,
                v<int64_t>(run_simple_agg(
                    "SELECT COUNT(*) FROM array_test WHERE arr_i32[2] = " +
                        std::to_string(10 * (row_idx + 2)) +
                        " AND x = " + std::to_string(7 + row_idx) +
                        " AND arr3_i32[2] = " + std::to_string(10 * (row_idx + 2)) +
                        " AND real_str LIKE 'real_str" + std::to_string(row_idx) + "';",
                    dt)));
      ASSERT_EQ(0,
                v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM array_test WHERE "
                                          "arr_i32[4] > 0 OR arr_i32[4] <= 0 OR "
                                          "arr3_i32[4] > 0 OR arr3_i32[4] <= 0;",
                                          dt)));
      ASSERT_EQ(0,
                v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM array_test WHERE "
                                          "arr_i32[0] > 0 OR arr_i32[0] <= 0 OR "
                                          "arr3_i32[0] > 0 OR arr3_i32[0] <= 0;",
                                          dt)));
    }
    for (size_t i = 1; i <= 6; ++i) {
      ASSERT_EQ(
          int64_t(g_array_test_row_count / 2),
          v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM array_test WHERE arr_bool[" +
                                        std::to_string(i) +
                                        "] AND "
                                        "arr6_bool[" +
                                        std::to_string(i) + "];",
                                    dt)));
    }
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE arr_bool[7];", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE arr6_bool[7];", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE arr_bool[0];", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE arr6_bool[0];", dt)));
    ASSERT_EQ(int64_t(0),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM array_test WHERE NOT "
                                        "(arr_i16[7] > 0 AND arr_i16[7] <= 0 AND "
                                        "arr3_i16[7] > 0 AND arr3_i16[7] <= 0);",
                                        dt)));
    ASSERT_EQ(int64_t(g_array_test_row_count),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM array_test WHERE NOT "
                                        "(arr_i16[2] > 0 AND arr_i16[2] <= 0 AND "
                                        "arr3_i16[2] > 0 AND arr3_i16[2] <= 0);",
                                        dt)));
  }
}

TEST_F(Select, ArrayCountDistinct) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    for (const unsigned int_width : {16, 32, 64}) {
      ASSERT_EQ(
          int64_t(g_array_test_row_count + 2),
          v<int64_t>(run_simple_agg("SELECT COUNT(distinct arr_i" +
                                        std::to_string(int_width) + ") FROM array_test;",
                                    dt)));
      auto result_rows =
          run_multiple_agg("SELECT COUNT(distinct arr_i" + std::to_string(int_width) +
                               ") FROM array_test GROUP BY x;",
                           dt);
      ASSERT_EQ(g_array_test_row_count, result_rows->rowCount());
      for (size_t row_idx = 0; row_idx < g_array_test_row_count; ++row_idx) {
        ASSERT_EQ(3, v<int64_t>(result_rows->getRowAt(row_idx, 0, true)));
      }

      ASSERT_EQ(
          int64_t(g_array_test_row_count + 2),
          v<int64_t>(run_simple_agg("SELECT COUNT(distinct arr3_i" +
                                        std::to_string(int_width) + ") FROM array_test;",
                                    dt)));
      auto fixed_result_rows =
          run_multiple_agg("SELECT COUNT(distinct arr3_i" + std::to_string(int_width) +
                               ") FROM array_test GROUP BY x;",
                           dt);
      ASSERT_EQ(g_array_test_row_count, fixed_result_rows->rowCount());
      for (size_t row_idx = 0; row_idx < g_array_test_row_count; ++row_idx) {
        ASSERT_EQ(3, v<int64_t>(fixed_result_rows->getRowAt(row_idx, 0, true)));
      }
    }
    for (const std::string float_type : {"float", "double"}) {
      ASSERT_EQ(
          int64_t(g_array_test_row_count + 2),
          v<int64_t>(run_simple_agg(
              "SELECT COUNT(distinct arr_" + float_type + ") FROM array_test;", dt)));
      ASSERT_EQ(
          int64_t(g_array_test_row_count + 2),
          v<int64_t>(run_simple_agg(
              "SELECT COUNT(distinct arr3_" + float_type + ") FROM array_test;", dt)));
    }
    ASSERT_EQ(int64_t(g_array_test_row_count + 2),
              v<int64_t>(
                  run_simple_agg("SELECT COUNT(distinct arr_str) FROM array_test;", dt)));
    ASSERT_EQ(2,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(distinct arr_bool) FROM array_test;", dt)));
    ASSERT_EQ(2,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(distinct arr6_bool) FROM array_test;", dt)));
  }
}

TEST_F(Select, ArrayAnyAndAll) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    unsigned power10 = 1;
    for (const unsigned int_width : {16, 32, 64}) {
      ASSERT_EQ(
          2,
          v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM array_test WHERE " +
                                        std::to_string(2 * power10) + " = ANY arr_i" +
                                        std::to_string(int_width) + " AND " +
                                        std::to_string(2 * power10) + " = ANY arr3_i" +
                                        std::to_string(int_width) + ";",
                                    dt)));
      ASSERT_EQ(
          int64_t(g_array_test_row_count) - 2,
          v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM array_test WHERE " +
                                        std::to_string(2 * power10) + " < ALL arr_i" +
                                        std::to_string(int_width) + " AND " +
                                        std::to_string(2 * power10) + " < ALL arr3_i" +
                                        std::to_string(int_width) + ";",
                                    dt)));
      power10 *= 10;
    }
    for (const std::string float_type : {"float", "double", "decimal"}) {
      ASSERT_EQ(
          int64_t(g_array_test_row_count),
          v<int64_t>(run_simple_agg(
              "SELECT COUNT(*) FROM array_test WHERE 1 < ANY arr_" + float_type + ";",
              dt)));
      ASSERT_EQ(
          int64_t(g_array_test_row_count),
          v<int64_t>(run_simple_agg(
              "SELECT COUNT(*) FROM array_test WHERE 2 < ANY arr_" + float_type + ";",
              dt)));
      ASSERT_EQ(
          int64_t(g_array_test_row_count),
          v<int64_t>(run_simple_agg(
              "SELECT COUNT(*) FROM array_test WHERE 0 < ALL arr_" + float_type + ";",
              dt)));
      ASSERT_EQ(
          int64_t(g_array_test_row_count),
          v<int64_t>(run_simple_agg(
              "SELECT COUNT(*) FROM array_test WHERE 0 < ALL arr3_" + float_type + ";",
              dt)));
    }
    ASSERT_EQ(int64_t(g_array_test_row_count),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE x - 5 = ANY arr_i16;", dt)));
    ASSERT_EQ(int64_t(g_array_test_row_count),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE x - 5 = ANY arr3_i16;", dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE 'aa' = ANY arr_str;", dt)));
    ASSERT_EQ(2,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE 'bb' = ANY arr_str;", dt)));
    ASSERT_EQ(
        int64_t(g_array_test_row_count),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM array_test WHERE CAST('t' AS boolean) = ANY arr_bool;",
            dt)));
    ASSERT_EQ(
        int64_t(g_array_test_row_count),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM array_test WHERE CAST('t' AS boolean) = ANY arr6_bool;",
            dt)));
    ASSERT_EQ(
        int64_t(0),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM array_test WHERE CAST('t' AS boolean) = ALL arr_bool;",
            dt)));
    ASSERT_EQ(int64_t(g_array_test_row_count - 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE 'bb' < ALL arr_str;", dt)));
    ASSERT_EQ(int64_t(g_array_test_row_count - 1),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE 'bb' <= ALL arr_str;", dt)));
    ASSERT_EQ(int64_t(1),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE 'bb' > ANY arr_str;", dt)));
    ASSERT_EQ(int64_t(2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE 'bb' >= ANY arr_str;", dt)));
    ASSERT_EQ(int64_t(0),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM array_test WHERE  real_str = ANY arr_str;", dt)));
    ASSERT_EQ(
        int64_t(g_array_test_row_count),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM array_test WHERE  real_str <> ANY arr_str;", dt)));
    ASSERT_EQ(
        int64_t(g_array_test_row_count - 1),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM array_test WHERE (NOT ('aa' = ANY arr_str));", dt)));
    // these two test just confirm that the regex does not mess with other similar
    // patterns
    ASSERT_EQ(
        int64_t(g_array_test_row_count),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) as SMALL FROM array_test;", dt)));
    ASSERT_EQ(
        int64_t(g_array_test_row_count),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) as COMPANY FROM array_test;", dt)));
  }
}

TEST_F(Select, ArrayUnsupported) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_THROW(run_multiple_agg("SELECT MIN(arr_i64) FROM array_test;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg("SELECT MIN(arr3_i64) FROM array_test;", dt),
                 std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT UNNEST(arr_str), COUNT(*) cc FROM array_test GROUP BY arr_str;", dt),
        std::runtime_error);
  }
}

TEST_F(Select, ExpressionRewrite) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT count(*) from test where f/2.0 >= 0.6;", dt);
    c("SELECT count(*) from test where d/0.5 < 5.0;", dt);
  }
}

TEST_F(Select, OrRewrite) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test WHERE str = 'foo' OR str = 'bar' OR str = 'baz' OR str "
      "= 'foo' OR str = 'bar' OR str "
      "= 'baz' OR str = 'foo' OR str = 'bar' OR str = 'baz' OR str = 'baz' OR str = "
      "'foo' OR str = 'bar' OR str = "
      "'baz';",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x = 7 OR x = 8 OR x = 7 OR x = 8 OR x = 7 OR x = "
      "8 OR x = 7 OR x = 8 OR x = 7 "
      "OR x = 8 OR x = 7 OR x = 8;",
      dt);
  }
}

TEST_F(Select, GpuSort) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT x, COUNT(*) AS val FROM gpu_sort_test GROUP BY x ORDER BY val DESC;", dt);
    c("SELECT y, COUNT(*) AS val FROM gpu_sort_test GROUP BY y ORDER BY val DESC;", dt);
    c("SELECT y, COUNT(*), COUNT(*) AS val FROM gpu_sort_test GROUP BY y ORDER BY val "
      "DESC;",
      dt);
    c("SELECT z, COUNT(*) AS val FROM gpu_sort_test GROUP BY z ORDER BY val DESC;", dt);
    c("SELECT t, COUNT(*) AS val FROM gpu_sort_test GROUP BY t ORDER BY val DESC;", dt);
  }
}

TEST_F(Select, SpeculativeTopNSort) {
  ScopeGuard reset = [orig = config().exec.parallel_top_min] {
    config().exec.parallel_top_min = orig;
  };
  size_t test_values[]{size_t(0), config().exec.parallel_top_min};
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    for (auto parallel_top_min : test_values) {
      config().exec.parallel_top_min = parallel_top_min;
      c("SELECT x, COUNT(*) AS val FROM gpu_sort_test GROUP BY x ORDER BY val DESC LIMIT "
        "2;",
        dt);
      c("SELECT x from (SELECT COUNT(*) AS val, x FROM gpu_sort_test GROUP BY x ORDER BY "
        "val ASC LIMIT 3);",
        dt);
      c("SELECT val from (SELECT y, COUNT(*) AS val FROM gpu_sort_test GROUP BY y ORDER "
        "BY val DESC LIMIT 3);",
        dt);
      c("SELECT w, APPROX_COUNT_DISTINCT(x) acd FROM test GROUP BY w ORDER BY acd LIMIT "
        "2;",
        "SELECT w, COUNT(DISTINCT x) acd FROM test GROUP BY w ORDER BY acd LIMIT 2;",
        dt);
      c("SELECT w, APPROX_COUNT_DISTINCT(x) acd FROM test GROUP BY w ORDER BY acd DESC "
        "LIMIT 2;",
        "SELECT w, COUNT(DISTINCT x) acd FROM test GROUP BY w ORDER BY acd DESC LIMIT 2;",
        dt);
    }
  }
}

TEST_F(Select, TopNSortWithWatchdogOn) {
  ScopeGuard reset = [top_min = config().exec.parallel_top_min,
                      top_max = config().exec.watchdog.parallel_top_max,
                      watchdog = config().exec.watchdog.enable] {
    config().exec.parallel_top_min = top_min;
    config().exec.watchdog.parallel_top_max = top_max;
    config().exec.watchdog.enable = watchdog;
  };
  config().exec.parallel_top_min = 0;
  config().exec.watchdog.parallel_top_max = 10;
  // Let's assume we have top-K query as SELECT ... ORDER BY ... LIMIT K
  // Currently, when columnar output is on (either by default or manually turned on)
  // QMD decides to use resultset's cardinality instead of K for its entry count
  // Then if we enable watchdog, we get the watchdog exception when sorting
  // if QMD's entry_count > config().exec.watchdog.parallel_top_max (also >
  // config().exec.parallel_top_min)
  // ("Sorting the result would be too slow")
  bool test_values[]{true, false};
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    for (auto watchdog : test_values) {
      config().exec.watchdog.enable = watchdog;
      EXPECT_NO_THROW(
          run_multiple_agg("SELECT x FROM gpu_sort_test ORDER BY x DESC", dt));
      EXPECT_NO_THROW(run_multiple_agg(
          "SELECT x FROM gpu_sort_test ORDER BY x DESC LIMIT 2 OFFSET 0;", dt));
      try {
        run_multiple_agg("SELECT x FROM gpu_sort_test ORDER BY x DESC LIMIT 8 OFFSET 0;",
                         dt);
      } catch (const WatchdogException& e) {
        EXPECT_TRUE(true);
      }
    }
  }
}

TEST_F(Select, GroupByPerfectHash) {
  const auto default_bigint_flag = config().exec.group_by.bigint_count;
  ScopeGuard reset = [default_bigint_flag] {
    config().exec.group_by.bigint_count = default_bigint_flag;
  };

  auto run_test = [](const bool bigint_count_flag) {
    config().exec.group_by.bigint_count = bigint_count_flag;
    for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
      SKIP_NO_GPU();
      // single-column perfect hash:
      c("SELECT COUNT(*) FROM test GROUP BY x ORDER BY x DESC;", dt);
      c("SELECT y, COUNT(*) FROM test GROUP BY y ORDER BY y DESC;", dt);
      c("SELECT str, COUNT(*) FROM test GROUP BY str ORDER BY str DESC;", dt);
      c("SELECT COUNT(*), z FROM test where x = 7 GROUP BY z ORDER BY z DESC;", dt);
      c("SELECT z as z0, z as z1, COUNT(*) FROM test GROUP BY z0, z1 ORDER BY z0 DESC;",
        dt);
      c("SELECT x, COUNT(y), SUM(y), AVG(y), MIN(y), MAX(y) FROM test GROUP BY x ORDER "
        "BY x DESC;",
        dt);
      c("SELECT y, SUM(fn), AVG(ff), MAX(f) from test GROUP BY y ORDER BY y DESC;", dt);

      {
        // all these key columns are small ranged to force perfect hash
        std::vector<std::pair<std::string, std::string>> query_ids;
        query_ids.emplace_back("big_int_null", "SUM(float_null), COUNT(*)");
        query_ids.emplace_back("id", "AVG(big_int_null), COUNT(*)");
        query_ids.emplace_back("id_null", "MAX(tiny_int), MIN(tiny_int)");
        query_ids.emplace_back("small_int",
                               "SUM(cast (id as double)), SUM(double_not_null)");
        query_ids.emplace_back("tiny_int", "COUNT(small_int_null), COUNT(*)");
        query_ids.emplace_back("tiny_int_null", "AVG(small_int), COUNT(tiny_int)");
        query_ids.emplace_back(
            "case when id = 6 then -17 when id = 5 then 33 else NULL end",
            "COUNT(*), AVG(small_int_null)");
        query_ids.emplace_back(
            "case when id = 5 then NULL when id = 6 then -57 else cast(61 as tinyint) "
            "end",
            "AVG(big_int), SUM(tiny_int)");
        query_ids.emplace_back(
            "case when float_not_null > 2 then -3 when float_null < 4 then "
            "87 else NULL end",
            "MAX(id), COUNT(*)");
        const std::string table_name("logical_size_test");
        for (auto& pqid : query_ids) {
          std::string query("SELECT " + pqid.first + ", " + pqid.second + " FROM ");
          query += (table_name + " GROUP BY " + pqid.first + " ORDER BY " + pqid.first);
          query += " ASC";
          c(query + " NULLS FIRST;", query + ";", dt);
        }
      }

      // multi-column perfect hash:
      c("SELECT str, x FROM test GROUP BY x, str ORDER BY str, x;", dt);
      c("SELECT str, x, MAX(smallint_nulls), AVG(y), COUNT(dn) FROM test GROUP BY x, "
        "str ORDER BY str, x;",
        dt);
      c("SELECT str, x, MAX(smallint_nulls), COUNT(dn), COUNT(*) as cnt FROM test "
        "GROUP BY x, str ORDER BY cnt, str;",
        dt);
      c("SELECT x, str, z, SUM(dn), MAX(dn), AVG(dn) FROM test GROUP BY x, str, "
        "z ORDER BY str, z, x;",
        dt);
      c("SELECT x, SUM(dn), str, MAX(dn), z, AVG(dn), COUNT(*) FROM test GROUP BY z, "
        "x, str ORDER BY str, z, x;",
        dt);
    }
  };
  // running with bigint_count flag disabled:
  run_test(false);

  // running with bigint_count flag enabled:
  run_test(true);
}

TEST_F(Select, GroupByBaselineHash) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT cast(x1 as double) as key, COUNT(*), SUM(x2), MIN(x3), MAX(x4) FROM "
      "random_test"
      " GROUP BY key ORDER BY key;",
      dt);
    c("SELECT cast(x2 as double) as key, COUNT(*), SUM(x1), AVG(x3), MIN(x4) FROM "
      "random_test"
      " GROUP BY key ORDER BY key;",
      dt);
    c("SELECT cast(x3 as double) as key, COUNT(*), AVG(x2), MIN(x1), COUNT(x4) FROM "
      "random_test"
      " GROUP BY key ORDER BY key;",
      dt);
    c("SELECT x4 as key, COUNT(*), AVG(x1), MAX(x2), MAX(x3) FROM random_test"
      " GROUP BY key ORDER BY key;",
      dt);
    c("SELECT x5 as key, COUNT(*), MAX(x1), MIN(x2), SUM(x3) FROM random_test"
      " GROUP BY key ORDER BY key;",
      dt);
    c("SELECT x1, x2, x3, x4, COUNT(*), MIN(x5) FROM random_test "
      "GROUP BY x1, x2, x3, x4 ORDER BY x1, x2, x3, x4;",
      dt);
    {
      std::string query(
          "SELECT x, COUNT(*) from (SELECT ofd - 2 as x FROM test) GROUP BY x ORDER BY "
          "x ASC");
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query(
          "SELECT x, COUNT(*) from (SELECT cast(ofd - 2 as bigint) as x FROM test) GROUP "
          "BY x ORDER BY x ASC");
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query(
          "SELECT x, COUNT(*) from (SELECT ofq - 2 as x FROM test) GROUP BY x ORDER BY "
          "x ASC");
      c(query + " NULLS FIRST;", query + ";", dt);
    }
  }
}

TEST_F(Select, GroupByConstrainedByInQueryRewrite) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) AS n, x FROM query_rewrite_test WHERE x IN (2, 5) GROUP BY x "
      "HAVING n > 0 ORDER BY n DESC;",
      dt);
    c("SELECT COUNT(*) AS n, x FROM query_rewrite_test WHERE x IN (2, 99) GROUP BY x "
      "HAVING n > 0 ORDER BY n DESC;",
      dt);

    c("SELECT COUNT(*) AS n, str FROM query_rewrite_test WHERE str IN ('str2', 'str5') "
      "GROUP BY str HAVING n > 0 "
      "ORDER "
      "BY n DESC;",
      dt);

    c("SELECT COUNT(*) AS n, str FROM query_rewrite_test WHERE str IN ('str2', 'str99') "
      "GROUP BY str HAVING n > 0 "
      "ORDER BY n DESC;",
      dt);
  }
}

TEST_F(Select, RedundantGroupBy) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT DISTINCT(x) from test where y < 10 and z > 30 GROUP BY x;", dt);
  }
}

TEST_F(Select, BigDecimalRange) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT CAST(d AS BIGINT) AS di, COUNT(*) FROM big_decimal_range_test GROUP BY d "
      "HAVING di > 0 ORDER BY d;",
      dt);
    c("SELECT d1*2 FROM big_decimal_range_test ORDER BY d1;", dt);
    c("SELECT 2*d1 FROM big_decimal_range_test ORDER BY d1;", dt);
    c("SELECT d1 * (CAST(d1 as INT) + 1) FROM big_decimal_range_test ORDER BY d1;", dt);
    c("SELECT (CAST(d1 as INT) + 1) * d1 FROM big_decimal_range_test ORDER BY d1;", dt);
  }
}

TEST_F(Select, ScalarSubquery) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT SUM(x) + SUM(y) FROM test GROUP BY z HAVING (SELECT x FROM test "
      "GROUP BY x HAVING x > 7 LIMIT 1) > 7 ORDER BY z;",
      dt);
    c("SELECT SUM(x) + SUM(y) FROM test GROUP BY z HAVING (SELECT d FROM test "
      "GROUP BY d HAVING d > 2.4 LIMIT 1) > 2.4 ORDER BY z;",
      dt);
    EXPECT_THROW(run_multiple_agg("SELECT 5 - (SELECT rowid FROM test);", dt),
                 std::runtime_error);
  }
}

TEST_F(Select, DecimalCompression) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    std::string omnisci_sql = "";
    std::string sqlite_sql = "";

    omnisci_sql =
        "SELECT AVG(big_dec), AVG(med_dec), AVG(small_dec) FROM "
        "decimal_compression_test;";
    sqlite_sql =
        "SELECT 1.0*AVG(big_dec), 1.0*AVG(med_dec), 1.0*AVG(small_dec) FROM "
        "decimal_compression_test;";
    c(omnisci_sql, sqlite_sql, dt);
    c(sqlite_sql, sqlite_sql, dt);

    omnisci_sql =
        "SELECT SUM(big_dec), SUM(med_dec), SUM(small_dec) FROM "
        "decimal_compression_test;";
    sqlite_sql =
        "SELECT 1.0*SUM(big_dec), 1.0*SUM(med_dec), 1.0*SUM(small_dec) FROM "
        "decimal_compression_test;";
    c(omnisci_sql, sqlite_sql, dt);
    c(sqlite_sql, sqlite_sql, dt);

    omnisci_sql =
        "SELECT MIN(big_dec), MIN(med_dec), MIN(small_dec) FROM "
        "decimal_compression_test;";
    sqlite_sql =
        "SELECT 1.0*MIN(big_dec), 1.0*MIN(med_dec), 1.0*MIN(small_dec) FROM "
        "decimal_compression_test;";
    c(omnisci_sql, sqlite_sql, dt);
    c(sqlite_sql, sqlite_sql, dt);

    omnisci_sql =
        "SELECT MAX(big_dec), MAX(med_dec), MAX(small_dec) FROM "
        "decimal_compression_test;";
    sqlite_sql =
        "SELECT 1.0*MAX(big_dec), 1.0*MAX(med_dec), 1.0*MAX(small_dec) FROM "
        "decimal_compression_test;";
    c(omnisci_sql, sqlite_sql, dt);
    c(sqlite_sql, sqlite_sql, dt);

    omnisci_sql =
        "SELECT big_dec, COUNT(*) as n, AVG(med_dec) as med_dec_avg, SUM(small_dec) as "
        "small_dec_sum FROM decimal_compression_test GROUP BY big_dec ORDER BY "
        "small_dec_sum;";
    sqlite_sql =
        "SELECT 1.0*big_dec, COUNT(*) as n, 1.0*AVG(med_dec) as med_dec_avg, "
        "1.0*SUM(small_dec) as small_dec_sum FROM decimal_compression_test GROUP BY "
        "big_dec ORDER BY small_dec_sum;";
    c(omnisci_sql, sqlite_sql, dt);
    c(sqlite_sql, sqlite_sql, dt);

    c("SELECT CASE WHEN big_dec > 0 THEN med_dec ELSE NULL END FROM "
      "decimal_compression_test WHERE big_dec < 0;",
      dt);
  }
}

TEST_F(Select, BigintGroupByColCompactionTest) {
  createTable("bigint_groupby_col_compaction_test", {{"c", SQLTypeInfo(kBIGINT)}});
  insertCsvValues("bigint_groupby_col_compaction_test",
                  "-6312639302689611776\n-6312639302689611776\n-6312639302689611776\n-"
                  "6336283200715718656\n-6312639302689603584");
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    const auto result = run_multiple_agg(
        "SELECT * FROM bigint_groupby_col_compaction_test GROUP BY c ORDER BY c;", dt);
    ASSERT_EQ(size_t(3), result->rowCount());
    const auto row1 = result->getNextRow(true, true);
    ASSERT_EQ(int64_t(-6336283200715718656), v<int64_t>(row1[0]));
    const auto row2 = result->getNextRow(true, true);
    ASSERT_EQ(int64_t(-6312639302689611776), v<int64_t>(row2[0]));
    const auto row3 = result->getNextRow(true, true);
    ASSERT_EQ(int64_t(-6312639302689603584), v<int64_t>(row3[0]));
  }
  dropTable("bigint_groupby_col_compaction_test");
}

class Drop : public ExecuteTestBase, public ::testing::Test {};

TEST_F(Drop, AfterDrop) {
  createTable("droptest", {{"i1", SQLTypeInfo(kINT)}});
  insertCsvValues("droptest", "1\n2");
  ASSERT_EQ(int64_t(3),
            v<int64_t>(run_simple_agg("SELECT SUM(i1) FROM droptest;",
                                      ExecutorDeviceType::CPU)));
  dropTable("droptest");
  createTable("droptest", {{"n1", SQLTypeInfo(kINT)}});
  insertCsvValues("droptest", "3\n4");
  ASSERT_EQ(int64_t(7),
            v<int64_t>(run_simple_agg("SELECT SUM(n1) FROM droptest;",
                                      ExecutorDeviceType::CPU)));
  dropTable("droptest");
}

TEST_F(Select, Empty) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM emptytab;", dt);
    c("SELECT SUM(x) FROM emptytab;", dt);
    c("SELECT SUM(y) FROM emptytab;", dt);
    c("SELECT SUM(t) FROM emptytab;", dt);
    c("SELECT SUM(f) FROM emptytab;", dt);
    c("SELECT SUM(d) FROM emptytab;", dt);
    c("SELECT SUM(dd) FROM emptytab;", dt);
    c("SELECT MIN(x) FROM emptytab;", dt);
    c("SELECT MIN(y) FROM emptytab;", dt);
    c("SELECT MIN(t) FROM emptytab;", dt);
    c("SELECT MIN(f) FROM emptytab;", dt);
    c("SELECT MIN(d) FROM emptytab;", dt);
    c("SELECT MIN(dd) FROM emptytab;", dt);
    c("SELECT MAX(x) FROM emptytab;", dt);
    c("SELECT MAX(y) FROM emptytab;", dt);
    c("SELECT MAX(t) FROM emptytab;", dt);
    c("SELECT MAX(f) FROM emptytab;", dt);
    c("SELECT MAX(d) FROM emptytab;", dt);
    c("SELECT MAX(dd) FROM emptytab;", dt);
    c("SELECT AVG(x) FROM emptytab;", dt);
    c("SELECT AVG(y) FROM emptytab;", dt);
    c("SELECT AVG(t) FROM emptytab;", dt);
    c("SELECT AVG(f) FROM emptytab;", dt);
    c("SELECT AVG(d) FROM emptytab;", dt);
    c("SELECT AVG(dd) FROM emptytab;", dt);
    c("SELECT COUNT(*) FROM test, emptytab;", dt);
    c("SELECT MIN(ts), MAX(ts) FROM emptytab;", dt);
    c("SELECT SUM(test.x) FROM test, emptytab;", dt);
    c("SELECT SUM(test.y) FROM test, emptytab;", dt);
    c("SELECT SUM(emptytab.x) FROM test, emptytab;", dt);
    c("SELECT SUM(emptytab.y) FROM test, emptytab;", dt);
    c("SELECT COUNT(*) FROM test WHERE x > 8;", dt);
    c("SELECT SUM(x) FROM test WHERE x > 8;", dt);
    c("SELECT SUM(f) FROM test WHERE x > 8;", dt);
    c("SELECT SUM(d) FROM test WHERE x > 8;", dt);
    c("SELECT SUM(dd) FROM test WHERE x > 8;", dt);
    c("SELECT SUM(dd) FROM emptytab GROUP BY x, y;", dt);
    c("SELECT COUNT(DISTINCT x) FROM emptytab;", dt);
    c("SELECT APPROX_COUNT_DISTINCT(x * 1000000) FROM emptytab;",
      "SELECT COUNT(DISTINCT x * 1000000) FROM emptytab;",
      dt);

    // Empty subquery results
    c("SELECT x, SUM(y) FROM emptytab WHERE x IN (SELECT x FROM emptytab GROUP "
      "BY x HAVING SUM(f) > 1.0) GROUP BY x ORDER BY x ASC;",
      dt);
  }
}

TEST_F(Select, Subqueries) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT str, SUM(y) AS n FROM test WHERE x > (SELECT COUNT(*) FROM test) - 14 "
      "GROUP BY str ORDER BY str ASC;",
      dt);
    c("SELECT COUNT(*) FROM test, (SELECT x FROM test_inner) AS inner_x WHERE test.x = "
      "inner_x.x;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x IN (SELECT x FROM test WHERE y > 42);", dt);
    c("SELECT COUNT(*) FROM test WHERE x IN (SELECT x FROM test GROUP BY x ORDER BY "
      "COUNT(*) DESC LIMIT 1);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x IN (SELECT x FROM test GROUP BY x);", dt);
    c("SELECT COUNT(*) FROM test WHERE x IN (SELECT x FROM join_test);", dt);
    c("SELECT MIN(yy), MAX(yy) FROM (SELECT AVG(y) as yy FROM test GROUP BY x);", dt);
    c("SELECT COUNT(*) FROM subquery_test WHERE x NOT IN (SELECT x + 1 FROM "
      "subquery_test GROUP BY x);",
      dt);
    c("SELECT MAX(ct) FROM (SELECT COUNT(*) AS ct, str AS foo FROM test GROUP BY foo);",
      dt);
    c("SELECT COUNT(*) FROM subquery_test WHERE x IN (SELECT x AS foobar FROM "
      "subquery_test GROUP BY foobar);",
      dt);
    c("SELECT * FROM (SELECT x FROM test ORDER BY x) ORDER BY x;", dt);
    c("SELECT AVG(y) FROM (SELECT * FROM test ORDER BY z LIMIT 5);", dt);
    c("SELECT COUNT(*) FROM subquery_test WHERE x NOT IN (SELECT x + 1 FROM "
      "subquery_test GROUP BY x);",
      dt);
    ASSERT_EQ(int64_t(0),
              v<int64_t>(run_simple_agg(
                  "SELECT * FROM (SELECT rowid FROM test WHERE rowid = 0);", dt)));
    c("SELECT COUNT(*) FROM test WHERE x NOT IN (SELECT x FROM test GROUP BY x ORDER BY "
      "COUNT(*));",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x NOT IN (SELECT x FROM test GROUP BY x);", dt);
    c("SELECT COUNT(*) FROM test WHERE f IN (SELECT DISTINCT f FROM test WHERE x > 7);",
      dt);
    c("SELECT emptytab. x, CASE WHEN emptytab. y IN (SELECT emptytab. y FROM emptytab "
      "GROUP BY emptytab. y) then "
      "emptytab. y END yy, sum(x) "
      "FROM emptytab GROUP BY emptytab. x, yy;",
      dt);
    c("WITH d1 AS (SELECT deptno, dname FROM dept LIMIT 10) SELECT ename, dname FROM "
      "emp, d1 WHERE emp.deptno = "
      "d1.deptno ORDER BY ename ASC LIMIT 10;",
      dt);
    c("SELECT x FROM (SELECT x, MAX(y), COUNT(*) AS n FROM test GROUP BY x HAVING MAX(y) "
      "> 42) ORDER BY n;",
      dt);
    c("SELECT CASE WHEN test.x IN (SELECT x FROM test_inner) THEN x ELSE NULL END AS c, "
      "COUNT(*) AS n FROM test WHERE "
      "y > 40 GROUP BY c ORDER BY n DESC;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE x IN (SELECT x FROM test WHERE x > (SELECT "
      "COUNT(*) FROM test WHERE x > 7) + 2 "
      "GROUP BY x);",
      dt);
    // ofd has an expression range between 1 ~ INT32_MAX which incurs OOM during test
    // so disable below two queries temporarily
    // todo (yoonmin): add them in later once we have safe fallback to baseline join for
    // this case
    // c("SELECT COUNT(*) FROM test WHERE ofd IN (SELECT ofd FROM test GROUP BY ofd);",
    // dt); c("SELECT COUNT(*) FROM test WHERE ofd NOT IN (SELECT ofd FROM test GROUP BY
    // ofd);",
    //  dt);
    c("SELECT COUNT(*) FROM test WHERE ss IN (SELECT ss FROM test GROUP BY ss);", dt);
    c("SELECT COUNT(*) FROM test WHERE ss NOT IN (SELECT ss FROM test GROUP BY ss);", dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT str FROM test_in_bitmap GROUP BY "
      "str);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str NOT IN (SELECT str FROM test_in_bitmap GROUP "
      "BY str);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT ss FROM test GROUP BY ss);", dt);
    c("SELECT COUNT(*) FROM test WHERE str NOT IN (SELECT ss FROM test GROUP BY ss);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE ss IN (SELECT str FROM test GROUP BY str);", dt);
    c("SELECT COUNT(*) FROM test WHERE ss NOT IN (SELECT str FROM test GROUP BY str);",
      dt);
    c("SELECT str, COUNT(*) FROM test WHERE x IN (SELECT x FROM test WHERE x > 8) GROUP "
      "BY str;",
      dt);
    c("SELECT COUNT(*) FROM test_in_bitmap WHERE str IN (SELECT ss FROM test GROUP BY "
      "ss);",
      dt);
    c("SELECT COUNT(*) FROM test_in_bitmap WHERE str NOT IN (SELECT ss FROM test GROUP "
      "BY ss);",
      dt);
    c("SELECT COUNT(*) FROM test_in_bitmap WHERE str IN (SELECT str FROM test GROUP BY "
      "str);",
      dt);
    c("SELECT COUNT(*) FROM test_in_bitmap WHERE str NOT IN (SELECT str FROM test GROUP "
      "BY str);",
      dt);
    c("SELECT COUNT(str) FROM (SELECT * FROM (SELECT * FROM test WHERE x = 7) WHERE y = "
      "42) WHERE t > 1000;",
      dt);
    c("SELECT x_cap, y FROM (SELECT CASE WHEN x > 100 THEN 100 ELSE x END x_cap, y, t "
      "FROM emptytab) GROUP BY x_cap, "
      "y;",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT DISTINCT str FROM test);", dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT DISTINCT str FROM test_inner) AND "
      "str IN (SELECT DISTINCT str FROM test_inner);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT str FROM test_inner) AND str IN "
      "(SELECT str FROM test_inner);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT DISTINCT str FROM test_inner) AND "
      "str IN (SELECT DISTINCT str FROM test_inner) AND str IN (SELECT DISTINCT str FROM "
      "test_inner);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT DISTINCT str FROM test_inner) AND "
      "str IN (SELECT str FROM test_inner) AND str IN (SELECT DISTINCT str FROM "
      "test_inner);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT  str FROM test_inner) AND str IN "
      "(SELECT str FROM test_inner) AND str IN (SELECT DISTINCT str FROM test_inner);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT DISTINCT str FROM test_inner) AND "
      "x IN (SELECT DISTINCT x FROM test_inner);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT DISTINCT str FROM test_inner) AND "
      "x IN (SELECT x FROM test_inner);",
      dt);
    c("SELECT COUNT(*) FROM test WHERE str IN (SELECT str FROM test_inner) AND x IN "
      "(SELECT x FROM test_inner);",
      dt);
    c("SELECT SUM((x - (SELECT AVG(x) FROM test)) * (x - (SELECT AVG(x) FROM test)) / "
      "((SELECT COUNT(x) FROM test) - "
      "1)) FROM test;",
      dt);
    EXPECT_THROW(run_multiple_agg("SELECT * FROM (SELECT * FROM test LIMIT 5);", dt),
                 std::runtime_error);
    EXPECT_THROW(run_simple_agg("SELECT AVG(SELECT x FROM test LIMIT 5) FROM test;", dt),
                 std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT COUNT(*) FROM test WHERE str < (SELECT str FROM test LIMIT 1);", dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT COUNT(*) FROM test WHERE str IN (SELECT x FROM test GROUP BY x);",
            dt),
        std::runtime_error);
    ASSERT_NEAR(static_cast<double>(2.057),
                v<double>(run_simple_agg(
                    "SELECT AVG(dd) / (SELECT STDDEV(dd) FROM test) FROM test;", dt)),
                static_cast<double>(0.10));
    c("SELECT R.x, R.f, count(*) FROM (SELECT x,y,z,t,f,d FROM test WHERE x >= 7 AND z < "
      "0 AND t > 1001 AND d < 3) AS R WHERE R.y > 0 AND z < 0 AND t > 1001 AND d "
      "< 3 GROUP BY R.x, R.f ORDER BY R.x;",
      dt);
    c("SELECT R.y, R.d, count(*) FROM (SELECT x,y,z,t,f,d FROM test WHERE y > 42 AND f > "
      "1.0) AS R WHERE R.x > 0 AND t > 1001 AND f > 1.0 GROUP BY "
      "R.y, R.d ORDER BY R.d;",
      dt);
    c("SELECT R.x, R.f, count(*) FROM (SELECT x,y,z,t,f,d FROM test WHERE x >= 7 AND z < "
      "0 AND t > 1001 AND d < 3 LIMIT 3) AS R WHERE R.y > 0 AND z < 0 AND t > 1001 AND d "
      "< 3 GROUP BY R.x, R.f ORDER BY R.f;",
      dt);
    c("SELECT R.y, R.d, count(*) FROM (SELECT x,y,z,t,f,d FROM test WHERE y > 42 AND f > "
      "1.0 ORDER BY x DESC LIMIT 2) AS R WHERE R.x > 0 AND t > 1001 AND f > 1.0 GROUP BY "
      "R.y, R.d ORDER BY R.y;",
      dt);
    c("SELECT x FROM test WHERE x = (SELECT MIN(X) m FROM test GROUP BY x HAVING x <= "
      "(SELECT MIN(x) FROM test));",
      dt);
    c("SELECT test.z, SUM(test.y) s FROM test JOIN (SELECT x FROM test_inner) b ON "
      "test.x = b.x GROUP BY test.z ORDER BY s;",
      dt);
    c("select * from (select distinct * from subquery_test) order by x;", dt);
    c("select sum(x) from (select distinct * from subquery_test);", dt);
  }
}

TEST_F(Select, Joins_Arrays) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(int64_t(0),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test, array_test_inner "
                                        "WHERE test.x = ALL array_test_inner.arr_i16;",
                                        dt)));
    ASSERT_EQ(int64_t(60),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test, array_test_inner "
                                        "WHERE test.x = ANY array_test_inner.arr_i16;",
                                        dt)));
    ASSERT_EQ(int64_t(2 * g_array_test_row_count * g_num_rows - 60),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test, array_test_inner "
                                        "WHERE test.x <> ALL array_test_inner.arr_i16;",
                                        dt)));
    ASSERT_EQ(int64_t(g_array_test_row_count),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test, array_test_inner "
                                        "WHERE 7 = array_test_inner.arr_i16[1];",
                                        dt)));
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test, array_test WHERE "
                                        "test.x = array_test.x AND 'bb' = ANY arr_str;",
                                        dt)));
    auto result_rows = run_multiple_agg(
        "SELECT UNNEST(array_test.arr_i16) AS a, test_inner.x, COUNT(*) FROM array_test, "
        "test_inner WHERE test_inner.x "
        "= array_test.arr_i16[1] GROUP BY a, test_inner.x;",
        dt);
    ASSERT_EQ(size_t(3), result_rows->rowCount());
    ASSERT_EQ(int64_t(g_array_test_row_count / 2 + g_array_test_row_count / 4),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test, test_inner WHERE EXTRACT(HOUR FROM test.m) "
                  "= 22 AND test.x = test_inner.x;",
                  dt)));
    ASSERT_EQ(
        int64_t(1),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM array_test, test_inner WHERE "
                                  "array_test.arr_i32[array_test.x - 5] = 20 AND "
                                  "array_test.x = "
                                  "test_inner.x;",
                                  dt)));
    // throw exception for full array joins
    EXPECT_THROW(run_simple_agg("SELECT COUNT(1) FROM array_test t1, array_test t2 WHERE "
                                "t1.arr_i32 = t2.arr_i32;",
                                dt),
                 std::runtime_error);
  }
}

TEST_F(Select, Joins_Fixed_Size_Array_Multi_Frag) {
  createTable("mf_f_arr",
              {{"c2", arrayType(kFLOAT, 2)},
               {"c3", arrayType(kFLOAT, 3)},
               {"c4", arrayType(kFLOAT, 4)}},
              {2});
  createTable("mf_d_arr",
              {{"c2", arrayType(kDOUBLE, 2)},
               {"c3", arrayType(kDOUBLE, 3)},
               {"c4", arrayType(kDOUBLE, 4)}},
              {2});
  createTable("mf_i_arr",
              {{"c2", arrayType(kINT, 2)},
               {"c3", arrayType(kINT, 3)},
               {"c4", arrayType(kINT, 4)}},
              {2});
  createTable("mf_bi_arr",
              {{"c2", arrayType(kBIGINT, 2)},
               {"c3", arrayType(kBIGINT, 3)},
               {"c4", arrayType(kBIGINT, 4)}},
              {2});
  createTable("mf_ti_arr",
              {{"c2", arrayType(kTINYINT, 2)},
               {"c3", arrayType(kTINYINT, 3)},
               {"c4", arrayType(kTINYINT, 4)}},
              {2});
  createTable("mf_si_arr",
              {{"c2", arrayType(kSMALLINT, 2)},
               {"c3", arrayType(kSMALLINT, 3)},
               {"c4", arrayType(kSMALLINT, 4)}},
              {2});
  createTable("mf_t_arr", {{"t2", arrayType(kTEXT, 2)}}, {2});

  auto insert_values = [&](const std::string& table_name) {
    std::ostringstream oss;
    for (int i = 1; i < 6; i++) {
      oss << "{\"c2\": [" << i << ", " << (i + 1) << "], \"c3\": [" << i << ", "
          << (i + 1) << ", " << (i + 2) << "], \"c4\": [" << i << ", " << (i + 1) << ", "
          << (i + 2) << ", " << (i + 3) << "]}" << std::endl;
    }
    insertJsonValues(table_name, oss.str());
  };

  insert_values("mf_f_arr");
  insert_values("mf_d_arr");
  insert_values("mf_i_arr");
  insert_values("mf_bi_arr");
  insert_values("mf_ti_arr");
  insert_values("mf_si_arr");

  insertJsonValues("mf_t_arr", R"__({"t2" : ["1", "22"]}
{"t2" : ["2", "33"]}
{"t2" : ["3", "44"]}
{"t2" : ["4", "55"]}
{"t2" : ["5", "66"]})__");

  auto test_query = [&](const std::string& table_name, ExecutorDeviceType dt) {
    std::ostringstream oss;
    oss << "SELECT COUNT(1) FROM " << table_name << " t1, " << table_name << " t2 WHERE ";
    auto common_part = oss.str();
    auto q1{common_part + "t1.c2[1] = t2.c2[1];"};
    ASSERT_EQ(int64_t(5), v<int64_t>(run_simple_agg(q1, dt)));
    auto q2{common_part + "t1.c3[1] = t2.c3[1];"};
    ASSERT_EQ(int64_t(5), v<int64_t>(run_simple_agg(q2, dt)));

    auto q3{common_part + "t1.c4[1] = t2.c4[1];"};
    ASSERT_EQ(int64_t(5), v<int64_t>(run_simple_agg(q3, dt)));

    auto q4{common_part + "t1.c2[2] = t2.c2[2] and t1.c2[1] = t1.c2[1];"};
    ASSERT_EQ(int64_t(5), v<int64_t>(run_simple_agg(q4, dt)));

    auto q5{common_part + "t1.c3[2] = t2.c3[2] and t1.c3[1] = t1.c3[1];"};
    ASSERT_EQ(int64_t(5), v<int64_t>(run_simple_agg(q5, dt)));

    auto q6{common_part + "t1.c4[2] = t2.c4[2] and t1.c4[1] = t1.c4[1];"};
    ASSERT_EQ(int64_t(5), v<int64_t>(run_simple_agg(q6, dt)));
  };

  // skip to test GPU device until we fix the #5425 issue
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    test_query("mf_f_arr", dt);
    test_query("mf_d_arr", dt);
    test_query("mf_i_arr", dt);
    test_query("mf_bi_arr", dt);
    test_query("mf_ti_arr", dt);
    test_query("mf_si_arr", dt);
    ASSERT_EQ(
        int64_t(5),
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(1) FROM mf_t_arr r1, mf_t_arr r2 WHERE r1.t2[1] = r2.t2[1]",
            dt)));
  }

  dropTable("mf_f_arr");
  dropTable("mf_d_arr");
  dropTable("mf_i_arr");
  dropTable("mf_bi_arr");
  dropTable("mf_ti_arr");
  dropTable("mf_si_arr");
  dropTable("mf_t_arr");
}

TEST_F(Select, Joins_EmptyTable) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT test.x, emptytab.x FROM test, emptytab WHERE test.x = emptytab.x;", dt);
    c("SELECT COUNT(*) FROM test, emptytab GROUP BY test.x;", dt);
    c("SELECT COUNT(*) FROM test, emptytab, test_inner where test.x = emptytab.x;", dt);
    c("SELECT test.x, emptytab.x FROM test LEFT JOIN emptytab ON test.y = emptytab.y "
      "ORDER BY test.x ASC;",
      dt);
  }
}

TEST_F(Select, Joins_Fragmented_SelfJoin_And_LoopJoin) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_THROW(
        run_multiple_agg("SELECT COUNT(*) FROM test a, test b WHERE b.x = b.x;", dt),
        std::runtime_error);
    EXPECT_THROW(run_multiple_agg(
                     "SELECT COUNT(*) FROM test a, test b, test c WHERE b.x = b.x;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg(
                     "SELECT COUNT(*) FROM test a, test b, test c WHERE c.x = c.x;", dt),
                 std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT COUNT(*) FROM test a, test b WHERE b.x = b.x AND b.y = b.y;", dt),
        std::runtime_error);
  }
}

TEST_F(Select, Joins_ImplicitJoins) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x = test_inner.x;", dt);
    c("SELECT COUNT(*) FROM test, hash_join_test WHERE test.t = hash_join_test.t;", dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x < test_inner.x + 1;", dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.real_str = test_inner.str;", dt);
    c("SELECT test_inner.x, COUNT(*) AS n FROM test, test_inner WHERE test.x = "
      "test_inner.x GROUP BY test_inner.x "
      "ORDER BY n;",
      dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.str = test_inner.str;", dt);
    c("SELECT test.str, COUNT(*) FROM test, test_inner WHERE test.str = test_inner.str "
      "GROUP BY test.str;",
      dt);
    c("WITH transient_strings AS (SELECT CASE WHEN str = 'foo' THEN 'foo' ELSE 'other' "
      "END AS str FROM test) SELECT COUNT(*) FROM test_inner, transient_strings WHERE "
      "test_inner.str = transient_strings.str;",
      dt);
    c("WITH transient_strings AS (SELECT str FROM test_inner WHERE str IN ('foo', "
      "'bars')) SELECT COUNT(*) FROM test_inner, transient_strings WHERE test_inner.str "
      "= "
      "transient_strings.str;",
      dt);
    c("SELECT test_inner.str, COUNT(*) FROM test, test_inner WHERE test.str = "
      "test_inner.str GROUP BY test_inner.str;",
      dt);
    c("SELECT test.str, COUNT(*) AS foobar FROM test, test_inner WHERE test.x = "
      "test_inner.x AND test.x > 6 GROUP BY "
      "test.str HAVING foobar > 5;",
      dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.real_str LIKE 'real_ba%' AND "
      "test.x = test_inner.x;",
      dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE LENGTH(test.real_str) = 8 AND test.x "
      "= test_inner.x;",
      dt);
    c("SELECT a.x, b.str FROM test a, join_test b WHERE a.str = b.str GROUP BY a.x, "
      "b.str ORDER BY a.x, b.str;",
      dt);
    c("SELECT a.x, b.str FROM test a, join_test b WHERE a.str = b.str ORDER BY a.x, "
      "b.str;",
      dt);
    c("SELECT COUNT(1) FROM test a, join_test b, test_inner c WHERE a.str = b.str AND "
      "b.x = c.x",
      dt);

    c("SELECT COUNT(*) FROM test a, join_test b, test_inner c WHERE a.x = b.x AND "
      "a.y = "
      "b.x AND a.x = c.x AND c.str = "
      "'foo';",
      dt);

    c("SELECT COUNT(*) FROM test a, test b WHERE a.x = b.x AND a.y = b.y;", dt);
    c("SELECT SUM(b.y) FROM test a, test b WHERE a.x = b.x AND a.y = b.y;", dt);
    c("SELECT COUNT(*) FROM test a, test b WHERE a.x = b.x AND a.str = b.str;", dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE (test.x = test_inner.x AND test.y = "
      "42 AND test_inner.str = 'foo') "
      "OR (test.x = test_inner.x AND test.y = 43 AND test_inner.str = 'foo');",
      dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x = test_inner.x OR test.x = "
      "test_inner.x;",
      dt);
    c("SELECT bar.str FROM test, bar WHERE test.str = bar.str;", dt);

    ASSERT_EQ(int64_t(3),
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test, join_test "
                                        "WHERE test.rowid = join_test.rowid;",
                                        dt)));
    ASSERT_EQ(7,
              v<int64_t>(run_simple_agg("SELECT test.x FROM test, test_inner WHERE "
                                        "test.x = test_inner.x AND test.rowid = 9;",
                                        dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test, test_inner WHERE "
                                        "test.x = test_inner.x AND test.rowid = 20;",
                                        dt)));
  }
}

TEST_F(Select, Joins_DifferentIntegerTypes) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x = test_inner.xx;", dt);
    c("SELECT test_inner.xx, COUNT(*) AS n FROM test, test_inner WHERE test.x = "
      "test_inner.xx GROUP BY test_inner.xx ORDER BY n;",
      dt);
  }
}

TEST_F(Select, Joins_FilterPushDown) {
  auto default_flag = config().opts.filter_pushdown.enable;
  auto default_lower_frac = config().opts.filter_pushdown.low_frac;
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    for (auto fpd : {std::make_pair(true, 1.0), std::make_pair(false, 0.0)}) {
      config().opts.filter_pushdown.enable = fpd.first;
      config().opts.filter_pushdown.low_frac = fpd.second;
      c("SELECT COUNT(*) FROM coalesce_cols_test_2 AS R, coalesce_cols_test_0 AS S "
        "WHERE R.y = S.y AND R.x > 2 AND (S.x > 1 OR S.y < 18);",
        dt);
      c("SELECT COUNT(*) FROM coalesce_cols_test_2 AS R, coalesce_cols_test_0 AS S "
        "WHERE R.x = S.x AND S.str = 'test1' AND ABS(S.dn - 2.2) < 0.001;",
        dt);
      c("SELECT S.y, COUNT(*) FROM coalesce_cols_test_2 AS R, coalesce_cols_test_0 AS S "
        "WHERE R.x = S.x AND S.t < time '12:40:23' AND S.d < date '2018-01-01' GROUP BY "
        "S.y ORDER BY S.y;",
        "SELECT R.y, COUNT(*) FROM coalesce_cols_test_2 AS R, coalesce_cols_test_0 AS S "
        "WHERE R.x = S.x AND S.t < time('12:40:23') AND S.d < date('2018-01-01') GROUP "
        "BY S.y "
        "ORDER BY S.y;",
        dt);
      c("SELECT R.y, COUNT(*) as cnt FROM coalesce_cols_test_2 AS R, "
        "coalesce_cols_test_1 AS S, coalesce_cols_test_0 AS T WHERE T.str = S.str AND "
        "S.x = R.x AND S.y < 10 GROUP "
        "BY R.y ORDER BY R.y;",
        dt);
      c("SELECT R.y, COUNT(*) as cnt FROM coalesce_cols_test_2 AS R, "
        "coalesce_cols_test_1 AS S, coalesce_cols_test_0 AS T WHERE T.y = S.y AND S.x = "
        "R.x AND T.x = 2 GROUP "
        "BY R.y ORDER BY R.y;",
        dt);
      c("SELECT R.y, COUNT(*) as cnt FROM coalesce_cols_test_2 AS R, "
        "coalesce_cols_test_1 AS S, coalesce_cols_test_0 AS T WHERE T.x = S.x AND S.y = "
        "R.y AND R.x < 20 AND S.y > 2 AND S.str <> 'foo' AND T.y < 18 AND T.x > 1 GROUP "
        "BY R.y ORDER BY R.y;",
        dt);
      c("SELECT T.x, COUNT(*) as cnt FROM coalesce_cols_test_2 AS R,"
        "coalesce_cols_test_1 AS S, "
        "coalesce_cols_test_0 AS T WHERE T.str = S.dup_str AND S.x = R.x AND T.y"
        "  = R.y AND R.x > 0 "
        "AND S.str ='test' AND S.y > 2 AND T.dup_str<> 'test4' GROUP BY T.x ORDER BY "
        "cnt;",
        dt);
      // self-join involved
      c("SELECT R.y, COUNT(*) as cnt FROM coalesce_cols_test_2 AS R, "
        "coalesce_cols_test_2 AS S, coalesce_cols_test_0 AS T WHERE T.x = S.x AND S.y = "
        "R.y AND R.x < 20 AND S.y > 2 AND S.str <> 'foo' AND T.y < 18 AND T.x > 1 GROUP "
        "BY R.y ORDER BY R.y;",
        dt);
      // BE-6050, filter pushdown for a query having subquery
      c("SELECT COUNT(1) FROM coalesce_cols_test_1 WHERE y IN (SELECT MAX(R.y) FROM "
        "coalesce_cols_test_1 R, (SELECT x, y FROM coalesce_cols_test_1) S WHERE R.y = "
        "S.y AND s.x < -999);",
        dt);
    }
  }
  // reloading default values
  config().opts.filter_pushdown.enable = default_flag;
  config().opts.filter_pushdown.low_frac = default_lower_frac;
}

TEST_F(Select, Joins_InnerJoin_TwoTables) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test a JOIN single_row_test b ON a.x = b.x;", dt);
    c("SELECT COUNT(*) from test a JOIN single_row_test b ON a.ofd = b.x;", dt);
    c("SELECT COUNT(*) FROM test JOIN test_inner ON test.x = test_inner.x;", dt);
    c("SELECT a.y, z FROM test a JOIN test_inner b ON a.x = b.x order by a.y;", dt);
    c("SELECT COUNT(*) FROM test a JOIN join_test b ON a.str = b.dup_str;", dt);
    c("SELECT COUNT(*) FROM test_inner_x a JOIN test_x b ON a.x = b.x;",
      dt);  // test_x must be replicated
    c("SELECT a.x FROM test a JOIN join_test b ON a.str = b.dup_str ORDER BY a.x;", dt);
    c("SELECT a.x FROM test_inner_x a JOIN test_x b ON a.x = b.x ORDER BY a.x;",
      dt);  // test_x must be replicated
    c("SELECT a.x FROM test a JOIN join_test b ON a.str = b.dup_str GROUP BY a.x ORDER "
      "BY a.x;",
      dt);
    c("SELECT a.x FROM test_inner_x a JOIN test_x b ON a.x = b.x GROUP BY a.x ORDER BY "
      "a.x;",
      dt);  // test_x must be replicated
    c("SELECT COUNT(*) FROM test JOIN test_inner ON test.x = test_inner.x AND "
      "test.rowid "
      "= test_inner.rowid;",
      dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.y = test_inner.y OR (test.y IS "
      "NULL AND test_inner.y IS NULL);",
      dt);
    c("SELECT COUNT(*) FROM test, join_test WHERE (test.str = join_test.dup_str OR "
      "(test.str IS NULL AND "
      "join_test.dup_str IS NULL));",
      dt);
    c("SELECT COUNT(*) from test_inner a, bweq_test b where a.x = b.x OR (a.x IS NULL "
      "and b.x IS NULL);",
      dt);
    c("SELECT t1.fixed_null_str FROM (SELECT fixed_null_str, SUM(x) n1 FROM test "
      "GROUP BY fixed_null_str) t1 INNER "
      "JOIN (SELECT fixed_null_str, SUM(y) n2 FROM test GROUP BY fixed_null_str) t2 "
      "ON ((t1.fixed_null_str = "
      "t2.fixed_null_str) OR (t1.fixed_null_str IS NULL AND t2.fixed_null_str IS "
      "NULL));",
      dt);
    c("SELECT t1.x, t1.y, t1.sum1, t2.sum2, Sum(Cast(t1.sum1 AS FLOAT)) / "
      "Sum(Cast(t2.sum2 AS FLOAT)) calc FROM (SELECT x, y, Sum(t) sum1 FROM test GROUP "
      "BY 1, 2) t1 INNER JOIN (SELECT y, Sum(x) sum2 FROM test_inner GROUP BY 1) t2 ON "
      "t1.y = t2.y GROUP BY 1, 2, 3, 4;",
      dt);
    c("SELECT t1.x, t1.y, t1.sum1, t2.sum2, Sum(Cast(t1.sum1 AS FLOAT)) / "
      "Sum(Cast(t2.sum2 AS FLOAT)) calc FROM (SELECT x, y, Sum(t) sum1 FROM test GROUP "
      "BY 1, 2) t1 INNER JOIN (SELECT y, Sum(x) sum2 FROM test GROUP BY 1) t2 ON "
      "t1.y = t2.y GROUP BY 1, 2, 3, 4;",
      dt);
    c("SELECT test.*, test_inner.* from test join test_inner on test.x = test_inner.x "
      "order by test.z;",
      dt);

    const auto watchdog_state = config().exec.watchdog.enable;
    ScopeGuard reset = [watchdog_state] {
      config().exec.watchdog.enable = watchdog_state;
    };
    config().exec.watchdog.enable = false;
    c(R"(SELECT str FROM test JOIN (SELECT 'foo' AS val, 12345 AS cnt) subq ON test.str = subq.val;)",
      dt);
  }
}

TEST_F(Select, Joins_InnerJoin_AtLeastThreeTables) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT count(*) FROM test AS a JOIN join_test AS b ON a.x = b.x JOIN test_inner "
      "AS c ON b.str = c.str;",
      dt);
    c("SELECT count(*) FROM test AS a JOIN join_test AS b ON a.x = b.x JOIN test_inner "
      "AS c ON b.str = c.str JOIN "
      "join_test AS d ON c.x = d.x;",
      dt);
    c("SELECT a.y, count(*) FROM test AS a JOIN join_test AS b ON a.x = b.x JOIN "
      "test_inner AS c ON b.str = c.str "
      "GROUP BY a.y;",
      dt);
    c("SELECT a.x AS x, a.y, b.str FROM test AS a JOIN join_test AS b ON a.x = b.x JOIN "
      "test_inner AS c ON b.str = "
      "c.str "
      "ORDER BY a.y;",
      dt);
    c("SELECT a.x, b.x, b.str, c.str FROM test AS a JOIN join_test AS b ON a.x = b.x "
      "JOIN test_inner AS c ON b.x = c.x "
      "ORDER BY b.str;",
      dt);
    c("SELECT a.x, b.x, c.x FROM test a JOIN test_inner b ON a.x = b.x JOIN join_test c "
      "ON b.x = c.x;",
      dt);
    c("SELECT count(*) FROM test AS a JOIN hash_join_test AS b ON a.x = b.x JOIN "
      "test_inner AS c ON b.str = c.str;",
      dt);
    c("SELECT count(*) FROM test AS a JOIN hash_join_test AS b ON a.x = b.x JOIN "
      "test_inner AS c ON b.str = c.str JOIN "
      "hash_join_test AS d ON c.x = d.x;",
      dt);
    c("SELECT count(*) FROM test AS a JOIN hash_join_test AS b ON a.x = b.x JOIN "
      "test_inner AS c ON b.str = c.str JOIN "
      "join_test AS d ON c.x = d.x;",
      dt);
    c("SELECT count(*) FROM test AS a JOIN join_test AS b ON a.x = b.x JOIN test_inner "
      "AS c ON b.str = c.str JOIN "
      "hash_join_test AS d ON c.x = d.x;",
      dt);
    c("SELECT a.x AS x, a.y, b.str FROM test AS a JOIN hash_join_test AS b ON a.x = b.x "
      "JOIN test_inner AS c ON b.str "
      "= c.str "
      "ORDER BY a.y;",
      dt);
    c("SELECT a.x, b.x, c.x FROM test a JOIN test_inner b ON a.x = b.x JOIN "
      "hash_join_test c ON b.x = c.x;",
      dt);
    c("SELECT a.x, b.x FROM test_inner a JOIN test_inner b ON a.x = b.x ORDER BY a.x;",
      dt);
    c("SELECT a.x, b.x FROM join_test a JOIN join_test b ON a.x = b.x ORDER BY a.x;", dt);
    c("SELECT COUNT(1) FROM test AS a JOIN join_test AS b ON a.x = b.x JOIN test_inner "
      "AS c ON a.t = c.x;",
      dt);
    c("SELECT COUNT(*) FROM test a JOIN test_inner b ON a.str = b.str JOIN "
      "hash_join_test c ON a.x = c.x JOIN "
      "join_test d ON a.x > d.x;",
      dt);
    c("SELECT a.x, b.str, c.str, d.y FROM hash_join_test a JOIN test b ON a.x = b.x "
      "JOIN "
      "join_test c ON b.x = c.x JOIN "
      "test_inner d ON b.x = d.x ORDER BY a.x, b.str;",
      dt);  // test must be replicated
    c("SELECT a.f, b.y, c.x from test AS a JOIN join_test AS b ON 40*a.f-1 = b.y JOIN "
      "test_inner AS c ON b.x = c.x;",
      dt);
  }
}

TEST_F(Select, Joins_InnerJoin_Filters) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT count(*) FROM test AS a JOIN join_test AS b ON a.x = b.x JOIN test_inner "
      "AS c ON b.str = c.str WHERE a.y "
      "< 43;",
      dt);
    c("SELECT SUM(a.x), b.str FROM test AS a JOIN join_test AS b ON a.x = b.x JOIN "
      "test_inner AS c ON b.str = c.str "
      "WHERE a.y "
      "= 43 group by b.str;",
      dt);
    c("SELECT COUNT(*) FROM test JOIN test_inner ON test.str = test_inner.str AND test.x "
      "= 7;",
      dt);
    c("SELECT test.x, test_inner.str FROM test JOIN test_inner ON test.str = "
      "test_inner.str AND test.x <> 7;",
      dt);
    c("SELECT count(*) FROM test AS a JOIN hash_join_test AS b ON a.x = b.x JOIN "
      "test_inner AS c ON b.str = c.str "
      "WHERE a.y "
      "< 43;",
      dt);
    c("SELECT SUM(a.x), b.str FROM test AS a JOIN hash_join_test AS b ON a.x = b.x JOIN "
      "test_inner AS c ON b.str = "
      "c.str "
      "WHERE a.y "
      "= 43 group by b.str;",
      dt);
    c("SELECT COUNT(*) FROM test a JOIN join_test b ON a.x = b.x JOIN test_inner c ON "
      "c.str = a.str WHERE c.str = "
      "'foo';",
      dt);
    c("SELECT COUNT(*) FROM test t1 JOIN test t2 ON t1.x = t2.x WHERE t1.y > t2.y;",
      dt);  // test must be replicated
    c("SELECT COUNT(*) FROM test t1 JOIN test t2 ON t1.x = t2.x WHERE t1.null_str = "
      "t2.null_str;",
      dt);  // test must be replicated
  }
}

TEST_F(Select, Joins_LeftJoinFiltered) {
  const bool left_join_hoisting_state = g_enable_left_join_filter_hoisting;
  ScopeGuard reset = [left_join_hoisting_state] {
    g_enable_left_join_filter_hoisting = left_join_hoisting_state;
  };

  auto check_explain_result = [](const std::string& query,
                                 const ExecutorDeviceType dt,
                                 const bool enable_filter_hoisting) {
    CompilationOptions co;
    co.device_type = dt;
    co.hoist_literals = true;
    ExecutionOptions eo;
    eo.allow_loop_joins = false;
    eo.just_explain = true;
    const auto query_explain_result = runSqlQuery(query, co, eo);
    const auto explain_result = query_explain_result.getRows();
    EXPECT_EQ(size_t(1), explain_result->rowCount());
    const auto crt_row = explain_result->getNextRow(true, true);
    EXPECT_EQ(size_t(1), crt_row.size());
    const auto explain_str = boost::get<std::string>(v<NullableString>(crt_row[0]));
    const auto n = explain_str.find("hoisted_left_join_filters_");
    const bool condition = n == std::string::npos;
    if (enable_filter_hoisting) {
      // expect a match
      EXPECT_FALSE(condition);
    } else {
      // expect no match
      EXPECT_TRUE(condition);
    }
  };

  for (bool enable_filter_hoisting : {false, true}) {
    g_enable_left_join_filter_hoisting = enable_filter_hoisting;

    for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
      SKIP_NO_GPU();

      {
        const std::string query =
            R"(SELECT COUNT(*) FROM test LEFT JOIN test_inner ON test.x = test_inner.x WHERE test.y > 42;)";
        c(query, dt);
        check_explain_result(query, dt, enable_filter_hoisting);
      }

      {
        const std::string query =
            R"(SELECT COUNT(*) FROM test LEFT JOIN test_inner ON test.x = test_inner.x LEFT JOIN test_inner_x ON test.x = test_inner_x.x WHERE test.y > 42;)";
        c(query, dt);
        check_explain_result(query, dt, enable_filter_hoisting);
      }

      {
        const std::string query =
            R"(SELECT a.x FROM test a INNER JOIN test_inner b ON (a.x = b.x AND a.y = b.y) LEFT JOIN test_inner_x c ON (a.x = c.x) WHERE a.x > 5 GROUP BY 1;)";
        c(query, dt);
        // filter hoisting disabled if LEFT JOIN is the not the first join condition
        check_explain_result(query, dt, /*enable_filter_hoisting=*/false);
      }

      {
        const std::string query =
            R"(SELECT a.x FROM test a LEFT JOIN test_inner_x c ON (a.x = c.x) INNER JOIN test_inner b ON (a.x = b.x AND a.y = b.y) WHERE a.y + 1 > 5 GROUP BY 1;)";
        c(query, dt);
        // filter hoisting disabled if LEFT JOIN is the not the first join condition
        check_explain_result(query, dt, /*enable_filter_hoisting=*/false);
      }
    }
  }
}

TEST_F(Select, Joins_LeftOuterJoin) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT test.x, test_inner.x FROM test LEFT OUTER JOIN test_inner ON test.x = "
      "test_inner.x ORDER BY test.x ASC;",
      dt);
    c("SELECT test.x key1, CASE WHEN test_inner.x IS NULL THEN 99 ELSE test_inner.x END "
      "key2 FROM test LEFT OUTER JOIN "
      "test_inner ON test.x = test_inner.x GROUP BY key1, key2 ORDER BY key1;",
      dt);
    c("SELECT test_inner.x key1 FROM test LEFT OUTER JOIN test_inner ON test.x = "
      "test_inner.x GROUP BY key1 HAVING "
      "key1 IS NOT NULL;",
      dt);
    c("SELECT COUNT(*) FROM test_inner a LEFT JOIN test b ON a.x = b.x;", dt);
    c("SELECT a.x, b.str FROM join_test a LEFT JOIN test b ON a.x = b.x ORDER BY a.x, "
      "b.str;",
      dt);
    c("SELECT a.x, b.str FROM join_test a LEFT JOIN test b ON a.x = b.x ORDER BY a.x, "
      "b.str;",
      dt);
    c("SELECT COUNT(*) FROM test_inner a LEFT OUTER JOIN test_x b ON a.x = b.x;", dt);
    c("SELECT COUNT(*) FROM test a LEFT OUTER JOIN join_test b ON a.str = b.dup_str;",
      dt);
    c("SELECT COUNT(*) FROM test a LEFT OUTER JOIN join_test b ON a.str = b.dup_str;",
      dt);
    c("SELECT a.x, b.str FROM test_inner a LEFT OUTER JOIN test_x b ON a.x = b.x ORDER "
      "BY a.x, b.str IS NULL, b.str;",
      dt);
    c("SELECT a.x, b.str FROM test a LEFT OUTER JOIN join_test b ON a.str = b.dup_str "
      "ORDER BY a.x, b.str IS NULL, "
      "b.str;",
      dt);
    c("SELECT a.x, b.str FROM test a LEFT OUTER JOIN join_test b ON a.str = b.dup_str "
      "ORDER BY a.x, b.str IS NULL, "
      "b.str;",
      dt);
    c("SELECT COUNT(*) FROM test_inner_x a LEFT JOIN test_x b ON a.x = b.x;", dt);
    c("SELECT COUNT(*) FROM test a LEFT JOIN join_test b ON a.str = b.dup_str;", dt);
    c("SELECT COUNT(*) FROM test a LEFT JOIN join_test b ON a.str = b.dup_str;", dt);
    c("SELECT a.x, b.str FROM test_inner_x a LEFT JOIN test_x b ON a.x = b.x ORDER BY "
      "a.x, b.str IS NULL, b.str;",
      dt);
    c("SELECT a.x, b.str FROM test a LEFT JOIN join_test b ON a.str = b.dup_str ORDER BY "
      "a.x, b.str IS NULL, b.str;",
      dt);
    c("SELECT a.x, b.str FROM test a LEFT JOIN join_test b ON a.str = b.dup_str ORDER BY "
      "a.x, b.str IS NULL, b.str;",
      dt);
    c("SELECT COUNT(*) FROM test LEFT JOIN test_inner ON test_inner.x = test.x WHERE "
      "test_inner.str = test.str;",
      dt);
    c("SELECT COUNT(*) FROM test LEFT JOIN test_inner ON test_inner.x < test.x WHERE "
      "test_inner.str = test.str;",
      dt);
    c("SELECT COUNT(*) FROM test LEFT JOIN test_inner ON test_inner.x > test.x WHERE "
      "test_inner.str = test.str;",
      dt);
    c("SELECT COUNT(*) FROM test LEFT JOIN test_inner ON test_inner.x >= test.x WHERE "
      "test_inner.str = test.str;",
      dt);
    c("SELECT COUNT(*) FROM test LEFT JOIN test_inner ON test_inner.x <= test.x WHERE "
      "test_inner.str = test.str;",
      dt);
    c("SELECT test_inner.y, COUNT(*) n FROM test LEFT JOIN test_inner ON test_inner.x = "
      "test.x WHERE test_inner.str = "
      "'foo' GROUP BY test_inner.y ORDER BY n DESC;",
      dt);
    c("SELECT a.x, COUNT(b.y) FROM test a LEFT JOIN test_inner b ON b.x = a.x AND b.str "
      "NOT LIKE 'box' GROUP BY a.x "
      "ORDER BY a.x;",
      dt);
    c("SELECT a.x FROM test a LEFT OUTER JOIN test_inner b ON TRUE ORDER BY a.x ASC;",
      "SELECT a.x FROM test a LEFT OUTER JOIN test_inner b ON 1 ORDER BY a.x ASC;",
      dt);
    c("SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test LEFT JOIN "
      "test_inner ON "
      "test.x > test_inner.x LEFT "
      "JOIN hash_join_test ON test.str <> hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC NULLS FIRST, hash_join_test.x ASC NULLS FIRST;",
      "SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test LEFT JOIN "
      "test_inner ON "
      "test.x > test_inner.x LEFT "
      "JOIN hash_join_test ON test.str <> hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC, hash_join_test.x ASC;",
      dt);
    c("SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test LEFT JOIN test_inner ON "
      "test.x = test_inner.x LEFT "
      "JOIN hash_join_test ON test.str = hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC NULLS FIRST, hash_join_test.x ASC NULLS FIRST;",
      "SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test LEFT JOIN test_inner ON "
      "test.x = test_inner.x LEFT "
      "JOIN hash_join_test ON test.str = hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC, hash_join_test.x ASC;",
      dt);
    c("SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test LEFT JOIN "
      "test_inner ON "
      "test.x > test_inner.x INNER "
      "JOIN hash_join_test ON test.str <> hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC NULLS FIRST, hash_join_test.x ASC NULLS FIRST;",
      "SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test LEFT JOIN "
      "test_inner ON "
      "test.x > test_inner.x INNER "
      "JOIN hash_join_test ON test.str <> hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC, hash_join_test.x ASC;",
      dt);
    c("SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test LEFT JOIN test_inner ON "
      "test.x = test_inner.x INNER "
      "JOIN hash_join_test ON test.str = hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC NULLS FIRST, hash_join_test.x ASC NULLS FIRST;",
      "SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test LEFT JOIN test_inner ON "
      "test.x = test_inner.x INNER "
      "JOIN hash_join_test ON test.str = hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC, hash_join_test.x ASC;",
      dt);
    c("SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test INNER JOIN test_inner "
      "ON test.x > test_inner.x LEFT "
      "JOIN hash_join_test ON test.str <> hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC NULLS FIRST, hash_join_test.x ASC NULLS FIRST;",
      "SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test INNER JOIN test_inner "
      "ON test.x > test_inner.x LEFT "
      "JOIN hash_join_test ON test.str <> hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC, hash_join_test.x ASC;",
      dt);
    c("SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test INNER JOIN test_inner "
      "ON test.x = test_inner.x LEFT "
      "JOIN hash_join_test ON test.str = hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC NULLS FIRST, hash_join_test.x ASC NULLS FIRST;",
      "SELECT test_inner.y, hash_join_test.x, COUNT(*) FROM test INNER JOIN test_inner "
      "ON test.x = test_inner.x LEFT "
      "JOIN hash_join_test ON test.str = hash_join_test.str GROUP BY test_inner.y, "
      "hash_join_test.x ORDER BY "
      "test_inner.y ASC, hash_join_test.x ASC;",
      dt);
    c("SELECT COUNT(*) FROM test LEFT JOIN test_inner ON test.str = test_inner.str AND "
      "test.x = test_inner.x;",
      dt);
  }
}

TEST_F(Select, Joins_LeftJoin_Filters) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT test.x, test_inner.x FROM test LEFT OUTER JOIN test_inner ON test.x = "
      "test_inner.x WHERE test.y > 40 "
      "ORDER BY test.x ASC;",
      dt);
    c("SELECT test.x, test_inner.x FROM test LEFT OUTER JOIN test_inner ON test.x = "
      "test_inner.x WHERE test.y > 42 "
      "ORDER BY test.x ASC;",
      dt);
    c("SELECT test.str AS foobar, test_inner.str FROM test LEFT OUTER JOIN test_inner ON "
      "test.x = test_inner.x WHERE "
      "test.y > 42 ORDER BY foobar DESC LIMIT 8;",
      dt);
    c("SELECT test.x AS foobar, test_inner.x AS inner_foobar, test.f AS f_foobar FROM "
      "test LEFT OUTER JOIN test_inner "
      "ON test.str = test_inner.str WHERE test.y > 40 ORDER BY foobar DESC, f_foobar "
      "DESC;",
      dt);
    c("SELECT test.str AS foobar, test_inner.str FROM test LEFT OUTER JOIN test_inner ON "
      "test.x = test_inner.x WHERE "
      "test_inner.str IS NOT NULL ORDER BY foobar DESC;",
      dt);
    c("SELECT COUNT(*) FROM test_inner a LEFT JOIN (SELECT * FROM test WHERE y > 40) b "
      "ON a.x = b.x;",
      dt);
    c("SELECT a.x, b.str FROM join_test a LEFT JOIN (SELECT * FROM test WHERE y > 40) b "
      "ON a.x = b.x ORDER BY a.x, "
      "b.str;",
      dt);
    // Bad join ordering
    c("SELECT COUNT(*) FROM join_test a LEFT JOIN test b ON a.x = b.x AND a.x = 7;", dt);
    c(R"(SELECT a.x, b.str FROM join_test a LEFT JOIN test b ON a.x = b.x AND a.x = 7 ORDER BY a.x, b.str;)",
      dt);
    c("SELECT COUNT(*) FROM join_test a LEFT JOIN test b ON a.x = b.x WHERE a.x = 7;",
      dt);
    c("SELECT a.x FROM join_test a LEFT JOIN test b ON a.x = b.x WHERE a.x = 7;", dt);
    // fold left join -> inner join optimization testing
    c(R"(SELECT a.o1, count(*) FROM test a LEFT JOIN test_inner b ON a.o1 = b.dt16 WHERE b.dt16 < '2020-01-01' GROUP BY 1;)",
      dt);
    c(R"(SELECT a.x, count(*) FROM test a LEFT JOIN test_inner b ON a.x = b.y WHERE a.x = cast('7' as integer) GROUP BY 1 ORDER BY 2;)",
      dt);
    c(R"(SELECT a.x, count(*) FROM test a LEFT JOIN test_inner b ON a.x = b.y WHERE a.x = cast('7' as integer) AND b.y IS NOT NULL GROUP BY 1 ORDER BY 1;)",
      dt);
    c(R"(SELECT a.x, count(*) FROM test a LEFT JOIN test_inner b ON a.x = b.y WHERE b.y IS NOT NULL GROUP BY 1 ORDER BY 1;)",
      dt);
    c(R"(SELECT a.o1, count(*) FROM test a LEFT JOIN test_empty b ON a.o1 = b.o1 GROUP BY 1 ORDER BY 2;)",
      dt);
    c(R"(SELECT a.o1, count(*) FROM test a LEFT JOIN test_empty b ON a.o1 = b.o1 WHERE (a.o1 >= '1990-01-01') GROUP BY 1 ORDER BY 2;)",
      dt);
    {
      auto result = run_multiple_agg(
          R"(SELECT a.o1, count(*) FROM test a LEFT JOIN test_empty b ON a.o1 = b.o1 WHERE (a.o1 >= DATE '1990-01-01') GROUP BY 1 ORDER BY 2;)",
          dt);
      EXPECT_EQ(result->rowCount(), size_t(1));
    }
    {
      auto result = run_multiple_agg(
          R"(SELECT a.o1, count(*) FROM test a LEFT JOIN test_empty b ON a.o1 = b.o1 WHERE (a.o1 >= DATE '1990-01-01' AND b.o1 IS NOT NULL) GROUP BY 1 ORDER BY 2;)",
          dt);
      EXPECT_EQ(result->rowCount(), size_t(0));
    }
    c(R"(SELECT a.o1, count(*) FROM test a LEFT JOIN test_empty b ON a.o1 = b.o1 WHERE (a.o1 >= CAST('1990-01-01' AS DATE)) GROUP BY 1 ORDER BY 2;)",
      dt);
  }
}

TEST_F(Select, Joins_LeftJoin_MultiQuals) {
  // a test to check whether we can evaluate left join having multiple-quals
  // with our hash join framework, instead of using loop join
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_NO_THROW(
        run_multiple_agg("SELECT a.x, COUNT(b.y) FROM test a LEFT JOIN test_inner b ON "
                         "b.x = a.x AND b.str NOT LIKE 'box' GROUP BY a.x ORDER BY a.x;",
                         dt,
                         false /*=allow_looo_join*/));
    EXPECT_NO_THROW(
        run_multiple_agg("SELECT a.x, b.x FROM test a LEFT JOIN test_inner b ON b.x = "
                         "a.x AND a.y < 10000 and a.y > -10000 and b.str like 'foo';",
                         dt,
                         false /*=allow_looo_join*/));
    EXPECT_NO_THROW(run_multiple_agg(
        "SELECT a.x, b.x FROM test a INNER JOIN test b ON a.x = b.x INNER JOIN test c ON "
        "(a.x = c.x AND a.y = c.y) LEFT JOIN test_inner d ON (d.x = a.x AND a.y < 10000 "
        "and a.y > -10000 and d.str like 'foo');",
        dt,
        false /*=allow_looo_join*/));
  }
}

TEST_F(Select, Joins_OuterJoin_OptBy_NullRejection) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    // single-column outer join predicate

    // 1. execute full outer join via left outer join
    //    a) return zero matching row
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "where a is not null and c < 2 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "where a is not null and c < 2 order by a,b,c,d,e,f;",
      dt);

    // reverse column order in outer join predicate
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on d = a "
      "where a is not null and c < 2 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on d = a "
      "where a is not null and c < 2 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "where a > 7 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "where a > 7 order by a,b,c,d,e,f;",
      dt);

    //    b) return a single matching row
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "where a is not null and c < 3 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "where a is not null and c < 3 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "where a > 6 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "where a > 6 order by a,b,c,d,e,f;",
      dt);

    //    c) return multiple matching rows (four rows)
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on b = e "
      "where b is not null and c < 7 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on b = e "
      "where b is not null and c < 7 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on b = e "
      "where b > 1 and c < 7 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on b = e "
      "where b > 1 and c < 7 order by a,b,c,d,e,f;",
      dt);

    //    d) expect to throw an error due to unsupported full outer join
    //    --> we need a filter predicate in probe-side (i.e., outer) table
    EXPECT_THROW(
        run_multiple_agg(
            "select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a "
            "= d where d is not null and c < 2 order by a,b,c,d,e,f;",
            dt),
        std::runtime_error);
    EXPECT_THROW(run_multiple_agg(
                     "select a,b,c,d,e,f from outer_join_foo full outer join "
                     "outer_join_bar on a = d where e is not null order by a,b,c,d,e,f;",
                     dt),
                 std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg(
            "select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a "
            "= d where f is not null and e < 2 order by a,b,c,d,e,f;",
            dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg("select a,b,c,d,e,f from outer_join_foo full outer join "
                         "outer_join_bar on a = d where c < 2 order by a,b,c,d,e,f;",
                         dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg("select a,b,c,d,e,f from outer_join_foo full outer join "
                         "outer_join_bar on a = d where d < 5 order by a,b,c,d,e,f;",
                         dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg("select a,b,c,d,e,f from outer_join_foo full outer join "
                         "outer_join_bar on a = d where e < 8 order by a,b,c,d,e,f;",
                         dt),
        std::runtime_error);

    // 2. execute full outer join via inner join
    //    a) return zero matching row
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on b = e "
      "where e is not null and b is not null and a < 0 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where b = e and e is not "
      "null and b is not null and a < 0 order by a,b,c,d,e,f;",
      dt);

    // reverse column order in outer join predicate
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on b = e "
      "where e is not null and b is not null and a < 0 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where b = e and e is not "
      "null and b is not null and a < 0 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on b = e "
      "where e < 5 and b < 0 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where b = e and e < 5 and "
      "b < 0 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on b = e "
      "where e > -14 and b < 0 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where b = e and e > -14 "
      "and b < 0 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on b = e "
      "where b between 1 and 4 and e < 0 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where b = e and b between "
      "1 and 4 and e < 0 order by a,b,c,d,e,f;",
      dt);

    //    b) return a single matching row
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "where d is not null and a is not null order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and d is not "
      "null and a is not null order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "where a is not null and d < 2 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and a is not "
      "null and d < 2 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "where a is not null and d > -14 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and a is not "
      "null and d > -14 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "where a is not null and d between 1 and 3 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and a is not "
      "null and d between 1 and 3 order by a,b,c,d,e,f;",
      dt);

    //    c) return multiple matching rows (four rows)
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on b = e "
      "where e is not null and b is not null order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where b = e and e is not "
      "null and b is not null order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on b = e "
      "where b is not null and e > -14 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where b = e and b is not "
      "null and e > -14 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on b = e "
      "where b is not null and e between 1 and 4 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where b = e and b is not "
      "null and e between 1 and 4 order by a,b,c,d,e,f;",
      dt);

    // 3. execute left outer join via inner join
    //    a) return zero matching row
    c("select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "where d is not null and a < 0 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and d is not "
      "null and a < 0 order by a,b,c,d,e,f;",
      dt);

    //    b) return a single matching row
    c("select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "where d is not null order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and d is not "
      "null order by a,b,c,d,e,f;",
      dt);

    //    c) return multiple matching rows (four rows)
    c("select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on b = e "
      "where e > 1 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where b = e and e > 1 "
      "order by a,b,c,d,e,f",
      dt);

    // multi-column outer join predicates
    // 1. execute full outer join via left outer join
    //    a) return zero matching row
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and b = e where a is not null and b < 2 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and b = e where a is not null and b < 2 order by a,b,c,d,e,f;",
      dt);

    //    b) return a single matching row
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and b = e where a is not null and b < 3 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and b = e where a is not null and b < 3 order by a,b,c,d,e,f;",
      dt);

    //    c) return multiple matching rows
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and b = e where a is not null and b is not null order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and b = e where a is not null and b is not null order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and b = e where a is not null and b < 6 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and b = e where a is not null and b < 6 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and c = f where a is not null and c < 7 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and c = f where a is not null and c < 7 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and b = e where a is not null and b is not null order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and b = e where a is not null and b is not null order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on c = f "
      "and b = e where b is not null and c is not null and b < 7 and a is not null order "
      "by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on c = f "
      "and b = e where b is not null and c is not null and b < 7 and a is not null order "
      "by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and c = f where a is not null and c is not null order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and c = f where a is not null and c is not null order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and b = e and c = f where a is not null and b is not null and c is not null order "
      "by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and b = e and c = f where a is not null and b is not null and c is not null order "
      "by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and b = e and c = f where a is not null and c is not null and b < 6 order by "
      "a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and b = e and c = f where b is not null and c is not null and b < 6 order by "
      "a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and b = e and c = f where a is not null and c is not null and b < 7 order by "
      "a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and b = e and c = f where a is not null and c is not null and b < 7 order by "
      "a,b,c,d,e,f;",
      dt);

    //    d) expect to throw an error due to unsupported full outer join
    //    --> we need a filter predicate in probe-side (i.e., outer) table
    EXPECT_THROW(
        run_multiple_agg(
            "select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a "
            "= d and c = f where d is not null and f < 2 order by a,b,c,d,e,f;",
            dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg(
            "select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a "
            "= d and c = f where a < 2 order by a,b,c,d,e,f;",
            dt),
        std::runtime_error);

    // 2. execute full outer join via inner join
    //    a) return zero matching row
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and b = e where a is not null and b is not null and d < 1 and e is not null order "
      "by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and b = e and "
      "a is not null and b is not null and d < 1 and e is not null order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on c = f "
      "and b = e where c is not null and b is not null and f > 4 and e is not null order "
      "by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where c = f and b = e and "
      "c is not null and b is not null and f > 4 and e is not null order by a,b,c,d,e,f;",
      dt);

    //    b) return a single matching row
    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and b = e where a is not null and b is not null and d < 999999 and e is not null "
      "order by "
      "a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and b = e and "
      "a is not null and b is not null and d < 999999 and e is not null order by "
      "a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on a = d "
      "and c = f where a is not null and c is not null and d < 9999999 and f is not null "
      "order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and c = f and "
      "a is not null and c is not null and d < 9999999 and f is not null order by "
      "a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo full outer join outer_join_bar on c = f "
      "and b = e where c is not null and b is not null and e < 9999999 and f is not null "
      "order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where c = f and b = e and "
      "c is not null and b is not null and e < 9999999 and f is not null order by "
      "a,b,c,d,e,f;",
      dt);

    // 3. execute left outer join via inner join
    //    a) return zero matching row
    c("select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and b = e where a is not null and e is not null and d < 1 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and b = e and "
      "a is not null and e is not null and d < 1 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on c = f "
      "and b = e where f > 4 order by a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where c = f and b = e and "
      "f > 4 order by a,b,c,d,e,f;",
      dt);

    //    b) return a single matching row
    c("select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on a = d "
      "and b = e where a is not null and e is not null and d < 999999 order by "
      "a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where a = d and b = e and "
      "a is not null and e is not null and d < 999999 order by a,b,c,d,e,f;",
      dt);

    c("select a,b,c,d,e,f from outer_join_foo left outer join outer_join_bar on c = f "
      "and b = e where e < 9999999 order by "
      "a,b,c,d,e,f;",
      "select a,b,c,d,e,f from outer_join_foo, outer_join_bar where c = f and b = e and "
      "c is not null and e < 9999999 order by a,b,c,d,e,f;",
      dt);

    {
      // [BE-5406] incorrectly rewriting left join when filter used
      auto test_query =
          "select count(1) from outer_join_foo t1 left outer join (select g from "
          "outer_join_bar2 where h = 1) as t2 on t1.a = t2.g;";
      c(test_query, test_query, dt);

      auto test_query2 =
          "select count(1) from outer_join_foo t1 left outer join (select g as h from "
          "outer_join_bar2 where h = 1) as t2 on t1.a = t2.h;";
      c(test_query2, test_query2, dt);

      auto test_query3 =
          "select count(1) from outer_join_foo t1 left outer join (select d, g as h, i "
          "from "
          "outer_join_bar2 where h = 1) as t2 on t1.a = t2.h;";
      c(test_query3, test_query3, dt);

      auto test_query4 =
          "select count(1) from outer_join_foo t1 left outer join (select g as h from "
          "outer_join_bar2 where h = 1) as t2 on t1.a = t2.h;";
      c(test_query4, test_query4, dt);

      auto test_query5 =
          "select count(1) from outer_join_foo t1 left outer join (select g as h from "
          "outer_join_bar2) as t2 on t1.a = t2.h and t2.h = 1;";
      c(test_query5, test_query5, dt);
    }

    {
      // [BE-5447] null rejection rule issue v2
      // reported query
      auto test_query1 =
          "select foo.c, tmp.c from outer_join_foo foo left outer join (select a, c from "
          "outer_join_foo where a = 1) tmp on tmp.a = foo.a order by 1, 2;";
      c(test_query1, test_query1, dt);
      auto test_query2 =
          "select foo.c, tmp.c from outer_join_foo foo left outer join (select a, c from "
          "outer_join_foo where a is not null) tmp on tmp.a = foo.a order by 1, 2;";
      c(test_query2, test_query2, dt);
      // reverse join column order
      auto test_query3 =
          "select foo.c, tmp.c from outer_join_foo foo left outer join (select a, c from "
          "outer_join_foo where a = 1) tmp on foo.a = tmp.a order by 1, 2;";
      c(test_query3, test_query3, dt);
      auto test_query4 =
          "select foo.c, tmp.c from outer_join_foo foo left outer join (select a, c from "
          "outer_join_foo where a is not null) tmp on foo.a = tmp.a order by 1, 2;";
      c(test_query4, test_query4, dt);
      auto test_query5 =
          "select foo.c, tmp.c from outer_join_foo foo left outer join (select c, b from "
          "outer_join_foo where a = 1) tmp on tmp.b = foo.b order by 1, 2;";
      c(test_query5, test_query5, dt);
      auto test_query6 =
          "select foo.c, tmp.c from outer_join_foo foo left outer join (select c, b from "
          "outer_join_foo where a is not null) tmp on tmp.b = foo.b order by 1, 2;";
      c(test_query6, test_query6, dt);
      auto test_query7 =
          "select foo.c, tmp.c from outer_join_foo foo left outer join (select a, b, c "
          "from "
          "outer_join_foo where a = 1) tmp on tmp.b = foo.b and foo.a = tmp.a order by "
          "1, 2;";
      c(test_query7, test_query7, dt);
      auto test_query8 =
          "select foo.c, tmp.c from outer_join_foo foo left outer join (select a, b, c "
          "from "
          "outer_join_foo where a is not null) tmp on tmp.b = foo.b and foo.a = tmp.a "
          "order by 1, 2;";
      c(test_query8, test_query8, dt);
      auto test_query9 =
          "select foo.c, tmp.c from outer_join_foo foo left outer "
          "join (select a, b, c from outer_join_foo where c = 1) tmp on tmp.a = foo.a "
          "and tmp.b = foo.b and tmp.c = foo.c order by 1, 2;";
      c(test_query9, test_query9, dt);
      auto test_query10 =
          "select foo.c, tmp.c from outer_join_foo foo left outer "
          "join (select a, b, c from outer_join_foo where c is not null) tmp on tmp.a = "
          "foo.a and tmp.b = foo.b and tmp.c = foo.c order by 1, 2;";
      c(test_query10, test_query10, dt);
    }

    {
      // [BE-5764] null rejection rule issue v3
      // reported query
      createTable("BE_5764_a",
                  {{"text_", SQLTypeInfo(kTEXT)},
                   {"days_", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 16, kNULLT)}});
      createTable("BE_5764_b",
                  {{"text_", SQLTypeInfo(kTEXT)},
                   {"days_", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 16, kNULLT)}});
      insertCsvValues("BE_5764_a", "A,2021-01-01");
      auto q1_res = run_multiple_agg(
          "SELECT BE_5764_a.days_ FROM BE_5764_a LEFT JOIN BE_5764_b ON (BE_5764_a.days_ "
          "= BE_5764_b.days_) WHERE (BE_5764_a.days_ >= '2020-11-20') GROUP BY 1;",
          dt);
      auto q2_res = run_multiple_agg(
          "SELECT BE_5764_a.days_ FROM BE_5764_a LEFT JOIN BE_5764_b ON (BE_5764_a.days_ "
          "= BE_5764_b.days_) WHERE (BE_5764_a.days_ >= DATE '2020-11-20') GROUP BY 1;",
          dt);
      CHECK_EQ(q1_res->rowCount(), (size_t)1);
      CHECK_EQ(q1_res->rowCount(), q2_res->rowCount());
      dropTable("BE_5764_a");
      dropTable("BE_5764_b");
    }

    {
      // [BE-6037] null rejection rule issue v4: IS NOT NULL filter epxr connected via
      // OR-logic
      createTable("BE_6037_a", {{"id", SQLTypeInfo(kINT)}});
      createTable("BE_6037_b", {{"id", SQLTypeInfo(kINT)}});
      createTable("BE_6037_c", {{"id", SQLTypeInfo(kINT)}});
      run_sqlite_query("DROP TABLE IF EXISTS BE_6037_a;");
      run_sqlite_query("DROP TABLE IF EXISTS BE_6037_b;");
      run_sqlite_query("DROP TABLE IF EXISTS BE_6037_c;");
      run_sqlite_query("CREATE TABLE BE_6037_a (id INT);");
      run_sqlite_query("CREATE TABLE BE_6037_b (id INT);");
      run_sqlite_query("CREATE TABLE BE_6037_c (id INT);");

      for (int i = 1; i <= 12; i++) {
        insertCsvValues("BE_6037_a", std::to_string(i));
        run_sqlite_query("INSERT INTO BE_6037_a VALUES ("s + std::to_string(i) + ");");
        if (i % 2 == 0) {
          insertCsvValues("BE_6037_b", std::to_string(i));
          run_sqlite_query("INSERT INTO BE_6037_b VALUES ("s + std::to_string(i) + ");");
        }
        if (i % 3 == 0) {
          insertCsvValues("BE_6037_c", std::to_string(i));
          run_sqlite_query("INSERT INTO BE_6037_c VALUES ("s + std::to_string(i) + ");");
        }
      }
      auto q1 =
          "SELECT COUNT(1) FROM BE_6037_a r1 LEFT JOIN BE_6037_b r2 ON r1.id = r2.id "
          "LEFT JOIN BE_6037_c r3 ON r1.id = r3.id WHERE r2.id IS NOT NULL OR r3.id IS "
          "NOT NULL;";
      auto q2 =
          "SELECT COUNT(1) FROM (SELECT r1.id a, r2.id b, r3.id c FROM BE_6037_a r1 LEFT "
          "JOIN BE_6037_b r2 ON r1.id = r2.id LEFT JOIN BE_6037_c r3 ON r1.id = r3.id) "
          "WHERE b IS NOT NULL OR c IS NOT NULL;";
      auto q3 =
          "SELECT COUNT(1) FROM BE_6037_a r1 LEFT JOIN BE_6037_b r2 ON r1.id = r2.id "
          "LEFT JOIN BE_6037_c r3 ON r1.id = r3.id WHERE r1.id IS NOT NULL AND r2.id IS "
          "NOT NULL OR r3.id IS NOT NULL;";
      auto q4 =
          "SELECT COUNT(1) FROM BE_6037_a r1 LEFT JOIN BE_6037_b r2 ON r1.id = r2.id "
          "LEFT JOIN BE_6037_c r3 ON r1.id = r3.id WHERE (r1.id IS NOT NULL AND r2.id IS "
          "NOT NULL) OR r3.id IS NOT NULL;";
      auto q5 =
          "SELECT COUNT(1) FROM BE_6037_a r1 LEFT JOIN BE_6037_b r2 ON r1.id = r2.id "
          "LEFT JOIN BE_6037_c r3 ON r1.id = r3.id WHERE r1.id IS NOT NULL AND (r2.id IS "
          "NOT NULL OR r3.id IS NOT NULL);";
      auto q6 =
          "SELECT COUNT(1) FROM BE_6037_a r1 LEFT JOIN BE_6037_b r2 ON r1.id = r2.id "
          "LEFT JOIN BE_6037_c r3 ON r1.id = r3.id WHERE r1.id IS NOT NULL OR (r2.id IS "
          "NOT NULL AND r3.id IS NOT NULL);";
      c(q1, dt);
      c(q2, dt);
      c(q3, dt);
      c(q4, dt);
      c(q5, dt);
      c(q6, dt);

      dropTable("BE_6037_a");
      dropTable("BE_6037_b");
      dropTable("BE_6037_c");
      run_sqlite_query("DROP TABLE IF EXISTS BE_6037_a;");
      run_sqlite_query("DROP TABLE IF EXISTS BE_6037_b;");
      run_sqlite_query("DROP TABLE IF EXISTS BE_6037_c;");
    }
  }
}

TEST_F(Select, Joins_MultiCompositeColumns) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT a.x, b.str FROM test AS a JOIN join_test AS b ON a.str = b.str AND a.x = "
      "b.x ORDER BY a.x, b.str;",
      dt);
    c("SELECT a.x, b.str FROM test AS a JOIN join_test AS b ON a.x = b.x AND a.str = "
      "b.str ORDER BY a.x, b.str;",
      dt);
    c("SELECT a.z, b.str FROM test a JOIN join_test b ON a.y = b.y AND a.x = b.x ORDER "
      "BY a.z, b.str;",
      dt);
    c("SELECT a.z, b.str FROM test a JOIN test_inner b ON a.y = b.y AND a.x = b.x ORDER "
      "BY a.z, b.str;",
      dt);
    c("SELECT COUNT(*) FROM test a JOIN join_test b ON a.x = b.x AND a.y = b.x JOIN "
      "test_inner c ON a.x = c.x WHERE "
      "c.str <> 'foo';",
      dt);
    c("SELECT a.x, b.x, d.str FROM test a JOIN test_inner b ON a.str = b.str JOIN "
      "hash_join_test c ON a.x = c.x JOIN "
      "join_test d ON a.x >= d.x AND a.x < d.x + 5 ORDER BY a.x, b.x;",
      dt);
    c("SELECT COUNT(*) FROM test, join_test WHERE (test.x = join_test.x OR (test.x IS "
      "NULL AND join_test.x IS NULL)) "
      "AND (test.y = join_test.y OR (test.y IS NULL AND join_test.y IS NULL));",
      dt);
    c("SELECT COUNT(*) FROM test, join_test WHERE (test.str = join_test.dup_str OR "
      "(test.str IS NULL AND "
      "join_test.dup_str IS NULL)) AND (test.x = join_test.x OR (test.x IS NULL AND "
      "join_test.x IS NULL));",
      dt);

    if (dt == ExecutorDeviceType::CPU) {
      // Clear CPU memory and hash table caches
      clearCpuMemory();
    }
  }
}

TEST_F(Select, Joins_BuildHashTable) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test, join_test WHERE test.str = join_test.dup_str;", dt);
    // Intentionally duplicate previous string join to cover hash table building.
    c("SELECT COUNT(*) FROM test, join_test WHERE test.str = join_test.dup_str;", dt);

    if (dt == ExecutorDeviceType::CPU) {
      // Clear CPU memory and hash table caches
      clearCpuMemory();
    }
  }
}

TEST_F(Select, Joins_CoalesceColumns) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_2 t1 "
      "ON t0.x = t1.x AND t0.y = t1.y;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_2 t1 "
      "ON t0.x = t1.x AND t0.str = t1.str;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_2 t1 "
      "ON t0.str = t1.str AND t0.dup_str = t1.dup_str;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_2 t1 "
      "ON t0.str = t1.str AND t0.dup_str = t1.dup_str AND t0.x = t1.x;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_1 t1 "
      "ON t0.x = t1.x AND t0.y = t1.y INNER JOIN coalesce_cols_test_2 t2 on t0.x = t2.x "
      "AND t1.y = t2.y;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_2 t1 "
      "ON t0.x = t1.x AND t0.d = t1.d;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_2 t1 "
      "ON t0.x = t1.x AND t0.d = t1.d AND t0.y = t1.y;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_2 t1 "
      "ON t0.d = t1.d AND t0.x = t1.x;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_2 t1 "
      "ON t0.d = t1.d AND t0.tz = t1.tz AND t0.x = t1.x;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_2 t1 "
      "ON t0.dn = t1.dn AND t0.tz = t1.tz AND t0.y = t1.y AND t0.x = t1.x;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_2 t1 "
      "ON t0.dn = t1.dn AND t0.y = t1.y AND t0.tz = t1.tz AND t0.x = t1.x;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_1 t1 "
      "ON t0.dn = t1.dn AND t0.y = t1.y AND t0.tz = t1.tz AND t0.x = t1.x INNER JOIN "
      "coalesce_cols_test_2 t2 ON t0.y = t2.y AND t0.tz = t1.tz AND t0.x = t1.x;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_1 t1 "
      "ON t0.dn = t1.dn AND t0.y = t1.y AND t0.tz = t1.tz AND t0.x = t1.x INNER JOIN "
      "coalesce_cols_test_2 t2 ON t0.d = t2.d AND t0.tz = t1.tz AND t0.x = t1.x;",
      dt);
    c("SELECT COUNT(*) FROM coalesce_cols_test_0 t0 INNER JOIN coalesce_cols_test_1 t1 "
      "ON t0.dn = t1.dn AND t0.str = t1.str AND t0.tz = t1.tz AND t0.x = t1.x INNER "
      "JOIN "
      "coalesce_cols_test_2 t2 ON t0.y = t2.y AND t0.tz = t1.tz AND t0.x = t1.x;",
      dt);
    if (dt == ExecutorDeviceType::CPU) {
      // Clear CPU memory and hash table caches
      clearCpuMemory();
    }
  }
}

TEST_F(Select, Joins_ComplexQueries) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test a JOIN (SELECT * FROM test WHERE y < 43) b ON a.x = "
      "b.x "
      "JOIN join_test c ON a.x = c.x "
      "WHERE a.fixed_str = 'foo';",
      dt);
    c("SELECT * FROM (SELECT a.y, b.str FROM test a JOIN join_test b ON a.x = b.x) ORDER "
      "BY y, str;",
      dt);
    c("SELECT x, dup_str FROM (SELECT * FROM test a JOIN join_test b ON a.x = b.x) WHERE "
      "y > 40 ORDER BY x, dup_str;",
      dt);
    c("SELECT a.x FROM (SELECT * FROM test WHERE x = 8) AS a JOIN (SELECT * FROM "
      "test_inner WHERE x = 7) AS b ON a.str "
      "= b.str WHERE a.y < 42;",
      dt);
    c("SELECT a.str as key0,a.fixed_str as key1,COUNT(*) AS color FROM test a JOIN "
      "(select str,count(*) "
      "from test group by str order by COUNT(*) desc limit 40) b on a.str=b.str JOIN "
      "(select "
      "fixed_str,count(*) from test group by fixed_str order by count(*) desc limit "
      "40) "
      "c on "
      "c.fixed_str=a.fixed_str GROUP BY key0, key1 ORDER BY key0,key1;",
      dt);
    c("SELECT COUNT(*) FROM test a JOIN (SELECT str FROM test) b ON a.str = b.str OR "
      "false;",
      "SELECT COUNT(*) FROM test a JOIN (SELECT str FROM test) b ON a.str = b.str OR "
      "0;",
      dt);
    c("SELECT * FROM (SELECT test.x, test.y, d, f FROM test JOIN test_inner ON "
      "test.x = test_inner.x ORDER BY f ASC LIMIT 4) ORDER BY d DESC;",
      dt);
    c(R"(SELECT c.x, count(*) as total FROM (SELECT x, y FROM test WHERE x < 8) c JOIN test_inner ON UNLIKELY(ROUND(c.x, 1) = test_inner.x) GROUP BY c.x;)",
      dt);
  }
}

TEST_F(Select, Joins_TimeAndDate) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    // Inner joins
    c("SELECT COUNT(*) FROM test a, test b WHERE a.m = b.m;", dt);
    c("SELECT COUNT(*) FROM test a, test b WHERE a.n = b.n;", dt);
    c("SELECT COUNT(*) FROM test a, test b WHERE a.o = b.o;", dt);

    c("SELECT COUNT(*) FROM test a, test_inner b WHERE a.m = b.ts;", dt);
    c("SELECT COUNT(*) FROM test a, test_inner b WHERE a.o = b.dt;", dt);
    c("SELECT COUNT(*) FROM test a, test_inner b WHERE a.o = b.dt32;", dt);
    c("SELECT COUNT(*) FROM test a, test_inner b WHERE a.o = b.dt16;", dt);

    // Empty
    c("SELECT COUNT(*) FROM test a, test_empty b WHERE a.m = b.m;", dt);
    c("SELECT COUNT(*) FROM test a, test_empty b WHERE a.n = b.n;", dt);
    c("SELECT COUNT(*) FROM test a, test_empty b WHERE a.o = b.o;", dt);
    c("SELECT COUNT(*) FROM test a, test_empty b WHERE a.o1 = b.o1;", dt);
    c("SELECT COUNT(*) FROM test a, test_empty b WHERE a.o2 = b.o2;", dt);

    // Bitwise path addition
    c("SELECT COUNT(*) FROM test a, test_inner b where a.m = b.ts or (a.m is null and "
      "b.ts is null);",
      dt);
    c("SELECT COUNT(*) FROM test a, test_inner b WHERE a.o = b.dt or (a.o is null and "
      "b.dt is null);",
      dt);
    c("SELECT COUNT(*) FROM test a, test_inner b WHERE a.o = b.dt32 or (a.o is null and "
      "b.dt32 is null);",
      dt);
    c("SELECT COUNT(*) FROM test a, test_inner b WHERE a.o = b.dt16 or (a.o is null and "
      "b.dt16 is null);",
      dt);

    // Inner joins across types
    c("SELECT COUNT(*) FROM test_inner a, test_inner b WHERE a.dt = b.dt;", dt);
    c("SELECT COUNT(*) FROM test_inner a, test_inner b WHERE a.dt32 = b.dt;", dt);
    c("SELECT COUNT(*) FROM test_inner a, test_inner b WHERE a.dt16 = b.dt;", dt);
    c("SELECT COUNT(*) FROM test_inner a, test_inner b WHERE a.dt = b.dt32;", dt);
    c("SELECT COUNT(*) FROM test_inner a, test_inner b WHERE a.dt32 = b.dt32;", dt);
    c("SELECT COUNT(*) FROM test_inner a, test_inner b WHERE a.dt = b.dt16;", dt);
    c("SELECT COUNT(*) FROM test_inner a, test_inner b WHERE a.dt32 = b.dt16;", dt);
    c("SELECT COUNT(*) FROM test_inner a, test_inner b WHERE a.dt16 = b.dt16;", dt);

    // Outer joins
    c("SELECT a.x, a.o, b.dt FROM test a JOIN test_inner b ON a.o = b.dt;", dt);

    if (dt == ExecutorDeviceType::CPU) {
      // Clear CPU memory and hash table caches
      clearCpuMemory();
    }
  }
}

TEST_F(Select, Joins_OneOuterExpression) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x - 1 = test_inner.x;", dt);
    c("SELECT COUNT(*) FROM test_inner, test WHERE test.x - 1 = test_inner.x;", dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x + 0 = test_inner.x;", dt);
    c("SELECT COUNT(*) FROM test_inner, test WHERE test.x + 0 = test_inner.x;", dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x + 1 = test_inner.x;", dt);
    c("SELECT COUNT(*) FROM test_inner, test WHERE test.x + 1 = test_inner.x;", dt);
    c("SELECT COUNT(*) FROM test a, test b WHERE a.o + INTERVAL '0' DAY = b.o;",
      "SELECT COUNT(*) FROM test a, test b WHERE a.o = b.o;",
      dt);
    c("SELECT COUNT(*) FROM test b, test a WHERE a.o + INTERVAL '0' DAY = b.o;",
      "SELECT COUNT(*) FROM test b, test a WHERE a.o = b.o;",
      dt);
  }
}

TEST_F(Select, Joins_Subqueries) {
  if (g_enable_columnar_output) {
    // TODO(adb): fixup these tests under columnar
    return;
  }

  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    // Subquery loop join
    auto result_rows = run_multiple_agg(
        R"(SELECT t, n FROM (SELECT UNNEST(arr_str) as t, COUNT(*) as n FROM  array_test GROUP BY t ORDER BY n DESC), unnest_join_test WHERE t <> x ORDER BY t LIMIT 1;)",
        dt);

    ASSERT_EQ(size_t(1), result_rows->rowCount());
    auto crt_row = result_rows->getNextRow(true, true);
    ASSERT_EQ(size_t(2), crt_row.size());
    ASSERT_EQ("aa", boost::get<std::string>(v<NullableString>(crt_row[0])));
    ASSERT_EQ(1, v<int64_t>(crt_row[1]));

    // Subquery equijoin requiring string translation
    const auto table_reordering_state = config().opts.from_table_reordering;
    config().opts.from_table_reordering = false;  // disable from table reordering
    ScopeGuard reset_from_table_reordering_state = [&table_reordering_state] {
      config().opts.from_table_reordering = table_reordering_state;
    };

    c("SELECT str1, n FROM (SELECT str str1, COUNT(*) n FROM test GROUP BY str HAVING "
      "COUNT(*) "
      "> 5), test_inner_x WHERE str1 = test_inner_x.str ORDER BY str;",
      dt);
    c("SELECT str1, n FROM (SELECT str str1, COUNT(*) n FROM test GROUP BY str), "
      "test_inner_y WHERE str1 = test_inner_y.str ORDER BY str;",
      dt);
    c("SELECT str1, n FROM (SELECT str str1, COUNT(*) n FROM test GROUP BY str HAVING "
      "COUNT(*) "
      "> 5), test_inner_y WHERE str1 = test_inner_y.str  ORDER BY str;",
      dt);
    c("WITH table_inner AS (SELECT str FROM test_inner_y LIMIT 1 OFFSET 1) SELECT str, "
      "n FROM (SELECT str str1, COUNT(*) n FROM test GROUP BY str ORDER BY str ASC "
      "LIMIT 1), table_inner WHERE str1 = table_inner.str ORDER BY str;",
      dt);
    c("WITH table_inner AS (SELECT CASE WHEN str = 'foo' THEN 'hello' ELSE str END "
      "str2 FROM test_inner_y) SELECT str1, n FROM (SELECT CASE WHEN str = 'foo' THEN "
      "'hello' ELSE str END str1, COUNT(*) n FROM test GROUP BY str ORDER BY str ASC), "
      "table_inner WHERE str1 = table_inner.str2 ORDER BY str1;",
      dt);
  }
}

class JoinTest : public ExecuteTestBase, public ::testing::Test {
 protected:
  ~JoinTest() override {}

  void SetUp() override {
    auto create_test_table = [](const std::string& table_name,
                                const size_t num_records,
                                const size_t start_index = 0) {
      createTable(
          table_name,
          {{"x", SQLTypeInfo(kINT, true)}, {"y", SQLTypeInfo(kINT)}, {"str", dictType()}},
          {50});

      TestHelpers::ValuesGenerator gen(table_name);
      const std::vector<std::string> strs{"foo"s, "bar"s, "hello"s, "world"s};
      for (size_t i = start_index; i < start_index + num_records; i++) {
        insertCsvValues(
            table_name,
            std::to_string(i) + ","s + std::to_string(i) + ","s + strs[i % 4]);
      }
    };

    create_test_table("jointest_a", 20, 0);
    create_test_table("jointest_b", 0, 0);
    create_test_table("jointest_c", 20, 10);
  }

  void TearDown() override {
    dropTable("jointest_a");
    dropTable("jointest_b");
    dropTable("jointest_c");
  }
};

TEST_F(JoinTest, EmptyJoinTables) {
  const auto table_reordering_state = config().opts.from_table_reordering;
  config().opts.from_table_reordering = false;  // disable from table reordering
  ScopeGuard reset_from_table_reordering_state = [&table_reordering_state] {
    config().opts.from_table_reordering = table_reordering_state;
  };

  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM jointest_a a INNER JOIN "
                                        "jointest_b b ON a.x = b.x;",
                                        dt)));

    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM jointest_b b INNER JOIN "
                                        "jointest_a a ON a.x = b.x;",
                                        dt)));

    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM jointest_c c INNER JOIN "
                                        "(SELECT a.x FROM jointest_a a INNER JOIN "
                                        "jointest_b b ON a.x = b.x) as j ON j.x = c.x;",
                                        dt)));

    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM jointest_a a INNER JOIN "
                                        "jointest_b b ON a.str = b.str;",
                                        dt)));

    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM jointest_b b INNER JOIN "
                                        "jointest_a a ON a.str = b.str;",
                                        dt)));

    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM jointest_a a INNER JOIN "
                                        "jointest_b b ON a.x = b.x AND a.y = b.y;",
                                        dt)));
  }
}

TEST_F(Select, Joins_MultipleOuterExpressions) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x - 1 = test_inner.x AND "
      "test.str = test_inner.str;",
      dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x + 0 = test_inner.x AND "
      "test.str = test_inner.str;",
      dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.str = test_inner.str AND test.x "
      "+ 0 = test_inner.x;",
      dt);
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x + 1 = test_inner.x AND "
      "test.str = test_inner.str;",
      dt);
    // The following query will fallback to loop join because we don't reorder the
    // expressions to be consistent with table order for composite equality yet.
    c("SELECT COUNT(*) FROM test, test_inner WHERE test.x + 0 = test_inner.x AND "
      "test_inner.str = test.str;",
      dt);
    c("SELECT COUNT(*) FROM test a, test b WHERE a.o + INTERVAL '0' DAY = b.o AND "
      "a.str "
      "= b.str;",
      "SELECT COUNT(*) FROM test a, test b WHERE a.o = b.o AND a.str = b.str;",
      dt);
    c("SELECT COUNT(*) FROM test a, test b WHERE a.o + INTERVAL '0' DAY = b.o AND "
      "a.x = "
      "b.x;",
      "SELECT COUNT(*) FROM test a, test b WHERE a.o = b.o AND a.x = b.x;",
      dt);
  }
}

TEST_F(Select, Joins_Decimal) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT COUNT(*) FROM hash_join_decimal_test as t1, hash_join_decimal_test as t2 "
      "WHERE t1.x = t2.x;",
      dt);
    c("SELECT COUNT(*) FROM hash_join_decimal_test as t1, hash_join_decimal_test as t2 "
      "WHERE t1.y = t2.y;",
      dt);
    c("SELECT t1.y, t2.x FROM hash_join_decimal_test as t1, hash_join_decimal_test as t2 "
      "WHERE t1.y = t2.y ORDER BY t1.y, t1.x;",
      dt);
    // disable loop joins, expect throw
    const auto trivial_join_loop_state = config().exec.join.trivial_loop_join_threshold;
    ScopeGuard reset = [&] {
      config().exec.join.trivial_loop_join_threshold = trivial_join_loop_state;
    };
    config().exec.join.trivial_loop_join_threshold = 1;

    EXPECT_ANY_THROW(
        run_multiple_agg("SELECT COUNT(*) FROM hash_join_decimal_test as t1, "
                         "hash_join_decimal_test as t2 "
                         "WHERE t1.x = t2.y;",
                         dt,
                         false));
    c("SELECT COUNT(*) FROM hash_join_decimal_test as t1, hash_join_decimal_test as t2 "
      "WHERE CAST(t1.x as INT) = CAST(t2.y as INT);",
      dt);
  }
}

TEST_F(Select, RuntimeFunctions) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c("SELECT SUM(ABS(-x + 1)) FROM test;", dt);
    c("SELECT SUM(ABS(-w + 1)) FROM test;", dt);
    c("SELECT SUM(ABS(-y + 1)) FROM test;", dt);
    c("SELECT SUM(ABS(-z + 1)) FROM test;", dt);
    c("SELECT SUM(ABS(-t + 1)) FROM test;", dt);
    c("SELECT SUM(ABS(-dd + 1)) FROM test;", dt);
    c("SELECT SUM(ABS(-f + 1)) FROM test;", dt);
    c("SELECT SUM(ABS(-d + 1)) FROM test;", dt);
    c("SELECT COUNT(*) FROM test WHERE ABS(CAST(x AS float)) >= 0;", dt);
    c("SELECT MIN(ABS(-ofd + 2)) FROM test;", dt);
    ASSERT_EQ(static_cast<int64_t>(2 * g_num_rows),
              v<int64_t>(
                  run_simple_agg("SELECT COUNT(*) FROM test WHERE SIGN(-dd) = -1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE SIGN(x - 7) = 0;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE SIGN(x - 7) = 1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE SIGN(x - 8) = -1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE SIGN(x - 8) = 0;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE SIGN(y - 42) = 0;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE SIGN(y - 42) = 1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE SIGN(y - 43) = -1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE SIGN(y - 43) = 0;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE SIGN(-f) = -1;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(2 * g_num_rows),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE SIGN(-d) = -1;", dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE SIGN(ofd) = 1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE SIGN(-ofd) = -1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE SIGN(ofd) IS NULL;", dt)));
    ASSERT_FLOAT_EQ(static_cast<double>(2 * g_num_rows),
                    v<double>(run_simple_agg(
                        "SELECT SUM(SIN(x) * SIN(x) + COS(x) * COS(x)) FROM test;", dt)));
    ASSERT_FLOAT_EQ(static_cast<double>(2 * g_num_rows),
                    v<double>(run_simple_agg(
                        "SELECT SUM(SIN(f) * SIN(f) + COS(f) * COS(f)) FROM test;", dt)));
    ASSERT_FLOAT_EQ(static_cast<double>(2 * g_num_rows),
                    v<double>(run_simple_agg(
                        "SELECT SUM(SIN(d) * SIN(d) + COS(d) * COS(d)) FROM test;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(2 * g_num_rows),
        v<double>(run_simple_agg(
            "SELECT SUM(SIN(dd) * SIN(dd) + COS(dd) * COS(dd)) FROM test;", dt)));
    ASSERT_FLOAT_EQ(static_cast<double>(2),
                    v<double>(run_simple_agg(
                        "SELECT FLOOR(CAST(2.3 AS double)) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(static_cast<float>(2),
                    v<float>(run_simple_agg(
                        "SELECT FLOOR(CAST(2.3 AS float)) FROM test LIMIT 1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2),
              v<int64_t>(run_simple_agg(
                  "SELECT FLOOR(CAST(2.3 AS BIGINT)) FROM test LIMIT 1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2),
              v<int64_t>(run_simple_agg(
                  "SELECT FLOOR(CAST(2.3 AS SMALLINT)) FROM test LIMIT 1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2),
              v<int64_t>(run_simple_agg(
                  "SELECT FLOOR(CAST(2.3 AS INT)) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(2),
        v<double>(run_simple_agg("SELECT FLOOR(2.3) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(2),
        v<double>(run_simple_agg("SELECT FLOOR(2.0) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(-3),
        v<double>(run_simple_agg("SELECT FLOOR(-2.3) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(-2),
        v<double>(run_simple_agg("SELECT FLOOR(-2.0) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(static_cast<double>(3),
                    v<double>(run_simple_agg(
                        "SELECT CEIL(CAST(2.3 AS double)) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(static_cast<float>(3),
                    v<float>(run_simple_agg(
                        "SELECT CEIL(CAST(2.3 AS float)) FROM test LIMIT 1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2),
              v<int64_t>(run_simple_agg(
                  "SELECT CEIL(CAST(2.3 AS BIGINT)) FROM test LIMIT 1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2),
              v<int64_t>(run_simple_agg(
                  "SELECT CEIL(CAST(2.3 AS SMALLINT)) FROM test LIMIT 1;", dt)));
    ASSERT_EQ(static_cast<int64_t>(2),
              v<int64_t>(run_simple_agg(
                  "SELECT CEIL(CAST(2.3 AS INT)) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(static_cast<double>(3),
                    v<double>(run_simple_agg("SELECT CEIL(2.3) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(static_cast<double>(2),
                    v<double>(run_simple_agg("SELECT CEIL(2.0) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(-2),
        v<double>(run_simple_agg("SELECT CEIL(-2.3) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(-2),
        v<double>(run_simple_agg("SELECT CEIL(-2.0) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<float>(4129511.320307),
        v<double>(run_simple_agg("SELECT DISTANCE_IN_METERS(-74.0059, "
                                 "40.7217,-122.416667 , 37.783333) FROM test LIMIT 1;",
                                 dt)));
    ASSERT_FLOAT_EQ(
        static_cast<int64_t>(1000),
        v<int64_t>(run_simple_agg(
            "SELECT TRUNCATE(CAST(1171 AS SMALLINT),-3) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(1000),
        v<double>(run_simple_agg(
            "SELECT TRUNCATE(CAST(1171.123 AS FLOAT),-3) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(1000),
        v<double>(run_simple_agg(
            "SELECT TRUNCATE(CAST(1171.123 AS DOUBLE),-3) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(1171.10),
        v<double>(run_simple_agg(
            "SELECT TRUNCATE(CAST(1171.123 AS DOUBLE),1) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<double>(1171.11),
        v<double>(run_simple_agg(
            "SELECT TRUNCATE(CAST(1171.113 AS FLOAT),2) FROM test LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(static_cast<float>(11000000000000),
                    v<float>(run_simple_agg(
                        "SELECT FLOOR(f / 1e-13) FROM test WHERE f < 1.2 LIMIT 1;", dt)));
    ASSERT_FLOAT_EQ(
        static_cast<float>(11000000000000),
        v<float>(run_simple_agg(
            "SELECT FLOOR(CAST(f / 1e-13 AS FLOAT)) FROM test WHERE f < 1.2 LIMIT 1;",
            dt)));
    ASSERT_FLOAT_EQ(
        std::numeric_limits<float>::min(),
        v<float>(run_simple_agg(
            "SELECT FLOOR(fn / 1e-13) FROM test WHERE fn IS NULL LIMIT 1;", dt)));
    {
      auto result = run_multiple_agg("SELECT fn, isnan(fn) FROM test;", dt);
      ASSERT_EQ(result->rowCount(), size_t(2 * g_num_rows));
      // Ensure the type for `isnan` is nullable
      const auto func_ti = result->getColType(1);
      ASSERT_FALSE(func_ti.get_notnull());
      for (size_t i = 0; i < g_num_rows; i++) {
        auto crt_row = result->getNextRow(false, false);
        ASSERT_EQ(crt_row.size(), size_t(2));
        if (std::numeric_limits<float>::min() == v<float>(crt_row[0])) {
          ASSERT_EQ(std::numeric_limits<int8_t>::min(), v<int64_t>(crt_row[1]));
        } else {
          ASSERT_EQ(0, v<int64_t>(crt_row[1]));
        }
      }
    }
  }
}

TEST_F(Select, TextGroupBy) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_THROW(run_multiple_agg(" select count(*) from (SELECT tnone, count(*) cc from "
                                  "text_group_by_test group by tnone);",
                                  dt),
                 std::runtime_error);
    ASSERT_EQ(static_cast<int64_t>(1),
              v<int64_t>(run_simple_agg("select count(*) from (SELECT tdict, count(*) cc "
                                        "from text_group_by_test group by tdict)",
                                        dt)));
    ASSERT_EQ(static_cast<int64_t>(1),
              v<int64_t>(run_simple_agg("select count(*) from (SELECT tdef, count(*) cc "
                                        "from text_group_by_test group by tdef)",
                                        dt)));
  }
}

TEST_F(Select, UnsupportedExtensions) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_THROW(run_multiple_agg(
                     "SELECT TRUNCATE(2016, CAST(1.0 as BIGINT)) FROM test LIMIT 1;", dt),
                 std::runtime_error);
  }
}

TEST_F(Select, UnsupportedSortOfIntermediateResult) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_THROW(run_multiple_agg("SELECT real_str FROM test ORDER BY x;", dt),
                 std::runtime_error);
  }
}

TEST_F(Select, PgShim) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT str, SUM(x), COUNT(str) FROM test WHERE \"y\" = 42 AND str = 'Shim All The "
      "Things!' GROUP BY str;",
      dt);
  }
}

TEST_F(Select, CaseInsensitive) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT X, COUNT(*) AS N FROM test GROUP BY teSt.x ORDER BY n DESC;", dt);
  }
}

TEST_F(Select, Deserialization) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT CAST(CAST(x AS float) * 0.0000000000 AS INT) FROM test;", dt);
  }
}

TEST_F(Select, DesugarTransform) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT * FROM emptytab ORDER BY emptytab. x;", dt);
    c("SELECT COUNT(*) FROM test WHERE x IN (SELECT x + 1 AS foo FROM test GROUP BY foo "
      "ORDER BY COUNT(*) DESC LIMIT "
      "1);",
      dt);
  }
}

TEST_F(Select, ArrowOutput) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c_arrow("SELECT str, COUNT(*) FROM test GROUP BY str ORDER BY str ASC;", dt);
    c_arrow("SELECT x, y, w, z, t, f, d, str, ofd, ofq FROM test ORDER BY x ASC, y ASC;",
            dt);
    c_arrow("SELECT null_str, COUNT(*) FROM test GROUP BY null_str;", dt);
    c_arrow("SELECT m,m_3,m_6,m_9 from test", dt);
    c_arrow("SELECT o, o1, o2 from test", dt);
    c_arrow("SELECT n from test", dt);
    c_arrow(
        "SELECT x, CASE WHEN x = 7 THEN 'foo' ELSE 'bar' END AS case_x FROM test "
        "WHERE str IN ('bar', 'baz') ORDER BY x ASC;",
        dt);
  }
}

TEST_F(Select, ArrowDictionaries) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    // Projection - should be dense
    c_arrow(
        "SELECT t FROM test_window_func_large_multi_frag WHERE i_1000 < 800 AND t <> 'e' "
        "ORDER "
        "BY "
        "t ASC;",
        dt,
        10000L,
        0.25);

    // Projection - should be sparse
    c_arrow(
        "SELECT t_unique FROM test_window_func_large_multi_frag WHERE i_1000 < 40 "
        "AND t <> 'd' ORDER BY t_unique ASC;",
        dt,
        10000L,
        0.25);

    // Group by - should be dense
    c_arrow(
        "SELECT t, COUNT(*) as n FROM test_window_func_large_multi_frag WHERE "
        "i_1000 < 800 AND t <> 'd' GROUP by t ORDER BY n DESC;",
        dt,
        3L,
        2.0);

    // Group by - should be sparse
    c_arrow(
        "SELECT t_unique, COUNT(*) as n FROM test_window_func_large_multi_frag WHERE "
        "i_1000 < 40 and t <> 'd' GROUP by t_unique ORDER BY "
        "t_unique ASC;",
        dt,
        10000L,
        0.25);
  }
}

TEST_F(Select, WatchdogTest) {
  const auto watchdog_state = config().exec.watchdog.enable;
  config().exec.watchdog.enable = true;
  ScopeGuard reset_Watchdog_state = [&watchdog_state] {
    config().exec.watchdog.enable = watchdog_state;
  };
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT x, SUM(f) AS n FROM test GROUP BY x ORDER BY n DESC LIMIT 5;", dt);
    c("SELECT COUNT(*) FROM test WHERE str = "
      "'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz';",
      dt);
  }
}

TEST_F(Select, PuntToCPU) {
  const auto cpu_retry_state = config().exec.heterogeneous.allow_cpu_retry;
  const auto cpu_step_retry_state =
      config().exec.heterogeneous.allow_query_step_cpu_retry;
  const auto watchdog_state = config().exec.watchdog.enable;
  config().exec.heterogeneous.allow_cpu_retry = false;
  config().exec.heterogeneous.allow_query_step_cpu_retry = false;
  config().exec.watchdog.enable = true;
  ScopeGuard reset_global_flag_state =
      [&cpu_retry_state, &cpu_step_retry_state, &watchdog_state] {
        config().exec.heterogeneous.allow_cpu_retry = cpu_retry_state;
        config().exec.heterogeneous.allow_query_step_cpu_retry = cpu_step_retry_state;
        config().exec.watchdog.enable = watchdog_state;
        g_gpu_mem_limit_percent = 0.9;  // Reset to 90%
      };

  const auto dt = ExecutorDeviceType::GPU;
  if (skip_tests(dt)) {
    return;
  }

  g_gpu_mem_limit_percent = 1e-10;
  EXPECT_THROW(run_multiple_agg("SELECT x, COUNT(*) FROM test GROUP BY x;", dt),
               std::runtime_error);
  EXPECT_THROW(run_multiple_agg("SELECT str, COUNT(*) FROM test GROUP BY str;", dt),
               std::runtime_error);

  config().exec.heterogeneous.allow_cpu_retry = true;
  EXPECT_NO_THROW(run_multiple_agg("SELECT x, COUNT(*) FROM test GROUP BY x;", dt));
  EXPECT_NO_THROW(run_multiple_agg(
      "SELECT COUNT(*) FROM test WHERE x IN (SELECT y FROM test WHERE y > 3);", dt));
}

TEST_F(Select, PuntQueryStepToCPU) {
  const auto cpu_retry_state = config().exec.heterogeneous.allow_cpu_retry;
  const auto cpu_step_retry_state =
      config().exec.heterogeneous.allow_query_step_cpu_retry;
  const auto watchdog_state = config().exec.watchdog.enable;
  config().exec.heterogeneous.allow_cpu_retry = false;
  config().exec.heterogeneous.allow_query_step_cpu_retry = false;
  config().exec.watchdog.enable = true;
  ScopeGuard reset_global_flag_state =
      [&cpu_retry_state, &cpu_step_retry_state, &watchdog_state] {
        config().exec.heterogeneous.allow_cpu_retry = cpu_retry_state;
        config().exec.heterogeneous.allow_query_step_cpu_retry = cpu_step_retry_state;
        config().exec.watchdog.enable = watchdog_state;
        g_gpu_mem_limit_percent = 0.9;  // Reset to 90%
      };

  const auto dt = ExecutorDeviceType::GPU;
  if (skip_tests(dt)) {
    return;
  }

  // Query is single step and can run on GPU
  EXPECT_NO_THROW(run_multiple_agg("SELECT x, COUNT(*) FROM test GROUP BY x;", dt));

  // Query is multi-step and second step can only run on CPU, will fail without
  // config().exec.heterogeneous.allow_cpu_retry Note: If and when we implement
  // APPROX_MEDIAN for GPU, this will fail and need adjustment
  EXPECT_THROW(run_multiple_agg("SELECT x, APPROX_MEDIAN(n) AS n_median FROM (SELECT x, "
                                "y, COUNT(*) AS n FROM test GROUP BY x, y) GROUP BY x;",
                                dt),
               std::runtime_error);

  config().exec.heterogeneous.allow_cpu_retry = false;
  config().exec.heterogeneous.allow_query_step_cpu_retry = true;

  EXPECT_NO_THROW(run_multiple_agg("SELECT x, COUNT(*) FROM test GROUP BY x;", dt));
  // Even without config().exec.heterogeneous.allow_cpu_retry = true, this should run with
  // config().exec.heterogeneous.allow_query_step_cpu_retry = true, as second step can
  // drop to CPU without triggering global punt to CPU
  EXPECT_NO_THROW(
      run_multiple_agg("SELECT x, APPROX_MEDIAN(n) AS n_median FROM (SELECT x, y, "
                       "COUNT(*) AS n FROM test GROUP BY x, y) GROUP BY x;",
                       dt));

  config().exec.heterogeneous.allow_cpu_retry = false;
  config().exec.heterogeneous.allow_query_step_cpu_retry = false;
  g_gpu_mem_limit_percent = 1e-10;

  // Out of memory errors caught pre-allocation should (currently) trigger a
  // QueryMustRunOnCPU exception and will be caught with either
  // config().exec.heterogeneous.allow_cpu_retry or
  // config().exec.heterogeneous.allow_query_step_cpu_retry

  EXPECT_THROW(run_multiple_agg("SELECT x, AVG(n) AS n_avg FROM (SELECT x, "
                                "y, COUNT(*) AS n FROM test GROUP BY x, y) GROUP BY x;",
                                dt),
               std::runtime_error);

  config().exec.heterogeneous.allow_cpu_retry = false;
  config().exec.heterogeneous.allow_query_step_cpu_retry = true;
  g_gpu_mem_limit_percent = 1e-10;

  EXPECT_NO_THROW(
      run_multiple_agg("SELECT x, AVG(n) AS n_avg FROM (SELECT x, y, "
                       "COUNT(*) AS n FROM test GROUP BY x, y) GROUP BY x;",
                       dt));
}

// Select.Time does a lot of DATEADD tests already.  These focus on high-precision
// timestamps before, across, and after the epoch=0 boundary.
TEST_F(Select, Dateadd) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // Comparing strings is preferred, but "Cast from TIMESTAMP(6) to TEXT not supported"
    EXPECT_EQ(timestampToInt64("1960-03-29 23:59:59.999999", dt),
              dateadd("month", 1, "1960-02-29 23:59:59.999999", dt));
    EXPECT_EQ(timestampToInt64("1960-03-29 23:59:59.999999", dt),
              dateadd("month", 1, "1960-02-29 23:59:59.999999", dt));

    EXPECT_EQ(timestampToInt64("1961-02-28 23:59:59.999999", dt),
              dateadd("year", 1, "1960-02-29 23:59:59.999999", dt));
    EXPECT_EQ(timestampToInt64("1960-03-29 23:59:59.999", dt),
              dateadd("month", 1, "1960-02-29 23:59:59.999", dt));

    EXPECT_EQ(timestampToInt64("2961-02-28 23:59:59.999", dt),
              dateadd("year", 1, "2960-02-29 23:59:59.999", dt));
    EXPECT_EQ(timestampToInt64("2960-03-29 23:59:59.999", dt),
              dateadd("month", 1, "2960-02-29 23:59:59.999", dt));

    EXPECT_EQ(timestampToInt64("1960-01-01 00:00:00.000000000", dt),
              dateadd("nanosecond", 1, "1959-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("1960-01-01 00:00:00.000000001", dt),
              dateadd("nanosecond", 2, "1959-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("1970-01-01 00:00:00.000000000", dt),
              dateadd("nanosecond", 1, "1969-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("1970-01-01 00:00:00.000000001", dt),
              dateadd("nanosecond", 2, "1969-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("2020-01-01 00:00:00.000000000", dt),
              dateadd("nanosecond", 1, "2019-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("2020-01-01 00:00:00.000000001", dt),
              dateadd("nanosecond", 2, "2019-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("1960-01-01 00:00:00.000000999", dt),
              dateadd("microsecond", 1, "1959-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("1960-01-01 00:00:00.000001999", dt),
              dateadd("microsecond", 2, "1959-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("1970-01-01 00:00:00.000000999", dt),
              dateadd("microsecond", 1, "1969-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("1970-01-01 00:00:00.000001999", dt),
              dateadd("microsecond", 2, "1969-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("2020-01-01 00:00:00.000000999", dt),
              dateadd("microsecond", 1, "2019-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("2020-01-01 00:00:00.000001999", dt),
              dateadd("microsecond", 2, "2019-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("1960-01-01 00:00:00.000999999", dt),
              dateadd("millisecond", 1, "1959-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("1960-01-01 00:00:00.001999999", dt),
              dateadd("millisecond", 2, "1959-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("1970-01-01 00:00:00.000999999", dt),
              dateadd("millisecond", 1, "1969-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("1970-01-01 00:00:00.001999999", dt),
              dateadd("millisecond", 2, "1969-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("2020-01-01 00:00:00.000999999", dt),
              dateadd("millisecond", 1, "2019-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("2020-01-01 00:00:00.001999999", dt),
              dateadd("millisecond", 2, "2019-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("1960-01-01 00:00:00.999999999", dt),
              dateadd("second", 1, "1959-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("1960-01-01 00:00:01.999999999", dt),
              dateadd("second", 2, "1959-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("1970-01-01 00:00:00.999999999", dt),
              dateadd("second", 1, "1969-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("1970-01-01 00:00:01.999999999", dt),
              dateadd("second", 2, "1969-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("2020-01-01 00:00:00.999999999", dt),
              dateadd("second", 1, "2019-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("2020-01-01 00:00:01.999999999", dt),
              dateadd("second", 2, "2019-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("1960-01-01 00:00:59.999999999", dt),
              dateadd("minute", 1, "1959-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("1960-01-01 00:01:59.999999999", dt),
              dateadd("minute", 2, "1959-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("1970-01-01 00:00:59.999999999", dt),
              dateadd("minute", 1, "1969-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("1970-01-01 00:01:59.999999999", dt),
              dateadd("minute", 2, "1969-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("2020-01-01 00:00:59.999999999", dt),
              dateadd("minute", 1, "2019-12-31 23:59:59.999999999", dt));
    EXPECT_EQ(timestampToInt64("2020-01-01 00:01:59.999999999", dt),
              dateadd("minute", 2, "2019-12-31 23:59:59.999999999", dt));

    EXPECT_EQ(timestampToInt64("2100-02-28 23:59:59.999999", dt),
              dateadd("decade", 2, "2080-02-29 23:59:59.999999", dt));
    EXPECT_EQ(timestampToInt64("1900-02-28 23:59:59.999", dt),
              dateadd("decade", -2, "1920-02-29 23:59:59.999", dt));

    EXPECT_EQ(timestampToInt64("2100-02-28 23:59:59.999999", dt),
              dateadd("century", 1, "2000-02-29 23:59:59.999999", dt));
    EXPECT_EQ(timestampToInt64("1900-02-28 23:59:59.999", dt),
              dateadd("century", -1, "2000-02-29 23:59:59.999", dt));

    EXPECT_EQ(timestampToInt64("3000-02-28 23:59:59.999999", dt),
              dateadd("millennium", 1, "2000-02-29 23:59:59.999999", dt));
    EXPECT_EQ(timestampToInt64("5000-02-28 23:59:59.999", dt),
              dateadd("millennium", 3, "2000-02-29 23:59:59.999", dt));
  }
}

// Test adding intervals that are higher precision than the timestamp being added to.
TEST_F(Select, DateaddHighPrecision) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // Comparing strings is preferred, but "Cast from TIMESTAMP(6) to TEXT not supported"
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59", dt),
              dateadd("millisecond", 999, "1960-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00", dt),
              dateadd("millisecond", 1000, "1960-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00", dt),
              dateadd("millisecond", 1999, "1960-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59", dt),
              dateadd("millisecond", -1, "1960-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59", dt),
              dateadd("millisecond", -1000, "1960-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:58", dt),
              dateadd("millisecond", -1001, "1960-03-01 00:00:00", dt));

    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59", dt),
              dateadd("microsecond", 999999, "1960-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59.999", dt),
              dateadd("microsecond", 999999, "1960-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00", dt),
              dateadd("microsecond", 1000000, "1960-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00.000", dt),
              dateadd("microsecond", 1000000, "1960-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00", dt),
              dateadd("microsecond", 1999999, "1960-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00.999", dt),
              dateadd("microsecond", 1999999, "1960-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59", dt),
              dateadd("microsecond", -1, "1960-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59.999", dt),
              dateadd("microsecond", -1, "1960-03-01 00:00:00.000", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59", dt),
              dateadd("microsecond", -1000000, "1960-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59.000", dt),
              dateadd("microsecond", -1000000, "1960-03-01 00:00:00.000", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:58", dt),
              dateadd("microsecond", -1000001, "1960-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:58.999", dt),
              dateadd("microsecond", -1000001, "1960-03-01 00:00:00.000", dt));

    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59", dt),
              dateadd("nanosecond", 999999999, "1960-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59.999", dt),
              dateadd("nanosecond", 999999999, "1960-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59.999999", dt),
              dateadd("nanosecond", 999999999, "1960-02-29 23:59:59.000000", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00", dt),
              dateadd("nanosecond", 1000000000, "1960-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00.000", dt),
              dateadd("nanosecond", 1000000000, "1960-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00.000000", dt),
              dateadd("nanosecond", 1000000000, "1960-02-29 23:59:59.000000", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00", dt),
              dateadd("nanosecond", 1999999999, "1960-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00.999", dt),
              dateadd("nanosecond", 1999999999, "1960-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("1960-03-01 00:00:00.999999", dt),
              dateadd("nanosecond", 1999999999, "1960-02-29 23:59:59.000000", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59", dt),
              dateadd("nanosecond", -1, "1960-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59.999", dt),
              dateadd("nanosecond", -1, "1960-03-01 00:00:00.000", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59.999999", dt),
              dateadd("nanosecond", -1, "1960-03-01 00:00:00.000000", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59", dt),
              dateadd("nanosecond", -1000000000, "1960-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59.000", dt),
              dateadd("nanosecond", -1000000000, "1960-03-01 00:00:00.000", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:59.000000", dt),
              dateadd("nanosecond", -1000000000, "1960-03-01 00:00:00.000000", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:58", dt),
              dateadd("nanosecond", -1000000001, "1960-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:58.999", dt),
              dateadd("nanosecond", -1000000001, "1960-03-01 00:00:00.000", dt));
    EXPECT_EQ(timestampToInt64("1960-02-29 23:59:58.999999", dt),
              dateadd("nanosecond", -1000000001, "1960-03-01 00:00:00.000000", dt));

    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59", dt),
              dateadd("millisecond", 999, "2000-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00", dt),
              dateadd("millisecond", 1000, "2000-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00", dt),
              dateadd("millisecond", 1999, "2000-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59", dt),
              dateadd("millisecond", -1, "2000-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59", dt),
              dateadd("millisecond", -1000, "2000-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:58", dt),
              dateadd("millisecond", -1001, "2000-03-01 00:00:00", dt));

    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59", dt),
              dateadd("microsecond", 999999, "2000-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59.999", dt),
              dateadd("microsecond", 999999, "2000-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00", dt),
              dateadd("microsecond", 1000000, "2000-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00.000", dt),
              dateadd("microsecond", 1000000, "2000-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00", dt),
              dateadd("microsecond", 1999999, "2000-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00.999", dt),
              dateadd("microsecond", 1999999, "2000-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59", dt),
              dateadd("microsecond", -1, "2000-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59.999", dt),
              dateadd("microsecond", -1, "2000-03-01 00:00:00.000", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59", dt),
              dateadd("microsecond", -1000000, "2000-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59.000", dt),
              dateadd("microsecond", -1000000, "2000-03-01 00:00:00.000", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:58", dt),
              dateadd("microsecond", -1000001, "2000-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:58.999", dt),
              dateadd("microsecond", -1000001, "2000-03-01 00:00:00.000", dt));

    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59", dt),
              dateadd("nanosecond", 999999999, "2000-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59.999", dt),
              dateadd("nanosecond", 999999999, "2000-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59.999999", dt),
              dateadd("nanosecond", 999999999, "2000-02-29 23:59:59.000000", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00", dt),
              dateadd("nanosecond", 1000000000, "2000-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00.000", dt),
              dateadd("nanosecond", 1000000000, "2000-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00.000000", dt),
              dateadd("nanosecond", 1000000000, "2000-02-29 23:59:59.000000", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00", dt),
              dateadd("nanosecond", 1999999999, "2000-02-29 23:59:59", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00.999", dt),
              dateadd("nanosecond", 1999999999, "2000-02-29 23:59:59.000", dt));
    EXPECT_EQ(timestampToInt64("2000-03-01 00:00:00.999999", dt),
              dateadd("nanosecond", 1999999999, "2000-02-29 23:59:59.000000", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59", dt),
              dateadd("nanosecond", -1, "2000-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59.999", dt),
              dateadd("nanosecond", -1, "2000-03-01 00:00:00.000", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59.999999", dt),
              dateadd("nanosecond", -1, "2000-03-01 00:00:00.000000", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59", dt),
              dateadd("nanosecond", -1000000000, "2000-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59.000", dt),
              dateadd("nanosecond", -1000000000, "2000-03-01 00:00:00.000", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:59.000000", dt),
              dateadd("nanosecond", -1000000000, "2000-03-01 00:00:00.000000", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:58", dt),
              dateadd("nanosecond", -1000000001, "2000-03-01 00:00:00", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:58.999", dt),
              dateadd("nanosecond", -1000000001, "2000-03-01 00:00:00.000", dt));
    EXPECT_EQ(timestampToInt64("2000-02-29 23:59:58.999999", dt),
              dateadd("nanosecond", -1000000001, "2000-03-01 00:00:00.000000", dt));
  }
}

TEST_F(Select, Datediff) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    EXPECT_EQ(
        999999997LL,
        datediff(
            "nanosecond", "1950-01-02 12:34:56.000000003", "1950-01-02 12:34:57", dt));
    EXPECT_EQ(
        -3,
        datediff(
            "nanosecond", "1950-01-02 12:34:56.000000003", "1950-01-02 12:34:56", dt));
    EXPECT_EQ(
        -1000000003,
        datediff(
            "nanosecond", "1950-01-02 12:34:56.000000003", "1950-01-02 12:34:55", dt));
    EXPECT_EQ(
        999999997LL,
        datediff(
            "nanosecond", "2000-01-02 12:34:56.000000003", "2000-01-02 12:34:57", dt));
    EXPECT_EQ(
        -3,
        datediff(
            "nanosecond", "2000-01-02 12:34:56.000000003", "2000-01-02 12:34:56", dt));
    EXPECT_EQ(
        -1000000003,
        datediff(
            "nanosecond", "2000-01-02 12:34:56.000000003", "2000-01-02 12:34:55", dt));

    EXPECT_EQ(
        0,
        datediff("second", "1950-01-02 12:34:56.000000003", "1950-01-02 12:34:57", dt));
    EXPECT_EQ(
        0,
        datediff("second", "1950-01-02 12:34:56.000000003", "1950-01-02 12:34:56", dt));
    EXPECT_EQ(
        -1,
        datediff("second", "1950-01-02 12:34:56.000000003", "1950-01-02 12:34:55", dt));
    EXPECT_EQ(
        0,
        datediff("second", "2000-01-02 12:34:56.000000003", "2000-01-02 12:34:57", dt));
    EXPECT_EQ(
        0,
        datediff("second", "2000-01-02 12:34:56.000000003", "2000-01-02 12:34:56", dt));
    EXPECT_EQ(
        -1,
        datediff("second", "2000-01-02 12:34:56.000000003", "2000-01-02 12:34:55", dt));

    EXPECT_EQ(0,
              datediff("second",
                       "1969-12-31 23:59:58.000000003",
                       "1969-12-31 23:59:59.000000002",
                       dt));
    EXPECT_EQ(1,
              datediff("second",
                       "1969-12-31 23:59:58.000000003",
                       "1969-12-31 23:59:59.000000003",
                       dt));
    EXPECT_EQ(1,
              datediff("second",
                       "1969-12-31 23:59:58.000000003",
                       "1969-12-31 23:59:59.000000004",
                       dt));
    EXPECT_EQ(0,
              datediff("second",
                       "1969-12-31 23:59:59.000000003",
                       "1970-01-01 00:00:00.000000002",
                       dt));
    EXPECT_EQ(1,
              datediff("second",
                       "1969-12-31 23:59:59.000000003",
                       "1970-01-01 00:00:00.000000003",
                       dt));
    EXPECT_EQ(1,
              datediff("second",
                       "1969-12-31 23:59:59.000000003",
                       "1970-01-01 00:00:00.000000004",
                       dt));
    EXPECT_EQ(0,
              datediff("second",
                       "1969-12-31 23:59:59.000000002",
                       "1969-12-31 23:59:58.000000003",
                       dt));
    EXPECT_EQ(-1,
              datediff("second",
                       "1969-12-31 23:59:59.000000003",
                       "1969-12-31 23:59:58.000000003",
                       dt));
    EXPECT_EQ(-1,
              datediff("second",
                       "1969-12-31 23:59:59.000000004",
                       "1969-12-31 23:59:58.000000003",
                       dt));
    EXPECT_EQ(0,
              datediff("second",
                       "1970-01-01 00:00:00.000000002",
                       "1969-12-31 23:59:59.000000003",
                       dt));
    EXPECT_EQ(-1,
              datediff("second",
                       "1970-01-01 00:00:00.000000003",
                       "1969-12-31 23:59:59.000000003",
                       dt));
    EXPECT_EQ(-1,
              datediff("second",
                       "1970-01-01 00:00:00.000000004",
                       "1969-12-31 23:59:59.000000003",
                       dt));
    EXPECT_EQ(
        6,
        datediff("millennium", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(
        7,
        datediff("millennium", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(
        69,
        datediff("century", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(
        70,
        datediff("century", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(
        699,
        datediff("decade", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(
        700,
        datediff("decade", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(6999,
              datediff("year", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(7000,
              datediff("year", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(
        7000 * 4 - 1,
        datediff("quarter", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(
        7000 * 4,
        datediff("quarter", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(
        7000 * 12 - 1,
        datediff("month", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(
        7000 * 12,
        datediff("month", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(2556697,
              datediff("day", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(2556698,
              datediff("day", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(
        2556698 * 4 - 1,
        datediff("quarterday", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(
        2556698 * 4,
        datediff("quarterday", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(2556698 * 24 - 1,
              datediff("hour", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(2556698 * 24,
              datediff("hour", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(
        2556698 * 24 * 60LL - 1,
        datediff("minute", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(
        2556698 * 24 * 60LL,
        datediff("minute", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(
        2556698 * 24 * 60LL * 60 - 1,
        datediff("second", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(
        2556698 * 24 * 60LL * 60,
        datediff("second", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
    EXPECT_EQ(
        2556698 * 24 * 60LL * 60 * 1000 - 1,
        datediff(
            "millisecond", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.002", dt));
    EXPECT_EQ(
        2556698 * 24 * 60LL * 60 * 1000,
        datediff(
            "millisecond", "1900-02-15 12:00:00.003", "8900-02-15 12:00:00.003", dt));
  }
}

TEST_F(Select, TimestampPrecision_DateTruncate) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(978307200000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(millennium, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(978307200000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(century, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1262304000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(decade, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1388534400000LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATE_TRUNC(year, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1417392000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(month, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1417996800000LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATE_TRUNC(week, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1417910400000L,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(week_sunday, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418428800000L,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(week_saturday, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(
        1418428800000LL,
        v<int64_t>(run_simple_agg("SELECT DATE_TRUNC(day, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418508000000LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATE_TRUNC(hour, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509380000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(minute, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(second, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(millisecond, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(microsecond, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(nanosecond, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(-30578688000000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(millennium, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(-2177452800000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(century, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(631152000000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(decade, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(915148800000000LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATE_TRUNC(year, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(930787200000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(month, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931132800000000LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATE_TRUNC(week, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931651200000000L,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(week_sunday, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931564800000000L,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(week_saturday, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(
        931651200000000LL,
        v<int64_t>(run_simple_agg("SELECT DATE_TRUNC(day, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701600000000LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATE_TRUNC(hour, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701720000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(minute, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(second, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773874000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(millisecond, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(microsecond, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(nanosecond, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(978307200000000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(millennium, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(978307200000000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(century, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(946684800000000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(decade, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1136073600000000000LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATE_TRUNC(year, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1143849600000000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(month, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1145836800000000000LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATE_TRUNC(week, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1145750400000000000L,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(week_sunday, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1145664000000000000L,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(week_saturday, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(
        1146009600000000000LL,
        v<int64_t>(run_simple_agg("SELECT DATE_TRUNC(day, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146020400000000000LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATE_TRUNC(hour, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023340000000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(minute, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344000000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(second, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344607000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(millisecond, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344607435000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(microsecond, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATE_TRUNC(nanosecond, m_9) FROM test limit 1;", dt)));
  }
}

TEST_F(Select, TimestampPrecision_Extract) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(1146023344LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(epoch from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146009600LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(dateepoch from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(4607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(nanosecond from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(4607435LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(microsecond from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(4607LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(millisecond from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(second from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(49LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(minute from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(hour from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(
                  run_simple_agg("SELECT EXTRACT(dow from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(isodow from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(17LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(week from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(17LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(week_sunday from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(17LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(week_saturday from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(26LL,
              v<int64_t>(
                  run_simple_agg("SELECT EXTRACT(day from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(116LL,
              v<int64_t>(
                  run_simple_agg("SELECT EXTRACT(doy from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(month from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(2LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(quarter from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(quarterday from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(2006LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(year from m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(epoch from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931651200LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(dateepoch from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(53874533000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(nanosecond from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(53874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(microsecond from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(53874LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(millisecond from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(53LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(second from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(2LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(minute from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(14LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(hour from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(
                  run_simple_agg("SELECT EXTRACT(dow from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(7LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(isodow from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(27LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(week from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(28LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(week_sunday from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(28LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(week_saturday from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(11LL,
              v<int64_t>(
                  run_simple_agg("SELECT EXTRACT(day from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(192LL,
              v<int64_t>(
                  run_simple_agg("SELECT EXTRACT(doy from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(7LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(month from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(quarter from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(quarterday from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(1999LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(year from m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(epoch from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418428800LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(dateepoch from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(15323000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(nanosecond from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(15323000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(microsecond from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(15323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(millisecond from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(15LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(second from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(23LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(minute from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(22LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(hour from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(6LL,
              v<int64_t>(
                  run_simple_agg("SELECT EXTRACT(dow from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(6LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(isodow from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(week from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(week_sunday from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(week_saturday from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(13LL,
              v<int64_t>(
                  run_simple_agg("SELECT EXTRACT(day from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(347LL,
              v<int64_t>(
                  run_simple_agg("SELECT EXTRACT(doy from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(month from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(quarter from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(quarterday from m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(2014LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(year from m_3) FROM test limit 1;", dt)));
  }
}

TEST_F(Select, TimestampPrecision_DatePart) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(2014LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATEPART('year', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('quarter', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('month', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(347LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('dayofyear', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(
        13LL,
        v<int64_t>(run_simple_agg("SELECT DATEPART('day', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(22LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATEPART('hour', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(23LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('minute', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(15LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('second', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(15323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('millisecond', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(15323000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('microsecond', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(15323000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('nanosecond', m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1999LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATEPART('year', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('quarter', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(7LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('month', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(192LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('dayofyear', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(
        11LL,
        v<int64_t>(run_simple_agg("SELECT DATEPART('day', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(14LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATEPART('hour', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(2LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('minute', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(53LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('second', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(53874LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('millisecond', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(53874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('microsecond', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(53874533000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('nanosecond', m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(2006LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATEPART('year', m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(2LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('quarter', m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('month', m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(116LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('dayofyear', m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(
        26LL,
        v<int64_t>(run_simple_agg("SELECT DATEPART('day', m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATEPART('hour', m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(49LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('minute', m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('second', m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(4607LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('millisecond', m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(4607435LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('microsecond', m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(4607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEPART('nanosecond', m_9) FROM test limit 1;", dt)));
    EXPECT_ANY_THROW(run_simple_agg("SELECT DATEPART(NULL, m_9) FROM test limit 1;", dt));
  }
}

TEST_F(Select, TimestampPrecision_DateAdd) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(1177559344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('year',1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1153885744607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('quarter', 1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1148615344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('month', 1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146109744607435125LL,
              v<int64_t>(
                  run_simple_agg("SELECT DATEADD('day',1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146026944607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('hour', 1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023404607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('minute', 1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023403607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('second', 59, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344932435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('millisecond', 325 , m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344607960125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('microsecond', 525, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344607436000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('nanosecond', 875, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1026396173874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('year',3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(955461773874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('quarter', 3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(947599373874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('month', 6, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(932824973874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('day',13, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931734173874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('hour', 9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931704053874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('minute', 38, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701783874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('second', 10, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773885533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('millisecond', 11 , m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773874678LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('microsecond', 145, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('nanosecond', 875, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(1734128595323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('year',10, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1450045395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('quarter', 4, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1423866195323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('month', 2, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1419805395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('day',15, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418516595323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('hour', 2, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418510055323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('minute', 11, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509415323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('second', 20, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395553,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('millisecond', 230 , m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('microsecond', 145, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('nanosecond', 875, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('nanosecond', 145000, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395553LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('microsecond', 230000, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509396553LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('millisecond', 1230, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(931701774885533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('millisecond', 1011 , m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701774874678LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('microsecond', 1000145, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(
        931701774874533LL,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('nanosecond', 1000000875, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023345932435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('millisecond', 1325 , m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023345607960125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('microsecond', 1000525, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(
        1146023345607436000LL,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('nanosecond', 1000000875, m_9) FROM test limit 1;", dt)));
    EXPECT_ANY_THROW(
        run_simple_agg("SELECT DATEADD(NULL, NULL, m_9) FROM test LIMIT 1;", dt));
    EXPECT_ANY_THROW(run_simple_agg(
        "SELECT DATEADD('microsecond', NULL, m_9) FROM test LIMIT 1;", dt));
  }
}

TEST_F(Select, TimestampPrecision_DateDiff) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(1146023344607435125LL - 931701773874533000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m_6, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773874533000LL - 1146023344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m_9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344607435125LL - 1418509395323000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m_3, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323000000LL - 1146023344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m_9, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344607435125LL - 1418509395000000000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395000000000LL - 1146023344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m_9, m) FROM test limit 1;", dt)));
    ASSERT_EQ((1146023344607435125LL - 931701773874533000LL) / 1000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m_6, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533000LL - 1146023344607435125LL) / 1000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m_9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1146023344607435125LL - 1418509395323000000LL) / 1000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m_3, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323000000LL - 1146023344607435125LL) / 1000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m_9, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((1146023344607435125LL - 1418509395000000000LL) / 1000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000000000LL - 1146023344607435125LL) / 1000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m_9, m) FROM test limit 1;", dt)));
    ASSERT_EQ((1146023344607435125LL - 931701773874533000LL) / (1000LL * 1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m_6, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533000LL - 1146023344607435125LL) / (1000LL * 1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m_9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1146023344607435125LL - 1418509395323000000LL) / (1000LL * 1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m_3, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323000000LL - 1146023344607435125LL) / (1000LL * 1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m_9, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((1146023344607435125LL - 1418509395000000000LL) / (1000LL * 1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000000000LL - 1146023344607435125LL) / (1000LL * 1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m_9, m) FROM test limit 1;", dt)));
    ASSERT_EQ((1146023344607435125LL - 931701773874533LL * 1000) / 1000000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m_6, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL * 1000 - 1146023344607435125LL) / 1000000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m_9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1146023344607435125LL - 1418509395323LL * 1000000) / 1000000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m_3, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323LL * 1000000 - 1146023344607435125LL) / 1000000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m_9, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((1146023344607435125LL - 1418509395LL * 1000000000) / 1000000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395LL * 1000000000 - 1146023344607435125LL) / 1000000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m_9, m) FROM test limit 1;", dt)));
    ASSERT_EQ((3572026LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m_6, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((-3572026LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m_9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(-4541434LL,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m_3, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((4541434LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m_9, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((-4541434LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((4541434LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m_9, m) FROM test limit 1;", dt)));
    ASSERT_EQ((59533LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m_6, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((-59533LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m_9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((-75690LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m_3, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((75690LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m_9, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((-75690LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((75690LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m_9, m) FROM test limit 1;", dt)));
    ASSERT_EQ((2480),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('day', m_6, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((-2480),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('day', m_9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(-3153,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('day', m_3, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((3153),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('day', m_9, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((-3153),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('day', m, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((3153),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('day', m_9, m) FROM test limit 1;", dt)));
    ASSERT_EQ((81),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m_6, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((-81),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m_9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((-103),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m_3, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((103),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m_9, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((-103),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((103),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m_9, m) FROM test limit 1;", dt)));
    ASSERT_EQ((6),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m_6, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((-6),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m_9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(-8,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m_3, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((8),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m_9, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((-8),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ((8),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m_9, m) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL - 1418509395323000LL) * 1000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m_3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323000LL - 931701773874533LL) * 1000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m_6, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL - 1418509395000000LL) * 1000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000000LL - 931701773874533LL) * 1000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m_6, m) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL - 1418509395323000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m_3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323000LL - 931701773874533LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m_6, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL - 1418509395000000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000000LL - 931701773874533LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m_6, m) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL - 1418509395323000LL) / (1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m_3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323000LL - 931701773874533LL) / (1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m_6, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL - 1418509395000000LL) / (1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000000LL - 931701773874533LL) / (1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m_6, m) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL - 1418509395323LL * 1000) / 1000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m_3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323LL * 1000 - 931701773874533LL) / 1000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m_6, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL - 1418509395LL * 1000000) / 1000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395LL * 1000000 - 931701773874533LL) / 1000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m_6, m) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL / 1000000 - 1418509395323 / 1000LL) / (60),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m_3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323LL / 1000 - 931701773874533LL / 1000000) / (60LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m_6, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL / 1000000 - 1418509395LL) / (60),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395LL - 931701773874533LL / 1000000) / (60),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m_6, m) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL / 1000000 - 1418509395323LL / 1000) / (60LL * 60LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m_3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323LL / 1000 - 931701773874533LL / 1000000) / (60LL * 60LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m_6, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL / 1000000 - 1418509395LL) / (60LL * 60LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395LL - 931701773874533LL / 1000000) / (60LL * 60LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m_6, m) FROM test limit 1;", dt)));
    ASSERT_EQ(
        (931701773874533LL / 1000000 - 1418509395323LL / 1000) / (60LL * 60LL * 24LL),
        v<int64_t>(
            run_simple_agg("SELECT DATEDIFF('day', m_3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(
        (1418509395323LL / 1000 - 931701773874533LL / 1000000) / (60LL * 60LL * 24LL),
        v<int64_t>(
            run_simple_agg("SELECT DATEDIFF('day', m_6, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((931701773874533LL / 1000000 - 1418509395LL) / (60LL * 60LL * 24LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('day', m, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395LL - 931701773874533LL / 1000000) / (60LL * 60LL * 24LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('day', m_6, m) FROM test limit 1;", dt)));
    ASSERT_EQ(185,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m_6, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(-185,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m_3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(185,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m_6, m) FROM test limit 1;", dt)));
    ASSERT_EQ(-185,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m_6, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(-15,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m_3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m_6, m) FROM test limit 1;", dt)));
    ASSERT_EQ(-15,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000LL - 1418509395323LL) * 1000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m_3, m) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323LL - 1418509395000) * 1000000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('nanosecond', m, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000LL - 1418509395323LL) * 1000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m_3, m) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323LL - 1418509395000LL) * 1000,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('microsecond', m, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000LL - 1418509395323LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m_3, m) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323LL - 1418509395000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('millisecond', m, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000LL - 1418509395323LL) / (1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m_3, m) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323LL - 1418509395000LL) / (1000LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('second', m, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000LL - 1418509395323LL) / (1000LL * 60LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m_3, m) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323LL - 1418509395000LL) / (1000LL * 60LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('minute', m, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395000LL - 1418509395323LL) / (1000LL * 1000LL * 60LL * 60LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m_3, m) FROM test limit 1;", dt)));
    ASSERT_EQ((1418509395323LL - 1418509395000LL) / (1000LL * 1000LL * 60LL * 60LL),
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('hour', m, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(
        (1418509395000LL - 1418509395323LL) / (1000LL * 1000LL * 60LL * 60LL * 24LL),
        v<int64_t>(
            run_simple_agg("SELECT DATEDIFF('day', m_3, m) FROM test limit 1;", dt)));
    ASSERT_EQ(
        (1418509395323LL - 1418509395000LL) / (1000LL * 1000LL * 60LL * 60LL * 24LL),
        v<int64_t>(
            run_simple_agg("SELECT DATEDIFF('day', m, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m_3, m) FROM test limit 1;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('month', m, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m_3, m) FROM test limit 1;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEDIFF('year', m, m_3) FROM test limit 1;", dt)));
    EXPECT_ANY_THROW(
        run_simple_agg("SELECT DATEDIFF(NULL, m, m_3) FROM test limit 1;", dt));
  }
}

TEST_F(Select, TimestampPrecision_TimestampAdd) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(1177559344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(YEAR,1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1153885744607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(QUARTER, 1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1148615344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MONTH, 1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146109744607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(DAY,1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146026944607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(HOUR, 1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023404607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MINUTE, 1, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023403607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(SECOND, 59, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1026396173874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(YEAR,3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(955461773874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(QUARTER, 3, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(947599373874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MONTH, 6, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(932824973874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(DAY,13, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931734173874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(HOUR, 9, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931704053874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MINUTE, 38, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701783874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(SECOND, 10, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(1734128595323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(YEAR,10, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1450045395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(QUARTER, 4, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1423866195323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MONTH, 2, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1419805395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(DAY,15, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418516595323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(HOUR, 2, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418510055323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MINUTE, 11, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509415323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(SECOND, 20, m_3) FROM test limit 1;", dt)));
  }
}

TEST_F(Select, TimestampPrecision_Interval) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(1177559344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 + INTERVAL '1' year) from test limit 1;", dt)));
    ASSERT_EQ(1148615344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 + INTERVAL '1' month) from test limit 1;", dt)));
    ASSERT_EQ(1146109744607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 + INTERVAL '1' day) from test limit 1;", dt)));
    ASSERT_EQ(1146026944607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 + INTERVAL '1' hour) from test limit 1;", dt)));
    ASSERT_EQ(1146023404607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 + INTERVAL '1' minute) from test limit 1;", dt)));
    ASSERT_EQ(1146023345607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 + INTERVAL '1' second) from test limit 1;", dt)));
    ASSERT_EQ(1114487344607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 - INTERVAL '1' year) from test limit 1;", dt)));
    ASSERT_EQ(1143344944607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 - INTERVAL '1' month) from test limit 1;", dt)));
    ASSERT_EQ(1145936944607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 - INTERVAL '1' day) from test limit 1;", dt)));
    ASSERT_EQ(1146019744607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 - INTERVAL '1' hour) from test limit 1;", dt)));
    ASSERT_EQ(1146023284607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 - INTERVAL '1' minute) from test limit 1;", dt)));
    ASSERT_EQ(1146023343607435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_9 - INTERVAL '1' second) from test limit 1;", dt)));
    ASSERT_EQ(963324173874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 + INTERVAL '1' year) from test limit 1;", dt)));
    ASSERT_EQ(934380173874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 + INTERVAL '1' month) from test limit 1;", dt)));
    ASSERT_EQ(931788173874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 + INTERVAL '1' day) from test limit 1;", dt)));
    ASSERT_EQ(931705373874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 + INTERVAL '1' hour) from test limit 1;", dt)));
    ASSERT_EQ(931701833874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 + INTERVAL '1' minute) from test limit 1;", dt)));
    ASSERT_EQ(931701774874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 + INTERVAL '1' second) from test limit 1;", dt)));
    ASSERT_EQ(900165773874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 - INTERVAL '1' year) from test limit 1;", dt)));
    ASSERT_EQ(929109773874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 - INTERVAL '1' month) from test limit 1;", dt)));
    ASSERT_EQ(931615373874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 - INTERVAL '1' day) from test limit 1;", dt)));
    ASSERT_EQ(931698173874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 - INTERVAL '1' hour) from test limit 1;", dt)));
    ASSERT_EQ(931701713874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 - INTERVAL '1' minute) from test limit 1;", dt)));
    ASSERT_EQ(931701772874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_6 - INTERVAL '1' second) from test limit 1;", dt)));
    ASSERT_EQ(1450045395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 + INTERVAL '1' year) from test limit 1;", dt)));
    ASSERT_EQ(1421187795323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 + INTERVAL '1' month) from test limit 1;", dt)));
    ASSERT_EQ(1418595795323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 + INTERVAL '1' day) from test limit 1;", dt)));
    ASSERT_EQ(1418512995323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 + INTERVAL '1' hour) from test limit 1;", dt)));
    ASSERT_EQ(1418509455323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 + INTERVAL '1' minute) from test limit 1;", dt)));
    ASSERT_EQ(1418509396323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 + INTERVAL '1' second) from test limit 1;", dt)));
    ASSERT_EQ(1386973395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 - INTERVAL '1' year) from test limit 1;", dt)));
    ASSERT_EQ(1415917395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 - INTERVAL '1' month) from test limit 1;", dt)));
    ASSERT_EQ(1418422995323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 - INTERVAL '1' day) from test limit 1;", dt)));
    ASSERT_EQ(1418505795323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 - INTERVAL '1' hour) from test limit 1;", dt)));
    ASSERT_EQ(1418509335323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 - INTERVAL '1' minute) from test limit 1;", dt)));
    ASSERT_EQ(1418509394323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT (m_3 - INTERVAL '1' second) from test limit 1;", dt)));
  }
}

TEST_F(Select, TimestampPrecision_HighPrecisionCastsWithIntervals) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(
        1146023345LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m_9 as timestamp(0)) + INTERVAL '1' second) from test limit 1;",
            dt)));
    ASSERT_EQ(
        1146023343607LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m_9 as timestamp(3)) - INTERVAL '1' second) from test limit 1;",
            dt)));
    ASSERT_EQ(
        1146023404607435,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m_9 as timestamp(6)) + INTERVAL '1' minute) from test limit 1;",
            dt)));
    ASSERT_EQ(
        931705373LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m_6 as timestamp(0)) + INTERVAL '1' hour) from test limit 1;",
            dt)));
    ASSERT_EQ(
        931698173874LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m_6 as timestamp(3)) - INTERVAL '1' hour) from test limit 1;",
            dt)));
    ASSERT_EQ(
        931701833874533000LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m_6 as timestamp(9)) + INTERVAL '1' minute) from test limit 1;",
            dt)));
    ASSERT_EQ(
        1450045395LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m_3 as timestamp(0)) + INTERVAL '1' year) from test limit 1;",
            dt)));
    ASSERT_EQ(
        1386973395323000LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m_3 as timestamp(6)) - INTERVAL '1' year) from test limit 1;",
            dt)));
    ASSERT_EQ(
        1450045395323000000LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m_3 as timestamp(9)) + INTERVAL '1' year) from test limit 1;",
            dt)));
    ASSERT_EQ(
        1418509335000LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m as timestamp(3)) - INTERVAL '1' minute) from test limit 1;",
            dt)));
    ASSERT_EQ(
        1418505795000000LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m as timestamp(6)) - INTERVAL '1' hour) from test limit 1;",
            dt)));
    ASSERT_EQ(
        1418509455000000000LL,
        v<int64_t>(run_simple_agg(
            "SELECT (cast(m as timestamp(9)) + INTERVAL '1' minute) from test limit 1;",
            dt)));
  }
}

TEST_F(Select, TimestampPrecision_CastFromInt) {
  char const* const queries[] = {
      // CAST(TINYINT column AS TIMESTAMP(*))
      "SELECT CAST('1970-01-01 00:01:32' AS TIMESTAMP)"
      " = CAST(w+100 AS TIMESTAMP) FROM test WHERE w+100=92 GROUP BY w;",
      "SELECT CAST('1970-01-01 00:00:00.092' AS TIMESTAMP(3))"
      " = CAST(w+100 AS TIMESTAMP(3)) FROM test WHERE w+100=92 GROUP BY w;",
      "SELECT CAST('1970-01-01 00:00:00.000092' AS TIMESTAMP(6))"
      " = CAST(w+100 AS TIMESTAMP(6)) FROM test WHERE w+100=92 GROUP BY w;",
      "SELECT CAST('1970-01-01 00:00:00.000000092' AS TIMESTAMP(9))"
      " = CAST(w+100 AS TIMESTAMP(9)) FROM test WHERE w+100=92 GROUP BY w;",
      // CAST(SMALLINT column AS TIMESTAMP(*))
      "SELECT CAST('1970-01-01 00:01:41' AS TIMESTAMP)"
      " = CAST(z AS TIMESTAMP) FROM test WHERE z=101 GROUP BY z;",
      "SELECT CAST('1970-01-01 00:00:00.101' AS TIMESTAMP(3))"
      " = CAST(z AS TIMESTAMP(3)) FROM test WHERE z=101 GROUP BY z;",
      "SELECT CAST('1970-01-01 00:00:00.000101' AS TIMESTAMP(6))"
      " = CAST(z AS TIMESTAMP(6)) FROM test WHERE z=101 GROUP BY z;",
      "SELECT CAST('1970-01-01 00:00:00.000000101' AS TIMESTAMP(9))"
      " = CAST(z AS TIMESTAMP(9)) FROM test WHERE z=101 GROUP BY z;",
      // CAST(INTEGER column AS TIMESTAMP(*))
      "SELECT CAST('1970-01-01 00:00:07' AS TIMESTAMP)"
      " = CAST(x AS TIMESTAMP) FROM test WHERE x=7 GROUP BY x;",
      "SELECT CAST('1970-01-01 00:00:00.007' AS TIMESTAMP(3))"
      " = CAST(x AS TIMESTAMP(3)) FROM test WHERE x=7 GROUP BY x;",
      "SELECT CAST('1970-01-01 00:00:00.000007' AS TIMESTAMP(6))"
      " = CAST(x AS TIMESTAMP(6)) FROM test WHERE x=7 GROUP BY x;",
      "SELECT CAST('1970-01-01 00:00:00.000000007' AS TIMESTAMP(9))"
      " = CAST(x AS TIMESTAMP(9)) FROM test WHERE x=7 GROUP BY x;",
      // CAST(BIGINT column AS TIMESTAMP(*))
      "SELECT CAST('1970-01-01 00:16:41' AS TIMESTAMP)"
      " = CAST(t AS TIMESTAMP) FROM test WHERE t=1001 GROUP BY t;",
      "SELECT CAST('1970-01-01 00:00:01.001' AS TIMESTAMP(3))"
      " = CAST(t AS TIMESTAMP(3)) FROM test WHERE t=1001 GROUP BY t;",
      "SELECT CAST('1970-01-01 00:00:00.001001' AS TIMESTAMP(6))"
      " = CAST(t AS TIMESTAMP(6)) FROM test WHERE t=1001 GROUP BY t;",
      "SELECT CAST('1970-01-01 00:00:00.000001001' AS TIMESTAMP(9))"
      " = CAST(t AS TIMESTAMP(9)) FROM test WHERE t=1001 GROUP BY t;"};
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    for (char const* const query : queries) {
      ASSERT_TRUE(v<int64_t>(run_simple_agg(query, dt))) << query;
    }
  }
}

TEST_F(Select, TimestampPrecision_CastsImplicitAndExplicit) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(
        1418509395000000000,
        v<int64_t>(run_simple_agg(
            "SELECT PG_DATE_TRUNC('second', m_9) FROM test where cast(m_9 as "
            "timestamp(0)) between "
            "TIMESTAMP(0) '2014-12-13 22:23:14' and TIMESTAMP(0) '2014-12-13 22:23:15'",
            dt)));
    ASSERT_EQ(1418509395607000000,
              v<int64_t>(run_simple_agg(
                  "SELECT PG_DATE_TRUNC('millisecond', m_9) FROM test where cast(m_9 as "
                  "timestamp(3)) between "
                  "TIMESTAMP(3) '2014-12-13 22:23:14.323' and TIMESTAMP(3) '2014-12-13 "
                  "22:23:15.999'",
                  dt)));
    ASSERT_EQ(
        1418509395607435000,
        v<int64_t>(run_simple_agg(
            "SELECT PG_DATE_TRUNC('microsecond', m_9) FROM test where cast(m_9 as "
            "timestamp(6)) between "
            "TIMESTAMP(6) '2014-12-13 22:23:15.607434' and TIMESTAMP(6) '2014-12-13 "
            "22:23:15.934566'",
            dt)));
    ASSERT_EQ(1146023344607435125,
              v<int64_t>(run_simple_agg(
                  "SELECT PG_DATE_TRUNC('nanosecond', m_9) FROM test where m_9 "
                  "between "
                  "TIMESTAMP(9) '2006-04-26 03:49:04.607435124' and TIMESTAMP(9) "
                  "'2006-04-26 03:49:04.607435126'",
                  dt)));
    ASSERT_EQ(
        1418509395000,
        v<int64_t>(run_simple_agg(
            "SELECT PG_DATE_TRUNC('second', m_3) FROM test where cast(m_3 as "
            "timestamp(0)) between "
            "TIMESTAMP(0) '2014-12-13 22:23:14' and TIMESTAMP(0) '2014-12-13 22:23:15'",
            dt)));
    ASSERT_EQ(1418509395323,
              v<int64_t>(run_simple_agg(
                  "SELECT PG_DATE_TRUNC('millisecond', m_3) FROM test where m_3 "
                  "between "
                  "TIMESTAMP(3) '2014-12-13 22:23:14.322' and TIMESTAMP(3) '2014-12-13 "
                  "22:23:15.324'",
                  dt)));
    ASSERT_EQ(1418509395323,
              v<int64_t>(run_simple_agg(
                  "SELECT PG_DATE_TRUNC('millisecond', m_3) FROM test where cast(m_3 as "
                  "timestamp(6)) between "
                  "TIMESTAMP(6) '2014-12-13 22:23:14.322999' and TIMESTAMP(6) "
                  "'2014-12-13 22:23:15.324000'",
                  dt)));
    ASSERT_EQ(1418509395323,
              v<int64_t>(run_simple_agg(
                  "SELECT PG_DATE_TRUNC('millisecond', m_3) FROM test where cast(m_3 as "
                  "timestamp(9)) between "
                  "TIMESTAMP(9) '2014-12-13 22:23:14.322999999' and TIMESTAMP(9) "
                  "'2014-12-13 22:23:15.324000000'",
                  dt)));
    ASSERT_EQ(
        1418509395000000,
        v<int64_t>(run_simple_agg(
            "SELECT PG_DATE_TRUNC('second', m_6) FROM test where cast(m_6 as "
            "timestamp(0)) between "
            "TIMESTAMP(0) '2014-12-13 22:23:14' and TIMESTAMP(0) '2014-12-13 22:23:15'",
            dt)));
    ASSERT_EQ(1418509395874533,
              v<int64_t>(run_simple_agg(
                  "SELECT PG_DATE_TRUNC('microsecond', m_6) FROM test where cast(m_6 as "
                  "timestamp(3)) between "
                  "TIMESTAMP(3) '2014-12-13 22:23:14.873' and TIMESTAMP(3) '2014-12-13 "
                  "22:23:15.875'",
                  dt)));
    ASSERT_EQ(1418509395874533,
              v<int64_t>(run_simple_agg(
                  "SELECT PG_DATE_TRUNC('nanosecond', m_6) FROM test where cast(m_6 as "
                  "timestamp(9)) between "
                  "TIMESTAMP(9) '2014-12-13 22:23:14.874532999' and TIMESTAMP(9) "
                  "'2014-12-13 22:23:15.874533001'",
                  dt)));
    ASSERT_EQ(
        1418509395,
        v<int64_t>(run_simple_agg(
            "SELECT PG_DATE_TRUNC('second', m) FROM test where cast(m as "
            "timestamp(3)) between "
            "TIMESTAMP(3) '2014-12-13 22:23:14' and TIMESTAMP(3) '2014-12-13 22:23:15'",
            dt)));
    ASSERT_ANY_THROW(
        run_simple_agg("SELECT PG_DATE_TRUNC(NULL, m) FROM test LIMIT 1;", dt));
  }
}

TEST_F(Select, TimestampPrecision_Dates) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM test where cast(o as timestamp(0)) between "
            "TIMESTAMP(0) '1999-09-08 22:23:14' and TIMESTAMP(0) '1999-09-09 22:23:15'",
            dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(3)) between "
                  "TIMESTAMP(3) '1999-09-08 12:12:31.500' and TIMESTAMP(3) '1999-09-09 "
                  "22:23:15'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(6)) between "
                  "TIMESTAMP(6) '1999-09-08 12:12:31.500' and TIMESTAMP(6) '1999-09-09 "
                  "22:23:15'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(9)) between "
                  "TIMESTAMP(9) '1999-09-08 12:12:31.500' and TIMESTAMP(9) '1999-09-09 "
                  "22:23:15'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(0)) between "
                  "TIMESTAMP(3) '1999-09-08 12:12:31.500' and TIMESTAMP(3) '1999-09-09 "
                  "22:23:15.500'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(3)) between "
                  "TIMESTAMP(0) '1999-09-08 12:12:31' and TIMESTAMP(0) '1999-09-09 "
                  "22:23:15'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(6)) between "
                  "TIMESTAMP(0) '1999-09-08 12:12:31' and TIMESTAMP(0) '1999-09-09 "
                  "22:23:15'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(9)) between "
                  "TIMESTAMP(0) '1999-09-08 12:12:31' and TIMESTAMP(0) '1999-09-09 "
                  "22:23:15'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(6)) between "
                  "TIMESTAMP(0) '1999-09-08 12:12:31' and TIMESTAMP(0) '1999-09-09 "
                  "22:23:15'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(9)) between "
                  "TIMESTAMP(3) '1999-09-08 12:12:31.099' and TIMESTAMP(3) '1999-09-09 "
                  "22:23:15.789'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(3)) = "
                  "TIMESTAMP(3) '1999-09-09 00:00:00.000'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(6)) >= "
                  "TIMESTAMP(6) '1999-09-08 23:23:59.999999'",
                  dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(9)) = "
                  "TIMESTAMP(9) '1999-09-09 00:00:00.000000001'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(
                  run_simple_agg("SELECT count(*) FROM test where cast(o as "
                                 "timestamp(3)) < TIMESTAMP(3) '1999-09-09 12:12:31.500'",
                                 dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(3)) < m_3", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(3)) >= m_3", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(
                  run_simple_agg("SELECT count(*) FROM test where cast(o as "
                                 "timestamp(3)) = TIMESTAMP(3) '1999-09-09 00:00:00.000'",
                                 dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(6)) = "
                  "TIMESTAMP(6) '1999-09-09 00:00:00.000000'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(o as timestamp(9)) = "
                  "TIMESTAMP(9) '1999-09-09 00:00:00.000000000'",
                  dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM test where cast(o as "
                                        "timestamp(3)) = '1999-09-09 00:00:00.000'",
                                        dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM test where cast(o as "
                                        "timestamp(6)) = '1999-09-09 00:00:00.000000'",
                                        dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg("SELECT count(*) FROM test where cast(o as "
                                        "timestamp(9)) = '1999-09-09 00:00:00.000000000'",
                                        dt)));
  }
}

TEST_F(Select, TimestampPrecision_HighPrecision) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(1418509395000,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m as TIMESTAMP(3)) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395000000,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m as TIMESTAMP(6)) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395000000000,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m as TIMESTAMP(9)) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323000,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m_3 as TIMESTAMP(6)) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323000000,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m_3 as TIMESTAMP(9)) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m_3 as TIMESTAMP(0)) FROM test limit 1;", dt)));

    ASSERT_EQ(931701773874533000,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m_6 as TIMESTAMP(9)) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773874,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m_6 as TIMESTAMP(3)) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m_6 as TIMESTAMP(0)) FROM test limit 1;", dt)));

    ASSERT_EQ(1146023344607435,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m_9 as TIMESTAMP(6)) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344607,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m_9 as TIMESTAMP(3)) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344,
              v<int64_t>(run_simple_agg(
                  "SELECT CAST(m_9 as TIMESTAMP(0)) FROM test limit 1;", dt)));

    ASSERT_EQ(20,
              v<int64_t>(run_simple_agg("select count(*) from test where m_3 > m;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("select count(*) from test where m_3 = m;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("select count(*) from test where m_3 < m;", dt)));
    ASSERT_EQ(20,
              v<int64_t>(run_simple_agg("select count(*) from test where m = m_3;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("select count(*) from test where m > m_3;", dt)));
    ASSERT_EQ(20,
              v<int64_t>(run_simple_agg("select count(*) from test where m < m_3;", dt)));

    ASSERT_EQ(
        5, v<int64_t>(run_simple_agg("select count(*) from test where m_6 > m_3;", dt)));
    ASSERT_EQ(
        15, v<int64_t>(run_simple_agg("select count(*) from test where m_6 < m_3;", dt)));
    ASSERT_EQ(
        0, v<int64_t>(run_simple_agg("select count(*) from test where m_6 = m_3;", dt)));
    ASSERT_EQ(
        15, v<int64_t>(run_simple_agg("select count(*) from test where m_3 > m_6;", dt)));
    ASSERT_EQ(
        5, v<int64_t>(run_simple_agg("select count(*) from test where m_3 < m_6;", dt)));
    ASSERT_EQ(
        0, v<int64_t>(run_simple_agg("select count(*) from test where m_3 = m_6;", dt)));

    ASSERT_EQ(
        5, v<int64_t>(run_simple_agg("select count(*) from test where m_6 > m_9;", dt)));
    ASSERT_EQ(
        15, v<int64_t>(run_simple_agg("select count(*) from test where m_6 < m_9;", dt)));
    ASSERT_EQ(
        0, v<int64_t>(run_simple_agg("select count(*) from test where m_6 = m_9;", dt)));
    ASSERT_EQ(
        15, v<int64_t>(run_simple_agg("select count(*) from test where m_9 > m_6;", dt)));
    ASSERT_EQ(
        5, v<int64_t>(run_simple_agg("select count(*) from test where m_9 < m_6;", dt)));
    ASSERT_EQ(
        0, v<int64_t>(run_simple_agg("select count(*) from test where m_9 = m_6;", dt)));

    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("select count(*) from test where m_6 > m;", dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("select count(*) from test where m_6 < m;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("select count(*) from test where m_6 = m;", dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("select count(*) from test where m > m_6;", dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("select count(*) from test where m < m_6;", dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("select count(*) from test where m = m_6;", dt)));

    ASSERT_EQ(
        10, v<int64_t>(run_simple_agg("select count(*) from test where m_9 > m_3;", dt)));
    ASSERT_EQ(
        10, v<int64_t>(run_simple_agg("select count(*) from test where m_9 < m_3;", dt)));
    ASSERT_EQ(
        0, v<int64_t>(run_simple_agg("select count(*) from test where m_9 = m_3;", dt)));
    ASSERT_EQ(
        10, v<int64_t>(run_simple_agg("select count(*) from test where m_3 > m_9;", dt)));
    ASSERT_EQ(
        10, v<int64_t>(run_simple_agg("select count(*) from test where m_3 < m_9;", dt)));
    ASSERT_EQ(
        0, v<int64_t>(run_simple_agg("select count(*) from test where m_3 = m_9;", dt)));

    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("select count(*) from test where m_9 > m;", dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("select count(*) from test where m_9 < m;", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg("select count(*) from test where m_9 = m;", dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("select count(*) from test where m > m_9;", dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("select count(*) from test where m < m_9;", dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg("select count(*) from test where m = m_9;", dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM test where m_3 <= TIMESTAMP(0) '2014-12-14 22:23:14';",
            dt)));
    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg("select count(*) from test where m_6 = "
                                        "TIMESTAMP(6) '2014-12-14 22:23:15.437321';",
                                        dt)));
    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg("select count(*) from test where m_9 = "
                                        "TIMESTAMP(9) '2014-12-14 22:23:15.934567401';",
                                        dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg(
            "select count(*) from test where m_6 = '2014-12-14 22:23:15.437321';", dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg(
            "select count(*) from test where m_9 = '2014-12-14 22:23:15.934567401';",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "select count(*) from test where cast(m_9 as timestamp(3)) = m_3;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where CAST(m as TIMESTAMP(3)) < m_3", dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where CAST(m as TIMESTAMP(3)) > m_3;", dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where CAST(m_3 as TIMESTAMP(0)) = m", dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM test where m_3 >= TIMESTAMP(0) '2014-12-14 22:23:14';",
            dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM test where cast(m as timestamp(0)) between "
            "TIMESTAMP(0) '2014-12-13 22:23:14' and TIMESTAMP(0) '2014-12-13 22:23:15'",
            dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows + g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(m as timestamp(3)) between "
                  "TIMESTAMP(3) '2014-12-12 22:23:15.320' and TIMESTAMP(3) '2014-12-13 "
                  "22:23:15.323'",
                  dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows + g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM test where cast(m_3 as timestamp(0)) between "
            "TIMESTAMP(0) '2014-12-13 22:23:14' and TIMESTAMP(3) '2014-12-13 22:23:15'",
            dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(m_6 as timestamp(3)) between "
                  "TIMESTAMP(3) '2014-12-13 22:23:15.870' and TIMESTAMP(3) '2014-12-13 "
                  "22:23:15.875'",
                  dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM test where cast(m_6 as timestamp(0)) between "
            "TIMESTAMP(0) '2014-12-13 22:23:14' and TIMESTAMP(3) '2014-12-13 22:23:15'",
            dt)));
    ASSERT_EQ(static_cast<int64_t>(g_num_rows / 2),
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(m_9 as timestamp(3)) between "
                  "TIMESTAMP(3) '2014-12-13 22:23:15.607' and TIMESTAMP(3) '2014-12-13 "
                  "22:23:15.608'",
                  dt)));
    ASSERT_EQ(
        static_cast<int64_t>(g_num_rows / 2),
        v<int64_t>(run_simple_agg(
            "SELECT count(*) FROM test where cast(m_9 as timestamp(0)) between "
            "TIMESTAMP(0) '2014-12-13 22:23:14' and TIMESTAMP(0) '2014-12-13 22:23:15'",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg("SELECT count(*) FROM test where cast(m_9 as "
                                  "timestamp(0)) >= TIMESTAMP(0) '2014-12-13 22:23:14';",
                                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(m_9 as "
                  "timestamp(3)) >= TIMESTAMP(3) '2014-12-13 22:23:14.607';",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(m_9 as "
                  "timestamp(6)) <= TIMESTAMP(6) '2014-12-13 22:23:14.607435';",
                  dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg("SELECT count(*) FROM test where cast(m_9 as "
                                  "timestamp(0)) <= TIMESTAMP(0) '2014-12-13 22:23:14';",
                                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(m_6 as timestamp(3)) >= "
                  "TIMESTAMP(3) '2014-12-13 22:23:14.607';",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(m_6 as timestamp(3)) <= "
                  "TIMESTAMP(3) '2014-12-13 22:23:14.607';",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(m_6 as timestamp(9)) >= "
                  "TIMESTAMP(9) '2014-12-13 22:23:14.874533';",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) FROM test where cast(m_6 as timestamp(9)) < "
                  "TIMESTAMP(9) '2014-12-14 22:23:14.437321';",
                  dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg("SELECT count(*) FROM test where cast(m_6 as "
                                  "timestamp(0)) >= TIMESTAMP(0) '2014-12-14 22:23:14';",
                                  dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg("SELECT count(*) FROM test where cast(m_6 as "
                                  "timestamp(0)) <= TIMESTAMP(0) '2014-12-14 22:23:14';",
                                  dt)));
  }
}

TEST_F(Select, TimestampPrecision_FunctionCompositions) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "select dateadd('minute',1,dateadd('millisecond',1,m)) = TIMESTAMP(0) "
                  "'2014-12-13 22:24:15' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "select dateadd('minute',1,dateadd('millisecond',1,m)) = TIMESTAMP(0) "
                  "'2014-12-13 22:24:15.001' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "select dateadd('minute',1,dateadd('millisecond',1,m)) = TIMESTAMP(0) "
                  "'2014-12-13 22:24:15.000' from test limit 1;",
                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "select dateadd('minute',1,dateadd('millisecond',1,cast (m_3 as "
            "TIMESTAMP(0)))) = TIMESTAMP(0) '2014-12-13 22:24:15.324' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "select dateadd('minute',1,dateadd('millisecond',1,cast(m_3 as "
            "timestamp(0)))) = TIMESTAMP(3) '2014-12-13 22:24:15.456' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "select dateadd('minute',1, dateadd('millisecond',111 , cast(m_6 as "
            "timestamp(0)))) = TIMESTAMP(3) '1999-07-11 14:03:53.985' from test limit 1;",
            dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "select dateadd('year',1,dateadd('millisecond',220,cast(m_9 as "
                  "timestamp(0)))) =  TIMESTAMP(3) '2007-04-26 03:49:04.827' from test "
                  "limit 1;",
                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "select dateadd('minute',1,dateadd('millisecond',1,cast(m as timestamp(3)))) "
            "= TIMESTAMP(3) '2014-12-13 22:24:15.001' from test limit 1;",
            dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "select dateadd('minute',1,dateadd('millisecond',1,m_3)) = "
                  "TIMESTAMP(3) '2014-12-13 22:24:15.324' from test limit 1;",
                  dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "select dateadd('minute',1,dateadd('millisecond',1,cast(m as timestamp(3)))) "
            "= TIMESTAMP(3) '2014-12-13 22:24:15.000' from test limit 1;",
            dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "select dateadd('minute',1, dateadd('millisecond',111 , cast(m_6 as "
            "timestamp(3)))) = TIMESTAMP(3) '1999-07-11 14:03:53.985' from test limit 1;",
            dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "select dateadd('year',1,dateadd('millisecond',220,cast(m_9 as "
                  "timestamp(3)))) =  TIMESTAMP(3) '2007-04-26 03:49:04.827' from test "
                  "limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('minute', 1, TIMESTAMP(3) '2017-05-31 1:11:11.123') = "
                  "TIMESTAMP(3) '2017-05-31 1:12:11.123' from test limit 1;",
                  dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('minute', 1, TIMESTAMP(3) '2017-05-31 1:11:11.123') = "
                  "TIMESTAMP(3) '2017-05-31 1:12:11.122' from test limit 1;",
                  dt)));
  }
}

TEST_F(Select, TimestampPrecision_FunctionsWithHighPrecisionsAndDates) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT PG_DATE_TRUNC('day' , PG_DATE_TRUNC('millisecond', "
                  "TIMESTAMP(3) '2017-05-31 1:11:11.451'))  = TIMESTAMP(9) '2017-05-31 "
                  "00:00:00.000000000' from test limit 1;",
                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT PG_DATE_TRUNC('day' , PG_DATE_TRUNC('millisecond', DATE "
            "'2017-05-31'))  = TIMESTAMP(3) '2017-05-31 00:00:00.000' from test limit 1;",
            dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT cast(PG_DATE_TRUNC('millisecond', TIMESTAMP(6) '2017-05-31 "
                  "1:11:11.451789') as DATE) = DATE '2017-05-31' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT cast(PG_DATE_TRUNC('microsecond', TIMESTAMP(9) '2017-05-31 "
                  "1:11:11.451789341') as DATE) = DATE '2017-05-31' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT cast(PG_DATE_TRUNC('nanosecond', TIMESTAMP(9) '2017-05-31 "
                  "1:11:11.451789345') as DATE) = DATE '2017-05-31' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('millisecond', 10, cast(o as TIMESTAMP(3))) =  "
                  "TIMESTAMP(3) '1999-09-09 00:00:00.010' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT PG_DATE_TRUNC('day', DATEADD('millisecond', 532, cast(DATE "
                  "'2012-05-01' as TIMESTAMP(3)))) =  cast(cast(TIMESTAMP(0) '2012-05-01 "
                  "00:00:00' as DATE) as TIMESTAMP(3)) from test limit 1;",
                  dt)));
    ASSERT_EQ(
        1,
        v<int64_t>(run_simple_agg(
            "SELECT CAST(PG_DATE_TRUNC('day', DATEADD('millisecond', 532, cast(DATE "
            "'2012-05-01' as TIMESTAMP(3)))) AS DATE) =  CAST(cast(cast(TIMESTAMP(0) "
            "'2012-05-01 "
            "00:00:00' as DATE) as TIMESTAMP(3)) AS DATE) from test limit 1;",
            dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('nanosecond', m_9) = "
                  "TIMESTAMP(9) '2006-04-26 03:49:04.607435125';",
                  dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('microsecond', m_9) = "
                  "TIMESTAMP(9) '2006-04-26 03:49:04.607435125';",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('microsecond', m_9) = "
                  "TIMESTAMP(6) '2006-04-26 03:49:04.607435125';",
                  dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('millisecond', m_9) = "
                  "TIMESTAMP(6) '2006-04-26 03:49:04.607435125';",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('millisecond', m_9) = "
                  "TIMESTAMP(3) '2006-04-26 03:49:04.607435125';",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('nanosecond', m_6) = "
                  "TIMESTAMP(9) '1999-07-11 14:02:53.874533123';",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('microsecond', m_6) = "
                  "TIMESTAMP(9) '1999-07-11 14:02:53.874533123';",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('microsecond', m_6) = "
                  "TIMESTAMP(6) '1999-07-11 14:02:53.874533123';",
                  dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('millisecond', m_6) = "
                  "TIMESTAMP(6) '1999-07-11 14:02:53.874533123';",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('millisecond', m_6) = "
                  "TIMESTAMP(3) '1999-07-11 14:02:53.874533123';",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('nanosecond', m_3) = "
                  "TIMESTAMP(9) '2014-12-13 22:23:15.323533123';",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('microsecond', m_3) = "
                  "TIMESTAMP(9) '2014-12-13 22:23:15.323533123';",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('microsecond', m_3) = "
                  "TIMESTAMP(6) '2014-12-13 22:23:15.323533123';",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('millisecond', m_3) = "
                  "TIMESTAMP(6) '2014-12-13 22:23:15.323533123';",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where PG_DATE_TRUNC('millisecond', m_3) = "
                  "TIMESTAMP(3) '2014-12-13 22:23:15.323533123';",
                  dt)));

    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('nanosecond', 101,  m_9) as "
            "DATE) = cast(TIMESTAMP(9) '2006-04-26 03:49:04.607435226' as DATE);",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('microsecond', 101, m_9) as "
            "DATE) = cast(TIMESTAMP(9) '2006-04-26 03:49:04.607536125' as DATE);",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('microsecond', 101, m_9) as "
            "DATE) = cast(TIMESTAMP(6) '2006-04-26 03:49:04.607536125' as DATE);",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('millisecond', 101, m_9) as "
            "DATE) = cast(TIMESTAMP(6) '2006-04-26 03:49:04.708435125' as DATE);",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('millisecond', 101, m_9) as "
            "DATE) = cast(TIMESTAMP(3) '2006-04-26 03:49:04.708435125' as DATE);",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('nanosecond', 101, m_6) as "
            "DATE) = cast(TIMESTAMP(9) '1999-07-11 14:02:53.874533224' as DATE);",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('microsecond', 101, m_6) as "
            "DATE) = cast(TIMESTAMP(9) '1999-07-11 14:02:53.874634123' as DATE);",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('microsecond', 101, m_6) as "
            "DATE) = cast(TIMESTAMP(6) '1999-07-11 14:02:53.874634123' as DATE);",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('millisecond', 101, m_6) as "
            "DATE) = cast(TIMESTAMP(6) '1999-07-11 14:02:53.975533123' as DATE);",
            dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('millisecond', 101, m_6) as "
            "DATE) = cast(TIMESTAMP(3) '1999-07-11 14:02:53.975533123' as DATE);",
            dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('nanosecond', 101, m_3) as "
            "DATE) = cast(TIMESTAMP(9) '2014-12-13 22:23:15.323533224' as DATE);",
            dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('microsecond', 101, m_3) as "
            "DATE) = cast(TIMESTAMP(9) '2014-12-13 22:23:15.323634124' as DATE);",
            dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('microsecond', 101, m_3) as "
            "DATE) = cast(TIMESTAMP(6) '2014-12-13 22:23:15.323634123' as DATE);",
            dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('millisecond', 101, m_3) as "
            "DATE) = cast(TIMESTAMP(6) '2014-12-13 22:23:15.424533123' as DATE);",
            dt)));
    ASSERT_EQ(
        15,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(DATEADD('millisecond', 101, m_3) as "
            "DATE) = cast(TIMESTAMP(3) '2014-12-13 22:23:15.424533123' as DATE);",
            dt)));

    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg(
                  "select count(*) from test where DATEADD('millisecond',10, m_9) =  "
                  "TIMESTAMP(9) '2014-12-13 22:23:15.617435763'",
                  dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where DATEADD('minute', 1, TIMESTAMP(6) "
            "'2017-05-31 1:11:11.12312') = TIMESTAMP(6) '2017-05-31 1:12:11.123120';",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where DATEADD('minute', 1, TIMESTAMP(6) "
            "'2017-05-31 1:11:11.12312') = TIMESTAMP(6) '2017-05-31 1:12:11.123121';",
            dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where DATEADD('minute', 1, TIMESTAMP(9) "
            "'2017-05-31 1:11:11.12312') = TIMESTAMP(9) '2017-05-31 1:12:11.123120000';",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where DATEADD('minute', 1, TIMESTAMP(9) "
            "'2017-05-31 1:11:11.12312') = TIMESTAMP(9) '2017-05-31 1:12:11.123120001';",
            dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where 1=0 OR DATEADD('minute', 1, TIMESTAMP(9) "
            "'2017-05-31 1:11:11.12312') = TIMESTAMP(9) '2017-05-31 1:12:11.123120000';",
            dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where 1=1 AND DATEADD('minute', 1, TIMESTAMP(9) "
            "'2017-05-31 1:11:11.12312') = TIMESTAMP(9) '2017-05-31 1:12:11.123120000';",
            dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where 1=1 AND PG_DATE_TRUNC('day', "
            "DATEADD('millisecond', 532, cast(DATE '2012-05-01' as TIMESTAMP(3)))) =  "
            "cast(cast(TIMESTAMP(0) '2012-05-01 00:00:00' as DATE) as TIMESTAMP(3));",
            dt)));
    ASSERT_EQ(20,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where 1=1 AND CAST(PG_DATE_TRUNC('day', "
                  "DATEADD('millisecond', 532, cast(DATE '2012-05-01' as TIMESTAMP(3)))) "
                  "AS DATE) =  CAST(cast(cast(TIMESTAMP(0) '2012-05-01 00:00:00' as "
                  "DATE) as TIMESTAMP(3)) AS DATE);",
                  dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11,  m_9) as "
            "DATE) = cast(TIMESTAMP(9) '2006-04-26 03:49:04.607435226' as DATE);",
            dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_9) as "
                  "DATE) = cast(TIMESTAMP(9) '2006-04-26 03:49:04.607536125' as DATE);",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_9) as "
                  "DATE) = cast(TIMESTAMP(6) '2006-04-26 03:49:04.607536125' as DATE);",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_9) as "
                  "DATE) = cast(TIMESTAMP(6) '2006-04-26 03:49:04.708435125' as DATE);",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_9) as "
                  "DATE) = cast(TIMESTAMP(3) '2006-04-26 03:49:04.708435125' as DATE);",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_6) as "
                  "DATE) = cast(TIMESTAMP(9) '1999-07-11 14:02:53.874533224' as DATE);",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_6) as "
                  "DATE) = cast(TIMESTAMP(9) '1999-07-11 14:02:53.874634123' as DATE);",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_6) as "
                  "DATE) = cast(TIMESTAMP(6) '1999-07-11 14:02:53.874634123' as DATE);",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_6) as "
                  "DATE) = cast(TIMESTAMP(6) '1999-07-11 14:02:53.975533123' as DATE);",
                  dt)));
    ASSERT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_6) as "
                  "DATE) = cast(TIMESTAMP(3) '1999-07-11 14:02:53.975533123' as DATE);",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_3) as "
                  "DATE) = cast(TIMESTAMP(9) '2014-12-13 22:23:15.323533224' as DATE);",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_3) as "
                  "DATE) = cast(TIMESTAMP(9) '2014-12-13 22:23:15.323634124' as DATE);",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_3) as "
                  "DATE) = cast(TIMESTAMP(6) '2014-12-13 22:23:15.323634123' as DATE);",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_3) as "
                  "DATE) = cast(TIMESTAMP(6) '2014-12-13 22:23:15.424533123' as DATE);",
                  dt)));
    ASSERT_EQ(15,
              v<int64_t>(run_simple_agg(
                  "SELECT count(*) from test where cast(TIMESTAMPADD(second, 11, m_3) as "
                  "DATE) = cast(TIMESTAMP(3) '2014-12-13 22:23:15.424533123' as DATE);",
                  dt)));
    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg(
                  "select count(*) from test where m_9 + INTERVAL '10' SECOND =  "
                  "TIMESTAMP(9) '2014-12-13 22:23:25.607435763'",
                  dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg("select count(*) from test where m_6 - INTERVAL '10' "
                                  "SECOND = TIMESTAMP(6) '1999-07-11 14:02:43.874533';",
                                  dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg("select count(*) from test where m_3 - INTERVAL '30' "
                                  "SECOND = TIMESTAMP(3) '2014-12-14 22:22:45.750';",
                                  dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg("SELECT count(*) from test where TIMESTAMP(6) "
                                  "'2017-05-31 1:11:11.12312' + INTERVAL '1' MINUTE  = "
                                  "TIMESTAMP(6) '2017-05-31 1:12:11.123120';",
                                  dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg("SELECT count(*) from test where TIMESTAMP(9) "
                                  "'2017-05-31 1:11:11.12312' + INTERVAL '1' MINUTE  = "
                                  "TIMESTAMP(9) '2017-05-31 1:12:11.123120000';",
                                  dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg("SELECT count(*) from test where 1=0 OR TIMESTAMP(9) "
                                  "'2017-05-31 1:11:11.12312' + INTERVAL '1' MINUTE  = "
                                  "TIMESTAMP(9) '2017-05-31 1:12:11.123120000';",
                                  dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg("SELECT count(*) from test where 1=1 AND TIMESTAMP(9) "
                                  "'2017-05-31 1:11:11.12312' + INTERVAL '1' MINUTE  = "
                                  "TIMESTAMP(9) '2017-05-31 1:12:11.123120000';",
                                  dt)));

    ASSERT_EQ(1146023344932435125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MILLISECOND, 325 , m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344607960125LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MICROSECOND, 525, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(1146023344607436000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(NANOSECOND, 875, m_9) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773885533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MILLISECOND, 11 , m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773874678LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MICROSECOND, 145, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(931701773874533LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(NANOSECOND, 875, m_6) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395553,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MILLISECOND, 230 , m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(MICROSECOND, 145, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(1418509395323LL,
              v<int64_t>(run_simple_agg(
                  "SELECT TIMESTAMPADD(NANOSECOND, 875, m_3) FROM test limit 1;", dt)));
    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg(
                  "select count(*) from test where TIMESTAMPADD(second,10, m_9) =  "
                  "TIMESTAMP(9) '2014-12-13 22:23:25.607435763'",
                  dt)));
    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg(
                  "select count(*) from test where TIMESTAMPADD(millisecond,10, m_9) =  "
                  "TIMESTAMP(9) '2014-12-13 22:23:15.617435763'",
                  dt)));
    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg(
                  "select count(*) from test where TIMESTAMPADD(microsecond,10, m_9) =  "
                  "TIMESTAMP(9) '2014-12-13 22:23:15.607445763'",
                  dt)));
    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg(
                  "select count(*) from test where TIMESTAMPADD(nanosecond,10, m_9) =  "
                  "TIMESTAMP(9) '2014-12-13 22:23:15.607435773'",
                  dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where TIMESTAMPADD(minute, 1, TIMESTAMP(6) "
            "'2017-05-31 1:11:11.12312') = TIMESTAMP(6) '2017-05-31 1:12:11.123120';",
            dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where TIMESTAMPADD(minute, 1, TIMESTAMP(9) "
            "'2017-05-31 1:11:11.12312') = TIMESTAMP(9) '2017-05-31 1:12:11.123120001';",
            dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where 1=0 OR TIMESTAMPADD(minute, 1,TIMESTAMP(9) "
            "'2017-05-31 1:11:11.12312') = TIMESTAMP(9) '2017-05-31 1:12:11.123120000';",
            dt)));
    ASSERT_EQ(
        20,
        v<int64_t>(run_simple_agg(
            "SELECT count(*) from test where 1=1 AND TIMESTAMPADD(minute, 1,TIMESTAMP(9) "
            "'2017-05-31 1:11:11.12312') = TIMESTAMP(9) '2017-05-31 1:12:11.123120000';",
            dt)));

    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('minute', 1, TIMESTAMP(6) '2017-05-31 1:11:11.12312')"
                  " = TIMESTAMP(6) '2017-05-31 1:12:11.123120' from test limit 1;",
                  dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('minute', 1, TIMESTAMP(6) '2017-05-31 1:11:11.4511')"
                  " = TIMESTAMP(6) '2017-05-31 1:12:11.451110' from test limit 1;",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT DATEADD('minute', 1, TIMESTAMP(9) '2017-05-31 1:11:11.12312')"
                  " = TIMESTAMP(9) '2017-05-31 1:12:11.123120000' from test limit 1;",
                  dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg(
            "SELECT DATEADD('minute', 1, TIMESTAMP(9) '2017-05-31 1:11:11.45113245') = "
            "TIMESTAMP(9) '2017-05-31 1:12:11.451132451' from test limit 1;",
            dt)));
    ASSERT_EQ(931788173874533,
              v<int64_t>(run_simple_agg("select cast('1999-07-12 14:02:53.874533' as "
                                        "TIMESTAMP(6)) from test limit 1;",
                                        dt)));
    ASSERT_EQ(931788173874533145,
              v<int64_t>(run_simple_agg("select cast('1999-07-12 14:02:53.874533145' as "
                                        "TIMESTAMP(9)) from test limit 1;",
                                        dt)));
  }
}

TEST_F(Select, TimestampPrecision_JoinOnDifferentPrecisions) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg("select count(*) from test where m_9 between "
                                  "TIMESTAMP(9) '2014-12-13 22:23:15.607435763' AND m_6;",
                                  dt)));
    ASSERT_EQ(
        10,
        v<int64_t>(run_simple_agg("select count(*) from test where m_9 between m_6 and "
                                  "TIMESTAMP(9) '2014-12-13 22:23:15.607435763';",
                                  dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg("select count(*) from test where m_9 between "
                                  "TIMESTAMP(9) '2014-12-13 22:23:15.607435763' AND m_3;",
                                  dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg("select count(*) from test where m_9 between m_3 and "
                                  "TIMESTAMP(9) '2014-12-13 22:23:15.607435763';",
                                  dt)));
    ASSERT_EQ(
        0,
        v<int64_t>(run_simple_agg("select count(*) from test where m_9 between "
                                  "TIMESTAMP(9) '2014-12-13 22:23:15.607435763' AND m;",
                                  dt)));
    ASSERT_EQ(
        5,
        v<int64_t>(run_simple_agg("select count(*) from test where m_9 between m and "
                                  "TIMESTAMP(9) '2014-12-13 22:23:15.607435763';",
                                  dt)));
    ASSERT_EQ(5,
              v<int64_t>(run_simple_agg(
                  "select count(*) from test where m_6 between DATEADD('microsecond', "
                  "551000, m_3) and TIMESTAMP(6) '2014-12-13 22:23:15.874533';",
                  dt)));
    ASSERT_EQ(0,
              v<int64_t>(run_simple_agg(
                  "select count(*) from test where m_6 between DATEADD('microsecond', "
                  "551000, m_3) and TIMESTAMP(6) '2014-12-13 22:23:15.874532';",
                  dt)));
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "select pg_extract('millisecond', DATEADD('microsecond', 551000, m_3)) "
                  "= pg_extract('millisecond', TIMESTAMP(6) '2014-12-13 "
                  "22:23:15.874533') from test limit 1;",
                  dt)));
  }
}

TEST_F(Select, TimestampPrecision_EmptyFilters) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      auto rows = run_multiple_agg(
          "SELECT PG_DATE_TRUNC('day', m), COUNT(*) FROM test WHERE m >= "
          "CAST('2014-12-14 22:23:15.000' AS TIMESTAMP(3)) AND m < CAST('2014-12-14 "
          "22:23:15.001' AS TIMESTAMP(3)) AND (CAST(m AS TIMESTAMP(3)) BETWEEN "
          "CAST('2014-12-14 22:23:15.000' AS TIMESTAMP(3)) AND CAST('2014-12-14 "
          "22:23:15.000' AS TIMESTAMP(3))) GROUP BY 1 ORDER BY 1;",
          dt);
      ASSERT_EQ(rows->rowCount(), size_t(1));
      auto count_row = rows->getNextRow(false, false);
      ASSERT_EQ(count_row.size(), size_t(2));
      ASSERT_EQ(5, v<int64_t>(count_row[1]));
    }
  }
}

TEST_F(Select, TimestampPrecisionFormat) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    createTable("ts_format",
                {{"ts_3", SQLTypeInfo(kTIMESTAMP, 3, 0)},
                 {"ts_6", SQLTypeInfo(kTIMESTAMP, 6, 0)},
                 {"ts_9", SQLTypeInfo(kTIMESTAMP, 9, 0)}});
    insertCsvValues("ts_format",
                    "2012-05-22 01:02:03,2012-05-22 01:02:03,2012-05-22 01:02:03");
    insertCsvValues("ts_format",
                    "2012-05-22 01:02:03,2012-05-22 01:02:03,2012-05-22 01:02:03");
    insertCsvValues("ts_format",
                    "2012-05-22 01:02:03.0,2012-05-22 01:02:03.0,2012-05-22 01:02:03.0");
    insertCsvValues("ts_format",
                    "2012-05-22 01:02:03.1,2012-05-22 01:02:03.1,2012-05-22 01:02:03.1");
    insertCsvValues(
        "ts_format",
        "2012-05-22 01:02:03.10,2012-05-22 01:02:03.10,2012-05-22 01:02:03.10");
    insertCsvValues(
        "ts_format",
        "2012-05-22 01:02:03.03,2012-05-22 01:02:03.03,2012-05-22 01:02:03.03");
    insertCsvValues("ts_format",
                    "2012-05-22 01:02:03.003,2012-05-22 "
                    "01:02:03.000003,2012-05-22 01:02:03.000000003");

    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg("SELECT count(ts_3) FROM ts_format where "
                                        "extract(epoch from ts_3) = 1337648523 "
                                        "AND extract('millisecond' from ts_3) = 3000;",
                                        dt)));
    ASSERT_EQ(2LL,
              v<int64_t>(run_simple_agg("SELECT count(ts_3) FROM ts_format where "
                                        "extract(epoch from ts_3) = 1337648523 "
                                        "AND extract('millisecond' from ts_3) = 3100;",
                                        dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg("SELECT count(ts_3) FROM ts_format where "
                                        "extract(epoch from ts_3) = 1337648523 "
                                        "AND extract('millisecond' from ts_3) = 3030;",
                                        dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg("SELECT count(ts_3) FROM ts_format where "
                                        "extract(epoch from ts_3) = 1337648523 "
                                        "AND extract('millisecond' from ts_3) = 3003;",
                                        dt)));

    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT count(ts_6) FROM ts_format where extract(epoch from ts_6) = "
                  "1337648523 AND extract('microsecond' from ts_6) = 3000000;",
                  dt)));
    ASSERT_EQ(2LL,
              v<int64_t>(run_simple_agg(
                  "SELECT count(ts_6) FROM ts_format where extract(epoch from ts_6) = "
                  "1337648523 AND extract('microsecond' from ts_6) = 3100000;",
                  dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT count(ts_6) FROM ts_format where extract(epoch from ts_6) = "
                  "1337648523 AND extract('microsecond' from ts_6) = 3030000;",
                  dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT count(ts_6) FROM ts_format where extract(epoch from ts_6) = "
                  "1337648523 AND extract('microsecond' from ts_6) = 3000003;",
                  dt)));

    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT count(ts_9) FROM ts_format where extract(epoch from ts_9) = "
                  "1337648523 AND extract('nanosecond' from ts_9) = 3000000000;",
                  dt)));
    ASSERT_EQ(2LL,
              v<int64_t>(run_simple_agg(
                  "SELECT count(ts_9) FROM ts_format where extract(epoch from ts_9) = "
                  "1337648523 AND extract('nanosecond' from ts_9) = 3100000000;",
                  dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT count(ts_9) FROM ts_format where extract(epoch from ts_9) = "
                  "1337648523 AND extract('nanosecond' from ts_9) = 3030000000;",
                  dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT count(ts_9) FROM ts_format where extract(epoch from ts_9) = "
                  "1337648523 AND extract('nanosecond' from ts_9) = 3000000003;",
                  dt)));

    dropTable("ts_format");
  }
}

TEST_F(Select, TimestampPrecisionOverflowUnderflow) {
  for (const auto& dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(1,
              v<int64_t>(
                  run_simple_agg("select count(*) from ts_overflow_underflow where (a >= "
                                 "TIMESTAMP(0) '2272-10-25 19:21:56' and a "
                                 "<= TIMESTAMP(0) '2274-10-25 19:21:56');",
                                 dt)));
    ASSERT_EQ(1,
              v<int64_t>(
                  run_simple_agg("select count(*) from ts_overflow_underflow where (a >= "
                                 "TIMESTAMP(3) '2272-10-25 19:21:56.000000000' and a "
                                 "<= TIMESTAMP(3) '2274-10-25 19:21:56.000000000');",
                                 dt)));
    ASSERT_EQ(1,
              v<int64_t>(
                  run_simple_agg("select count(*) from ts_overflow_underflow where (a >= "
                                 "TIMESTAMP(6) '2272-10-25 19:21:56.000000000' and a "
                                 "<= TIMESTAMP(6) '2274-10-25 19:21:56.000000000');",
                                 dt)));
    ASSERT_EQ(1,
              v<int64_t>(
                  run_simple_agg("select count(*) from ts_overflow_underflow where (b >= "
                                 "TIMESTAMP(3) '2272-10-25 19:21:56.000000000' and b "
                                 "<= TIMESTAMP(3) '2274-10-25 19:21:56.000000000');",
                                 dt)));
    ASSERT_EQ(2,
              v<int64_t>(
                  run_simple_agg("select count(*) from ts_overflow_underflow where (b >= "
                                 "TIMESTAMP(6) '2262-10-25 19:21:56.000000000' and b "
                                 "<= TIMESTAMP(6) '2274-10-25 19:21:56.000000000');",
                                 dt)));
    ASSERT_THROW(run_simple_agg("select count(*) from ts_overflow_underflow where (b >= "
                                "TIMESTAMP(9) '2272-10-25 19:21:56.000000000' and b "
                                "<= TIMESTAMP(9) '2274-10-25 19:21:56.000000000');",
                                dt),
                 std::runtime_error);
    ASSERT_THROW(run_simple_agg("select count(*) from ts_overflow_underflow where (a >= "
                                "TIMESTAMP(9) '2272-10-25 19:21:56.000000000' "
                                "and a <= TIMESTAMP(9) '2274-10-25 19:21:56.000000000');",
                                dt),
                 std::runtime_error);
    ASSERT_THROW(run_simple_agg("select count(*) from ts_overflow_underflow where (a >= "
                                "TIMESTAMP(9) '2252-10-25 19:21:56.000000000' "
                                "and a <= TIMESTAMP(9) '2261-10-25 19:21:56.000000000');",
                                dt),
                 std::runtime_error);
    ASSERT_THROW(run_simple_agg("select count(*) from ts_overflow_underflow where (b >= "
                                "TIMESTAMP(9) '2252-10-25 19:21:56.000000000' "
                                "and b <= TIMESTAMP(9) '2261-10-25 19:21:56.000000000');",
                                dt),
                 std::runtime_error);
  }
}

TEST_F(Select, CurrentUser) {
  for (const auto& dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(1,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test_current_user WHERE u = CURRENT_USER;", dt)));
    ASSERT_EQ(
        2,
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test_current_user WHERE u <> CURRENT_USER;", dt)));
    ASSERT_EQ("SESSIONLESS_USER",
              boost::get<std::string>(
                  v<NullableString>(run_simple_agg("SELECT CURRENT_USER;", dt))));
  }
}
namespace {

void validate_timestamp_agg(const ResultSet& row,
                            const int64_t expected_ts,
                            const double expected_mean,
                            const int64_t expected_count) {
  const auto crt_row = row.getNextRow(true, true);
  if (!expected_count) {
    ASSERT_EQ(size_t(0), crt_row.size());
    return;
  }
  ASSERT_EQ(size_t(3), crt_row.size());
  const auto actual_ts = v<int64_t>(crt_row[0]);
  ASSERT_EQ(actual_ts, expected_ts);
  const auto actual_mean = v<double>(crt_row[1]);
  ASSERT_EQ(actual_mean, expected_mean);
  const auto actual_count = v<int64_t>(crt_row[2]);
  ASSERT_EQ(actual_count, expected_count);
  const auto nrow = row.getNextRow(true, true);
  ASSERT_TRUE(nrow.empty());
}

}  // namespace

TEST_F(Select, TimestampCastAggregates) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    createTable("timestamp_agg",
                {{"val", SQLTypeInfo(kINT)},
                 {"dt", SQLTypeInfo(kDATE, kENCODING_DATE_IN_DAYS, 32, kNULLT)},
                 {"ts", SQLTypeInfo(kTIMESTAMP, 0, 0)},
                 {"ts3", SQLTypeInfo(kTIMESTAMP, 3, 0)},
                 {"ts6", SQLTypeInfo(kTIMESTAMP, 6, 0)},
                 {"ts9", SQLTypeInfo(kTIMESTAMP, 9, 0)}});
    insertCsvValues(
        "timestamp_agg",
        "100,2014-12-13,2014-12-13 22:23:15,2014-12-13 22:23:15.123,2014-12-13 "
        "22:23:15.123456,2014-12-13 22:23:15.123456789");
    insertCsvValues(
        "timestamp_agg",
        "150,2014-12-13,2014-12-13 22:23:15,2014-12-13 22:23:15.123,2014-12-13 "
        "22:23:15.123456,2014-12-13 22:23:15.123456789");
    insertCsvValues(
        "timestamp_agg",
        "200,2014-12-14,2014-12-14 22:23:14,2014-12-13 22:23:15.120,2014-12-13 "
        "22:23:15.123450,2014-12-13 22:23:15.123456780");
    insertCsvValues(
        "timestamp_agg",
        "600,2014-12-14,2014-12-14 22:23:14,2014-12-13 22:23:15.120,2014-12-13 "
        "22:23:15.123450,2014-12-13 22:23:15.123456780");
    insertCsvValues(
        "timestamp_agg",
        "600,2014-12-14,2014-12-14 22:23:14,2014-12-14 22:23:15.120,2014-12-14 "
        "22:23:15.123450,2014-12-14 22:23:15.123456780");
    insertCsvValues(
        "timestamp_agg",
        "180,2014-12-14,2014-12-14 22:23:14,2014-12-13 22:23:15.120,2014-12-14 "
        "22:23:15.123450,2014-12-14 22:23:15.123456780");

    const std::vector<std::tuple<std::string, int64_t, double, int64_t, std::string>> params{
        // Date
        std::make_tuple("CAST(dt as timestamp(0))"s, 1418428800, 125.0, 2, ""s),
        std::make_tuple("CAST(dt as timestamp(3))"s, 1418428800000, 125.0, 2, ""s),
        std::make_tuple("CAST(dt as timestamp(6))"s, 1418428800000000, 125.0, 2, ""s),
        std::make_tuple("CAST(dt as timestamp(9))"s, 1418428800000000000, 125.0, 2, ""s),
        std::make_tuple("DATE_TRUNC(millisecond, dt)"s, 1418428800, 125.0, 2, ""s),
        std::make_tuple("DATE_TRUNC(microsecond, dt)"s, 1418428800, 125.0, 2, ""s),
        std::make_tuple("DATE_TRUNC(nanosecond, dt)"s, 1418428800, 125.0, 2, ""s),
        std::make_tuple("DATE_TRUNC(nanosecond, dt)"s, 1418428800, 125.0, 2, ""s),
        std::make_tuple(
            "DATE_TRUNC(second, dt)"s,
            1418428800,
            125.0,
            2,
            "WHERE CAST(dt AS TIMESTAMP(0)) BETWEEN CAST('2014-12-13 00:00:00' AS TIMESTAMP(0)) AND CAST('2014-12-13 22:05:54' AS TIMESTAMP(0))"s),
        std::make_tuple(
            "DATE_TRUNC(second, dt)"s,
            0,
            0.0,
            0,
            "WHERE CAST(dt AS TIMESTAMP(3)) BETWEEN CAST('2014-12-12 00:00:00.000' AS TIMESTAMP(3)) AND CAST('2014-12-12 23:59:59.999' AS TIMESTAMP(3))"s),
        std::make_tuple(
            "DATE_TRUNC(nanosecond, dt)"s,
            1418428800,
            125.0,
            2,
            "WHERE CAST(dt AS TIMESTAMP(6)) BETWEEN CAST('2014-12-13 00:00:00.000000' AS TIMESTAMP(6)) AND CAST('2014-12-13 22:05:54.999911' AS TIMESTAMP(6))"s),
        // Timestamp(s)
        std::make_tuple("CAST(ts as date)"s, 1418428800, 125.0, 2, ""s),
        std::make_tuple("CAST(ts as timestamp(3))"s, 1418509395000, 125.0, 2, ""s),
        std::make_tuple("CAST(ts as timestamp(6))"s, 1418509395000000, 125.0, 2, ""s),
        std::make_tuple("CAST(ts as timestamp(9))"s, 1418509395000000000, 125.0, 2, ""s),
        std::make_tuple("DATE_TRUNC(millisecond, ts)"s, 1418509395, 125.0, 2, ""s),
        std::make_tuple("DATE_TRUNC(microsecond, ts)"s, 1418509395, 125.0, 2, ""s),
        std::make_tuple("DATE_TRUNC(nanosecond, ts)"s, 1418509395, 125.0, 2, ""s),
        std::make_tuple(
            "DATE_TRUNC(second, ts)"s,
            1418509395,
            125.0,
            2,
            "WHERE CAST(ts AS TIMESTAMP(0)) BETWEEN CAST('2014-12-13 22:23:15' AS TIMESTAMP(0)) AND CAST('2014-12-14 22:23:13' AS TIMESTAMP(0))"s),
        std::make_tuple(
            "DATE_TRUNC(microsecond, ts)"s,
            0,
            0.0,
            0,
            "WHERE CAST(ts AS TIMESTAMP(3)) BETWEEN CAST('2014-12-13 22:23:16.000' AS TIMESTAMP(3)) AND CAST('2014-12-13 22:23:13.999' AS TIMESTAMP(3))"s),
        std::make_tuple(
            "DATE_TRUNC(millisecond, ts)"s,
            1418595794,
            395.0,
            4,
            "WHERE CAST(ts AS TIMESTAMP(6)) BETWEEN CAST('2014-12-13 22:23:16.000001' AS TIMESTAMP(6)) AND CAST('2014-12-14 22:23:14.000111' AS TIMESTAMP(6))"s),
        // Timestamp(ms)
        std::make_tuple("CAST(ts3 as date)"s, 1418428800, 246.0, 5, ""s),
        std::make_tuple("CAST(ts3 as timestamp(0))"s, 1418509395, 246.0, 5, ""s),
        std::make_tuple(
            "CAST(ts3 as timestamp(6))"s, 1418509395120000, 326.6666666666667, 3, ""s),
        std::make_tuple(
            "CAST(ts3 as timestamp(9))"s, 1418509395120000000, 326.6666666666667, 3, ""s),
        std::make_tuple(
            "DATE_TRUNC(millisecond, ts3)"s, 1418509395120, 326.6666666666667, 3, ""s),
        std::make_tuple(
            "DATE_TRUNC(microsecond, ts3)"s, 1418509395120, 326.6666666666667, 3, ""s),
        std::make_tuple(
            "DATE_TRUNC(nanosecond, ts3)"s, 1418509395120, 326.6666666666667, 3, ""s),
        std::make_tuple(
            "DATE_TRUNC(nanosecond, ts3)"s,
            1418595795120,
            600.0,
            1,
            "WHERE CAST(ts3 AS TIMESTAMP(3)) BETWEEN CAST('2014-12-13 22:23:15.124' AS TIMESTAMP(3)) AND CAST('2014-12-14 22:23:15.120' AS TIMESTAMP(3))"s),
        std::make_tuple(
            "DATE_TRUNC(microsecond, ts3)"s,
            0,
            0.0,
            0,
            "WHERE CAST(ts3 AS TIMESTAMP(3)) BETWEEN CAST('2014-12-13 22:23:16.000' AS TIMESTAMP(3)) AND CAST('2014-12-13 22:23:13.999' AS TIMESTAMP(3))"s),
        std::make_tuple(
            "DATE_TRUNC(millisecond, ts3)"s,
            1418509395123,
            125.0,
            2,
            "WHERE CAST(ts3 AS TIMESTAMP(6)) BETWEEN CAST('2014-12-13 22:23:15.122999' AS TIMESTAMP(6)) AND CAST('2014-12-14 22:23:15.000111' AS TIMESTAMP(6))"s),
        // // Timestamp(us)
        std::make_tuple("CAST(ts6 as date)"s, 1418428800, 262.5, 4, ""s),
        std::make_tuple("CAST(ts6 as timestamp(0))"s, 1418509395, 262.5, 4, ""s),
        std::make_tuple("CAST(ts6 as timestamp(3))"s, 1418509395123, 262.5, 4, ""s),
        std::make_tuple("CAST(ts6 as timestamp(9))"s, 1418509395123450000, 400.0, 2, ""s),
        std::make_tuple("DATE_TRUNC(millisecond, ts6)"s, 1418509395123000, 262.5, 4, ""s),
        std::make_tuple("DATE_TRUNC(microsecond, ts6)"s, 1418509395123450, 400.0, 2, ""s),
        std::make_tuple("DATE_TRUNC(nanosecond, ts6)"s, 1418509395123450, 400.0, 2, ""s),
        std::make_tuple(
            "DATE_TRUNC(nanosecond, ts6)"s,
            1418509395123456,
            125.0,
            2,
            "WHERE CAST(ts6 AS TIMESTAMP(6)) BETWEEN CAST('2014-12-13 22:23:15.123451' AS TIMESTAMP(6)) AND CAST('2014-12-14 22:23:15.123449' AS TIMESTAMP(6))"s),
        std::make_tuple(
            "DATE_TRUNC(microsecond, ts6)"s,
            0,
            0.0,
            0,
            "WHERE CAST(ts6 AS TIMESTAMP(3)) BETWEEN CAST('2014-12-13 22:23:16.000' AS TIMESTAMP(3)) AND CAST('2014-12-14 22:23:15.122' AS TIMESTAMP(3))"s),
        std::make_tuple(
            "DATE_TRUNC(millisecond, ts6)"s,
            1418509395123000,
            400.0,
            2,
            "WHERE CAST(ts6 AS TIMESTAMP(6)) BETWEEN CAST('2014-12-13 22:23:15.123449' AS TIMESTAMP(6)) AND CAST('2014-12-13 22:23:15.123451' AS TIMESTAMP(6))"s),
        // Timestamp(ns)
        std::make_tuple("CAST(ts9 as date)"s, 1418428800, 262.5, 4, ""s),
        std::make_tuple("CAST(ts9 as timestamp(0))"s, 1418509395, 262.5, 4, ""s),
        std::make_tuple("CAST(ts9 as timestamp(3))"s, 1418509395123, 262.5, 4, ""s),
        std::make_tuple("CAST(ts9 as timestamp(6))"s, 1418509395123456, 262.5, 4, ""s),
        std::make_tuple(
            "DATE_TRUNC(millisecond, ts9)"s, 1418509395123000000, 262.5, 4, ""s),
        std::make_tuple(
            "DATE_TRUNC(microsecond, ts9)"s, 1418509395123456000, 262.5, 4, ""s),
        std::make_tuple(
            "DATE_TRUNC(nanosecond, ts9)"s, 1418509395123456780, 400.0, 2, ""s),
        std::make_tuple(
            "DATE_TRUNC(microsecond, ts9)"s,
            1418509395123456000,
            262.5,
            4,
            "WHERE CAST(ts9 AS TIMESTAMP(6)) BETWEEN CAST('2014-12-13 22:23:15.123456' AS TIMESTAMP(6)) AND CAST('2014-12-14 22:23:15.123455' AS TIMESTAMP(6))"s),
        std::make_tuple(
            "DATE_TRUNC(microsecond, ts9)"s,
            0,
            0.0,
            0,
            "WHERE CAST(ts9 AS TIMESTAMP(3)) BETWEEN CAST('2014-12-13 22:23:16.000' AS TIMESTAMP(3)) AND CAST('2014-12-14 22:23:15.122' AS TIMESTAMP(3))"s),
        std::make_tuple(
            "DATE_TRUNC(millisecond, ts9)"s,
            1418509395123000000,
            400.0,
            2,
            "WHERE CAST(ts9 AS TIMESTAMP(9)) BETWEEN CAST('2014-12-13 22:23:15.123456780' AS TIMESTAMP(9)) AND CAST('2014-12-13 22:23:15.123456781' AS TIMESTAMP(9))"s)};

    for (auto& param : params) {
      const auto row = run_multiple_agg(
          "SELECT " + std::get<0>(param) +
              " as tg, avg(val), count(*) from timestamp_agg " + std::get<4>(param) +
              " group by "
              "tg order by tg limit 1;",
          dt);
      validate_timestamp_agg(
          *row, std::get<1>(param), std::get<2>(param), std::get<3>(param));
    }
    dropTable("timestamp_agg");
  }
}

// Tests generated from scripts/pg_test/generate_extract_tests.rb
TEST_F(Select, ExtractFromNegativeTimes) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ(11LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(345LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(-1769003452LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(10LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(
        8000000LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(
        8000LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(9LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(8LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(
        49LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(1913LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '1913-12-11 10:09:08');", dt)));
    ASSERT_EQ(11LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(345LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(-1769003460LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(10LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(9LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(
        49LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(1913LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '1913-12-11 10:09:00');", dt)));
    ASSERT_EQ(11LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(345LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(-1769004000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(10LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(
        49LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(1913LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '1913-12-11 10:00:00');", dt)));
    ASSERT_EQ(11LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(345LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(-1769040000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(
        49LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(1913LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '1913-12-11 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(335LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(-1769904000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(49LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(
        49LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(
        48LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(1913LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '1913-12-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(-1798761600LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        1LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        53LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(1913LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '1913-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        53LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        52LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(1970LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '1970-01-01 00:00:00');", dt)));
    ASSERT_EQ(11LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(345LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(1386756548LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(10LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(
        8000000LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(
        8000LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(9LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(8LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(2013LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '2013-12-11 10:09:08');", dt)));
    ASSERT_EQ(11LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(345LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(1386756540LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(10LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(9LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(2013LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '2013-12-11 10:09:00');", dt)));
    ASSERT_EQ(11LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(345LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(1386756000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(10LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(2013LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '2013-12-11 10:00:00');", dt)));
    ASSERT_EQ(11LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(345LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(1386720000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(3LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(50LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(
        50LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(2013LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '2013-12-11 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(335LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(1385856000LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(7LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(12LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(4LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(48LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(
        49LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(
        49LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(2013LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '2013-12-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DAY FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(2LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOW FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(DOY FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(1356998400LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(EPOCH FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(HOUR FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(2LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(ISODOW FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        0LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MINUTE FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(MONTH FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(QUARTER FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(0LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(SECOND FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(1LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        1LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(
        1LL,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
    ASSERT_EQ(2013LL,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(YEAR FROM TIMESTAMP '2013-01-01 00:00:00');", dt)));
  }
}

TEST_F(Select, WeekOne) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // 2009 Jan 4 is a Sunday
    EXPECT_EQ(52L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2008-12-28 23:59:59');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2008-12-29 00:00:00');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2009-01-04 23:59:59');", dt)));
    EXPECT_EQ(2L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2009-01-05 00:00:00');", dt)));
    EXPECT_EQ(
        53L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2009-01-03 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2009-01-04 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2009-01-10 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2009-01-11 00:00:00');", dt)));
    EXPECT_EQ(
        53L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2009-01-02 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2009-01-03 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2009-01-09 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2009-01-10 00:00:00');", dt)));
    // 2010 Jan 4 is a Monday
    EXPECT_EQ(53L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2010-01-03 23:59:59');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2010-01-04 00:00:00');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2010-01-10 23:59:59');", dt)));
    EXPECT_EQ(2L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2010-01-11 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2010-01-02 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2010-01-03 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2010-01-09 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2010-01-10 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2010-01-01 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2010-01-02 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2010-01-08 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2010-01-09 00:00:00');", dt)));
    // 2005 Jan 4 is a Tuesday
    EXPECT_EQ(53L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2005-01-02 23:59:59');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2005-01-03 00:00:00');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2005-01-09 23:59:59');", dt)));
    EXPECT_EQ(2L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2005-01-10 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2005-01-01 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2005-01-02 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2005-01-08 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2005-01-09 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2004-12-31 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2005-01-01 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2005-01-07 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2005-01-08 00:00:00');", dt)));
    // 2012 Jan 4 is a Wednesday
    EXPECT_EQ(52L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2012-01-01 23:59:59');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2012-01-02 00:00:00');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2012-01-08 23:59:59');", dt)));
    EXPECT_EQ(2L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2012-01-09 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2011-12-31 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2012-01-01 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2012-01-07 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2012-01-08 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2011-12-30 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2011-12-31 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2012-01-06 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2012-01-07 00:00:00');", dt)));
    // 2007 Jan 4 is a Thursday
    EXPECT_EQ(52L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2006-12-31 23:59:59');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2007-01-01 00:00:00');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2007-01-07 23:59:59');", dt)));
    EXPECT_EQ(2L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2007-01-08 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2006-12-30 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2006-12-31 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2007-01-06 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2007-01-07 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2006-12-29 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2006-12-30 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2007-01-05 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2007-01-06 00:00:00');", dt)));
    // 2008 Jan 4 is a Friday
    EXPECT_EQ(52L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2007-12-30 23:59:59');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2007-12-31 00:00:00');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2008-01-06 23:59:59');", dt)));
    EXPECT_EQ(2L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2008-01-07 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2007-12-29 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2007-12-30 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2008-01-05 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2008-01-06 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2007-12-28 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2007-12-29 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2008-01-04 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2008-01-05 00:00:00');", dt)));
    // 2003 Jan 4 is a Saturday
    EXPECT_EQ(52L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2002-12-29 23:59:59');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2002-12-30 00:00:00');", dt)));
    EXPECT_EQ(1L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2003-01-05 23:59:59');", dt)));
    EXPECT_EQ(2L,
              v<int64_t>(run_simple_agg(
                  "SELECT EXTRACT(WEEK FROM TIMESTAMP '2003-01-06 00:00:00');", dt)));
    EXPECT_EQ(
        52L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2002-12-28 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2002-12-29 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2003-01-04 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SUNDAY FROM TIMESTAMP '2003-01-05 00:00:00');", dt)));
    EXPECT_EQ(
        53L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2003-01-03 23:59:59');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2003-01-04 00:00:00');", dt)));
    EXPECT_EQ(
        1L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2003-01-10 23:59:59');", dt)));
    EXPECT_EQ(
        2L,
        v<int64_t>(run_simple_agg(
            "SELECT EXTRACT(WEEK_SATURDAY FROM TIMESTAMP '2003-01-11 00:00:00');", dt)));
  }
}

TEST_F(Select, NonCorrelated_Exists) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c("SELECT ename FROM emp WHERE EXISTS (SELECT dname FROM dept WHERE deptno > 40) "
      "ORDER BY ename;",
      dt);
    c("SELECT ename FROM emp WHERE EXISTS (SELECT dname FROM dept WHERE deptno < 20) "
      "ORDER BY ename;",
      dt);
    c("SELECT ename FROM emp WHERE NOT EXISTS (SELECT dname FROM dept WHERE deptno > 40) "
      "ORDER BY ename;",
      dt);
    c("SELECT ename FROM emp WHERE NOT EXISTS (SELECT dname FROM dept WHERE deptno < 20) "
      "ORDER BY ename;",
      dt);
    c("SELECT ename FROM emp WHERE EXISTS (SELECT * FROM dept WHERE deptno > 40) "
      "ORDER BY ename;",
      dt);
    c("SELECT ename FROM emp WHERE NOT EXISTS (SELECT * FROM dept WHERE deptno < 20) "
      "ORDER BY ename;",
      dt);
  }
}

TEST_F(Select, Correlated_Exists) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c("SELECT ename FROM emp E WHERE EXISTS (SELECT D.dname FROM dept D WHERE "
      "D.deptno > 40 and E.deptno = D.deptno) ORDER BY ename;",
      dt);
    c("SELECT ename FROM emp E WHERE EXISTS (SELECT D.dname FROM dept D WHERE "
      "D.deptno < 20 and E.deptno = D.deptno) ORDER BY ename;",
      dt);
    c("SELECT ename FROM emp E WHERE NOT EXISTS (SELECT D.dname FROM dept D WHERE "
      "D.deptno > 40 and E.deptno = D.deptno) ORDER BY ename;",
      dt);
    c("SELECT ename FROM emp E WHERE NOT EXISTS (SELECT D.dname FROM dept D WHERE "
      "D.deptno < 20 and E.deptno = D.deptno) ORDER BY ename;",
      dt);
    c("SELECT ename FROM emp E WHERE EXISTS (SELECT * from dept D WHERE "
      "D.deptno > 40 and E.deptno = D.deptno) ORDER BY ename;",
      dt);
    c("SELECT ename FROM emp E WHERE NOT EXISTS (SELECT * from dept D WHERE "
      "D.deptno > 40 and E.deptno = D.deptno) ORDER BY ename;",
      dt);
  }
}

TEST_F(Select, Correlated_In) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    c("SELECT f.val FROM corr_in_facts f WHERE f.val IN (SELECT l.val FROM "
      "corr_in_lookup l WHERE f.id = "
      "l.id) AND f.val > 3",
      dt);
    c("SELECT f.val FROM corr_in_facts f WHERE f.val IN (SELECT l.val FROM "
      "corr_in_lookup l WHERE f.id "
      "<> l.id) AND f.val > 3",
      dt);
    c("SELECT f.id FROM corr_in_facts f WHERE f.id IN (SELECT l.id FROM corr_in_lookup l "
      "WHERE f.val <> "
      "l.val) AND f.val < 2",
      dt);
    c("SELECT f.id FROM corr_in_facts f WHERE f.id IN (SELECT l.id FROM corr_in_lookup l "
      "WHERE f.val = "
      "l.val) AND f.val < 2",
      dt);
  }
}

TEST_F(Select, QuotedColumnIdentifier) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    createTable("identifier_test",
                {{"id", SQLTypeInfo(kINT)}, {"sum", SQLTypeInfo(kBIGINT)}});
    insertCsvValues("identifier_test", "1,1\n2,2\n3,3");

    EXPECT_ANY_THROW(run_simple_agg("SELECT sum FROM identifier_test;", dt));

    ASSERT_EQ(static_cast<int64_t>(1),
              v<int64_t>(run_simple_agg(
                  "SELECT \"sum\" FROM identifier_test where id = 1;", dt)));

    dropTable("identifier_test");
  }
}

TEST_F(Select, ROUND) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    {
      // Check no 2nd parameter
      // the cast is required. SQLite seems to only return FLOATs
      // clang-format off
      std::string sqlLite_select = "SELECT CAST(ROUND(s16) AS SMALLINT) AS r_s16, "
          "CAST(ROUND(s32) AS INT) AS r_s32, "
          "CAST(ROUND(s64) AS BIGINT) AS r_s64, "
          "ROUND(f32) AS r_f32, "
          "ROUND(f64) AS r_f64, "
          "ROUND(n64) AS r_n64, "
          "ROUND(d64) AS r_d64 FROM test_rounding ORDER BY f64 ASC";
      // clang-format on

      // clang-format off
      std::string select = "SELECT CAST(ROUND(s16) AS SMALLINT) AS r_s16, "
          "CAST(ROUND(s32) AS INT) AS r_s32, "
          "CAST(ROUND(s64) AS BIGINT) AS r_s64, "
          "ROUND(f32) AS r_f32, "
          "ROUND(f64) AS r_f64, "
          "ROUND(n64) AS r_n64, "
          "ROUND(d64) AS r_d64 FROM test_rounding ORDER BY f64 ASC NULLS FIRST";
      // clang-format on
      c(select, sqlLite_select, dt);
    }

    // Check negative 2nd parameter
    for (int n = -9; n < 0; n++) {
      std::string i = std::to_string(n);
      std::string rounding_base = std::to_string((int)pow(10, std::abs(n))) + ".0";

      // clang-format off
      std::string sqlLite_select = "SELECT CAST(ROUND((s16/"+rounding_base+")) * "+rounding_base+" AS SMALLINT) AS r_s16, "
              "CAST(ROUND((s32/"+rounding_base+")) * "+rounding_base+" AS INT) AS r_s32, "
              "CAST(ROUND((s64/"+rounding_base+")) * "+rounding_base+" AS BIGINT) AS r_s64, "
              "ROUND((f32/"+rounding_base+")) * "+rounding_base+" AS r_f32, "
              "ROUND((f64/"+rounding_base+")) * "+rounding_base+" AS r_f64, "
              "ROUND((n64/"+rounding_base+")) * "+rounding_base+" AS r_n64, "
              "ROUND((d64/"+rounding_base+")) * "+rounding_base+" AS r_d64 FROM test_rounding ORDER BY f64 ASC";
      // clang-format on

      // clang-format off
      std::string select = "SELECT ROUND(s16, "+i+") AS r_s16, "
              "ROUND(s32, "+i+") AS r_s32, "
              "ROUND(s64, "+i+") AS r_s64, "
              "ROUND(f32, "+i+") AS r_f32, "
              "ROUND(f64, "+i+") AS r_f64, "
              "ROUND(n64, "+i+") AS r_n64, "
              "ROUND(d64, "+i+") AS r_d64 FROM test_rounding ORDER BY f64 ASC NULLS FIRST";
      // clang-format on
      c(select, sqlLite_select, dt);
    }

    // check positive 2nd parameter
    for (int n = 0; n < 10; n++) {
      std::string i = std::to_string(n);

      // the cast is required. SQLite seems to only return FLOATs
      // clang-format off
      std::string sqlLite_select = "SELECT CAST(ROUND(s16, "+i+") AS SMALLINT) AS r_s16, "
              "CAST(ROUND(s32, "+i+") AS INT) AS r_s32, "
              "CAST(ROUND(s64, "+i+") AS BIGINT) AS r_s64, "
              "ROUND(f32, "+i+") AS r_f32, "
              "ROUND(f64, "+i+") AS r_f64, "
              "ROUND(n64, "+i+") AS r_n64, "
              "ROUND(d64, "+i+") AS r_d64 FROM test_rounding ORDER BY f64 ASC";
      // clang-format on

      // clang-format off
      std::string select = "SELECT CAST(ROUND(s16, "+i+") AS SMALLINT) AS r_s16, "
              "CAST(ROUND(s32, "+i+") AS INT) AS r_s32, "
              "CAST(ROUND(s64, "+i+") AS BIGINT) AS r_s64, "
              "ROUND(f32, "+i+") AS r_f32, "
              "ROUND(f64, "+i+") AS r_f64, "
              "ROUND(n64, "+i+") AS r_n64, "
              "ROUND(d64, "+i+") AS r_d64 FROM test_rounding ORDER BY f64 ASC NULLS FIRST";
      // clang-format on
      c(select, sqlLite_select, dt);
    }

    // check null 2nd parameter
    // the cast is required. SQLite seems to only return FLOATs
    // clang-format off
    std::string select = "SELECT CAST(ROUND(s16, (SELECT s16 FROM test_rounding WHERE s16 IS NULL)) AS SMALLINT) AS r_s16, "
        "CAST(ROUND(s32, (SELECT s16 FROM test_rounding WHERE s16 IS NULL)) AS INT) AS r_s32, "
        "CAST(ROUND(s64, (SELECT s16 FROM test_rounding WHERE s16 IS NULL)) AS BIGINT) AS r_s64, "
        "ROUND(f32, (SELECT s16 FROM test_rounding WHERE s16 IS NULL)) AS r_f32, "
        "ROUND(f64, (SELECT s16 FROM test_rounding WHERE s16 IS NULL)) AS r_f64, "
        "ROUND(n64, (SELECT s16 FROM test_rounding WHERE s16 IS NULL)) AS r_n64, "
        "ROUND(d64, (SELECT s16 FROM test_rounding WHERE s16 IS NULL)) AS r_d64 FROM test_rounding";
    c(select, dt);
    // clang-format on

    // check that no -0.0 (negative zero) is returned
    TargetValue val_s16 = run_simple_agg(
        "SELECT ROUND(CAST(-1.7 as SMALLINT), -1) as r_val FROM test_rounding WHERE s16 "
        "IS NULL;",
        dt);
    TargetValue val_s32 = run_simple_agg(
        "SELECT ROUND(CAST(-1.7 as INT), -1) as r_val FROM test_rounding WHERE s16 IS "
        "NULL;",
        dt);
    TargetValue val_s64 = run_simple_agg(
        "SELECT ROUND(CAST(-1.7 as BIGINT), -1) as r_val FROM test_rounding WHERE s16 IS "
        "NULL;",
        dt);
    TargetValue val_f32 = run_simple_agg(
        "SELECT ROUND(CAST(-1.7 as FLOAT), -1) as r_val FROM test_rounding WHERE s16 IS "
        "NULL;",
        dt);
    TargetValue val_f64 = run_simple_agg(
        "SELECT ROUND(CAST(-1.7 as DOUBLE), -1) as r_val FROM test_rounding WHERE s16 IS "
        "NULL;",
        dt);
    TargetValue val_n64 = run_simple_agg(
        "SELECT ROUND(CAST(-1.7 as NUMERIC(10,5)), -1) as r_val FROM test_rounding WHERE "
        "s16 IS NULL;",
        dt);
    TargetValue val_d64 = run_simple_agg(
        "SELECT ROUND(CAST(-1.7 as DECIMAL(10,5)), -1) as r_val FROM test_rounding WHERE "
        "s16 IS NULL;",
        dt);

    ASSERT_TRUE(0 == boost::get<int64_t>(boost::get<ScalarTargetValue>(val_s16)));
    ASSERT_TRUE(0 == boost::get<int64_t>(boost::get<ScalarTargetValue>(val_s32)));
    ASSERT_TRUE(0 == boost::get<int64_t>(boost::get<ScalarTargetValue>(val_s64)));

    ASSERT_FLOAT_EQ(0.0f, boost::get<float>(boost::get<ScalarTargetValue>(val_f32)));
    ASSERT_FALSE(std::signbit(boost::get<float>(boost::get<ScalarTargetValue>(val_f32))));

    ASSERT_DOUBLE_EQ(0.0, boost::get<double>(boost::get<ScalarTargetValue>(val_f64)));
    ASSERT_FALSE(
        std::signbit(boost::get<double>(boost::get<ScalarTargetValue>(val_f64))));

    ASSERT_DOUBLE_EQ(0.0, boost::get<double>(boost::get<ScalarTargetValue>(val_n64)));
    ASSERT_FALSE(
        std::signbit(boost::get<double>(boost::get<ScalarTargetValue>(val_f64))));

    ASSERT_DOUBLE_EQ(0.0, boost::get<double>(boost::get<ScalarTargetValue>(val_d64)));
    ASSERT_FALSE(
        std::signbit(boost::get<double>(boost::get<ScalarTargetValue>(val_f64))));
  }
}

TEST_F(Select, Sample) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    ASSERT_EQ("else",
              boost::get<std::string>(v<NullableString>(run_simple_agg(
                  "SELECT SAMPLE(CASE WHEN x IN (9) THEN str ELSE 'else' END) FROM test;",
                  dt))));
    // Our "SAMPLE" operator is generally termed "ANY_VALUE", and the latter
    // is natively supported by Calcite. Test this as an alias for "SAMPLE".
    ASSERT_EQ(
        "else",
        boost::get<std::string>(v<NullableString>(run_simple_agg(
            "SELECT ANY_VALUE(CASE WHEN x IN (9) THEN str ELSE 'else' END) FROM test;",
            dt))));
    {
      const auto rows = run_multiple_agg(
          "SELECT SAMPLE(real_str), COUNT(*) FROM test WHERE x > 8;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(2), crt_row.size());
      const auto nullable_str = v<NullableString>(crt_row[0]);
      const auto null_ptr = boost::get<void*>(&nullable_str);
      ASSERT_TRUE(null_ptr && !*null_ptr);
      ASSERT_EQ(0, v<int64_t>(crt_row[1]));
      const auto empty_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(0), empty_row.size());
    };
    {
      const auto rows = run_multiple_agg(
          "SELECT SAMPLE(real_str), COUNT(*) FROM test WHERE x > 7;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(2), crt_row.size());
      const auto nullable_str = v<NullableString>(crt_row[0]);
      const auto str_ptr = boost::get<std::string>(&nullable_str);
      ASSERT_TRUE(str_ptr);
      ASSERT_EQ("real_bar", boost::get<std::string>(*str_ptr));
      ASSERT_EQ(static_cast<int64_t>(g_num_rows / 2), v<int64_t>(crt_row[1]));
      const auto empty_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(0), empty_row.size());
    };
    {
      const auto rows = run_multiple_agg(
          "SELECT SAMPLE(real_str), COUNT(*) FROM test WHERE x > 7 GROUP BY x;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(2), crt_row.size());
      const auto nullable_str = v<NullableString>(crt_row[0]);
      const auto str_ptr = boost::get<std::string>(&nullable_str);
      ASSERT_TRUE(str_ptr);
      ASSERT_EQ("real_bar", boost::get<std::string>(*str_ptr));
      ASSERT_EQ(static_cast<int64_t>(g_num_rows / 2), v<int64_t>(crt_row[1]));
      const auto empty_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(0), empty_row.size());
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT SAMPLE(arr_i64), COUNT(*) FROM array_test WHERE x = 8;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(2), crt_row.size());
      compare_array(crt_row[0], std::vector<int64_t>{200, 300, 400});
      ASSERT_EQ(static_cast<int64_t>(1), v<int64_t>(crt_row[1]));
      const auto empty_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(0), empty_row.size());
    };
    {
      const auto rows = run_multiple_agg(
          "SELECT SAMPLE(arr_i64), COUNT(*) FROM array_test WHERE x = 8 GROUP BY x;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(2), crt_row.size());
      compare_array(crt_row[0], std::vector<int64_t>{200, 300, 400});
      ASSERT_EQ(static_cast<int64_t>(1), v<int64_t>(crt_row[1]));
      const auto empty_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(0), empty_row.size());
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT x, SAMPLE(arr_i64), SAMPLE(real_str), COUNT(*) FROM array_test "
          "WHERE x = 8 GROUP BY x;",
          dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(4), crt_row.size());
      compare_array(crt_row[1], std::vector<int64_t>{200, 300, 400});
      const auto nullable_str = v<NullableString>(crt_row[2]);
      const auto str_ptr = boost::get<std::string>(&nullable_str);
      ASSERT_TRUE(str_ptr);
      ASSERT_EQ("real_str1", boost::get<std::string>(*str_ptr));
      ASSERT_EQ(static_cast<int64_t>(1), v<int64_t>(crt_row[3]));
      const auto empty_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(0), empty_row.size());
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT SAMPLE(arr3_i64), COUNT(*) FROM array_test WHERE x = 8;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(2), crt_row.size());
      compare_array(crt_row[0], std::vector<int64_t>{200, 300, 400});
      ASSERT_EQ(static_cast<int64_t>(1), v<int64_t>(crt_row[1]));
      const auto empty_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(0), empty_row.size());
    };
    {
      const auto rows = run_multiple_agg(
          "SELECT SAMPLE(arr3_i64), COUNT(*) FROM array_test WHERE x = 8 GROUP BY x;",
          dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(2), crt_row.size());
      compare_array(crt_row[0], std::vector<int64_t>{200, 300, 400});
      ASSERT_EQ(static_cast<int64_t>(1), v<int64_t>(crt_row[1]));
      const auto empty_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(0), empty_row.size());
    }
    auto check_sample_rowid = [](const int64_t val) {
      const std::set<int64_t> valid_row_ids{15, 16, 17, 18, 19};
      ASSERT_TRUE(valid_row_ids.find(val) != valid_row_ids.end())
          << "Last sample rowid value " << val << " is invalid";
    };
    {
      const auto rows = run_multiple_agg(
          "SELECT AVG(d), AVG(f), str, SAMPLE(rowid) FROM test WHERE d > 2.4 GROUP "
          "BY str;",
          dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(4), crt_row.size());
      const auto d = v<double>(crt_row[0]);
      ASSERT_EQ(2.6, d);
      const auto f = v<double>(crt_row[1]);
      ASSERT_EQ(1.3, f);
      const auto nullable_str = v<NullableString>(crt_row[2]);
      const auto str_ptr = boost::get<std::string>(&nullable_str);
      ASSERT_TRUE(str_ptr);
      ASSERT_EQ("baz", boost::get<std::string>(*str_ptr));
      const auto rowid = v<int64_t>(crt_row[3]);
      check_sample_rowid(rowid);
    };
    {
      const auto rows = run_multiple_agg("SELECT SAMPLE(str) FROM test WHERE x > 8;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(1), crt_row.size());
      const auto nullable_str = v<NullableString>(crt_row[0]);
      ASSERT_FALSE(boost::get<void*>(nullable_str));
    };
    {
      const auto rows = run_multiple_agg(
          "SELECT x, SAMPLE(fixed_str), SUM(t) FROM test GROUP BY x ORDER BY x DESC;",
          dt);
      const auto first_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), first_row.size());
      ASSERT_EQ(int64_t(8), v<int64_t>(first_row[0]));
      const auto nullable_str = v<NullableString>(first_row[1]);
      const auto str_ptr = boost::get<std::string>(&nullable_str);
      ASSERT_TRUE(str_ptr);
      ASSERT_EQ(int64_t(5010), v<int64_t>(first_row[2]));
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT z, COUNT(*), SAMPLE(f) FROM test GROUP BY z ORDER BY z;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), crt_row.size());
      ASSERT_EQ(int64_t(-78), v<int64_t>(crt_row[0]));
      ASSERT_EQ(int64_t(5), v<int64_t>(crt_row[1]));
      ASSERT_NEAR(float(1.2), v<float>(crt_row[2]), 0.01);
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT z, COUNT(*), SAMPLE(fn) FROM test GROUP BY z ORDER BY z;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), crt_row.size());
      ASSERT_EQ(int64_t(-78), v<int64_t>(crt_row[0]));
      ASSERT_EQ(int64_t(5), v<int64_t>(crt_row[1]));
      ASSERT_NEAR(float(-101.2), v<float>(crt_row[2]), 0.01);
      const auto null_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), null_row.size());
      ASSERT_EQ(int64_t(101), v<int64_t>(null_row[0]));
      ASSERT_EQ(int64_t(10), v<int64_t>(null_row[1]));
      ASSERT_NEAR(std::numeric_limits<float>::min(), v<float>(null_row[2]), 0.001);
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT z, COUNT(*), SAMPLE(d) FROM test GROUP BY z ORDER BY z;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), crt_row.size());
      ASSERT_EQ(int64_t(-78), v<int64_t>(crt_row[0]));
      ASSERT_EQ(int64_t(5), v<int64_t>(crt_row[1]));
      ASSERT_NEAR(double(2.4), v<double>(crt_row[2]), 0.01);
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT z, COUNT(*), SAMPLE(dn) FROM test GROUP BY z ORDER BY z;", dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), crt_row.size());
      ASSERT_EQ(int64_t(-78), v<int64_t>(crt_row[0]));
      ASSERT_EQ(int64_t(5), v<int64_t>(crt_row[1]));
      ASSERT_NEAR(double(-2002.4), v<double>(crt_row[2]), 0.01);
      const auto null_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), null_row.size());
      ASSERT_EQ(int64_t(101), v<int64_t>(null_row[0]));
      ASSERT_EQ(int64_t(10), v<int64_t>(null_row[1]));
      ASSERT_NEAR(std::numeric_limits<double>::min(), v<double>(null_row[2]), 0.001);
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT z, COUNT(*), SAMPLE(d), SAMPLE(f) FROM test GROUP BY z ORDER BY z;",
          dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(4), crt_row.size());
      ASSERT_EQ(int64_t(-78), v<int64_t>(crt_row[0]));
      ASSERT_EQ(int64_t(5), v<int64_t>(crt_row[1]));
      ASSERT_NEAR(double(2.4), v<double>(crt_row[2]), 0.01);
      ASSERT_NEAR(float(1.2), v<float>(crt_row[3]), 0.01);
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT z, COUNT(*), SAMPLE(fn), SAMPLE(dn) FROM test GROUP BY z ORDER BY z;",
          dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(4), crt_row.size());
      ASSERT_EQ(int64_t(-78), v<int64_t>(crt_row[0]));
      ASSERT_EQ(int64_t(5), v<int64_t>(crt_row[1]));
      ASSERT_NEAR(float(-101.2), v<float>(crt_row[2]), 0.01);
      ASSERT_NEAR(double(-2002.4), v<double>(crt_row[3]), 0.01);
      const auto null_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(4), null_row.size());
      ASSERT_EQ(int64_t(101), v<int64_t>(null_row[0]));
      ASSERT_EQ(int64_t(10), v<int64_t>(null_row[1]));
      ASSERT_NEAR(std::numeric_limits<float>::min(), v<float>(null_row[2]), 0.001);
      ASSERT_NEAR(std::numeric_limits<double>::min(), v<double>(null_row[3]), 0.001);
    }
    {
      const auto rows = run_multiple_agg(
          "SELECT z, COUNT(*), SAMPLE(fn), SAMPLE(x), SAMPLE(dn) FROM test GROUP BY z "
          "ORDER BY z;",
          dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(5), crt_row.size());
      ASSERT_EQ(int64_t(-78), v<int64_t>(crt_row[0]));
      ASSERT_EQ(int64_t(5), v<int64_t>(crt_row[1]));
      ASSERT_NEAR(float(-101.2), v<float>(crt_row[2]), 0.01);
      ASSERT_EQ(int64_t(8), v<int64_t>(crt_row[3]));
      ASSERT_NEAR(double(-2002.4), v<double>(crt_row[4]), 0.01);
      const auto null_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(5), null_row.size());
      ASSERT_EQ(int64_t(101), v<int64_t>(null_row[0]));
      ASSERT_EQ(int64_t(10), v<int64_t>(null_row[1]));
      ASSERT_NEAR(std::numeric_limits<float>::min(), v<float>(null_row[2]), 0.001);
      ASSERT_EQ(int64_t(7), v<int64_t>(null_row[3]));
      ASSERT_NEAR(std::numeric_limits<double>::min(), v<double>(null_row[4]), 0.001);
    }
  }
}

TEST_F(Select, WindowFunctionRank) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    std::string part1 =
        "SELECT x, y, ROW_NUMBER() OVER (PARTITION BY y ORDER BY x ASC) r1, RANK() OVER "
        "(PARTITION BY y ORDER BY x ASC) r2, DENSE_RANK() OVER (PARTITION BY y ORDER BY "
        "x "
        "DESC) r3 FROM " +
        table_name + " ORDER BY x ASC";
    std::string part2 = ", y ASC, r1 ASC, r2 ASC, r3 ASC;";
    c(part1 + " NULLS FIRST" + part2, part1 + part2, dt);
  }
}

TEST_F(Select, WindowFunctionOneRowPartitions) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    std::string part1 = "SELECT y, RANK() OVER (PARTITION BY y ORDER BY n ASC";
    std::string part2 =
        "r FROM (SELECT y, COUNT(*) n FROM " + table_name + " GROUP BY y) ORDER BY y ASC";
    c(part1 + " NULLS FIRST) " + part2 + " NULLS FIRST;", part1 + ") " + part2 + ";", dt);
  }
}

TEST_F(Select, WindowFunctionEmptyPartitions) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;

  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    {
      // Evidently SQLite now allows NULLS FIRST/LAST, so we shouldn't need the string
      // splicing we do in the rest of our tests?
      std::string query = "SELECT x, ROW_NUMBER() OVER (ORDER BY x NULLS FIRST) FROM " +
                          table_name + " ORDER BY x NULLS FIRST;";
      c(query, query, dt);
    }

    // Empty partition with group by
    {
      std::string query =
          "SELECT x, AVG(t) AS avg_t, AVG(t) - AVG(AVG(t)) OVER () as avg_t_diff FROM "
          "" +
          table_name + " WHERE x IS NOT NULL GROUP BY x ORDER BY x;";
      c(query, query, dt);
    }

    // Empty partition with order by
    {
      std::string query =
          "SELECT d, x, y, t, LAG(t) OVER(ORDER BY t ASC NULLS FIRST) AS "
          "lag_t_order_by_t, "
          "LEAD(d) OVER(ORDER BY t NULLS FIRST) AS lead_d_order_by_t, LAG(x) OVER (ORDER "
          "BY t NULLS FIRST) as lag_x_order_by_t, x - SUM(t) OVER (PARTITION BY x ORDER "
          "by "
          "t ASC NULLS FIRST) as x_t_diff, SUM(t) OVER () as total_t FROM " +
          table_name +
          " WHERE d IS NOT NULL ORDER BY d ASC NULLS FIRST, t ASC NULLS FIRST;";
      c(query, query, dt);
    }

    {
      // Manually verified against Postgres
      // TODO(todd): Rework to also run in SQLite, which doesn't support DATE_TRUNC
      std::string query =
          "SELECT DATE_TRUNC(DAY, d) AS binned_day, COUNT(*) AS n, SUM(x) AS sum_x, "
          "COUNT(*) - LAG(COUNT(*)) OVER ( ORDER BY DATE_TRUNC(DAY, d) ) AS "
          "lag_n_order_by_d, SUM(x) / SUM(SUM(x+1)) OVER ( ORDER BY DATE_TRUNC(DAY, d)) "
          "AS "
          "sum_over_lag_sum_x FROM " +
          table_name +
          " GROUP BY DATE_TRUNC(DAY, d) ORDER BY "
          "DATE_TRUNC(DAY, d) NULLS FIRST;";
      EXPECT_NO_THROW(run_multiple_agg(query, dt));
    }
  }
}

TEST_F(Select, WindowFunctionInitialGroupBy) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;

  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    {
      std::string query = "SELECT y, COUNT(*) OVER () AS n FROM " + table_name +
                          " GROUP BY y ORDER BY y NULLS FIRST;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT y, COUNT(*) OVER (ORDER BY AVG(x) ASC NULLS FIRST) AS n FROM " +
          table_name + " GROUP BY y ORDER BY y NULLS FIRST;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT y, x, COUNT(*) OVER (ORDER BY AVG(x) ASC NULLS FIRST) AS n FROM " +
          table_name + " GROUP BY y, x ORDER BY y NULLS FIRST, x NULLS FIRST;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT y, x, COUNT(*) OVER (ORDER BY AVG(x) ASC NULLS FIRST) AS n FROM " +
          table_name + " GROUP BY y, x ORDER BY y NULLS FIRST, x NULLS FIRST;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT y, x, LAG(AVG(t)) OVER (ORDER BY MIN(x) ASC NULLS FIRST) AS lag_avg_t "
          "FROM " +
          table_name + " GROUP BY y, x ORDER BY y NULLS FIRST, x NULLS FIRST;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT y, x, SUM(t) AS sum_t, LAG(SUM(t)) OVER (ORDER BY MIN(x) ASC NULLS "
          "FIRST) AS lag_sum_t, SUM(x) OVER (ORDER BY MIN(x) ASC NULLS FIRST) AS sum_x "
          "FROM " +
          table_name + " GROUP BY y, x ORDER BY y NULLS FIRST, x NULLS FIRST;";
      c(query, query, dt);
    }
  }
}

TEST_F(Select, WindowFunctionSubquery) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;

  auto replace_date_trunc = [](const std::string& date_trunc_query) {
    const std::string date_trunc_day_expr{"DATE_TRUNC(DAY, d)"};
    const std::string sqlite_date_trunc_day_substitution{
        "CAST((julianday(d) - 2440587.5) * 86400 AS INT)"};
    const size_t pos = date_trunc_query.find(date_trunc_day_expr);
    if (pos == std::string::npos) {
      return date_trunc_query;
    }
    std::string sqlite_str(date_trunc_query);
    return sqlite_str.replace(
        pos, date_trunc_day_expr.size(), sqlite_date_trunc_day_substitution);
  };

  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    {
      std::string query =
          "SELECT lag_sum_t, lag_sum_t + 1 AS lag_sum_t_plus_1 FROM (SELECT "
          "DATE_TRUNC(DAY, d) AS binned_day, LAG(SUM(t)) OVER (ORDER BY MIN(t) NULLS "
          "FIRST) AS lag_sum_t FROM " +
          table_name + " GROUP BY binned_day) ORDER BY binned_day NULLS FIRST;";
      std::string sqlite_query = replace_date_trunc(query);
      c(query, sqlite_query, dt);
    }

    {
      std::string query =
          "SELECT SUM(lag_sum_t) AS sum_lag_sum_t FROM (SELECT DATE_TRUNC(DAY, d) AS "
          "binned_day, LAG(SUM(t)) OVER (ORDER BY MIN(t) NULLS FIRST) AS lag_sum_t "
          "FROM " +
          table_name + " GROUP BY binned_day)";
      std::string sqlite_query = replace_date_trunc(query);
      c(query, sqlite_query, dt);
    }

    {
      std::string query =
          "SELECT SUM(lag_sum_t) AS sum_lag_sum_t FROM (SELECT DATE_TRUNC(DAY, d) AS "
          "binned_day, COUNT(*) AS n, SUM(t) AS sum_t, LAG(SUM(t)) OVER (PARTITION BY y "
          "ORDER BY MIN(t) NULLS FIRST) AS lag_sum_t FROM " +
          table_name + " GROUP BY binned_day, y)";
      std::string sqlite_query = replace_date_trunc(query);
      c(query, sqlite_query, dt);
    }
    {
      std::string query =
          "SELECT y, SUM(SUM(n)) OVER (ORDER BY SUM(lag_sum_t) ASC NULLS FIRST) as "
          "sum_n, SUM(lag_sum_t) AS sum_lag_sum_t FROM (SELECT DATE_TRUNC(DAY, d) AS "
          "binned_day, y, COUNT(*) AS n, SUM(t) AS sum_t, LAG(SUM(t)) OVER (PARTITION BY "
          "y ORDER BY MIN(t) NULLS FIRST) AS lag_sum_t FROM test_window_func GROUP BY "
          "binned_day, y) GROUP BY y ORDER BY y ASC NULLS FIRST;";
      std::string sqlite_query = replace_date_trunc(query);
      c(query, sqlite_query, dt);
    }
  }
}

TEST_F(Select, WindowFunctionPercentRank) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    std::string part1 =
        "SELECT x, y, PERCENT_RANK() OVER (PARTITION BY y ORDER BY x ASC) p FROM " +
        table_name + " ORDER BY x ASC";
    std::string part2 = ", y ASC, p ASC;";
    c(part1 + " NULLS FIRST" + part2, part1 + part2, dt);
  }
}

TEST_F(Select, WindowFunctionTile) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    std::string part1 =
        "SELECT x, y, NTILE(2) OVER (PARTITION BY y ORDER BY x ASC) n FROM " +
        table_name + " ORDER BY x ASC";
    std::string part2 = ", y ASC, n ASC;";
    c(part1 + " NULLS FIRST" + part2, part1 + part2, dt);
  }
}

TEST_F(Select, WindowFunctionCumeDist) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    std::string part1 =
        "SELECT x, y, CUME_DIST() OVER (PARTITION BY y ORDER BY x ASC) c FROM " +
        table_name + " ORDER BY x ASC";
    std::string part2 = ", y ASC, c ASC;";
    c(part1 + " NULLS FIRST" + part2, part1 + part2, dt);
  }
}

TEST_F(Select, WindowFunctionFiltered) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    std::string query =
        "SELECT MAX(CASE WHEN y <> 'aaa' THEN t ELSE NULL END) OVER (PARTITION BY x "
        "ORDER BY t ASC) AS labelrank_max_filtered FROM " +
        table_name +
        " ORDER BY t ASC NULLS FIRST, y ASC NULLS FIRST, labelrank_max_filtered NULLS "
        "FIRST;";
    c(query, query, dt);
  }
}

// lag(expr, offset)
// lead(expr, offset)
// SQLite: "If the offset argument is provided, then it must be a non-negative integer."
// https://www.sqlite.org/windowfunctions.html
// OmniSci allows for offset < 0, so we swap LAG and LEAD and use -offset to test.
// SQLite : ASC -> ASC NULLS FIRST
// OmniSci: ASC -> ASC NULLS LAST
// and vice-versa for DESC. To prevent conflict, add NULLS FIRST/LAST if there are NULLS.
TEST_F(Select, WindowFunctionLag) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    // First test default lag (1)
    {
      std::string part1 =
          "SELECT x, y, LAG(x + 5) OVER (PARTITION BY y ORDER BY x ASC NULLS LAST) l "
          "FROM " +
          table_name +
          " ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, l ASC NULLS "
          "FIRST;";
      c(part1, dt);
    }
    {
      std::string part1 =
          "SELECT x, LAG(y) OVER (PARTITION BY y ORDER BY x ASC NULLS LAST) l FROM " +
          table_name + " ORDER BY x ASC NULLS FIRST, l ASC NULLS FIRST;";
      c(part1, dt);
    }

    for (int lag = -5; lag <= 5; ++lag) {
      {
        std::string part1 =
            "SELECT x, y, LAG(x + 5, " + std::to_string(lag) +
            ") OVER (PARTITION BY y ORDER BY x ASC NULLS FIRST) l FROM " + table_name +
            " ORDER BY x ASC NULLS LAST, y ASC NULLS LAST, l ASC NULLS LAST;";
        if (lag < 0) {
          std::string sqlite = boost::replace_first_copy(part1, "LAG", "LEAD");
          boost::erase_first(sqlite, "-");
          c(part1, sqlite, dt);
        } else {
          c(part1, dt);
        }
      }
      {
        std::string part1 = "SELECT x, LAG(y, " + std::to_string(lag) +
                            ") OVER (PARTITION BY y ORDER BY x ASC NULLS LAST) l FROM " +
                            table_name +
                            " ORDER BY x ASC NULLS FIRST, l ASC NULLS FIRST;";
        if (lag < 0) {
          std::string sqlite = boost::replace_first_copy(part1, "LAG", "LEAD");
          boost::erase_first(sqlite, "-");
          c(part1, sqlite, dt);
        } else {
          c(part1, dt);
        }
      }
    }
  }
}

TEST_F(Select, WindowFunctionMultiOrderBy) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name :
       {"test_window_func_large", "test_window_func_large_multi_frag"}) {
    {
      std::string query =
          "SELECT LAG(f) OVER (ORDER BY f NULLS FIRST, d NULLS FIRST) AS f_lag FROM " +
          table_name + " ORDER BY f_lag ASC NULLS FIRST;";
      c(query, query, dt);
    }

    {
      std::string query =
          "SELECT LAG(f) OVER (ORDER BY d NULLS FIRST, f DESC NULLS FIRST) AS f_lag "
          "FROM " +
          table_name + " ORDER BY f_lag ASC NULLS FIRST;";
      c(query, query, dt);
    }

    {
      std::string query =
          "SELECT LAG(d) OVER (ORDER BY f DESC NULLS FIRST, f ASC NULLS FIRST) AS d_lag "
          "FROM " +
          table_name + " ORDER BY d_lag ASC NULLS FIRST;";
      c(query, query, dt);
    }

    {
      std::string query =
          "SELECT LAG(d) OVER (ORDER BY d DESC NULLS FIRST, d DESC NULLS FIRST) AS d_lag "
          "FROM " +
          table_name + " ORDER BY d_lag ASC NULLS FIRST;";
      c(query, query, dt);
    }

    {
      std::string query =
          "SELECT LAG(i_unique) OVER (ORDER BY i_20 ASC NULLS FIRST, i_unique DESC NULLS "
          "FIRST) AS i_unique_lag FROM " +
          table_name + " ORDER BY i_unique_lag ASC NULLS FIRST;";
      c(query, query, dt);
    }

    {
      std::string query =
          "SELECT LAG(i_unique) OVER (ORDER BY i_20 ASC NULLS FIRST, i_1000 DESC NULLS "
          "FIRST, d ASC NULLS FIRST, i_unique DESC NULLS FIRST) AS i_unique_lag FROM " +
          table_name + " ORDER BY i_unique_lag ASC NULLS FIRST;";
      c(query, query, dt);
    }
  }
}

TEST_F(Select, WindowFunctionFirst) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    {
      std::string part1 =
          "SELECT x, y, FIRST_VALUE(x + 5) OVER (PARTITION BY y ORDER BY x ASC NULLS "
          "LAST) "
          "f FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = ", y ASC NULLS LAST, f ASC;";
      c(part1 + " NULLS FIRST" + part2, part1 + part2, dt);
    }
    {
      std::string part1 =
          "SELECT x, FIRST_VALUE(y) OVER (PARTITION BY t ORDER BY x ASC) f FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = ", f ASC NULLS LAST;";
      c(part1 + " NULLS FIRST" + part2, part1 + part2, dt);
    }
  }
}

TEST_F(Select, WindowFunctionLead) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    // First test default lead (1)
    {
      std::string part1 =
          "SELECT x, y, LEAD(x) OVER (PARTITION BY y ORDER BY x DESC NULLS FIRST) l "
          "FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = ", y ASC NULLS LAST, l ASC";
      c(part1 + " NULLS FIRST" + part2 + " NULLS FIRST;", part1 + part2 + ";", dt);
    }
    {
      std::string part1 =
          "SELECT x, LEAD(y) OVER (PARTITION BY y ORDER BY x DESC NULLS FIRST) l FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = ", l ASC";
      c(part1 + " NULLS FIRST" + part2 + " NULLS FIRST;", part1 + part2 + ";", dt);
    }

    for (int lead = -5; lead <= 5; ++lead) {
      {
        std::string part1 =
            "SELECT x, y, LEAD(x, " + std::to_string(lead) +
            ") OVER (PARTITION BY y ORDER BY x DESC NULLS FIRST) l FROM " + table_name +
            " ORDER BY x ASC NULLS FIRST, y ASC NULLS LAST, l ASC NULLS FIRST;";
        if (lead < 0) {
          std::string sqlite = boost::replace_first_copy(part1, "LEAD", "LAG");
          boost::erase_first(sqlite, "-");
          c(part1, sqlite, dt);
        } else {
          c(part1, dt);
        }
      }
      {
        std::string part1 =
            "SELECT x, LEAD(y, " + std::to_string(lead) +
            ") OVER (PARTITION BY y ORDER BY x DESC NULLS FIRST) l FROM " + table_name +
            " ORDER BY x ASC NULLS FIRST, l ASC NULLS FIRST;";
        if (lead < 0) {
          std::string sqlite = boost::replace_first_copy(part1, "LEAD", "LAG");
          boost::erase_first(sqlite, "-");
          c(part1, sqlite, dt);
        } else {
          c(part1, dt);
        }
      }
    }
  }
}

TEST_F(Select, WindowFunctionLast) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    {
      std::string part1 =
          "SELECT x, y, FIRST_VALUE(x + 5) OVER (PARTITION BY y ORDER BY x ASC) f, "
          "LAST_VALUE(x) OVER (PARTITION BY y ORDER BY x DESC) l FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = ", y ASC, f ASC, l ASC;";
      c(part1 + " NULLS FIRST" + part2, part1 + part2, dt);
    }
    {
      std::string part1 =
          "SELECT x, LAST_VALUE(y) OVER (PARTITION BY t ORDER BY x ASC) f "
          "FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = ", f ASC;";
      c(part1 + " NULLS FIRST" + part2, part1 + part2, dt);
    }
  }
}

TEST_F(Select, WindowFunctionAggregate) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    {
      std::string part1 =
          "SELECT x, y, AVG(x) OVER (PARTITION BY y ORDER BY x ASC) a, MIN(x) OVER "
          "(PARTITION BY y ORDER BY x ASC) m1, MAX(x) OVER (PARTITION BY y ORDER BY x "
          "DESC) m2, SUM(x) OVER (PARTITION BY y ORDER BY x ASC) s, COUNT(x) OVER "
          "(PARTITION BY y ORDER BY x ASC) c FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = "a ASC, m1 ASC, m2 ASC, s ASC, c ASC;";
      c(part1 + " NULLS FIRST, y ASC NULLS FIRST, " + part2,
        part1 + ", y ASC, " + part2,
        dt);
    }
    {
      std::string part1 =
          "SELECT x, y, AVG(CAST(x AS FLOAT)) OVER (PARTITION BY y ORDER BY x ASC) a, "
          "MIN(CAST(x AS FLOAT)) OVER (PARTITION BY y ORDER BY x ASC) m1, MAX(CAST(x AS "
          "FLOAT)) OVER (PARTITION BY y ORDER BY x DESC) m2, SUM(CAST(x AS FLOAT)) OVER "
          "(PARTITION BY y ORDER BY x ASC) s, COUNT(CAST(x AS FLOAT)) OVER (PARTITION BY "
          "y "
          "ORDER BY x ASC) c FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = "a ASC, m1 ASC, m2 ASC, s ASC, c ASC;";
      c(part1 + " NULLS FIRST, y ASC NULLS FIRST, " + part2,
        part1 + ", y ASC, " + part2,
        dt);
    }
    {
      std::string part1 =
          "SELECT x, y, AVG(CAST(x AS DOUBLE)) OVER (PARTITION BY y ORDER BY x ASC) a, "
          "MIN(CAST(x AS DOUBLE)) OVER (PARTITION BY y ORDER BY x ASC) m1, MAX(CAST(x AS "
          "DOUBLE)) OVER (PARTITION BY y ORDER BY x DESC) m2, SUM(CAST(x AS DOUBLE)) "
          "OVER "
          "(PARTITION BY y ORDER BY x ASC) s, COUNT(CAST(x AS DOUBLE)) OVER (PARTITION "
          "BY "
          "y ORDER BY x ASC) c FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = "a ASC, m1 ASC, m2 ASC, s ASC, c ASC;";
      c(part1 + " NULLS FIRST, y ASC NULLS FIRST, " + part2,
        part1 + ", y ASC, " + part2,
        dt);
    }
    {
      std::string part1 =
          "SELECT x, y, AVG(CAST(x AS DECIMAL(10, 2))) OVER (PARTITION BY y ORDER BY x "
          "ASC) a, MIN(CAST(x AS DECIMAL(10, 2))) OVER (PARTITION BY y ORDER BY x ASC) "
          "m1, "
          "MAX(CAST(x AS DECIMAL(10, 2))) OVER (PARTITION BY y ORDER BY x DESC) m2, "
          "SUM(CAST(x AS DECIMAL(10, 2))) OVER (PARTITION BY y ORDER BY x ASC) s, "
          "COUNT(CAST(x AS DECIMAL(10, 2))) OVER (PARTITION BY y ORDER BY x ASC) c "
          "FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = "a ASC, m1 ASC, m2 ASC, s ASC, c ASC;";
      c(part1 + " NULLS FIRST, y ASC NULLS FIRST, " + part2,
        part1 + ", y ASC, " + part2,
        dt);
    }
    {
      std::string part1 =
          "SELECT x, y, AVG(x) OVER (PARTITION BY y ORDER BY d ASC) a, MIN(x) OVER "
          "(PARTITION BY y ORDER BY f ASC) m1, MAX(x) OVER (PARTITION BY y ORDER BY dd "
          "DESC) m2, SUM(x) OVER (PARTITION BY y ORDER BY d ASC) s, COUNT(x) OVER "
          "(PARTITION BY y ORDER BY f ASC) c FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = "a ASC, m1 ASC, m2 ASC, s ASC, c ASC;";
      c(part1 + " NULLS FIRST, y ASC NULLS FIRST, " + part2,
        part1 + ", y ASC, " + part2,
        dt);
    }
    {
      std::string query =
          "SELECT y, COUNT(t) OVER (PARTITION BY y ORDER BY x ASC) s FROM " + table_name +
          " ORDER BY s ASC, y ASC";
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query =
          "SELECT y, COUNT(CAST(t AS FLOAT)) OVER (PARTITION BY y ORDER BY x ASC) s "
          "FROM " +
          table_name + " ORDER BY s ASC, y ASC";
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query =
          "SELECT y, COUNT(CAST(t AS DOUBLE)) OVER (PARTITION BY y ORDER BY x ASC) s "
          "FROM " +
          table_name + " ORDER BY s ASC, y ASC";
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query =
          "SELECT y, COUNT(CAST(t AS DECIMAL(10, 2))) OVER (PARTITION BY y ORDER BY x "
          "ASC) "
          "s FROM " +
          table_name + " ORDER BY s ASC, y ASC";
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query =
          "SELECT y, MAX(d) OVER (PARTITION BY y ORDER BY x ASC) m FROM " + table_name +
          " ORDER BY y ASC";
      c(query + " NULLS FIRST, m ASC NULLS FIRST;", query + ", m ASC;", dt);
    }
    {
      std::string query =
          "SELECT y, MIN(d) OVER (PARTITION BY y ORDER BY x ASC) m FROM " + table_name +
          " ORDER BY y ASC";
      c(query + " NULLS FIRST, m ASC NULLS FIRST;", query + ", m ASC;", dt);
    }
    {
      std::string query =
          "SELECT y, COUNT(d) OVER (PARTITION BY y ORDER BY x ASC) m FROM " + table_name +
          " ORDER BY y ASC";
      c(query + " NULLS FIRST, m ASC NULLS FIRST;", query + ", m ASC;", dt);
    }
    {
      std::string query =
          "SELECT x, COUNT(t) OVER (PARTITION BY x ORDER BY x ASC) m FROM " + table_name +
          " WHERE x < 5 ORDER BY x ASC";
      c(query + " NULLS FIRST, m ASC NULLS FIRST;", query + ", m ASC;", dt);
    }
    {
      std::string query =
          "SELECT x, COUNT(t) OVER (PARTITION BY y ORDER BY x ASC) m FROM " + table_name +
          " WHERE x < 5 ORDER BY x ASC";
      c(query + " NULLS FIRST, m ASC NULLS FIRST;", query + ", m ASC;", dt);
    }
    {
      std::string query =
          "SELECT COUNT(*) OVER (PARTITION BY y ORDER BY x ASC), x, y FROM " +
          table_name + " ORDER BY x LIMIT 1;";
      const auto rows = run_multiple_agg(query, dt);
      ASSERT_EQ(rows->rowCount(), size_t(1));
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(crt_row.size(), size_t(3));
      ASSERT_EQ(v<int64_t>(crt_row[0]), int64_t(1));
      ASSERT_EQ(v<int64_t>(crt_row[1]), int64_t(0));
      ASSERT_EQ(boost::get<std::string>(v<NullableString>(crt_row[2])), "aaa");
    }

    {
      std::string query =
          "SELECT x, RANK() OVER (PARTITION BY y ORDER BY n ASC NULLS FIRST) r FROM "
          "(SELECT x, "
          "y, COUNT(*) n FROM " +
          table_name +
          " GROUP BY x, y) ORDER BY x ASC NULLS FIRST, y "
          "ASC NULLS FIRST;";
      c(query, query, dt);
    }
    {
      std::string query = "SELECT x, y, t, SUM(SUM(x)) OVER (PARTITION BY y, t) FROM " +
                          table_name +
                          " GROUP BY x, y, t ORDER BY x ASC NULLS FIRST, y ASC NULLS "
                          "FIRST, t ASC NULLS FIRST;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT x, y, t, SUM(x) * SUM(SUM(x)) OVER (PARTITION BY y, t) FROM " +
          table_name +
          " GROUP BY x, y, t ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, t ASC NULLS "
          "FIRST;";
      c(query, query, dt);
    }
  }
}

TEST_F(Select, WindowFunctionAggregateNoOrder) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    {
      std::string part1 =
          "SELECT x, y, AVG(x) OVER (PARTITION BY y) a, MIN(x) OVER (PARTITION BY y) m1, "
          "MAX(x) OVER (PARTITION BY y) m2, SUM(x) OVER (PARTITION BY y) s, COUNT(x) "
          "OVER "
          "(PARTITION BY y) c FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = "a ASC, m1 ASC, m2 ASC, s ASC, c ASC;";
      c(part1 + " NULLS FIRST, y ASC NULLS FIRST, " + part2,
        part1 + ", y ASC, " + part2,
        dt);
    }
    {
      std::string part1 =
          "SELECT x, y, AVG(CAST(x AS FLOAT)) OVER (PARTITION BY y) a, MIN(CAST(x AS "
          "FLOAT)) OVER (PARTITION BY y) m1, MAX(CAST(x AS FLOAT)) OVER (PARTITION BY y) "
          "m2, SUM(CAST(x AS FLOAT)) OVER (PARTITION BY y) s, COUNT(CAST(x AS FLOAT)) "
          "OVER "
          "(PARTITION BY y) c FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = "a ASC, m1 ASC, m2 ASC, s ASC, c ASC;";
      c(part1 + " NULLS FIRST, y ASC NULLS FIRST, " + part2,
        part1 + ", y ASC, " + part2,
        dt);
    }
    {
      std::string part1 =
          "SELECT x, y, AVG(CAST(x AS DOUBLE)) OVER (PARTITION BY y) a, MIN(CAST(x AS "
          "DOUBLE)) OVER (PARTITION BY y) m1, MAX(CAST(x AS DOUBLE)) OVER (PARTITION BY "
          "y) "
          "m2, SUM(CAST(x AS DOUBLE)) OVER (PARTITION BY y) s, COUNT(CAST(x AS DOUBLE)) "
          "OVER (PARTITION BY y) c FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = "a ASC, m1 ASC, m2 ASC, s ASC, c ASC;";
      c(part1 + " NULLS FIRST, y ASC NULLS FIRST, " + part2,
        part1 + ", y ASC, " + part2,
        dt);
    }
    {
      std::string part1 =
          "SELECT x, y, AVG(CAST(x AS DECIMAL(10, 2))) OVER (PARTITION BY y) a, "
          "MIN(CAST(x "
          "AS DECIMAL(10, 2))) OVER (PARTITION BY y) m1, MAX(CAST(x AS DECIMAL(10, 2))) "
          "OVER (PARTITION BY y) m2, SUM(CAST(x AS DECIMAL(10, 2))) OVER (PARTITION BY "
          "y) "
          "s, COUNT(CAST(x AS DECIMAL(10, 2))) OVER (PARTITION BY y) c FROM " +
          table_name + " ORDER BY x ASC";
      std::string part2 = "a ASC, m1 ASC, m2 ASC, s ASC, c ASC;";
      c(part1 + " NULLS FIRST, y ASC NULLS FIRST, " + part2,
        part1 + ", y ASC, " + part2,
        dt);
    }
    {
      std::string query = "SELECT y, COUNT(t) OVER (PARTITION BY y) c FROM " +
                          table_name +
                          " ORDER BY c "
                          "ASC, y ASC";
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query =
          "SELECT y, COUNT(CAST(t AS FLOAT)) OVER (PARTITION BY y) c FROM " + table_name +
          " ORDER BY c ASC, y ASC";
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query =
          "SELECT y, COUNT(CAST(t AS DOUBLE)) OVER (PARTITION BY y) c FROM " +
          table_name + " ORDER BY c ASC, y ASC";
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query =
          "SELECT y, COUNT(CAST(t AS DECIMAL(10, 2))) OVER (PARTITION BY y) c FROM " +
          table_name + " ORDER BY c ASC, y ASC";
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query = "SELECT y, MAX(d) OVER (PARTITION BY y) m FROM " + table_name +
                          " ORDER BY y ASC";
      c(query + " NULLS FIRST, m ASC NULLS FIRST;", query + ", m ASC;", dt);
    }
    {
      std::string query = "SELECT y, MIN(d) OVER (PARTITION BY y) m FROM " + table_name +
                          " ORDER BY y ASC";
      c(query + " NULLS FIRST, m ASC NULLS FIRST;", query + ", m ASC;", dt);
    }
    {
      std::string query = "SELECT y, COUNT(d) OVER (PARTITION BY y) m FROM " +
                          table_name + " ORDER BY y ASC";
      c(query + " NULLS FIRST, m ASC NULLS FIRST;", query + ", m ASC;", dt);
    }
  }
}

TEST_F(Select, WindowFunctionSum) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    {
      std::string query =
          "SELECT total FROM (SELECT SUM(n) OVER (PARTITION BY y) AS total FROM (SELECT "
          "y, "
          "COUNT(*) AS n FROM " +
          table_name + " GROUP BY y)) ORDER BY total ASC;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT total FROM (SELECT SUM(x) OVER (PARTITION BY y) AS total FROM (SELECT "
          "x, y "
          "FROM " +
          table_name + ")) ORDER BY total ASC NULLS FIRST";
      c(query, query, dt);
    }
  }
}

TEST_F(Select, WindowFunctionComplexExpressions) {
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;
  for (std::string table_name : {"test_window_func", "test_window_func_multi_frag"}) {
    {
      std::string query =
          "SELECT x, y, ROW_NUMBER() OVER (PARTITION BY y ORDER BY x ASC) - 1 r1 FROM " +
          table_name + " ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, r1 ASC;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT x, y, ROW_NUMBER() OVER (PARTITION BY y ORDER BY x ASC) - 1 r1, RANK() "
          "OVER (PARTITION BY y ORDER BY x ASC) + 1 r2, 1 + ( DENSE_RANK() OVER "
          "(PARTITION BY y ORDER BY x DESC) * 200 ) r3 FROM " +
          table_name +
          " ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, r1 ASC, r2 ASC, r3 ASC;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT x, y, x - RANK() OVER (PARTITION BY x ORDER BY x ASC) r1, RANK() OVER "
          "(PARTITION BY y ORDER BY t ASC) / 2 r2 FROM " +
          table_name + " ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, r1 ASC, r2 ASC;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT x, y, t - AVG(f) OVER (PARTITION BY y ORDER BY x ASC) - 1 r1, AVG(dd) "
          "OVER (PARTITION BY y ORDER BY t ASC) / 2 r2 FROM " +
          table_name +
          " ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, t ASC NULLS FIRST, r1 ASC, r2 "
          "ASC;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT x, y, t - AVG(f) OVER (PARTITION BY y ORDER BY x ASC) - 1 r1, CASE "
          "WHEN x > 5 THEN 10 ELSE SUM(x) OVER (PARTITION BY y ORDER BY t ASC) END r2 "
          "FROM " +
          table_name +
          " ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, t ASC NULLS FIRST, r1 ASC, r2 "
          "ASC;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT x, y, t - AVG(f) OVER (PARTITION BY y ORDER BY x ASC) - 1 r1, CASE "
          "WHEN x > 5 THEN SUM(x) OVER (PARTITION BY y ORDER BY t ASC) ELSE 10 END r2 "
          "FROM " +
          table_name +
          " ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, t ASC NULLS FIRST, r1 ASC, r2 "
          "ASC;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT x, y, t - AVG(f) OVER (PARTITION BY y ORDER BY x ASC) - 1 r1, CASE "
          "WHEN SUM(x) OVER (PARTITION BY y ORDER BY t ASC) > 1 THEN 5 ELSE 10 END r2 "
          "FROM " +
          table_name +
          " ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, t ASC NULLS FIRST, r1 ASC, r2 "
          "ASC;";
      c(query, query, dt);
    }
    {
      std::string query =
          "SELECT x, y, t - SUM(f) OVER (PARTITION BY y ORDER BY x ASC) - 1 r1, CASE "
          "WHEN x > 5 THEN 10 ELSE AVG(x) OVER (PARTITION BY y ORDER BY t ASC) END r2 "
          "FROM " +
          table_name +
          " ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, t ASC NULLS FIRST, r1 ASC, r2 "
          "ASC;";
      c(query, query, dt);
    }
    {
      // TODO(adb): support more complex embedded case expressions
      std::string query =
          "SELECT x, y, t - AVG(f) OVER (PARTITION BY y ORDER BY x ASC) - 1 r1, CASE "
          "WHEN x > 5 THEN AVG(dd) OVER (PARTITION BY y ORDER BY t ASC) ELSE SUM(x) OVER "
          "(PARTITION BY y ORDER BY t ASC) END r2 FROM " +
          table_name +
          " ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, t ASC NULLS FIRST, r1 ASC, r2 "
          "ASC;";
      EXPECT_THROW(run_multiple_agg(query, dt), std::runtime_error);
    }
  }
}

TEST_F(Select, FilterNodeCoalesce) {
  // If we do not coalesce the filter with a subsequent project (manufacturing one if
  // neccessary), we currently pull all table columns into memory, which is highly
  // undesirable. For window functions with a preceding filter node, we can not coalesce
  // the filter node into the window function projection node, as this leads to incorrect
  // results, so we manufacture a preceding projection

  // Running on GPU with new inter-mixed executon means memory is not all in one buffer
  // pool
  const ExecutorDeviceType dt = ExecutorDeviceType::CPU;

  // One-level projection - sanity test
  {
    // Clear CPU memory and hash table caches
    clearCpuMemory();
    {
      std::string query =
          "SELECT x, t, x * t  FROM test_window_func WHERE x >= 3 ORDER BY x ASC NULLS "
          "FIRST, t ASC NULLS FIRST;";
      c(query, query, dt);
    }
    const auto buffer_pool_stats = getBufferPoolStats();
    ASSERT_GE(buffer_pool_stats.num_buffers, static_cast<size_t>(2));
    ASSERT_EQ(buffer_pool_stats.num_tables, static_cast<size_t>(1));
    ASSERT_EQ(buffer_pool_stats.num_columns, static_cast<size_t>(2));
    ASSERT_EQ(buffer_pool_stats.num_fragments, static_cast<size_t>(1));
    ASSERT_EQ(buffer_pool_stats.num_chunks, static_cast<size_t>(2));
  }

  // Single-step window function
  {
    // Clear CPU memory and hash table caches
    clearCpuMemory();
    {
      std::string query =
          "SELECT x, y, LAG(f) OVER (PARTITION BY y ORDER BY x ASC) f_lag FROM "
          "test_window_func WHERE x >= 3 ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, "
          "f_lag ASC;";
      c(query, query, dt);
    }

    const auto buffer_pool_stats = getBufferPoolStats();
    ASSERT_GE(buffer_pool_stats.num_buffers, static_cast<size_t>(3));
    ASSERT_EQ(buffer_pool_stats.num_tables, static_cast<size_t>(1));
    ASSERT_EQ(buffer_pool_stats.num_columns, static_cast<size_t>(3));
    ASSERT_EQ(buffer_pool_stats.num_fragments, static_cast<size_t>(1));
    ASSERT_EQ(buffer_pool_stats.num_chunks, static_cast<size_t>(3));
  }

  // Multi-step window function to ensure project is inserted before each window step
  {
    // Clear CPU memory and hash table caches
    clearCpuMemory();
    {
      std::string query =
          "SELECT x, y, RANK() OVER (PARTITION BY y ORDER BY x ASC NULLS FIRST) rk FROM "
          "(SELECT x, y, LAG(f) OVER (PARTITION BY y ORDER BY x ASC) f_lag FROM "
          "test_window_func WHERE x >= 3) foo WHERE x >= 3 ORDER BY x ASC NULLS FIRST, y "
          "ASC NULLS FIRST, f_lag ASC;";
      c(query, query, dt);
    }

    const auto buffer_pool_stats = getBufferPoolStats();
    ASSERT_GE(buffer_pool_stats.num_buffers, static_cast<size_t>(3));
    ASSERT_EQ(buffer_pool_stats.num_tables, static_cast<size_t>(1));
    ASSERT_EQ(buffer_pool_stats.num_columns, static_cast<size_t>(3));
    ASSERT_EQ(buffer_pool_stats.num_fragments, static_cast<size_t>(1));
    ASSERT_EQ(buffer_pool_stats.num_chunks, static_cast<size_t>(3));
  }

  // Multi-fragment window function with filter should run due to preceding compound node
  {
    // Clear CPU memory and hash table caches
    clearCpuMemory();
    {
      std::string query =
          "SELECT x, y, d, SUM(d) OVER (PARTITION BY x ORDER BY d ASC NULLS FIRST) sum_d "
          "FROM test_x WHERE x > 6 ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, d ASC "
          "NULLS FIRST;";
      c(query, query, dt);
    }

    const auto buffer_pool_stats = getBufferPoolStats();
    ASSERT_GE(buffer_pool_stats.num_buffers, static_cast<size_t>(30));
    ASSERT_EQ(buffer_pool_stats.num_tables, static_cast<size_t>(1));
    ASSERT_EQ(buffer_pool_stats.num_columns, static_cast<size_t>(3));
    ASSERT_EQ(buffer_pool_stats.num_fragments, static_cast<size_t>(10));
    ASSERT_EQ(buffer_pool_stats.num_chunks, static_cast<size_t>(30));
  }

  {
    // Clear CPU memory and hash table caches
    clearCpuMemory();
    {
      std::string query =
          "SELECT x, y, d, SUM(d) OVER (PARTITION BY x ORDER BY d ASC NULLS FIRST) sum_d "
          "FROM test_x ORDER BY x ASC NULLS FIRST, y ASC NULLS FIRST, d ASC NULLS FIRST;";
      c(query, query, dt);
    }
    const auto buffer_pool_stats = getBufferPoolStats();
    ASSERT_GE(buffer_pool_stats.num_buffers, static_cast<size_t>(30));
    ASSERT_EQ(buffer_pool_stats.num_tables, static_cast<size_t>(1));
    ASSERT_EQ(buffer_pool_stats.num_columns, static_cast<size_t>(3));
    ASSERT_EQ(buffer_pool_stats.num_fragments, static_cast<size_t>(10));
    ASSERT_EQ(buffer_pool_stats.num_chunks, static_cast<size_t>(30));
  }
}

TEST_F(Select, EmptyString) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    EXPECT_THROW(run_multiple_agg("", dt), std::exception);
  }
}

TEST_F(Select, MultiStepColumnarization) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      std::string query(
          "SELECT T.x, MAX(T.y) FROM(SELECT x, log10(SUM(f)) as y FROM test GROUP BY x) "
          "as T "
          "GROUP BY T.x ORDER BY T.x;");
      const auto result = run_multiple_agg(query, dt);
      const auto first_row = result->getNextRow(true, true);
      ASSERT_EQ(size_t(2), first_row.size());
      ASSERT_EQ(int64_t(7), v<int64_t>(first_row[0]));
      ASSERT_FLOAT_EQ(double(1.243038177490234), v<double>(first_row[1]));
      const auto second_row = result->getNextRow(true, true);
      ASSERT_EQ(int64_t(8), v<int64_t>(second_row[0]));
      ASSERT_FLOAT_EQ(double(0.778151273727417), v<double>(second_row[1]));
    }
    // single-column perfect hash, columnarization, and then a projection
    c("SELECT id, SUM(big_int) / SUM(float_not_null), MAX(small_int) / MAX(tiny_int), "
      "MIN(tiny_int) + MIN(small_int) FROM logical_size_test GROUP BY id ORDER BY id;",
      dt);
    c("SELECT id, AVG(small_int) + MIN(big_int) + MAX(tiny_int) + SUM(double_not_null) "
      "/ SUM(float_not_null) FROM logical_size_test GROUP BY id ORDER BY id;",
      dt);
    {
      std::string query(
          "SELECT fixed_str, COUNT(CAST(y AS double)) + SUM(x) FROM test GROUP BY "
          "fixed_str ORDER BY fixed_str ASC");
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      const auto result = run_multiple_agg(
          "SELECT id, SAMPLE(small_int), SUM(big_int) / COUNT(tiny_int) FROM "
          "logical_size_test GROUP BY id ORDER BY id LIMIT 1 OFFSET 1;",
          dt);
      const auto first_row = result->getNextRow(true, true);
      ASSERT_EQ(size_t(3), first_row.size());
      ASSERT_EQ(int64_t(5), v<int64_t>(first_row[0]));
      ASSERT_TRUE((int64_t(79) == v<int64_t>(first_row[1])) ||
                  ((int64_t(76) == v<int64_t>(first_row[1]))));
      ASSERT_EQ(int64_t(2252), v<int64_t>(first_row[2]));
    }
    // multi-column perfect hash, columnarization, and then a projection
    c("SELECT id, small_int, MAX(float_not_null) + MAX(double_not_null), MAX(id_null), "
      "MAX(small_int_null), MAX(tiny_int), MAX(tiny_int_null), MAX(float_null), "
      "MAX(double_null), "
      "MIN(id_null), MIN(small_int_null), MIN(tiny_int), MIN(tiny_int_null), "
      "MIN(float_null), MIN(double_null), "
      "COUNT(id_null), COUNT(small_int_null), COUNT(tiny_int), COUNT(tiny_int_null), "
      "COUNT(float_null), COUNT(double_null) "
      "FROM logical_size_test GROUP BY id, small_int ORDER BY id, small_int;",
      dt);

    c("SELECT small_int, tiny_int, id, SUM(float_not_null) "
      "/ (case when COUNT(big_int) = 0 then 1 else COUNT(big_int) end) FROM "
      "logical_size_test GROUP BY small_int, tiny_int, id ORDER BY id, tiny_int, "
      "small_int;",
      dt);
    {
      std::string query(
          "SELECT x, fixed_str, COUNT(*), SUM(t), SUM(dd), SUM(dd_notnull), MAX(ofd), "
          "MAX(ufd), COUNT(ofq), COUNT(ufq) FROM test GROUP BY x, fixed_str ORDER BY x, "
          "fixed_str ASC");
      c(query + " NULLS FIRST;", query + ";", dt);
    }
    {
      std::string query(
          "SELECT DATE_TRUNC(MONTH, o) AS month_, DATE_TRUNC(DAY, m) AS day_, COUNT(*), "
          "SUM(x) + SUM(y), SAMPLE(t) FROM test GROUP BY month_, day_ ORDER BY month_, "
          "day_ LIMIT 1;");
      const auto result = run_multiple_agg(query, dt);
      const auto first_row = result->getNextRow(true, true);
      ASSERT_EQ(size_t(5), first_row.size());
      ASSERT_EQ(int64_t(936144000), v<int64_t>(first_row[0]));
      ASSERT_EQ(int64_t(1418428800), v<int64_t>(first_row[1]));
      ASSERT_EQ(int64_t(10), v<int64_t>(first_row[2]));
      ASSERT_EQ(int64_t(490), v<int64_t>(first_row[3]));
      ASSERT_EQ(int64_t(1001), v<int64_t>(first_row[4]));
    }
    // baseline hash, columnarization, and then a projection
    c("SELECT cast (id as double) as key0, count(*) as cnt, big_int as key1 from "
      "logical_size_test group by key0, key1 having cnt < 4 order by key0, key1;",
      dt);
    c("SELECT cast (id as float) as key0, COUNT(*), SUM(float_not_null) + "
      "SUM(double_not_null), MAX(tiny_int_null), MIN(tiny_int) as min0, "
      "AVG(big_int) FROM logical_size_test GROUP BY key0 ORDER BY min0;",
      dt);
    {
      std::string query(
          "SELECT CAST(x as float) as key0, DATE_TRUNC(microsecond, m_6) as key1, dd as "
          "key2, EXTRACT(epoch from m) as key3, fixed_str as key4, COUNT(*), (SUM(y) + "
          "SUM(t)) / AVG(z), SAMPLE(f) + SAMPLE(d) FROM test GROUP BY key0, key1, key2, "
          "key3, key4 ORDER BY key2 LIMIT 1;");
      const auto result = run_multiple_agg(query, dt);
      const auto first_row = result->getNextRow(true, true);
      ASSERT_EQ(size_t(8), first_row.size());
      ASSERT_NEAR(float(7), v<float>(first_row[0]), 0.01);
      ASSERT_EQ(int64_t(931701773874533), v<int64_t>(first_row[1]));
      ASSERT_NEAR(double(111.1), v<double>(first_row[2]), 0.01);
      ASSERT_EQ(int64_t(1418509395), v<int64_t>(first_row[3]));
      ASSERT_EQ(std::string("foo"),
                boost::get<std::string>(v<NullableString>(first_row[4])));
      ASSERT_EQ(int64_t(10), v<int64_t>(first_row[5]));
      ASSERT_NEAR(double(103.267), v<double>(first_row[6]), 0.01);
      ASSERT_NEAR(double(3.3), v<double>(first_row[7]), 0.01);
    }
  }
}

TEST_F(Select, LogicalSizedColumns) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // non-grouped aggregate:
    c("SELECT MIN(tiny_int), MAX(tiny_int), MIN(tiny_int_null), MAX(tiny_int_null), "
      "COUNT(tiny_int), SUM(tiny_int), AVG(tiny_int) FROM logical_size_test;",
      dt);
    // single-column perfect hash group by:
    c("SELECT id, COUNT(tiny_int), COUNT(tiny_int_null), MAX(tiny_int), MIN(TINY_INT),"
      "SUM(tiny_int), SUM(tiny_int_null), AVG(tiny_int), AVG(tiny_int_null) "
      "FROM logical_size_test GROUP BY id ORDER BY id;",
      dt);
    c("SELECT id, COUNT(small_int_null), COUNT(small_int), SUM(small_int_null), "
      "SUM(small_int), AVG(small_int_null), AVG(small_int) "
      "FROM logical_size_test GROUP BY id ORDER BY id",
      dt);
    c("SELECT id, MAX(tiny_int), MAX(small_int_null), MAX(big_int), MAX(tiny_int_null),"
      "MAX(id_null), MAX(small_int) FROM logical_size_test GROUP BY id ORDER BY id;",
      dt);
    c("SELECT id, MIN(tiny_int), MIN(small_int_null), MIN(big_int_null), "
      "MIN(tiny_int_null),"
      "MIN(big_int), MIN(small_int) FROM logical_size_test GROUP BY id ORDER BY id;",
      dt);
    c("SELECT id, MAX(big_int_null), COUNT(small_int_null), COUNT(tiny_int) FROM "
      "logical_size_test GROUP BY id ORDER BY id;",
      dt);
    // single-slot SAMPLE statement:
    // 16-bit sample
    {
      const auto rows = run_multiple_agg(
          "SELECT id, SAMPLE(small_int), COUNT(*) FROM logical_size_test"
          " WHERE tiny_int < 0 GROUP BY id ORDER BY id;",
          dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), crt_row.size());
      ASSERT_EQ(int64_t(4), v<int64_t>(crt_row[0]));
      ASSERT_EQ(int64_t(75), v<int64_t>(crt_row[1]));
      ASSERT_EQ(int64_t(1), v<int64_t>(crt_row[2]));
    }
    // 8-bit sample
    {
      const auto rows = run_multiple_agg(
          "SELECT id, SAMPLE(tiny_int), MAX(small_int_null) FROM logical_size_test "
          " WHERE small_int < 76 GROUP BY id ORDER BY id;",
          dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), crt_row.size());
      ASSERT_EQ(int64_t(4), v<int64_t>(crt_row[0]));
      ASSERT_EQ(int64_t(-13), v<int64_t>(crt_row[1]));
      ASSERT_EQ(int64_t(-112), v<int64_t>(crt_row[2]));
    }
    // multi-slot SAMPLE statements:
    // CAS on 16-bit
    {
      const auto rows = run_multiple_agg(
          "SELECT id, SAMPLE(small_int), SAMPLE(tiny_int), SAMPLE(tiny_int_null), "
          "SAMPLE(small_int_null), SAMPLE(float_not_null) FROM logical_size_test"
          " WHERE big_int < 3000 GROUP BY id ORDER BY id;",
          dt);
      const auto crt_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(6), crt_row.size());
      ASSERT_EQ(int64_t(4), v<int64_t>(crt_row[0]));
      ASSERT_EQ(int64_t(75), v<int64_t>(crt_row[1]));
      ASSERT_EQ(int64_t(-13), v<int64_t>(crt_row[2]));
      ASSERT_EQ(int64_t(-125), v<int64_t>(crt_row[3]));
      ASSERT_EQ(int64_t(-112), v<int64_t>(crt_row[4]));
      ASSERT_NEAR(float(2.5), v<float>(crt_row[5]), 0.01);
    }
    // CAS on 8-bit:
    {
      const auto rows = run_multiple_agg(
          "SELECT id, SAMPLE(tiny_int), SAMPLE(small_int) FROM logical_size_test"
          " WHERE double_not_null < 20.0 GROUP BY id ORDER BY id;",
          dt);
      const auto first_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), first_row.size());
      ASSERT_EQ(int64_t(4), v<int64_t>(first_row[0]));
      ASSERT_TRUE(int64_t(20) == v<int64_t>(first_row[1]) ||
                  int64_t(16) == v<int64_t>(first_row[1]));
      ASSERT_EQ(int64_t(78), v<int64_t>(first_row[2]));
      const auto second_row = rows->getNextRow(true, true);
      ASSERT_EQ(size_t(3), second_row.size());
      ASSERT_EQ(int64_t(5), v<int64_t>(second_row[0]));
      ASSERT_EQ(int64_t(23), v<int64_t>(second_row[1]));
      ASSERT_EQ(int64_t(79), v<int64_t>(second_row[2]));
    }
  }
}

TEST_F(Select, GroupEmptyBlank) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    createTable(
        "blank_test", {{"t1", dictType(4, true)}, {"i1", SQLTypeInfo(kINT)}}, {10});
    insertCsvValues("blank_test", ",1\na,2");

    run_sqlite_query("DROP TABLE IF EXISTS blank_test;");
    run_sqlite_query("CREATE TABLE blank_test (t1 TEXT NOT NULL, i1 INTEGER);");
    run_sqlite_query("INSERT INTO blank_test VALUES('',1);");
    run_sqlite_query("INSERT INTO blank_test VALUES('a',2);");

    c("select t1 from blank_test group by t1 order by t1;", dt);

    run_sqlite_query("DROP TABLE IF EXISTS blank_test;");
    dropTable("blank_test");
  }
}

// Uses tables from import_union_all_tests().
TEST_F(Select, UnionAll) {
  for (auto dt : {ExecutorDeviceType::CPU /*, ExecutorDeviceType::GPU*/}) {
    SKIP_NO_GPU();
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 BETWEEN 111 AND 115"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      " WHERE b0 BETWEEN 211 AND 217"
      " ORDER BY a1;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      " ORDER BY a0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 116"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      " ORDER BY a0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      " WHERE b0 < 216"
      " ORDER BY a0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 116"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      " WHERE b0 < 216"
      " ORDER BY a0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 115"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      " WHERE b0 < 216"
      " ORDER BY a0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 116"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      " WHERE b0 < 215"
      " ORDER BY a0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 100"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      " WHERE b0 < 216"
      " ORDER BY a0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 116"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      " WHERE b0 < 200"
      " ORDER BY a0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 116"
      " UNION ALL"
      " SELECT MAX(b0), b1 % 2, MAX(b2), MAX(b3) FROM union_all_b"
      " GROUP BY b1 % 2"
      " ORDER BY a0;",
      dt);
    c("SELECT MAX(b0) max0, b1 % 2, MAX(b2), MAX(b3) FROM union_all_b"
      " GROUP BY b1 % 2"
      " UNION ALL"
      " SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 116"
      " ORDER BY max0;",
      dt);
    c("SELECT MAX(a0) max0, a1 % 3, MAX(a2), MAX(a3) FROM union_all_a"
      " GROUP BY a1 % 3"
      " UNION ALL"
      " SELECT MAX(b0), b1 % 2, MAX(b2), MAX(b3) FROM union_all_b"
      " GROUP BY b1 % 2"
      " ORDER BY max0;",
      dt);
    c("SELECT MAX(a0) max0, a1 % 2, MAX(a2), MAX(a3) FROM union_all_a"
      " GROUP BY a1 % 2"
      " UNION ALL"
      " SELECT MAX(b0), b1 % 3, MAX(b2), MAX(b3) FROM union_all_b"
      " GROUP BY b1 % 3"
      " ORDER BY max0;",
      dt);
    c("SELECT MAX(a0) max0, a1 % 3, MAX(a2), MAX(a3) FROM union_all_a"
      " GROUP BY a1 % 3"
      " UNION ALL"
      " SELECT MAX(b0), b1 % 2, MAX(b2), MAX(b3) FROM union_all_b"
      " GROUP BY b1 % 2"
      " UNION ALL"
      " SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 116"
      " ORDER BY max0;",
      dt);
    c("SELECT MAX(a0) max0, a1 % 2, MAX(a2), MAX(a3) FROM union_all_a"
      " GROUP BY a1 % 2"
      " UNION ALL"
      " SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 116"
      " UNION ALL"
      " SELECT MAX(b0), b1 % 3, MAX(b2), MAX(b3) FROM union_all_b"
      " GROUP BY b1 % 3"
      " ORDER BY max0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 116"
      " UNION ALL"
      " SELECT MAX(a0), a1 % 2, MAX(a2), MAX(a3) FROM union_all_a"
      " GROUP BY a1 % 2"
      " UNION ALL"
      " SELECT MAX(b0), b1 % 3, MAX(b2), MAX(b3) FROM union_all_b"
      " GROUP BY b1 % 3"
      " ORDER BY a0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 116"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      " WHERE b0 < 215"
      " UNION ALL"
      " SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 117"
      " UNION ALL"
      " SELECT MAX(a0), a1 % 2, MAX(a2), MAX(a3) FROM union_all_a"
      " GROUP BY a1 % 2"
      " UNION ALL"
      " SELECT MAX(b0), b1 % 3, MAX(b2), MAX(b3) FROM union_all_b"
      " GROUP BY b1 % 3"
      " ORDER BY a0;",
      dt);
    c("SELECT MAX(b0) max0, b1 % 3, MAX(b2), MAX(b3) FROM union_all_b"
      " GROUP BY b1 % 3"
      " HAVING b1 % 3 = 1"
      " UNION ALL"
      " SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 117"
      " ORDER BY max0;",
      dt);
    c("SELECT a0, a1, a2, a3 FROM union_all_a"
      " WHERE a0 < 117"
      " UNION ALL"
      " SELECT MAX(b0) max0, b1 % 3, MAX(b2), MAX(b3) FROM union_all_b"
      " GROUP BY b1 % 3"
      " HAVING b1 % 3 = 1"
      " ORDER BY a0;",
      dt);
    // sqlite sorts NULLs differently, and doesn't recognize NULLS FIRST/LAST.
    c("SELECT str FROM test"
      " UNION ALL"
      " SELECT COALESCE(shared_dict,'NULL') FROM test"
      " ORDER BY str;",
      dt);
    c("SELECT DISTINCT * FROM ("
      " SELECT a0, a1, a2, a3 FROM union_all_a"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      ") ORDER BY a0, a1, a2, a3;",
      dt);
    c("SELECT * FROM ("
      " SELECT a0, a1, a2, a3 FROM union_all_a"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      ") GROUP BY a0, a1, a2, a3"
      " ORDER BY a0, a1, a2, a3;",
      dt);
    c("SELECT * FROM ("
      " SELECT a0, a1, a2, a3 FROM union_all_a"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      ") GROUP BY a0, a1, a2, a3"
      " ORDER BY a0, a1, a2, a3;",
      dt);
    c("SELECT * FROM ("
      " SELECT a0, a1, a2, a3 FROM union_all_a"
      " UNION ALL"
      " SELECT b0, b1, b2, b3 FROM union_all_b"
      ") GROUP BY a0, a1, a2, a3"
      " ORDER BY a0, a1, a2, a3 LIMIT 4;",
      dt);
    // The goal is that these should work.
    // Exception: Subqueries of a UNION must have exact same data types.
    EXPECT_THROW(run_multiple_agg("SELECT str FROM test UNION ALL "
                                  "SELECT real_str FROM test ORDER BY str;",
                                  dt),
                 std::runtime_error);
    // Exception: Columnar conversion not supported for variable length types
    EXPECT_THROW(run_multiple_agg("SELECT real_str FROM test UNION ALL "
                                  "SELECT real_str FROM test ORDER BY real_str;",
                                  dt),
                 std::runtime_error);
    // Exception: Subqueries of a UNION must have exact same data types.
    EXPECT_THROW(run_multiple_agg("SELECT str FROM test UNION ALL "
                                  "SELECT fixed_str FROM test ORDER BY str;",
                                  dt),
                 std::runtime_error);
    // Exception: UNION ALL not yet supported in this context.
    EXPECT_THROW(run_multiple_agg("SELECT COUNT(*) FROM ("
                                  " SELECT a0, a1, a2, a3 FROM union_all_a"
                                  " UNION ALL"
                                  " SELECT b0, b1, b2, b3 FROM union_all_b"
                                  ");",
                                  dt),
                 std::runtime_error);
  }
}

TEST_F(Select, VariableLengthAggs) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // non-encoded strings:
    c("SELECT x, COUNT(real_str) FROM test GROUP BY x ORDER BY x desc;", dt);
    EXPECT_THROW(run_multiple_agg(
                     "SELECT x, MIN(real_str) FROM test GROUP BY x ORDER BY x DESC;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg(
                     "SELECT x, MAX(real_str) FROM test GROUP BY x ORDER BY x DESC;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg(
                     "SELECT x, SUM(real_str) FROM test GROUP BY x ORDER BY x DESC;", dt),
                 std::runtime_error);
    EXPECT_THROW(run_multiple_agg(
                     "SELECT x, AVG(real_str) FROM test GROUP BY x ORDER BY x DESC;", dt),
                 std::runtime_error);

    // arrays:
    {
      std::string query("SELECT x, COUNT(arr_i16) FROM array_test GROUP BY x;");
      auto result = run_multiple_agg(query, dt);
      ASSERT_EQ(result->rowCount(), size_t(g_array_test_row_count));
    }
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT x, MIN(arr_i32) FROM array_test GROUP BY x ORDER BY x DESC;", dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT x, MAX(arr3_i64) FROM array_test GROUP BY x ORDER BY x DESC;", dt),
        std::runtime_error);
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT x, SUM(arr3_float) FROM array_test GROUP BY x ORDER BY x DESC;", dt),
        std::exception);
    EXPECT_THROW(
        run_multiple_agg(
            "SELECT x, AVG(arr_double) FROM array_test GROUP BY x ORDER BY x DESC;", dt),
        std::exception);
  }
}

TEST_F(Select, Interop) {
  config().exec.enable_interop = true;
  ScopeGuard interop_guard = [] { config().exec.enable_interop = false; };
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    c("SELECT 'dict_' || str c1, 'fake_' || substring(real_str, 6) c2, x + 56 c3, f c4, "
      "-d c5, smallint_nulls c6 FROM test WHERE ('fake_' || substring(real_str, 6)) LIKE "
      "'%_ba%' ORDER BY c1 ASC, c2 ASC, c3 ASC, c4 ASC, c5 ASC, c6 ASC;",
      "SELECT 'dict_' || str c1, 'fake_' || substr(real_str, 6) c2, x + 56 c3, f c4, -d "
      "c5, smallint_nulls c6 FROM test WHERE ('fake_' || substr(real_str, 6)) LIKE "
      "'%_ba%' ORDER BY c1 ASC, c2 ASC, c3 ASC, c4 ASC, c5 ASC, c6 ASC;",
      dt);
    c("SELECT 'dict_' || str c1, 'fake_' || substring(real_str, 6) c2, x + 56 c3, f c4, "
      "-d c5, smallint_nulls c6 FROM test ORDER BY c1 ASC, c2 ASC, c3 ASC, c4 ASC, c5 "
      "ASC, c6 ASC;",
      "SELECT 'dict_' || str c1, 'fake_' || substr(real_str, 6) c2, x + 56 c3, f c4, -d "
      "c5, smallint_nulls c6 FROM test ORDER BY c1 ASC, c2 ASC, c3 ASC, c4 ASC, c5 ASC, "
      "c6 ASC;",
      dt);
    c("SELECT str || '_dict' AS c1, COUNT(*) c2 FROM test GROUP BY str ORDER BY c1 ASC, "
      "c2 ASC;",
      dt);
    c("SELECT str || '_dict' AS c1, COUNT(*) c2 FROM test WHERE x <> 8 GROUP BY str "
      "ORDER BY c1 ASC, c2 ASC;",
      dt);
    {
      std::string part1 =
          "SELECT x, y, ROW_NUMBER() OVER (PARTITION BY y ORDER BY x ASC) - 1 r1, RANK() "
          "OVER (PARTITION BY y ORDER BY x ASC) r2, DENSE_RANK() OVER (PARTITION BY y "
          "ORDER BY x DESC) r3 FROM test_window_func ORDER BY x ASC";
      std::string part2 = ", y ASC, r1 ASC, r2 ASC, r3 ASC;";
      c(part1 + " NULLS FIRST" + part2, part1 + part2, dt);
    }
    c("SELECT CAST(('fake_' || SUBSTRING(real_str, 6)) LIKE '%_ba%' AS INT) b from test "
      "ORDER BY b;",
      "SELECT ('fake_' || SUBSTR(real_str, 6)) LIKE '%_ba%' b from test ORDER BY b;",
      dt);
  }
  config().exec.enable_interop = false;
}

// Test https://github.com/omnisci/omniscidb/issues/463
TEST_F(Select, LeftJoinDictionaryGenerationIssue463) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    createTable("issue463_table1",
                {{"playerID", dictType()},
                 {"yearID", SQLTypeInfo(kBIGINT)},
                 {"stint", SQLTypeInfo(kBIGINT)},
                 {"teamID", dictType()},
                 {"lgID", dictType()},
                 {"G", SQLTypeInfo(kBIGINT)},
                 {"AB", SQLTypeInfo(kBIGINT)},
                 {"R", SQLTypeInfo(kBIGINT)},
                 {"H", SQLTypeInfo(kBIGINT)},
                 {"X2B", SQLTypeInfo(kBIGINT)},
                 {"X3B", SQLTypeInfo(kBIGINT)},
                 {"HR", SQLTypeInfo(kBIGINT)},
                 {"RBI", SQLTypeInfo(kBIGINT)},
                 {"SB", SQLTypeInfo(kBIGINT)},
                 {"CS", SQLTypeInfo(kBIGINT)},
                 {"BB", SQLTypeInfo(kBIGINT)},
                 {"SO", SQLTypeInfo(kBIGINT)},
                 {"IBB", SQLTypeInfo(kBIGINT)},
                 {"HBP", SQLTypeInfo(kBIGINT)},
                 {"SH", SQLTypeInfo(kBIGINT)},
                 {"SF", SQLTypeInfo(kBIGINT)},
                 {"GIDP", SQLTypeInfo(kBIGINT)}});
    insertCsvValues("issue463_table1",
                    "keefeti01,1880,1,TRN,NL,12,43,4,10,3,0,0,3,0,0,1,12,0,0,0,0,0");
    createTable("issue463_table2",
                {{"playerID", dictType()},
                 {"awardID", dictType()},
                 {"yearID", SQLTypeInfo(kBIGINT)},
                 {"lgID", dictType()},
                 {"tie", dictType()},
                 {"notes", dictType()}});
    insertCsvValues("issue463_table2", "keefeti01,Pitching Triple Crown,1888,NL,0,0");

    run_sqlite_query("DROP TABLE IF EXISTS issue463_table1;");
    run_sqlite_query("DROP TABLE IF EXISTS issue463_table2;");
    run_sqlite_query(
        "CREATE TABLE issue463_table1 ("
        " playerID TEXT ENCODING DICT(32),"
        " yearID BIGINT,"
        " stint BIGINT,"
        " teamID TEXT ENCODING DICT(32),"
        " lgID TEXT ENCODING DICT(32),"
        " G BIGINT,"
        " AB BIGINT,"
        " R BIGINT,"
        " H BIGINT,"
        " X2B BIGINT,"
        " X3B BIGINT,"
        " HR BIGINT,"
        " RBI BIGINT,"
        " SB BIGINT,"
        " CS BIGINT,"
        " BB BIGINT,"
        " SO BIGINT,"
        " IBB BIGINT,"
        " HBP BIGINT,"
        " SH BIGINT,"
        " SF BIGINT,"
        " GIDP BIGINT);");
    run_sqlite_query(
        "CREATE TABLE issue463_table2 ("
        " playerID TEXT ENCODING DICT(32),"
        " awardID TEXT ENCODING DICT(32),"
        " yearID BIGINT,"
        " lgID TEXT ENCODING DICT(32),"
        " tie TEXT ENCODING DICT(32),"
        " notes TEXT ENCODING DICT(32));");
    run_sqlite_query(
        "INSERT INTO issue463_table1 VALUES "
        "('keefeti01',1880,1,'TRN','NL',12,43,4,10,3,0,0,3,0,0,1,12,0,0,0,0,0);");
    run_sqlite_query(
        "INSERT INTO issue463_table2 VALUES ('keefeti01','Pitching Triple "
        "Crsqlite_comparator_own',1888,'NL',0,0);");
    // SELECT returns no rows
    char const* select_norows = R"Quote463(SELECT t0.*
FROM (
  SELECT *
  FROM issue463_table1
  WHERE "yearID" = 2015
) t0
  LEFT JOIN (
    SELECT "playerID", "awardID", "tie", "notes"
    FROM (
      SELECT *
      FROM issue463_table2
      WHERE "lgID" = 'NL'
    ) t2
  ) t1
    ON t0."playerID" = t1."playerID";
)Quote463";
    // SELECT returns one row
    char const* select_onerow = R"Quote463(SELECT t0.*
FROM (
  SELECT *
  FROM issue463_table1
  WHERE "yearID" = 1880
) t0
  LEFT JOIN (
    SELECT "playerID", "awardID", "tie", "notes"
    FROM (
      SELECT *
      FROM issue463_table2
      WHERE "lgID" = 'NL'
    ) t2
  ) t1
    ON t0."playerID" = t1."playerID";
)Quote463";
    c(select_norows, dt);
    c(select_onerow, dt);

    run_sqlite_query("DROP TABLE IF EXISTS issue463_table1;");
    run_sqlite_query("DROP TABLE IF EXISTS issue463_table2;");
    dropTable("issue463_table1");
    dropTable("issue463_table2");
  }
}

// The subquery has an aggregate column that is not projected to the outer query,
// and so is eliminated by an RA optimization. This tests internal logic that still
// accesses the string "Chicago" from a StringDictionary with generation=-1.
TEST_F(Select, StringFromEliminatedColumn) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    createTable("flights", {{"plane_model", dictType()}, {"dest_city", dictType()}}, {2});
    run_sqlite_query("DROP TABLE IF EXISTS flights;");
    run_sqlite_query("CREATE TABLE flights (plane_model TEXT, dest_city TEXT);");
    for (std::string plane_model : {"B-1", "B-2", "B-3", "B-4"}) {
      for (auto dest_city : {"Austin", "Dallas", "Chicago"}) {
        run_sqlite_query("INSERT INTO flights VALUES ('" + plane_model + "', '" +
                         dest_city + "');");
        insertCsvValues("flights", plane_model + "," + dest_city);
      }
    }
    char const* select =
        "SELECT plane_model "
        "FROM ("
        "  SELECT plane_model, SUM(CASE WHEN dest_city IN ('Austin', 'Dallas') AND "
        "plane_model IN (SELECT plane_model FROM flights WHERE dest_city = 'Chicago') "
        "THEN 1 ELSE 0 END) AS \"mycolumn\""
        "  FROM flights"
        "  GROUP BY plane_model"
        ") ORDER BY plane_model;";
    c(select, dt);
    // Previously triggered:
    // StringDictionaryProxy.cpp:68 Check failed: generation_ >= 0 (-1 >= 0)
    c("SELECT str FROM ("
      " SELECT str, COUNT(str IN (SELECT str FROM test WHERE ss = 'fish')) AS mycolumn"
      " FROM test"
      " GROUP BY str"
      ") ORDER BY str;",
      dt);

    run_sqlite_query("DROP TABLE IF EXISTS flights;");
    dropTable("flights");
  }
}

TEST_F(Select, VarlenLazyFetch) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      const auto query(
          "SELECT t, real_str, array_i16 FROM varlen_table where rowid = 222;");
      auto result = run_multiple_agg(query, dt);
      const auto first_row = result->getNextRow(true, true);
      ASSERT_EQ(size_t(3), first_row.size());
      ASSERT_EQ(int64_t(95), v<int64_t>(first_row[0]));
      ASSERT_EQ(boost::get<std::string>(v<NullableString>(first_row[1])), "number222");
      compare_array(first_row[2], std::vector<int64_t>({444, 445}));
    }
  }
}

TEST_F(Select, SampleRatio) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    // looking for 8-12 pass at a 50% sampling rate. This is dependent on data
    // distribution.
    const auto num_rows = v<int64_t>(
        run_simple_agg("SELECT COUNT(*) FROM test WHERE sample_ratio(0.5);", dt));
    EXPECT_GE(num_rows, 8);
    EXPECT_LE(num_rows, 12);

    EXPECT_EQ(0, v<int64_t>(run_simple_agg("SELECT sample_ratio(null);", dt)));

    EXPECT_NO_THROW(run_multiple_agg("SELECT sample_ratio(0);", dt));
    EXPECT_NO_THROW(run_multiple_agg("SELECT sample_ratio(0.5);", dt));
    EXPECT_NO_THROW(run_multiple_agg("SELECT sample_ratio(1);", dt));
    EXPECT_NO_THROW(
        run_multiple_agg("SELECT sample_ratio((SELECT max(x) FROM test));", dt));

    EXPECT_ANY_THROW(run_multiple_agg("SELECT sample_ratio(str) FROM test LIMIT 1;", dt));
    EXPECT_ANY_THROW(
        run_multiple_agg("SELECT sample_ratio(null_str) FROM test LIMIT 1;", dt));
    EXPECT_ANY_THROW(run_multiple_agg("SELECT sample_ratio(bn) FROM test LIMIT 1;", dt));
    EXPECT_ANY_THROW(
        run_multiple_agg("SELECT sample_ratio(arr_i64) FROM array_test LIMIT 1;", dt));

    EXPECT_NO_THROW(run_multiple_agg("SELECT sample_ratio(w) FROM test LIMIT 1;", dt));
    EXPECT_NO_THROW(run_multiple_agg("SELECT sample_ratio(x) FROM test LIMIT 1;", dt));
    EXPECT_NO_THROW(run_multiple_agg("SELECT sample_ratio(y) FROM test LIMIT 1;", dt));
    EXPECT_NO_THROW(run_multiple_agg("SELECT sample_ratio(f) FROM test LIMIT 1;", dt));
    EXPECT_NO_THROW(run_multiple_agg("SELECT sample_ratio(fn) FROM test LIMIT 1;", dt));
    EXPECT_NO_THROW(run_multiple_agg("SELECT sample_ratio(d) FROM test LIMIT 1;", dt));
    EXPECT_NO_THROW(
        run_multiple_agg("SELECT sample_ratio(arr_i64[0]) FROM array_test LIMIT 1;", dt));
  }
}

TEST_F(Select, OffsetInFragment) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // With fragment_size of 2, we should have 10 frags
    EXPECT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE offset_in_fragment() = 1;", dt)));
    EXPECT_EQ(
        10,
        v<int64_t>(run_simple_agg(
            "SELECT COUNT(*) FROM test WHERE offset_in_fragment() = CAST(1 AS INT);",
            dt)));
    EXPECT_EQ(10,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE offset_in_fragment() < 1;", dt)));
    EXPECT_EQ(20,
              v<int64_t>(run_simple_agg(
                  "SELECT COUNT(*) FROM test WHERE offset_in_fragment() <= 1;", dt)));
    EXPECT_EQ(
        10,
        v<int64_t>(run_simple_agg("SELECT COUNT(*) FROM test WHERE offset_in_fragment() "
                                  "<= 1 AND offset_in_fragment() != 0;",
                                  dt)));
    EXPECT_EQ(
        1,
        v<int64_t>(run_simple_agg("SELECT CAST(AVG(offset_in_fragment()) AS BIGINT) FROM "
                                  "test WHERE offset_in_fragment() > 0;",
                                  dt)));
    const auto num_rows = v<int64_t>(run_simple_agg(
        "SELECT COUNT(*) FROM (SELECT COUNT(*) FROM test WHERE offset_in_fragment() <= "
        "10 GROUP BY MOD(offset_in_fragment(), 2));",
        dt));
    EXPECT_EQ(num_rows, 2);
  }
}

// Additional integer parsing tests in ImportTestInt.ImportBadInt and ImportGoodInt.
TEST_F(Select, ParseIntegerExceptions) {
  struct TestPair {
    std::string query;
    std::string exception;
  };
  std::vector<TestPair> const tests{
      {"SELECT * FROM test WHERE ''=2147483647;",
       "Invalid conversion from \"\" to INTEGER"},
      {"SELECT * FROM test WHERE ''=2147483648;",
       "Invalid conversion from \"\" to BIGINT"},
      {"SELECT * FROM test WHERE '9223372036854775808'=9223372036854775807;",
       "Integer 9223372036854775808 is out of range for BIGINT"},
      {"SELECT * FROM test WHERE '1e3.0'=1;",
       "Unexpected character \".\" encountered in INTEGER value 1e3.0"}};
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    for (auto const& test : tests) {
      try {
        run_multiple_agg(test.query, dt);
        EXPECT_TRUE(false) << "Exception expected for query: " << test.query;
      } catch (std::runtime_error const& e) {
        EXPECT_EQ(e.what(), test.exception);
      } catch (...) {
        EXPECT_TRUE(false) << "std::runtime_error expected for query: " << test.query;
      }
    }
  }
}

class SubqueryTestEnv : public ExecuteTestBase, public ::testing::Test {
 protected:
  void SetUp() override {
    auto create_test_table = [](const std::string& table_name) {
      createTable(table_name,
                  {{"r1", SQLTypeInfo(kINT)},
                   {"r2", SQLTypeInfo(kINT)},
                   {"r3", SQLTypeInfo(kINT)}});
      insertCsvValues(table_name, "1,2,3\n2,3,4\n3,4,5\n4,5,6\n1,3,4");

      run_sqlite_query("DROP TABLE IF EXISTS " + table_name + ";");
      run_sqlite_query("CREATE TABLE " + table_name + " (r1 int, r2 int, r3 int);");
      run_sqlite_query("INSERT INTO " + table_name + " VALUES (1,2,3);");
      run_sqlite_query("INSERT INTO " + table_name + " VALUES (2,3,4);");
      run_sqlite_query("INSERT INTO " + table_name + " VALUES (3,4,5);");
      run_sqlite_query("INSERT INTO " + table_name + " VALUES (4,5,6);");
      run_sqlite_query("INSERT INTO " + table_name + " VALUES (1,3,4);");
    };

    create_test_table("R1");
    create_test_table("R2");
    create_test_table("R3");
  }

  void TearDown() override {
    auto drop_table = [](const std::string& table_name) {
      dropTable(table_name);
      run_sqlite_query("DROP TABLE IF EXISTS " + table_name + ";");
    };

    drop_table("R1");
    drop_table("R2");
    drop_table("R3");
  }
};

TEST_F(SubqueryTestEnv, SubqueryTest) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    // multi-step subquery
    c(R"(select t1.r1, t1.r2, t1.r3 from R1 t1 where t1.r1 in (select t2.r1 from R1 t2 join (select * from R1) t3 on t2.r1 = t3.r1) order by 1, 2;)",
      dt);
    c(R"(select t1.r1, t1.r2, t1.r3 from R1 t1 where t1.r3 in (select t2.r3 from R1 t2 join (select * from R1) t3 on t2.r1 = t3.r1) order by 1, 2)",
      dt);
    // multi-step subquery different tables
    c(R"(select t1.r1, t1.r2, t1.r3 from R2 t1 where t1.r1 in (select t2.r1 from R1 t2 join (select * from R1) t3 on t2.r1 = t3.r1) order by 1, 2;)",
      dt);
    c(R"(select t1.r1, t1.r2, t1.r3 from R1 t1 where t1.r1 in (select t2.r1 from R2 t2 join (select * from R1) t3 on t2.r1 = t3.r1) order by 1, 2;)",
      dt);
    c(R"(select t1.r1, t1.r2, t1.r3 from R1 t1 where t1.r1 in (select t2.r1 from R2 t2 join (select * from R3) t3 on t2.r1 = t3.r1) order by 1, 2;)",
      dt);
    // multi-step multi-subquery
    c(R"(select t1.r1, t1.r2, t1.r3 from R1 t1 where t1.r1 > (SELECT min(t2.r1) FROM R1 t2 where t2.r2 < 3) and t1.r2 >= (SELECT max(t3.r2) FROM R1 t3 where t3.r3 > (SELECT avg(t4.r3) FROM R1 t4 where t4.r1 < 2)) order by 1, 2;)",
      dt);
    c(R"(select (select sum(x - y) from ( select count(1) as x, ( select count(1) from ( select distinct str as py from test_inner ) ) as y from ( select str as x from test_inner group by str ) ) ) from test_inner;)",
      dt);
  }
}

class ManyRowsTest : public ExecuteTestBase, public ::testing::Test {
 protected:
  void SetUp() override {
    createTable(table_name,
                {{"t", SQLTypeInfo(kTINYINT)},
                 {"x", SQLTypeInfo(kINT)},
                 {"y", SQLTypeInfo(kBIGINT)}});

    // add one additional "duplicate" row at the end
    for (size_t i = 0; i < row_count - 1; i++) {
      insertCsvValues(
          table_name,
          std::to_string(i) + "," + std::to_string(i) + "," + std::to_string(i));
    }

    insertCsvValues(table_name, "1,2,3");
  }

  void TearDown() override { dropTable(table_name); }

  static const std::string table_name;
  static const size_t row_count;
};

const std::string ManyRowsTest::table_name = "many_rows";
const size_t ManyRowsTest::row_count = 129;

// ensure lazy fetch works properly for queries where the number of rows exceeds the bit
// width of the target type
TEST_F(ManyRowsTest, Projection) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    EXPECT_NO_THROW({
      auto result = run_multiple_agg(
          R"(SELECT t, x, y from many_rows ORDER BY x ASC NULLS FIRST;)", dt);
      EXPECT_EQ(result->rowCount(), ManyRowsTest::row_count);
      for (size_t i = 0; i < ManyRowsTest::row_count; i++) {
        auto row = result->getNextRow(false, false);
        EXPECT_EQ(row.size(), size_t(3));
      }
    });
  }
}

int main(int argc, char** argv) {
  auto config = std::make_shared<Config>();

  g_is_test_env = true;

  std::cout << "Starting ExecuteTest" << std::endl;

  testing::InitGoogleTest(&argc, argv);
  namespace po = boost::program_options;

  po::options_description desc("Options");

  // these two are here to allow passing correctly google testing parameters
  desc.add_options()("gtest_list_tests", "list all test");
  desc.add_options()("gtest_filter", "filters tests, use --help for details");

  desc.add_options()("disable-literal-hoisting", "Disable literal hoisting");
  desc.add_options()("from-table-reordering",
                     po::value<bool>(&config->opts.from_table_reordering)
                         ->default_value(config->opts.from_table_reordering)
                         ->implicit_value(true),
                     "Enable automatic table reordering in FROM clause");
  desc.add_options()("bigint-count",
                     po::value<bool>(&config->exec.group_by.bigint_count)
                         ->default_value(config->exec.group_by.bigint_count)
                         ->implicit_value(false),
                     "Use 64-bit count");
  desc.add_options()("disable-shared-mem-group-by",
                     po::value<bool>(&config->exec.group_by.enable_gpu_smem_group_by)
                         ->default_value(config->exec.group_by.enable_gpu_smem_group_by)
                         ->implicit_value(false),
                     "Enable/disable using GPU shared memory for GROUP BY.");
  desc.add_options()("enable-columnar-output",
                     po::value<bool>(&g_enable_columnar_output)
                         ->default_value(g_enable_columnar_output)
                         ->implicit_value(true),
                     "Enable/disable using columnar output format.");
  desc.add_options()("enable-bump-allocator",
                     po::value<bool>(&g_enable_bump_allocator)
                         ->default_value(g_enable_bump_allocator)
                         ->implicit_value(true),
                     "Enable the bump allocator for projection queries on GPU.");
  desc.add_options()(
      "enable-heterogeneous",
      po::value<bool>(&config->exec.heterogeneous.enable_heterogeneous_execution)
          ->default_value(config->exec.heterogeneous.enable_heterogeneous_execution)
          ->implicit_value(true),
      "Allow heterogeneous execution.");
  desc.add_options()(
      "force-heterogeneous-distribution",
      po::value<bool>(&config->exec.heterogeneous.forced_heterogeneous_distribution)
          ->default_value(config->exec.heterogeneous.forced_heterogeneous_distribution)
          ->implicit_value(true));
  desc.add_options()(
      "cpu-prop", po::value<unsigned>(&config->exec.heterogeneous.forced_cpu_proportion));
  desc.add_options()(
      "gpu-prop", po::value<unsigned>(&config->exec.heterogeneous.forced_gpu_proportion));
  desc.add_options()("dump-ir",
                     po::value<bool>()->default_value(false)->implicit_value(true),
                     "Dump IR and PTX for all executed queries to file."
                     " Currently only supports single node tests.");
  desc.add_options()("use-disk-cache",
                     "Use the disk cache for all tables with minimum size settings.");
  desc.add_options()("use-groupby-buffer-desc",
                     po::value<bool>(&config->exec.group_by.use_groupby_buffer_desc)
                         ->default_value(config->exec.group_by.use_groupby_buffer_desc)
                         ->implicit_value(true),
                     "Use GroupBy Buffer Descriptor for hash tables.");

  desc.add_options()(
      "test-help",
      "Print all ExecuteTest specific options (for gtest options use `--help`).");

  logger::LogOptions log_options(argv[0]);
  log_options.severity_ = logger::Severity::FATAL;
  log_options.set_options();  // update default values
  desc.add(log_options.get_options());

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  po::notify(vm);

  if (vm.count("test-help")) {
    std::cout << "Usage: ExecuteTest" << std::endl << std::endl;
    std::cout << desc << std::endl;
    return 0;
  }

  if (vm["dump-ir"].as<bool>()) {
    // Only log IR, PTX channels to file with no rotation size.
    log_options.channels_ = {logger::Channel::IR, logger::Channel::PTX};
    log_options.rotation_size_ = std::numeric_limits<size_t>::max();
  }

  logger::init(log_options);

  if (vm.count("disable-literal-hoisting")) {
    config->exec.codegen.hoist_literals = false;
  }

  config->exec.window_func.enable = true;
  config->exec.enable_interop = false;

  init(config);

  int err{0};
  try {
    ExecuteTestBase::createAndPopulateTestTables();
    if (!err) {
      err = RUN_ALL_TESTS();
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
    return -1;
  }

  Executor::nukeCacheOfExecutors();

  printStats();
  reset();

  return err;
}
