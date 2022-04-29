/*
 * Copyright 2019 OmniSci, Inc.
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

/**
 * @file StringFunctionsTest.cpp
 * @brief Test suite for string functions
 */

#include "ArrowSQLRunner/ArrowSQLRunner.h"
#include "TestHelpers.h"

#include "QueryEngine/ResultSet.h"
#include "Shared/scope.h"
#include "TestHelpers.h"

#include <gtest/gtest.h>
#include <boost/format.hpp>
#include <boost/locale/generator.hpp>

#define SKIP_NO_GPU()                                        \
  if (skip_tests(dt)) {                                      \
    CHECK(dt == ExecutorDeviceType::GPU);                    \
    LOG(WARNING) << "GPU not available, skipping GPU tests"; \
    continue;                                                \
  }

extern bool g_enable_string_functions;
extern unsigned g_trivial_loop_join_threshold;

using namespace TestHelpers;
using namespace TestHelpers::ArrowSQLRunner;

bool skip_tests(const ExecutorDeviceType device_type) {
#ifdef HAVE_CUDA
  return device_type == ExecutorDeviceType::GPU && !gpusPresent();
#else
  return device_type == ExecutorDeviceType::GPU;
#endif
}

namespace {

class AssertValueEqualsVisitor : public boost::static_visitor<> {
 public:
  AssertValueEqualsVisitor(const size_t& row, const size_t& column)
      : row(row), column(column) {}

  template <typename T, typename U>
  void operator()(const T& expected, const U& actual) const {
    if (std::is_pointer<U>::value) {
      EXPECT_EQ(1UL, 1UL);
    } else {
      FAIL() << boost::format(
                    "Values are of different types. Expected result set value: %s is of "
                    "type: %s while actual result set value: %s is of type: %s. At row: "
                    "%d, column: %d") %
                    expected % typeid(expected).name() % actual % typeid(actual).name() %
                    row % column;
    }
  }

  template <typename T>
  void operator()(const T& expected, const T& actual) const {
    EXPECT_EQ(expected, actual) << boost::format("At row: %d, column: %d") % row % column;
  }

 private:
  size_t row;
  size_t column;
};

template <>
void AssertValueEqualsVisitor::operator()<NullableString>(
    const NullableString& expected,
    const NullableString& actual) const {
  boost::apply_visitor(AssertValueEqualsVisitor(row, column), expected, actual);
}

void assert_value_equals(ScalarTargetValue& expected,
                         ScalarTargetValue& actual,
                         const size_t& row,
                         const size_t& column) {
  boost::apply_visitor(AssertValueEqualsVisitor(row, column), expected, actual);
}

void compare_result_set(
    const std::vector<std::vector<ScalarTargetValue>>& expected_result_set,
    const std::shared_ptr<ResultSet>& actual_result_set) {
  auto row_count = actual_result_set->rowCount(false);
  ASSERT_EQ(expected_result_set.size(), row_count)
      << "Returned result set does not have the expected number of rows";

  if (row_count == 0) {
    return;
  }

  auto expected_column_count = expected_result_set[0].size();
  auto column_count = actual_result_set->colCount();
  ASSERT_EQ(expected_column_count, column_count)
      << "Returned result set does not have the expected number of columns";
  ;

  for (size_t r = 0; r < row_count; ++r) {
    auto row = actual_result_set->getNextRow(true, true);
    for (size_t c = 0; c < column_count; c++) {
      auto column_value = boost::get<ScalarTargetValue>(row[c]);
      auto expected_column_value = expected_result_set[r][c];
      assert_value_equals(expected_column_value, column_value, r, c);
    }
  }
}
}  // namespace

// begin LOWER function tests

/**
 * @brief Class used for setting up and tearing down tables and records that are required
 * by the LOWER function test cases
 */
class StringFunctionTest : public testing::Test {
 public:
  void SetUp() override {
    createTable("string_function_test_people",
                {{"id", SQLTypeInfo(kINT)},
                 {"first_name", dictType()},
                 {"last_name", SQLTypeInfo(kTEXT)},
                 {"full_name", dictType()},
                 {"age", SQLTypeInfo(kINT)},
                 {"country_code", dictType()},
                 {"us_phone_number", dictType()},
                 {"zip_plus_4", dictType()},
                 {"personal_motto", dictType()},
                 {"raw_email", dictType()}});
    insertCsvValues("string_function_test_people",
                    R"(
1,JOHN,SMITH,John SMITH,25,us,555-803-2144,90210-7743,"All for one and one for all.","Shoot me a note at therealjohnsmith@omnisci.com"\n
2,John,Banks,John BANKS,30,Us,555-803-8244,94104-8123,"One plus one does not equal two.","Email: john_banks@mapd.com"\n
3,JOHN,Wilson,John WILSON,20,cA,555-614-9814,null,"What is the sound of one hand clapping?","JOHN.WILSON@geops.net"\n
4,Sue,Smith,Sue SMITH,25,CA,555-614-2282,null,"Nothing exists entirely alone. Everything is always in relation to everything else.","Find me at sue4tw@example.com, or reach me at sue.smith@example.com. I''d love to hear from you!")",
                    /*escaping=*/true,
                    /*quoting=*/true);

    createTable("string_function_test_countries",
                {{"id", SQLTypeInfo(kINT)},
                 {"code", dictType()},
                 {"arrow_code", dictType()},
                 {"name", dictType()},
                 {"capital", SQLTypeInfo(kTEXT)}});
    insertCsvValues("string_function_test_countries",
                    R"(
1,US,>>US<<,United States,Washington\n
2,ca,>>CA<<,Canada,Ottawa\n
3,Gb,>>GB<<,United Kingdom,London\n
4,dE,>>DE<<,Germany,Berlin)");
  }

  void TearDown() override {
    dropTable("string_function_test_people");
    dropTable("string_function_test_countries");
  }
};

TEST_F(StringFunctionTest, Lowercase) {
  auto result_set = run_multiple_agg(
      "select lower(first_name) from string_function_test_people order by id asc;",
      ExecutorDeviceType::CPU);
  std::vector<std::vector<ScalarTargetValue>> expected_result_set{
      {"john"}, {"john"}, {"john"}, {"sue"}};
  compare_result_set(expected_result_set, result_set);
}

TEST_F(StringFunctionTest, LowercaseLiteral) {
  auto result_set =
      run_multiple_agg("select lower('fUnNy CaSe');", ExecutorDeviceType::CPU);
  std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"funny case"}};

  compare_result_set(expected_result_set, result_set);
}

TEST_F(StringFunctionTest, Uppercase) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select upper(first_name) from string_function_test_people order by id asc;", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"JOHN"}, {"JOHN"}, {"JOHN"}, {"SUE"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, UppercaseLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select upper('fUnNy CaSe');", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"FUNNY CASE"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, InitCap) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select initcap(full_name) from string_function_test_people order by id asc", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"John Smith"}, {"John Banks"}, {"John Wilson"}, {"Sue Smith"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, InitCapLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select initcap('fUnNy CaSe');", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"Funny Case"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, Reverse) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select reverse(full_name) from string_function_test_people order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"HTIMS nhoJ"}, {"SKNAB nhoJ"}, {"NOSLIW nhoJ"}, {"HTIMS euS"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, ReverseLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select reverse('fUnNy CaSe');", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"eSaC yNnUf"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, Repeat) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select repeat(full_name, 2) from string_function_test_people order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"John SMITHJohn SMITH"},
        {"John BANKSJohn BANKS"},
        {"John WILSONJohn WILSON"},
        {"Sue SMITHSue SMITH"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, RepeatLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select repeat('fUnNy CaSe', 3);", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"fUnNy CaSefUnNy CaSefUnNy CaSe"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, Concat) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select name || ', Earth' from string_function_test_countries order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"United States, Earth"},
        {"Canada, Earth"},
        {"United Kingdom, Earth"},
        {"Germany, Earth"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, ReverseConcat) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select 'Country: ' || code from string_function_test_countries order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"Country: US"}, {"Country: ca"}, {"Country: Gb"}, {"Country: dE"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, ConcatLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select 'fUnNy CaSe' || ' is the case.';", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"fUnNy CaSe is the case."}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, LPad) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select lpad(name, 14) from string_function_test_countries order by id asc;", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {" United States"}, {"        Canada"}, {"United Kingdom"}, {"       Germany"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, LPadTruncate) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select lpad(name, 5) from string_function_test_countries order by id asc;", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"Unite"}, {"Canad"}, {"Unite"}, {"Germa"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, LPadCustomChars) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select lpad(name, 14, '>|<') from string_function_test_countries order by id "
        "asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {">United States"}, {">|<>|<>|Canada"}, {"United Kingdom"}, {">|<>|<>Germany"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, DISABLED_LPadLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select lpad('123', 2);", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"  123"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, RPad) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select rpad(name, 20) from string_function_test_countries order by id asc;", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"United States       "},
        {"Canada              "},
        {"United Kingdom      "},
        {"Germany             "}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, RPadLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select rpad('$323.', 8, '98') from string_function_test_countries order by id "
        "asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"$323.989"}, {"$323.989"}, {"$323.989"}, {"$323.989"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, TrimBothDefault) {
  // Will be a no-op as default trim character is space
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select trim(arrow_code) from string_function_test_countries order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {">>US<<"}, {">>CA<<"}, {">>GB<<"}, {">>DE<<"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, TrimBothCustom) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // Implicit 'BOTH
    auto result_set1 = run_multiple_agg(
        "select trim('<>' from arrow_code) from string_function_test_countries order "
        "by id asc;",
        dt);
    // explicit syntax
    auto result_set2 = run_multiple_agg(
        "select trim(both '<>' from arrow_code) from string_function_test_countries "
        "order by id asc;",
        dt);

    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"US"}, {"CA"}, {"GB"}, {"DE"}};
    compare_result_set(expected_result_set, result_set1);
    compare_result_set(expected_result_set, result_set2);
  }
}

TEST_F(StringFunctionTest, TrimBothLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set1 = run_multiple_agg("select trim(both ' !' from ' Oops!');", dt);
    auto result_set2 = run_multiple_agg("select trim(' !' from ' Oops!');", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"Oops"}};
    compare_result_set(expected_result_set, result_set1);
    compare_result_set(expected_result_set, result_set2);
  }
}

TEST_F(StringFunctionTest, LeftTrim) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();

    // Trim with 'LEADING'
    auto result_set1 = run_multiple_agg(
        "select trim(leading '<>#' from arrow_code) from "
        "string_function_test_countries order by id asc;",
        dt);

    // Explicit LTrim
    auto result_set2 = run_multiple_agg(
        "select ltrim(arrow_code, '<>#') from string_function_test_countries order by "
        "id asc;",
        dt);

    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"US<<"}, {"CA<<"}, {"GB<<"}, {"DE<<"}};

    compare_result_set(expected_result_set, result_set1);
    compare_result_set(expected_result_set, result_set2);
  }
}

TEST_F(StringFunctionTest, LeftTrimLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // Trim with 'LEADING'
    auto result_set1 = run_multiple_agg("select trim(leading '$' from '$19.99$');", dt);

    // LTrim
    auto result_set2 = run_multiple_agg("select ltrim('$19.99$', '$');", dt);

    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"19.99$"}};

    compare_result_set(expected_result_set, result_set1);
    compare_result_set(expected_result_set, result_set2);
  }
}

TEST_F(StringFunctionTest, RightTrim) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // Trim with 'TRAILING'
    auto result_set1 = run_multiple_agg(
        "select trim(trailing '<> ' from arrow_code) from "
        "string_function_test_countries order by id asc;",
        dt);

    // RTrim
    auto result_set2 = run_multiple_agg(
        "select rtrim(arrow_code, '<> ') from string_function_test_countries order by "
        "id asc;",
        dt);

    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {">>US"}, {">>CA"}, {">>GB"}, {">>DE"}};

    compare_result_set(expected_result_set, result_set1);
    compare_result_set(expected_result_set, result_set2);
  }
}

TEST_F(StringFunctionTest, RightTrimLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // Trim with 'TRAILING'
    auto result_set1 =
        run_multiple_agg("select trim(trailing '|' from '|half pipe||');", dt);

    // RTrim
    auto result_set2 = run_multiple_agg("select rtrim('|half pipe||', '|');", dt);

    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"|half pipe"}};

    compare_result_set(expected_result_set, result_set1);
    compare_result_set(expected_result_set, result_set2);
  }
}

TEST_F(StringFunctionTest, Substring) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      auto result_set1 = run_multiple_agg(
          "select substring(full_name, 1, 4) from string_function_test_people order by "
          "id asc;",
          dt);
      auto result_set2 = run_multiple_agg(
          "select substring(full_name from 1 for 4) from string_function_test_people "
          "order by "
          "id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"John"}, {"John"}, {"John"}, {"Sue "}};
      compare_result_set(expected_result_set, result_set1);
      compare_result_set(expected_result_set, result_set2);
    }
    {
      // Test null inputs
      auto result_set1 = run_multiple_agg(
          "select substring(zip_plus_4, 1, 5) from string_function_test_people order by "
          "id asc;",
          dt);

      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"90210"}, {"94104"}, {""}, {""}};
    }
  }
}

TEST_F(StringFunctionTest, SubstringNegativeWrap) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select substring(full_name, -3, 2) from string_function_test_people order by "
        "id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"IT"}, {"NK"}, {"SO"}, {"IT"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, SubstringLengthOffEnd) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select substring(code, 2, 10) from string_function_test_countries order by id "
        "asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"S"}, {"a"}, {"b"}, {"E"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, SubstringLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select substring('fUnNy CaSe', 4, 4);", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"Ny C"}};
    compare_result_set(expected_result_set, result_set);
  }
}

// Test that index of 0 is equivalent to index of 1 (first character)
TEST_F(StringFunctionTest, SubstringLengthZeroStartLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set1 = run_multiple_agg("select substring('12345', 1, 3);", dt);
    auto result_set2 = run_multiple_agg("select substring('12345', 0, 3);", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"123"}};
    compare_result_set(expected_result_set, result_set1);
    compare_result_set(expected_result_set, result_set2);
  }
}

TEST_F(StringFunctionTest, SubstrAlias) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select substr(us_phone_number, 5, 3) from string_function_test_people order "
        "by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"803"}, {"803"}, {"614"}, {"614"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, SubstrAliasLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select substr('fUnNy CaSe', 4, 4);", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"Ny C"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, Overlay) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select overlay(us_phone_number placing '6273' from 9) from "
        "string_function_test_people order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"555-803-6273"}, {"555-803-6273"}, {"555-614-6273"}, {"555-614-6273"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, OverlayInsert) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select overlay(us_phone_number placing '+1-' from 1 for 0) from "
        "string_function_test_people order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"+1-555-803-2144"},
                                                                    {"+1-555-803-8244"},
                                                                    {"+1-555-614-9814"},
                                                                    {"+1-555-614-2282"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, OverlayLiteralNoFor) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select overlay('We all love big data.' PLACING 'fast' FROM 13);", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"We all love fastdata."}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, OverlayLiteralWithFor) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select overlay('We all love big data.' PLACING 'fast' FROM 13 FOR 3);", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"We all love fast data."}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, Replace) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select replace(us_phone_number, '803', '#^!') from "
        "string_function_test_people order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"555-#^!-2144"}, {"555-#^!-8244"}, {"555-614-9814"}, {"555-614-2282"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, DISABLED_ReplaceEmptyReplacement) {
  // Todo: Determine why Calcite is not accepting 2-parameter version
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select replace(us_phone_number, '555-') from "
        "string_function_test_people order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"803-2144"}, {"803-8244"}, {"614-9814"}, {"614-2282"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, ReplaceLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set =
        run_multiple_agg("select replace('We all love big data.', 'big', 'fast');", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"We all love fast data."}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, DISABLED_ReplaceLiteralEmptyReplacement) {
  // Todo: Determine why Calcite is not accepting 2-parameter version
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set =
        run_multiple_agg("select replace('We all love big data.', 'big');", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"We all love data."}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, SplitPart) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select split_part(us_phone_number, '-', 2) from string_function_test_people "
        "order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"803"}, {"803"}, {"614"}, {"614"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, SplitPartNegativeIndex) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select split_part(us_phone_number, '-', -1) from "
        "string_function_test_people order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"2144"}, {"8244"}, {"9814"}, {"2282"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, SplitPartLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select split_part('192.168.0.1', '.', 2);", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"168"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, SplitPartLiteralNegativeIndex) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select split_part('192.168.0.1', '.', -1);", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"1"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, SplitPartLiteralNullIndex) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg("select split_part('192.168.0.1', '.', 5);", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{""}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, RegexpReplace2Args) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select regexp_replace(name, 'United[[:space:]]') from "
        "string_function_test_countries order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"States"}, {"Canada"}, {"Kingdom"}, {"Germany"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, RegexpReplace3Args) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select regexp_replace(name, 'United[[:space:]]([[:alnum:]])', 'The United "
        "$1') from string_function_test_countries order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"The United States"}, {"Canada"}, {"The United Kingdom"}, {"Germany"}};
    compare_result_set(expected_result_set, result_set);
  }
}

// 4th argument is position
TEST_F(StringFunctionTest, RegexpReplace4Args) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      auto result_set = run_multiple_agg(
          "select regexp_replace(personal_motto, '([Oo]ne)[[:space:]]', '$1..two ', "
          "4) from string_function_test_people order by id asc",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"All for one..two and one..two for all."},
          // Note we don't replace the first One due to start position argument of 4
          {"One plus one..two does not equal two."},
          {"What is the sound of one..two hand clapping?"},
          {"Nothing exists entirely alone. Everything is always in relation to "
           "everything else."}};
      compare_result_set(expected_result_set, result_set);
    }
    // Test negative position, should wrap
    {
      auto result_set = run_multiple_agg(
          "select regexp_replace(personal_motto, '([Oo]ne)[[:space:]]', '$1..two ', "
          "-18) from string_function_test_people order by id asc",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"All for one and one..two for all."},
          // Note we don't replace the first One due to start position argument of 4
          {"One plus one does not equal two."},
          {"What is the sound of one..two hand clapping?"},
          {"Nothing exists entirely alone. Everything is always in relation to "
           "everything else."}};
      compare_result_set(expected_result_set, result_set);
    }
  }
}

// 5th argument is occurrence
TEST_F(StringFunctionTest, RegexpReplace5Args) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // 0 for 5th (occurrence) arguments says to replace all matches
    {
      auto result_set = run_multiple_agg(
          "select regexp_replace(personal_motto, '([Oo]ne)[[:space:]]', '$1..two ', "
          "1, 0) from string_function_test_people order by id asc",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"All for one..two and one..two for all."},
          {"One..two plus one..two does not equal two."},
          {"What is the sound of one..two hand clapping?"},
          {"Nothing exists entirely alone. Everything is always in relation to "
           "everything else."}};
      compare_result_set(expected_result_set, result_set);
    }
    {
      // Replace second match
      auto result_set = run_multiple_agg(
          "select regexp_replace(personal_motto, '([Oo]ne)[[:space:]]', '$1..two ', "
          "1, 2) from string_function_test_people order by id asc",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"All for one and one..two for all."},
          // Note we don't replace the first One due to start position argument of 4
          {"One plus one..two does not equal two."},
          {"What is the sound of one hand clapping?"},
          {"Nothing exists entirely alone. Everything is always in relation to "
           "everything else."}};
      compare_result_set(expected_result_set, result_set);
    }
    {
      // Replace second to last match via negative wrapping
      auto result_set = run_multiple_agg(
          "select regexp_replace(personal_motto, '([Oo]ne)[[:space:]]', '$1..two ', "
          "1, -2) from string_function_test_people order by id asc",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"All for one..two and one for all."},
          // Note we don't replace the first One due to start position argument of 4
          {"One..two plus one does not equal two."},
          {"What is the sound of one hand clapping?"},
          {"Nothing exists entirely alone. Everything is always in relation to "
           "everything else."}};
      compare_result_set(expected_result_set, result_set);
    }
  }
}

// 6th argument is regex parameters
TEST_F(StringFunctionTest, RegexpReplace6Args) {
  // Currently only support 'c' (case sensitive-default) and 'i' (case insensitive) for
  // RegexpReplace
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // Test 'c' - case sensitive
    {
      auto result_set = run_multiple_agg(
          "select regexp_replace(personal_motto, '(one)[[:space:]]', '$1..two ', 1, "
          "0, 'c') from string_function_test_people order by id asc",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"All for one..two and one..two for all."},
          // Note "One" in next entry doesn't match due to case sensitive search
          {"One plus one..two does not equal two."},
          {"What is the sound of one..two hand clapping?"},
          {"Nothing exists entirely alone. Everything is always in relation to "
           "everything else."}};
      compare_result_set(expected_result_set, result_set);
    }
    // Test 'i' - case insensitive
    {
      auto result_set = run_multiple_agg(
          "select regexp_replace(personal_motto, '(one)[[:space:]]', '$1..two ', 1, "
          "0, 'i') from string_function_test_people order by id asc",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"All for one..two and one..two for all."},
          // With case insensitive search, "One" will match
          {"One..two plus one..two does not equal two."},
          {"What is the sound of one..two hand clapping?"},
          {"Nothing exists entirely alone. Everything is always in relation to "
           "everything else."}};
      compare_result_set(expected_result_set, result_set);
    }
    // Test that invalid regex param causes exception
    {
      EXPECT_ANY_THROW(run_multiple_agg(
          "select regexp_replace(personal_motto, '(one)[[:space:]]', '$1..two ', 1, "
          "0, 'iz') from string_function_test_people order by id asc;",
          dt));
    }
  }
}

TEST_F(StringFunctionTest, RegexpReplaceLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select regexp_replace('How much wood would a wood chuck chuck if a wood "
        "chuck could chuck wood?', 'wo[[:alnum:]]+d', 'metal', 1, 0, 'i');",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"How much metal metal a metal chuck chuck if a metal chuck could chuck metal?"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, RegexpReplaceLiteralSpecificMatch) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select regexp_replace('How much wood would a wood chuck chuck if a wood "
        "chuck could chuck wood?', 'wo[[:alnum:]]+d', 'should', 1, 2, 'i');",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"How much wood should a wood chuck chuck if a wood chuck could chuck wood?"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, RegexpSubstr2Args) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select regexp_substr(raw_email, '[[:alnum:].-_]+@[[:alnum:]]+.[[:alnum:]]+') "
        "from string_function_test_people order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"therealjohnsmith@omnisci.com"},
        {"john_banks@mapd.com"},
        {"JOHN.WILSON@geops.net"},
        {"sue4tw@example.com"}};
    compare_result_set(expected_result_set, result_set);
  }
}

// 3rd arg is start position
TEST_F(StringFunctionTest, RegexpSubstr3Args) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select regexp_substr(raw_email, '[[:alnum:].-_]+@[[:alnum:]]+.[[:alnum:]]+', "
        "20) from string_function_test_people order by id asc;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"therealjohnsmith@omnisci.com"}, {""}, {""}, {"sue.smith@example.com"}};
    compare_result_set(expected_result_set, result_set);
  }
}

// 4th arg is the occurence index
TEST_F(StringFunctionTest, RegexpSubstr4Args) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      auto result_set = run_multiple_agg(
          "select regexp_substr(raw_email, '[[:alnum:].-_]+@[[:alnum:]]+.[[:alnum:]]+', "
          "1, 2) from string_function_test_people order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {""}, {""}, {""}, {"sue.smith@example.com"}};
      compare_result_set(expected_result_set, result_set);
    }
    // Test negative wrapping
    {
      auto result_set = run_multiple_agg(
          "select regexp_substr(raw_email, '[[:alnum:].-_]+@[[:alnum:]]+.[[:alnum:]]+', "
          "1, -1) from string_function_test_people order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"therealjohnsmith@omnisci.com"},
          {"john_banks@mapd.com"},
          {"JOHN.WILSON@geops.net"},
          {"sue.smith@example.com"}};
      compare_result_set(expected_result_set, result_set);
    }
  }
}

// 5th arg is regex params, 6th is sub-match index if 'e' is specified as regex param
TEST_F(StringFunctionTest, RegexpSubstr5Or6Args) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // case sensitive
    {
      auto result_set = run_multiple_agg(
          "select regexp_substr(raw_email, "
          "'john[[:alnum:].-_]+@[[:alnum:]]+.[[:alnum:]]+', 1, 1, 'c') from "
          "string_function_test_people order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"johnsmith@omnisci.com"}, {"john_banks@mapd.com"}, {""}, {""}};
      compare_result_set(expected_result_set, result_set);
    }
    // case insensitive
    {
      auto result_set = run_multiple_agg(
          "select regexp_substr(raw_email, "
          "'john[[:alnum:].-_]+@[[:alnum:]]+.[[:alnum:]]+', 1, 1, 'i') from "
          "string_function_test_people order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"johnsmith@omnisci.com"},
          {"john_banks@mapd.com"},
          {"JOHN.WILSON@geops.net"},
          {""}};
      compare_result_set(expected_result_set, result_set);
    }
    // extract sub-matches
    {
      // Get the email domain (second sub-match)
      auto result_set = run_multiple_agg(
          "select regexp_substr(raw_email, "
          "'([[:alnum:].-_]+)@([[:alnum:]]+.[[:alnum:]]+)', 1, 1, 'ce', 2) from "
          "string_function_test_people order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"omnisci.com"}, {"mapd.com"}, {"geops.net"}, {"example.com"}};
      compare_result_set(expected_result_set, result_set);
    }
    {
      // Sub-match has no effect if extract ('e') is not specified
      auto result_set = run_multiple_agg(
          "select regexp_substr(raw_email, "
          "'([[:alnum:].-_]+)@([[:alnum:]]+.[[:alnum:]]+)', 1, 1, 'i', 2) from "
          "string_function_test_people order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"therealjohnsmith@omnisci.com"},
          {"john_banks@mapd.com"},
          {"JOHN.WILSON@geops.net"},
          {"sue4tw@example.com"}};
      compare_result_set(expected_result_set, result_set);
    }
    // Throw error if regex param is not valid
    {
      EXPECT_ANY_THROW(run_multiple_agg(
          "select regexp_substr(raw_email, "
          "'([[:alnum:].-_]+)@([[:alnum:]]+.[[:alnum:]]+)', 1, 1, 'z', 2) from "
          "string_function_test_people order by id asc;",
          dt));
    }
    // Throw error if case regex param not specified
    {
      EXPECT_ANY_THROW(run_multiple_agg(
          "select regexp_substr(raw_email, "
          "'([[:alnum:].-_]+)@([[:alnum:]]+.[[:alnum:]]+)', 1, 1, 'e', 2) from "
          "string_function_test_people order by id asc;",
          dt));
    }
  }
}

TEST_F(StringFunctionTest, RegexpSubstrLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select regexp_substr('Feel free to send us an email at spam@devnull.com!', "
        "'[[:alnum:]]+@[[:alnum:]]+.[[:alnum:]]+',  1, -1, 'i', 0);",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"spam@devnull.com"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, StringFunctionEqualsFilterLHS) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      auto result_set = run_multiple_agg(
          "select first_name, last_name from string_function_test_people "
          "where lower(country_code) = 'us';",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"JOHN", "SMITH"},
                                                                      {"John", "Banks"}};
      compare_result_set(expected_result_set, result_set);
    }

    {
      auto result_set = run_multiple_agg(
          "select COUNT(*) from string_function_test_people "
          "where initcap(first_name) = 'John';",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{{int64_t(3)}};

      compare_result_set(expected_result_set, result_set);
    }

    {
      auto result_set = run_multiple_agg(
          "select lower(first_name), first_name from string_function_test_people "
          "where upper('johN') = first_name;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"john", "JOHN"},
                                                                      {"john", "JOHN"}};
      compare_result_set(expected_result_set, result_set);
    }
  }
}

TEST_F(StringFunctionTest, StringFunctionEqualsFilterRHS) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      auto result_set = run_multiple_agg(
          "select first_name, last_name from string_function_test_people "
          "where 'us' = lower(country_code);",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"JOHN", "SMITH"},
                                                                      {"John", "Banks"}};
      compare_result_set(expected_result_set, result_set);
    }

    {
      auto result_set = run_multiple_agg(
          "select COUNT(*) from string_function_test_people "
          "where 'John' = initcap(first_name);",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{{int64_t(3)}};

      compare_result_set(expected_result_set, result_set);
    }

    {
      auto result_set = run_multiple_agg(
          "select lower(first_name), first_name from string_function_test_people "
          "where first_name = upper('johN');",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"john", "JOHN"},
                                                                      {"john", "JOHN"}};
      compare_result_set(expected_result_set, result_set);
    }
  }
}

TEST_F(StringFunctionTest, StringFunctionFilterBothSides) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      auto result_set = run_multiple_agg(
          "select first_name, last_name from string_function_test_people "
          "where lower('US') = lower(country_code);",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"JOHN", "SMITH"},
                                                                      {"John", "Banks"}};
      compare_result_set(expected_result_set, result_set);
    }

    {
      auto result_set = run_multiple_agg(
          "select COUNT(*) from string_function_test_people "
          "where initcap('joHN') = initcap(first_name);",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{{int64_t(3)}};

      compare_result_set(expected_result_set, result_set);
    }

    {
      auto result_set = run_multiple_agg(
          "select first_name, lower(first_name), first_name from "
          "string_function_test_people "
          "where upper(first_name) = upper('johN');",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"JOHN", "john", "JOHN"}, {"John", "john", "John"}, {"JOHN", "john", "JOHN"}};
      compare_result_set(expected_result_set, result_set);
    }

    {
      auto result_set = run_multiple_agg(
          "select first_name, full_name from string_function_test_people "
          "where initcap(first_name) = split_part(full_name, ' ', 1);",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"JOHN", "John SMITH"},
          {"John", "John BANKS"},
          {"JOHN", "John WILSON"},
          {"Sue", "Sue SMITH"}};
      compare_result_set(expected_result_set, result_set);
    }
  }
}

TEST_F(StringFunctionTest, MultipleFilters) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      auto result_set = run_multiple_agg(
          "select first_name, last_name from string_function_test_people "
          "where lower(country_code) = 'us' or lower(first_name) = 'sue';",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"JOHN", "SMITH"}, {"John", "Banks"}, {"Sue", "Smith"}};
      compare_result_set(expected_result_set, result_set);
    }
    {
      auto result_set = run_multiple_agg(
          "select first_name, last_name from string_function_test_people "
          "where lower(country_code) = 'us' or upper(country_code) = 'CA';",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"JOHN", "SMITH"}, {"John", "Banks"}, {"JOHN", "Wilson"}, {"Sue", "Smith"}};
      compare_result_set(expected_result_set, result_set);
    }
  }
}

TEST_F(StringFunctionTest, MixedFilters) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select first_name, last_name from string_function_test_people "
        "where lower(country_code) = 'ca' and age > 20;",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"Sue", "Smith"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, ChainedOperators) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      auto result_set = run_multiple_agg(
          "select initcap(split_part(full_name, ' ', 2)) as surname from "
          "string_function_test_people order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"Smith"}, {"Banks"}, {"Wilson"}, {"Smith"}};
      compare_result_set(expected_result_set, result_set);
    }
    {
      auto result_set = run_multiple_agg(
          "select upper(split_part(split_part(regexp_substr(raw_email, "
          "'[[:alnum:].-_]+@[[:alnum:]]+.[[:alnum:]]+', "
          "1, -1), '@', -1), '.', 1)) as upper_domain from "
          "string_function_test_people order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"OMNISCI"}, {"MAPD"}, {"GEOPS"}, {"EXAMPLE"}};
      compare_result_set(expected_result_set, result_set);
    }
    {
      auto result_set = run_multiple_agg(
          "select lower(split_part(split_part(regexp_substr(raw_email, "
          "'[[:alnum:].-_]+@[[:alnum:]]+.[[:alnum:]]+', "
          "1, -1), '@', -1), '.', 2)), "
          "upper(split_part(split_part(regexp_substr(raw_email, "
          "'[[:alnum:].-_]+@[[:alnum:]]+.[[:alnum:]]+', "
          "1, -1), '@', -1), '.', 1)) as upper_domain from "
          "string_function_test_people where substring(replace(raw_email, 'com', "
          "'org') from -3 for 3) = 'org' order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"com", "OMNISCI"},
                                                                      {"com", "MAPD"}};
      compare_result_set(expected_result_set, result_set);
    }
  }
}

TEST_F(StringFunctionTest, CaseStatement) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    // Single column, string op only on output
    {
      auto result_set = run_multiple_agg(
          "select case when first_name = 'JOHN' then lower(first_name) else "
          "upper(first_name) end "
          "as case_stmt from string_function_test_people order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"john"}, {"JOHN"}, {"john"}, {"SUE"}};
      compare_result_set(expected_result_set, result_set);
    }
    // Single column, string ops on inputs and outputs, with additional literal
    {
      auto result_set = run_multiple_agg(
          "select case when split_part(us_phone_number, '-', 2) = '614' then "
          "split_part(us_phone_number, '-', 3) "
          "when split_part(us_phone_number, '-', 3) = '2144' then "
          "substring(us_phone_number from 1 for 3) else "
          "'Surprise' end as case_stmt from string_function_test_people order by id asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"555"}, {"Surprise"}, {"9814"}, {"2282"}};
    }
    // Multi-column, string ops on inputs and outputs, with null and additional literal
    {
      auto result_set = run_multiple_agg(
          "select case when split_part(us_phone_number, '-', 2) = trim('614 ') then null "
          "when split_part(us_phone_number, '-', 3) = '214' || '4' then "
          "regexp_substr(zip_plus_4, "
          "'^[[:digit:]]+') else upper(country_code) end as case_stmt from "
          "string_function_test_people "
          "order by id asc;",
          dt);

      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"90210"}, {"US"}, {""}, {""}};
      compare_result_set(expected_result_set, result_set);
    }
  }
}

TEST_F(StringFunctionTest, GroupBy) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    {
      auto result_set = run_multiple_agg(
          "select lower(first_name), count(*) from string_function_test_people "
          "group by lower(first_name) order by 2 desc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"john", int64_t(3)}, {"sue", int64_t(1)}};
      compare_result_set(expected_result_set, result_set);
    }
    {
      auto result_set = run_multiple_agg(
          "select regexp_substr(raw_email, "
          "'([[:alnum:].-_]+)@([[:alnum:]]+).([[:alnum:]]+)', 1, 1, 'ie', 3) as tld, "
          "count(*) as n from string_function_test_people group by tld order by tld asc;",
          dt);
      std::vector<std::vector<ScalarTargetValue>> expected_result_set{
          {"com", int64_t(3)}, {"net", int64_t(1)}};
      compare_result_set(expected_result_set, result_set);
    }
  }
}

TEST_F(StringFunctionTest, SelectLiteral) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select first_name, lower('SMiTH') from string_function_test_people;", dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{
        {"JOHN", "smith"}, {"John", "smith"}, {"JOHN", "smith"}, {"Sue", "smith"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, LowercaseNonEncodedTextColumn) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    try {
      run_multiple_agg("select lower(last_name) from string_function_test_people;", dt);
      FAIL() << "An exception should have been thrown for this test case";
    } catch (const std::exception& e) {
      ASSERT_STREQ(
          "Error instantiating LOWER operator. Currently only text-encoded "
          "dictionary-encoded column inputs are allowed, but a none-encoded text column "
          "argument was received.",
          e.what());
    }
  }
}

TEST_F(StringFunctionTest, LowercaseNonTextColumn) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    try {
      run_multiple_agg("select lower(age) from string_function_test_people;", dt);
      FAIL() << "An exception should have been thrown for this test case";
    } catch (const std::exception& e) {
      ASSERT_STREQ(
          "Error instantiating LOWER operator. Expected text type for argument 1 "
          "(operand).",
          e.what());
    }
  }
}

TEST_F(StringFunctionTest, LowercaseNullColumn) {
  insertCsvValues(
      "string_function_test_people",
      R"(5, null, 'Empty', null, 25, 'US', '555-123-4567', '12345-8765', 'One.', 'null@nullbin.org')");
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    auto result_set = run_multiple_agg(
        "select lower(first_name), last_name from string_function_test_people where "
        "last_name = 'Empty';",
        dt);
    std::vector<std::vector<ScalarTargetValue>> expected_result_set{{"", "Empty"}};
    compare_result_set(expected_result_set, result_set);
  }
}

TEST_F(StringFunctionTest, SelectLowercase_StringFunctionsDisabled) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    SKIP_NO_GPU();
    g_enable_string_functions = false;
    try {
      run_multiple_agg("select lower(first_name) from string_function_test_people;", dt);
      FAIL() << "An exception should have been thrown for this test case";
    } catch (const std::exception& e) {
      ASSERT_STREQ("Function LOWER not supported.", e.what());
      g_enable_string_functions = true;
    }
  }
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);

  init();
  g_enable_string_functions = true;

  // Use system locale setting by default (as done in the server).
  boost::locale::generator generator;
  std::locale::global(generator.generate(""));

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }

  g_enable_string_functions = false;
  reset();
  return err;
}
