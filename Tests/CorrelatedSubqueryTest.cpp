/*
 * Copyright 2018, OmniSci, Inc.
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

#include "ArrowSQLRunner.h"
#include "TestHelpers.h"

#include "../Parser/parser.h"
#include "../QueryEngine/ArrowResultSet.h"
#include "../QueryEngine/Execute.h"
#include "../Shared/file_delete.h"

#include <gtest/gtest.h>

#include <ctime>
#include <iostream>

using namespace TestHelpers;
using namespace TestHelpers::ArrowSQLRunner;

bool skip_tests_on_gpu(const ExecutorDeviceType device_type) {
#ifdef HAVE_CUDA
  return device_type == ExecutorDeviceType::GPU && !(QR::get()->gpusPresent());
#else
  return device_type == ExecutorDeviceType::GPU;
#endif
}

void setupTest(SQLTypeInfo valueType, int factsCount, int lookupCount) {
  dropTable("test_facts");
  dropTable("test_lookup");

  createTable(
      "test_facts",
      {{"id", SQLTypeInfo(kINT)}, {"val", valueType}, {"lookup_id", SQLTypeInfo(kINT)}},
      {3});
  createTable("test_lookup", {{"id", SQLTypeInfo(kINT)}, {"val", valueType}});

  // populate facts table
  std::stringstream facts;
  for (int i = 0; i < factsCount; i++) {
    int id = i;
    int val = i;
    facts << id << "," << val << "," << std::endl;
  }
  insertCsvValues("test_facts", facts.str());

  // populate lookup table
  std::stringstream lookup;
  for (int i = 0; i < lookupCount; i++) {
    int id = i;
    int val = i;
    lookup << id << "," << val << std::endl;
  }
  insertCsvValues("test_lookup", lookup.str());
}

auto getIntValue(const TargetValue& mapd_variant) {
  const auto scalar_mapd_variant = boost::get<ScalarTargetValue>(&mapd_variant);
  const auto mapd_val_as_p = boost::get<int64_t>(scalar_mapd_variant);
  const auto mapd_val = *mapd_val_as_p;
  return mapd_val;
};

auto getDoubleValue(const TargetValue& mapd_variant) {
  const auto scalar_mapd_variant = boost::get<ScalarTargetValue>(&mapd_variant);
  const auto mapd_val_as_p = boost::get<double>(scalar_mapd_variant);
  const auto mapd_val = *mapd_val_as_p;
  return mapd_val;
};

auto getValue(const TargetValue& mapd_variant) {
  const auto scalar_mapd_variant = boost::get<ScalarTargetValue>(&mapd_variant);
  const auto mapd_valf_as_p = boost::get<float>(scalar_mapd_variant);
  const auto mapd_vald_as_p = boost::get<double>(scalar_mapd_variant);
  const auto mapd_vali_as_p = boost::get<int64_t>(scalar_mapd_variant);
  const auto mapd_as_str_p = boost::get<NullableString>(scalar_mapd_variant);

  if (mapd_valf_as_p) {
    return (double)*mapd_valf_as_p;
  } else if (mapd_vald_as_p) {
    const auto mapd_val = *mapd_vald_as_p;
    return mapd_val;
  } else if (mapd_vali_as_p) {
    const auto mapd_val = *mapd_vali_as_p;
    return (double)mapd_val;
  } else if (mapd_as_str_p) {
    const auto mapd_str_notnull = boost::get<std::string>(mapd_as_str_p);
    return (double)std::stoi(*mapd_str_notnull);
  }

  throw std::runtime_error("Unexpected variant");
};

void runSingleValueTestValidation(SQLTypeInfo colType, ExecutorDeviceType dt) {
  ASSERT_ANY_THROW(run_multiple_agg("SELECT SINGLE_VALUE(id) FROM test_facts;", dt));
  ASSERT_ANY_THROW(
      run_multiple_agg("SELECT SINGLE_VALUE(id) FROM test_facts group by val;", dt));

  {
    auto results = run_multiple_agg("SELECT SINGLE_VALUE(val) FROM test_facts;", dt);
    ASSERT_EQ(uint64_t(1), results->rowCount());
    const auto select_crt_row = results->getNextRow(true, true);
    auto val = getValue(select_crt_row[0]);
    ASSERT_EQ(1, val);
  }

  {
    auto results = run_multiple_agg(
        "SELECT id, SINGLE_VALUE(val) FROM test_facts GROUP BY id ORDER BY id;", dt);
    ASSERT_EQ(uint64_t(3), results->rowCount());
    auto select_crt_row = results->getNextRow(true, true);
    ASSERT_EQ(1, getValue(select_crt_row[0]));
    ASSERT_EQ(1, getValue(select_crt_row[1]));
    select_crt_row = results->getNextRow(true, true);
    ASSERT_EQ(2, getValue(select_crt_row[0]));
    ASSERT_EQ(1, getValue(select_crt_row[1]));
    select_crt_row = results->getNextRow(true, true);
    ASSERT_EQ(3, getValue(select_crt_row[0]));
    ASSERT_EQ(1, getValue(select_crt_row[1]));
  }

  if (!colType.is_string()) {
    auto results = run_multiple_agg(
        "SELECT id+1, val FROM (SELECT id, SINGLE_VALUE(val) as val FROM test_facts "
        "GROUP BY id) ORDER BY id;",
        dt);
    ASSERT_EQ(uint64_t(3), results->rowCount());
    auto select_crt_row = results->getNextRow(true, true);
    ASSERT_EQ(2, getValue(select_crt_row[0]));
    ASSERT_EQ(1, getValue(select_crt_row[1]));
    select_crt_row = results->getNextRow(true, true);
    ASSERT_EQ(3, getValue(select_crt_row[0]));
    ASSERT_EQ(1, getValue(select_crt_row[1]));
    select_crt_row = results->getNextRow(true, true);
    ASSERT_EQ(4, getValue(select_crt_row[0]));
    ASSERT_EQ(1, getValue(select_crt_row[1]));
  }
}

void runSingleValueTest(SQLTypeInfo colType, ExecutorDeviceType dt) {
  if (skip_tests_on_gpu(dt)) {
    return;
  }

  dropTable("test_facts");
  createTable("test_facts", {{"id", colType}, {"val", colType}}, {3});

  insertCsvValues("test_facts", "1,1\n2,1\n3,1");

  runSingleValueTestValidation(colType, dt);

  insertCsvValues("test_facts", "1,1\n2,1\n3,1");

  runSingleValueTestValidation(colType, dt);

  insertCsvValues("test_facts", "1,\n2,1\n3,1");
  insertCsvValues("test_facts", "1,1\n2,\n3,1");
  insertCsvValues("test_facts", "1,1\n2,1\n3,");

  runSingleValueTestValidation(colType, dt);

  insertCsvValues("test_facts", "1,2");

  ASSERT_ANY_THROW(run_multiple_agg("SELECT SINGLE_VALUE(id) FROM test_facts;", dt));
  ASSERT_ANY_THROW(
      run_multiple_agg("SELECT SINGLE_VALUE(id) FROM test_facts group by val;", dt));

  ASSERT_ANY_THROW(run_multiple_agg("SELECT SINGLE_VALUE(val) FROM test_facts;", dt));
  ASSERT_ANY_THROW(run_multiple_agg(
      "SELECT id, SINGLE_VALUE(val) FROM test_facts GROUP BY id ORDER BY id;", dt));

  {
    auto results = run_multiple_agg(
        "SELECT id, SINGLE_VALUE(val) FROM test_facts WHERE id NOT IN (CAST (1 as  " +
            colType.get_type_name() +
            " )) GROUP BY id ORDER BY "
            "id;",
        dt);
    ASSERT_EQ(uint64_t(2), results->rowCount());
    auto select_crt_row = results->getNextRow(true, true);
    ASSERT_EQ(2, getValue(select_crt_row[0]));
    ASSERT_EQ(1, getValue(select_crt_row[1]));
    select_crt_row = results->getNextRow(true, true);
    ASSERT_EQ(3, getValue(select_crt_row[0]));
    ASSERT_EQ(1, getValue(select_crt_row[1]));
  }
}

TEST(Select, SingleValue) {
  for (auto dt : {ExecutorDeviceType::CPU, ExecutorDeviceType::GPU}) {
    runSingleValueTest(kTINYINT, dt);
    runSingleValueTest(kSMALLINT, dt);
    runSingleValueTest(kINT, dt);
    runSingleValueTest(kBIGINT, dt);
    runSingleValueTest({kDECIMAL, 10, 2, false}, dt);
    runSingleValueTest(kFLOAT, dt);
    runSingleValueTest(kDOUBLE, dt);
    runSingleValueTest(dictType(), dt);
  }
}

TEST(Select, Correlated) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);
  std::string sql =
      "SELECT id, val, (SELECT test_lookup.id FROM test_lookup WHERE "
      "test_lookup.val = test_facts.val) as lookup_id FROM test_facts";
  auto results = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  ASSERT_EQ(static_cast<uint32_t>(factsCount), results->rowCount());

  auto INT_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kINT, false));

  for (int i = 0; i < factsCount; i++) {
    const auto select_crt_row = results->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    auto lookupId = getIntValue(select_crt_row[2]);

    ASSERT_EQ(id, val);

    if (id < lookupCount) {
      ASSERT_EQ(lookupId, id);
    } else {
      ASSERT_EQ(lookupId, INT_NULL_SENTINEL);
    }
  }
}

TEST(Select, CorrelatedWithDouble) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kDOUBLE, factsCount, lookupCount);
  std::string sql =
      "SELECT id, val, (SELECT test_lookup.id FROM test_lookup WHERE "
      "test_lookup.val = test_facts.val) as lookup_id FROM test_facts";
  auto results = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  ASSERT_EQ(static_cast<uint32_t>(factsCount), results->rowCount());

  auto INT_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kINT, false));

  for (int i = 0; i < factsCount; i++) {
    const auto select_crt_row = results->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getDoubleValue(select_crt_row[1]);
    auto lookupId = getIntValue(select_crt_row[2]);

    ASSERT_EQ(id, val);

    if (id < lookupCount) {
      ASSERT_EQ(lookupId, id);
    } else {
      ASSERT_EQ(lookupId, INT_NULL_SENTINEL);
    }
  }
}

TEST(Select, CorrelatedWithInnerDuplicatesFails) {
  int factsCount = 13;
  int lookupCount = 5;

  setupTest(kINT, factsCount, lookupCount);
  insertCsvValues("test_lookup", "5,0\n6,1\n7,2\n8,3\n9,4");

  std::string sql =
      "SELECT id, val, (SELECT test_lookup.id FROM test_lookup WHERE test_lookup.val = "
      "test_facts.val) as lookup_id FROM test_facts";
  ASSERT_ANY_THROW(run_multiple_agg(sql, ExecutorDeviceType::CPU));
}

TEST(Select, CorrelatedWithInnerDuplicatesAndMinId) {
  int factsCount = 13;
  int lookupCount = 5;

  setupTest(kINT, factsCount, lookupCount);
  insertCsvValues("test_lookup", "5,0\n6,1\n7,2\n8,3\n9,4");

  std::string sql =
      "SELECT id, val, (SELECT MIN(test_lookup.id) FROM test_lookup WHERE "
      "test_lookup.val = test_facts.val) as lookup_id FROM test_facts";
  auto results = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  ASSERT_EQ(static_cast<uint32_t>(factsCount), results->rowCount());

  auto INT_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kINT, false));

  for (int i = 0; i < factsCount; i++) {
    const auto select_crt_row = results->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    auto lookupId = getIntValue(select_crt_row[2]);

    ASSERT_EQ(id, val);

    if (id < lookupCount) {
      ASSERT_EQ(lookupId, id);
    } else {
      ASSERT_EQ(lookupId, INT_NULL_SENTINEL);
    }
  }
}

TEST(Select, DISABLED_CorrelatedWithInnerDuplicatesDescIdOrder) {
  // this test is disabled, because the inner ordering does not work
  int factsCount = 13;
  int lookupCount = 5;

  setupTest(kINT, factsCount, lookupCount);
  insertCsvValues("test_lookup", "5,0\n6,1\n7,2\n8,3\n9,4");

  std::string sql =
      "SELECT id, val, (SELECT test_lookup.id FROM test_lookup WHERE "
      "test_lookup.val = test_facts.val ORDER BY test_lookup.id DESC LIMIT 1) as "
      "lookup_id FROM test_facts";
  auto results = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  ASSERT_EQ(static_cast<uint32_t>(factsCount), results->rowCount());

  auto INT_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kINT, false));

  for (int i = 0; i < factsCount; i++) {
    const auto select_crt_row = results->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    auto lookupId = getIntValue(select_crt_row[2]);

    ASSERT_EQ(id, val);

    if (id < lookupCount) {
      ASSERT_EQ(lookupId, id);
    } else {
      ASSERT_EQ(lookupId, INT_NULL_SENTINEL);
    }
  }
}

TEST(Select, CorrelatedWithInnerDuplicatesAndMaxId) {
  int factsCount = 13;
  int lookupCount = 5;

  setupTest(kINT, factsCount, lookupCount);
  insertCsvValues("test_lookup", "5,0\n6,1\n7,2\n8,3\n9,4");

  std::string sql =
      "SELECT id, val, (SELECT MAX(test_lookup.id) FROM test_lookup WHERE "
      "test_lookup.val = test_facts.val) as lookup_id FROM test_facts";
  auto results = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  ASSERT_EQ(static_cast<uint32_t>(factsCount), results->rowCount());

  auto INT_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kINT, false));

  for (int i = 0; i < factsCount; i++) {
    const auto select_crt_row = results->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    auto lookupId = getIntValue(select_crt_row[2]);

    ASSERT_EQ(id, val);

    if (id < lookupCount) {
      ASSERT_EQ(lookupId, id + 5);
    } else {
      ASSERT_EQ(lookupId, INT_NULL_SENTINEL);
    }
  }
}

TEST(Select, DISABLED_CorrelatedWithInnerDuplicatesAndAscIdOrder) {
  // this test is disabled, because the inner ordering does not work
  int factsCount = 13;
  int lookupCount = 5;

  setupTest(kINT, factsCount, lookupCount);
  insertCsvValues("test_lookup", "5,0\n6,1\n7,2\n8,3\n9,4");

  std::string sql =
      "SELECT id, val, (SELECT test_lookup.id FROM test_lookup WHERE "
      "test_lookup.val = test_facts.val ORDER BY test_lookup.id ASC LIMIT 1) as "
      "lookup_id FROM "
      "test_facts";
  auto results = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  ASSERT_EQ(static_cast<uint32_t>(factsCount), results->rowCount());

  auto INT_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kINT, false));

  for (int i = 0; i < factsCount; i++) {
    const auto select_crt_row = results->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    auto lookupId = getIntValue(select_crt_row[2]);

    ASSERT_EQ(id, val);

    if (id < lookupCount) {
      ASSERT_EQ(lookupId, id + 5);
    } else {
      ASSERT_EQ(lookupId, INT_NULL_SENTINEL);
    }
  }
}

TEST(Select, CorrelatedWithOuterSortAscending) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);
  std::string sql =
      "SELECT id, val, (SELECT test_lookup.id FROM test_lookup WHERE "
      "test_lookup.val = test_facts.val) as lookup_id FROM test_facts ORDER BY id ASC";
  auto results = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  ASSERT_EQ(static_cast<uint32_t>(factsCount), results->rowCount());

  auto INT_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kINT, false));

  for (int i = 0; i < factsCount; i++) {
    const auto select_crt_row = results->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    auto lookupId = getIntValue(select_crt_row[2]);

    ASSERT_EQ(id, i);
    ASSERT_EQ(id, val);

    if (id < lookupCount) {
      ASSERT_EQ(lookupId, id);
    } else {
      ASSERT_EQ(lookupId, INT_NULL_SENTINEL);
    }
  }
}

TEST(Select, CorrelatedWithOuterSortDescending) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);
  std::string sql =
      "SELECT id, val, (SELECT test_lookup.id FROM test_lookup WHERE "
      "test_lookup.val = test_facts.val) as lookup_id FROM test_facts ORDER BY id DESC";
  auto results = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  ASSERT_EQ(static_cast<uint32_t>(factsCount), results->rowCount());

  auto INT_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kINT, false));

  for (int i = factsCount - 1; i >= 0; i--) {
    const auto select_crt_row = results->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    auto lookupId = getIntValue(select_crt_row[2]);

    ASSERT_EQ(id, i);
    ASSERT_EQ(id, val);

    if (id < lookupCount) {
      ASSERT_EQ(lookupId, id);
    } else {
      ASSERT_EQ(lookupId, INT_NULL_SENTINEL);
    }
  }
}

TEST(Select, CorrelatedWithInnerSortDisallowed) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);
  std::string sql =
      "SELECT id, (SELECT test_lookup.id FROM test_lookup WHERE test_lookup.val = "
      "test_facts.val LIMIT 1) as lookup_id FROM test_facts;";
  ASSERT_ANY_THROW(run_multiple_agg(sql, ExecutorDeviceType::CPU));

  sql =
      "SELECT id, (SELECT test_lookup.id FROM test_lookup WHERE test_lookup.val = "
      "test_facts.val LIMIT 1 OFFSET 1) as lookup_id FROM test_facts;";
  ASSERT_ANY_THROW(run_multiple_agg(sql, ExecutorDeviceType::CPU));

  sql =
      "SELECT id, (SELECT test_lookup.id FROM test_lookup WHERE test_lookup.val = "
      "test_facts.val ORDER BY test_lookup.id) as lookup_id FROM test_facts;";
  ASSERT_ANY_THROW(run_multiple_agg(sql, ExecutorDeviceType::CPU));

  sql =
      "SELECT id, (SELECT test_lookup.id FROM test_lookup WHERE test_lookup.val = "
      "test_facts.val ORDER BY test_lookup.id LIMIT 1) as lookup_id FROM test_facts;";
  ASSERT_ANY_THROW(run_multiple_agg(sql, ExecutorDeviceType::CPU));
}

TEST(Select, NonCorrelatedWithInnerSortAllowed) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);
  insertCsvValues("test_lookup", "5,0");

  std::string sql =
      "SELECT id, (SELECT test_lookup.id FROM test_lookup WHERE test_lookup.val = 0 "
      "LIMIT 1) as lookup_id FROM test_facts;";
  ASSERT_NO_THROW(run_multiple_agg(sql, ExecutorDeviceType::CPU));

  sql =
      "SELECT id, (SELECT test_lookup.id FROM test_lookup WHERE test_lookup.val = 0 "
      "LIMIT 1 OFFSET 1 ) as lookup_id FROM test_facts;";
  ASSERT_NO_THROW(run_multiple_agg(sql, ExecutorDeviceType::CPU));

  sql =
      "SELECT id, (SELECT test_lookup.id FROM test_lookup WHERE test_lookup.val = 1 "
      "ORDER BY test_lookup.id) as lookup_id FROM test_facts;";
  ASSERT_NO_THROW(run_multiple_agg(sql, ExecutorDeviceType::CPU));

  sql =
      "SELECT id, (SELECT test_lookup.id FROM test_lookup WHERE test_lookup.val = 1 "
      "ORDER BY test_lookup.id LIMIT 1) as lookup_id FROM test_facts;";
  ASSERT_NO_THROW(run_multiple_agg(sql, ExecutorDeviceType::CPU));
}

TEST(Select, CorrelatedWhere) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);
  std::string sql =
      "SELECT id, val, lookup_id FROM test_facts WHERE (SELECT test_lookup.id "
      "FROM test_lookup WHERE test_lookup.val = test_facts.val) < 100 ORDER BY id ASC";
  auto results = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  ASSERT_EQ(static_cast<uint32_t>(lookupCount), results->rowCount());

  auto INT_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kINT, false));

  for (int i = 0; i < 5; i++) {
    const auto select_crt_row = results->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    auto lookupId = getIntValue(select_crt_row[2]);

    ASSERT_EQ(id, val);
    ASSERT_EQ(lookupId, INT_NULL_SENTINEL);
  }
}

TEST(Select, CorrelatedWhereNull) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);
  std::string sql =
      "SELECT id, val, lookup_id FROM test_facts WHERE (SELECT test_lookup.id "
      "FROM test_lookup WHERE test_lookup.val = test_facts.val) IS NULL ORDER BY id ASC";
  auto results = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  ASSERT_EQ(static_cast<uint32_t>(factsCount - lookupCount), results->rowCount());

  auto INT_NULL_SENTINEL = inline_int_null_val(SQLTypeInfo(kINT, false));

  for (int i = lookupCount; i < factsCount; i++) {
    const auto select_crt_row = results->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    auto lookupId = getIntValue(select_crt_row[2]);

    ASSERT_EQ(id, val);
    ASSERT_EQ(lookupId, INT_NULL_SENTINEL);
  }
}

TEST(Select, Exists_NoJoinCorrelation) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);

  std::string sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE EXISTS "
      "(SELECT 1 FROM test_lookup l);";
  auto results1 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  uint32_t numResultRows = results1->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(factsCount), numResultRows);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE NOT EXISTS "
      "(SELECT 1 FROM test_lookup l);";
  auto results2 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results2->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(0), numResultRows);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE EXISTS "
      "(SELECT * FROM test_lookup l where l.val > 10000);";
  auto results3 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results3->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(0), numResultRows);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE NOT EXISTS "
      "(SELECT * FROM test_lookup l where l.val > 10000);";
  auto results4 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results4->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(factsCount), numResultRows);
}

TEST(Select, JoinCorrelation) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);

  // single join-correlation with filter predicates
  std::string sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE EXISTS "
      "(SELECT l.id FROM test_lookup l WHERE l.id = fact.id AND l.val > 3);";
  auto results1 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  uint32_t numResultRows = results1->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(1), numResultRows);
  const auto select_crt_row = results1->getNextRow(true, false);
  auto id = getIntValue(select_crt_row[0]);
  auto val = getIntValue(select_crt_row[1]);
  ASSERT_EQ(id, 4);
  ASSERT_EQ(val, 4);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE NOT EXISTS "
      "(SELECT l.id FROM test_lookup l WHERE l.id = fact.id AND l.val > 3);";
  auto results2 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results2->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(12), numResultRows);
  bool correct = true;
  for (uint32_t i = 0; i < numResultRows; i++) {
    const auto select_crt_row = results2->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    if (id == 4 && val == 4) {
      correct = false;
    }
  }
  ASSERT_EQ(correct, true);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE EXISTS "
      "(SELECT l.id FROM test_lookup l WHERE l.id <> fact.id AND l.val > 3);";
  auto results3 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results3->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(12), numResultRows);
  correct = true;
  for (uint32_t i = 0; i < numResultRows; i++) {
    const auto select_crt_row = results3->getNextRow(true, false);
    auto id = getIntValue(select_crt_row[0]);
    auto val = getIntValue(select_crt_row[1]);
    if (id == 4 && val == 4) {
      correct = false;
    }
  }
  ASSERT_EQ(correct, true);

  // asterisks in SELECT clause of inner query
  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE EXISTS "
      "(SELECT l.id FROM test_lookup l WHERE l.id = fact.id AND l.val > 3);";
  auto results5 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results5->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(1), numResultRows);
  const auto select_crt_row5 = results5->getNextRow(true, false);
  id = getIntValue(select_crt_row5[0]);
  val = getIntValue(select_crt_row5[1]);
  ASSERT_EQ(id, 4);
  ASSERT_EQ(val, 4);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE NOT EXISTS "
      "(SELECT l.id FROM test_lookup l WHERE l.id <> fact.id AND l.val > 3);";
  auto results6 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results6->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(1), numResultRows);
  const auto select_crt_row6 = results6->getNextRow(true, false);
  id = getIntValue(select_crt_row6[0]);
  val = getIntValue(select_crt_row6[1]);
  ASSERT_EQ(id, 4);
  ASSERT_EQ(val, 4);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE NOT EXISTS "
      "(SELECT l.id FROM test_lookup l WHERE l.id <> fact.id AND l.val > 3);";
  auto results4 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results4->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(1), numResultRows);
  const auto select_crt_row4 = results4->getNextRow(true, false);
  id = getIntValue(select_crt_row4[0]);
  val = getIntValue(select_crt_row4[1]);
  ASSERT_EQ(id, 4);
  ASSERT_EQ(val, 4);
}

TEST(Select, JoinCorrelation_withMultipleExists) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);

  // # EXISTS clause: 2
  std::string sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE EXISTS"
      "(SELECT l.id FROM test_lookup l WHERE l.id = fact.id AND l.val > 3) AND EXISTS"
      "(SELECT l2.id FROM test_lookup l2 WHERE l2.id = fact.id AND l2.val > 3);";
  auto results1 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  uint32_t numResultRows = results1->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(1), numResultRows);
  const auto select_crt_row = results1->getNextRow(true, false);
  auto id = getIntValue(select_crt_row[0]);
  auto val = getIntValue(select_crt_row[1]);
  ASSERT_EQ(id, 4);
  ASSERT_EQ(val, 4);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE EXISTS"
      "(SELECT l.id FROM test_lookup l WHERE l.id = fact.id AND l.val > 3) AND NOT EXISTS"
      "(SELECT l2.id FROM test_lookup l2 WHERE l2.id = fact.id AND l2.val > 5);";
  auto results2 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results2->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(1), numResultRows);
  const auto select_crt_row2 = results2->getNextRow(true, false);
  id = getIntValue(select_crt_row2[0]);
  val = getIntValue(select_crt_row2[1]);
  ASSERT_EQ(id, 4);
  ASSERT_EQ(val, 4);

  // # EXISTS clause: 3
  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE NOT EXISTS"
      "(SELECT l.id FROM test_lookup l WHERE l.id <> fact.id AND l.val > 5) AND EXISTS"
      "(SELECT l2.id FROM test_lookup l2 WHERE l2.id = fact.id AND l2.val > 3) AND EXISTS"
      "(SELECT l3.id FROM test_lookup l3 WHERE l3.id = fact.id AND l3.val > 3);";
  auto results3 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results3->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(1), numResultRows);
  const auto select_crt_row3 = results3->getNextRow(true, false);
  id = getIntValue(select_crt_row3[0]);
  val = getIntValue(select_crt_row3[1]);
  ASSERT_EQ(id, 4);
  ASSERT_EQ(val, 4);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE NOT EXISTS"
      "(SELECT l.id FROM test_lookup l WHERE l.id <> fact.id AND l.val > 5) AND EXISTS"
      "(SELECT l2.id FROM test_lookup l2 WHERE l2.id = fact.id AND l2.val > 3) AND NOT "
      "EXISTS (SELECT l3.id FROM test_lookup l3 WHERE l3.id = fact.id AND l3.val > 3);";
  auto results4 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results4->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(0), numResultRows);

  // asterisks in SELECT clause of inner query
  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE NOT EXISTS"
      "(SELECT * FROM test_lookup l WHERE l.id <> fact.id AND l.val > 5) AND EXISTS"
      "(SELECT * FROM test_lookup l2 WHERE l2.id = fact.id AND l2.val > 3) AND EXISTS"
      "(SELECT * FROM test_lookup l3 WHERE l3.id = fact.id AND l3.val > 3);";
  auto results5 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results5->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(1), numResultRows);
  const auto select_crt_row5 = results5->getNextRow(true, false);
  id = getIntValue(select_crt_row5[0]);
  val = getIntValue(select_crt_row5[1]);
  ASSERT_EQ(id, 4);
  ASSERT_EQ(val, 4);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE NOT EXISTS"
      "(SELECT * FROM test_lookup l WHERE l.id <> fact.id AND l.val > 5) AND EXISTS"
      "(SELECT * FROM test_lookup l2 WHERE l2.id = fact.id AND l2.val > 3) AND NOT "
      "EXISTS (SELECT * FROM test_lookup l3 WHERE l3.id = fact.id AND l3.val > 3);";
  auto results6 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  numResultRows = results6->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(0), numResultRows);
}

TEST(Select, JoinCorrelation_InClause) {
  int factsCount = 13;
  int lookupCount = 5;
  setupTest(kINT, factsCount, lookupCount);

  std::string sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE fact.val IN (SELECT l.val "
      "FROM test_lookup l WHERE fact.id = l.id) AND fact.val > 3;";
  auto results1 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  uint32_t numResultRows = results1->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(1), numResultRows);
  const auto select_crt_row = results1->getNextRow(true, false);
  auto id = getIntValue(select_crt_row[0]);
  auto val = getIntValue(select_crt_row[1]);
  ASSERT_EQ(id, 4);
  ASSERT_EQ(val, 4);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE fact.val IN (SELECT l.val "
      "FROM test_lookup l WHERE fact.id = l.id);";
  auto results2 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  uint32_t numResultRows2 = results2->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(lookupCount), numResultRows2);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE fact.val IN (SELECT l.val "
      "FROM test_lookup l WHERE fact.id <> l.id);";
  auto results3 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  uint32_t numResultRows3 = results3->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(0), numResultRows3);

  // a query having more than one correlated IN clauses
  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE fact.val IN (SELECT l.val "
      "FROM test_lookup l WHERE fact.id = l.id) AND fact.val > 1 AND fact.val IN (SELECT "
      "l2.val FROM test_lookup l2 WHERE fact.id = l2.id);";
  auto results4 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  uint32_t numResultRows4 = results4->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(3), numResultRows4);

  sql =
      "SELECT fact.id, fact.val FROM test_facts fact WHERE fact.val IN (SELECT l.val "
      "FROM test_lookup l WHERE fact.id = l.id) AND fact.val > 1 AND fact.val IN (SELECT "
      "l2.val FROM test_lookup l2 WHERE fact.id = l2.id) AND fact.id > 3;";
  auto results5 = run_multiple_agg(sql, ExecutorDeviceType::CPU);
  uint32_t numResultRows5 = results5->rowCount();
  ASSERT_EQ(static_cast<uint32_t>(1), numResultRows5);
  const auto select_crt_row5 = results5->getNextRow(true, false);
  id = getIntValue(select_crt_row5[0]);
  val = getIntValue(select_crt_row5[1]);
  ASSERT_EQ(id, 4);
  ASSERT_EQ(val, 4);
}

TEST(Select, DISABLED_Very_Large_In) {
  // unsupported updates
  const auto file_path =
      boost::filesystem::path("../../Tests/Import/datafiles/very_large_in.csv");
  if (boost::filesystem::exists(file_path)) {
    boost::filesystem::remove(file_path);
  }
  std::ofstream file_out(file_path.string());
  // 100K unique integer
  for (int i = 1; i <= 100000; i++) {
    if (file_out.is_open()) {
      const auto val = ::toString(i);
      file_out << val << ",\'" << val << "\'\n";
    }
  }
  file_out.close();
  auto drop_table = []() { QR::get()->runDDLStatement("DROP TABLE IF EXISTS T_100K;"); };
  auto create_table = []() {
    QR::get()->runDDLStatement(
        "CREATE TABLE T_100K (x int not null, y text encoding dict);");
  };
  auto import_table = []() {
    std::string import_stmt{
        "COPY T_100K FROM "
        "'../../Tests/Import/datafiles/very_large_in.csv' WITH "
        "(header='false')"};
    QR::get()->runDDLStatement(import_stmt);
  };

  drop_table();
  create_table();
  import_table();

  std::string q1 =
      "SELECT COUNT(1) FROM T_100K WHERE rowid IN (SELECT MAX(F.rowid) FROM T_100K F "
      "INNER JOIN (SELECT x FROM T_100K) F1 ON F.x = F1.x GROUP BY F.x);";
  auto result = QR::get()->runSQL(q1, ExecutorDeviceType::CPU);
  const auto crt_row = result->getNextRow(true, false);
  auto res_val = getIntValue(crt_row[0]);
  EXPECT_EQ(100000, res_val);

  std::string update_q1 =
      "UPDATE T_100K SET x = 100001 WHERE rowid IN (SELECT MAX(F.rowid) FROM T_100K "
      "F INNER JOIN (SELECT x FROM T_100K) F1 ON F.x = F1.x GROUP BY F.x);";
  // since IN-clause is decorrelated and evaluated as our hash-join,
  // so we should not get the exception regarding IN-clause
  EXPECT_NO_THROW(QR::get()->runSQL(update_q1, ExecutorDeviceType::CPU));
  std::string query2 = "SELECT COUNT(1) FROM T_100K WHERE x = 100001;";
  auto result2 = QR::get()->runSQL(query2, ExecutorDeviceType::CPU);
  const auto crt_row2 = result2->getNextRow(true, false);
  auto res_val2 = getIntValue(crt_row2[0]);
  EXPECT_EQ(100000, res_val2);

  drop_table();
  create_table();
  import_table();

  std::string update_q2 =
      "UPDATE T_100K SET y = '100001' WHERE rowid IN (SELECT MAX(F.rowid) FROM T_100K "
      "F INNER JOIN (SELECT x FROM T_100K) F1 ON F.x = F1.x GROUP BY F.x);";
  EXPECT_NO_THROW(QR::get()->runSQL(update_q2, ExecutorDeviceType::CPU));
  std::string query3 = "SELECT COUNT(1) FROM T_100K WHERE y = '100001';";
  auto result3 = QR::get()->runSQL(query3, ExecutorDeviceType::CPU);
  const auto crt_row3 = result3->getNextRow(true, false);
  auto res_val3 = getIntValue(crt_row3[0]);
  EXPECT_EQ(100000, res_val3);

  drop_table();
  create_table();
  import_table();

  std::string update_q3 =
      "UPDATE T_100K SET x = 200000, y = '2000000' WHERE rowid IN (SELECT MAX(F.rowid) "
      "FROM T_100K "
      "F INNER JOIN (SELECT x FROM T_100K) F1 ON F.x = F1.x GROUP BY F.x);";
  EXPECT_NO_THROW(QR::get()->runSQL(update_q3, ExecutorDeviceType::CPU));
  std::string query4 = "SELECT COUNT(1) FROM T_100K WHERE y = '2000000';";
  auto result4 = QR::get()->runSQL(query4, ExecutorDeviceType::CPU);
  const auto crt_row4 = result4->getNextRow(true, false);
  auto res_val4 = getIntValue(crt_row4[0]);
  EXPECT_EQ(100000, res_val4);

  QR::get()->runDDLStatement("DROP TABLE IF EXISTS INT_100K;");
  boost::filesystem::remove(file_path);
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  TestHelpers::init_logger_stderr_only(argc, argv);

  init();

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  reset();
  return err;
}
