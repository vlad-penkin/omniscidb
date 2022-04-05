#include <benchmark/benchmark.h>

#include "DataMgr/ForeignStorage/ArrowForeignStorage.h"
#include "QueryRunner/QueryRunner.h"

#include <boost/program_options.hpp>

#ifndef BASE_PATH
#define BASE_PATH "../Tests/tmp"
#endif

extern bool g_enable_heterogeneous_execution;
extern bool g_enable_multifrag_heterogeneous_execution;

using namespace std;
using QR = QueryRunner::QueryRunner;

inline void run_ddl_statement(const string& input_str) {
  QR::get()->runDDLStatement(input_str);
}

TargetValue run_simple_agg(const string& query_str) {
  auto rows = QR::get()->runSQL(query_str, ExecutorDeviceType::CPU, false);
  auto crt_row = rows->getNextRow(true, true);
  //   CHECK_EQ(size_t(1), crt_row.size()) << query_str;
  return crt_row[0];
}

template <class T>
T v(const TargetValue& r) {
  auto scalar_r = boost::get<ScalarTargetValue>(&r);
  CHECK(scalar_r);
  auto p = boost::get<T>(scalar_r);
  CHECK(p);
  return *p;
}

const char* trips_table_ddl = R"(
CREATE TEMPORARY TABLE trips (
pickup_datetime TIMESTAMP,
passenger_count SMALLINT,
trip_distance DECIMAL(14,2),
total_amount DECIMAL(14,2),
cab_type VARCHAR(6) ENCODING DICT
) WITH (storage_type='CSV:/data/taxi/tmp/trips_reduced_xaa.csv', fragment_size=1000000);
)";

void taxi_q1(benchmark::State& state) {
  run_ddl_statement("drop table if exists trips;");
  run_ddl_statement(trips_table_ddl);
  for (auto _ : state) {
    run_simple_agg("select cab_type, count(*) from trips group by cab_type");
  }
}

void taxi_q2(benchmark::State& state) {
  run_ddl_statement("drop table if exists trips;");
  run_ddl_statement(trips_table_ddl);
  for (auto _ : state) {
    run_simple_agg(
        "SELECT passenger_count, avg(total_amount) FROM trips GROUP BY passenger_count");
  }
}

void taxi_q3(benchmark::State& state) {
  run_ddl_statement("drop table if exists trips;");
  run_ddl_statement(trips_table_ddl);
  for (auto _ : state) {
    run_simple_agg(
        "SELECT passenger_count, extract(year from pickup_datetime) AS pickup_year, "
        "count(*) FROM trips GROUP BY passenger_count, pickup_year");
  }
}

void taxi_q4(benchmark::State& state) {
  run_ddl_statement("drop table if exists trips;");
  run_ddl_statement(trips_table_ddl);
  for (auto _ : state) {
    run_simple_agg(
        "SELECT passenger_count, extract(year from pickup_datetime) AS pickup_year, "
        "cast(trip_distance as int) AS distance, count(*) AS the_count FROM trips GROUP "
        "BY passenger_count, pickup_year, distance ORDER BY pickup_year, the_count desc");
  }
}

BENCHMARK(taxi_q2);

int main(int argc, char* argv[]) {
  ::benchmark::Initialize(&argc, argv);

  namespace po = boost::program_options;

  po::options_description desc("Options");
  desc.add_options()("enable-heterogeneous",
                     po::value<bool>(&g_enable_heterogeneous_execution)
                         ->default_value(g_enable_heterogeneous_execution)
                         ->implicit_value(true),
                     "Allow heterogeneous execution.");
  desc.add_options()("enable-multifrag",
                     po::value<bool>(&g_enable_multifrag_heterogeneous_execution)
                         ->default_value(g_enable_multifrag_heterogeneous_execution)
                         ->implicit_value(true),
                     "Allow multifrag heterogeneous execution.");
  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << "Usage:" << std::endl << desc << std::endl;
  }
  QR::init(BASE_PATH);

  ::benchmark::RunSpecifiedBenchmarks();
  QR::reset();
}