/*
 * Copyright 2018 OmniSci, Inc.
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

#include "CardinalityEstimator.h"
#include "ErrorHandling.h"
#include "ExpressionRewrite.h"
#include "RelAlgExecutor.h"

int64_t g_large_ndv_threshold = 10000000;
size_t g_large_ndv_multiplier = 256;

namespace Analyzer {

size_t LargeNDVEstimator::getBufferSize() const {
  return 1024 * 1024 * g_large_ndv_multiplier;
}

}  // namespace Analyzer

size_t ResultSet::getNDVEstimator() const {
  CHECK(dynamic_cast<const Analyzer::NDVEstimator*>(estimator_.get()));
  CHECK(host_estimator_buffer_);
  auto bits_set = bitmap_set_size(host_estimator_buffer_, estimator_->getBufferSize());
  if (bits_set == 0) {
    // empty result set, return 1 for a groups buffer size of 1
    return 1;
  }
  const auto total_bits = estimator_->getBufferSize() * 8;
  CHECK_LE(bits_set, total_bits);
  const auto unset_bits = total_bits - bits_set;
  const auto ratio = static_cast<double>(unset_bits) / total_bits;
  if (ratio == 0.) {
    LOG(WARNING)
        << "Failed to get a high quality cardinality estimation, falling back to "
           "approximate group by buffer size guess.";
    return 0;
  }
  return -static_cast<double>(total_bits) * log(ratio);
}

size_t RelAlgExecutor::getNDVEstimation(const WorkUnit& work_unit,
                                        const int64_t range,
                                        const bool is_agg,
                                        const CompilationOptions& co,
                                        const ExecutionOptions& eo) {
  const auto estimator_exe_unit = create_ndv_execution_unit(work_unit.exe_unit, range);
  size_t one{1};
  ColumnCacheMap column_cache;
  try {
    const auto estimator_result =
        executor_->executeWorkUnit(one,
                                   is_agg,
                                   get_table_infos(work_unit.exe_unit, executor_),
                                   estimator_exe_unit,
                                   co,
                                   eo,
                                   false,
                                   data_provider_,
                                   column_cache);
    if (estimator_result.empty()) {
      return 1;
    }
    CHECK_EQ(estimator_result.getFragCount(), 1);
    return std::max(estimator_result[0]->getNDVEstimator(), size_t(1));
  } catch (const QueryExecutionError& e) {
    if (e.getErrorCode() == Executor::ERR_OUT_OF_TIME) {
      throw std::runtime_error("Cardinality estimation query ran out of time");
    }
    if (e.getErrorCode() == Executor::ERR_INTERRUPTED) {
      throw std::runtime_error("Cardinality estimation query has been interrupted");
    }
    throw std::runtime_error("Failed to run the cardinality estimation query: " +
                             getErrorMessageFromCode(e.getErrorCode()));
  }
  UNREACHABLE();
  return 0;
}

RelAlgExecutionUnit create_ndv_execution_unit(const RelAlgExecutionUnit& ra_exe_unit,
                                              const int64_t range) {
  const bool use_large_estimator = range > g_large_ndv_threshold;
  return {ra_exe_unit.input_descs,
          ra_exe_unit.input_col_descs,
          ra_exe_unit.simple_quals,
          ra_exe_unit.quals,
          ra_exe_unit.join_quals,
          {},
          {},
          use_large_estimator
              ? makeExpr<Analyzer::LargeNDVEstimator>(ra_exe_unit.groupby_exprs)
              : makeExpr<Analyzer::NDVEstimator>(ra_exe_unit.groupby_exprs),
          SortInfo{{}, SortAlgorithm::Default, 0, 0},
          0,
          ra_exe_unit.query_hint,
          ra_exe_unit.query_plan_dag,
          ra_exe_unit.hash_table_build_plan_dag,
          ra_exe_unit.table_id_to_node_map,
          false,
          ra_exe_unit.union_all};
}

RelAlgExecutionUnit create_count_all_execution_unit(
    const RelAlgExecutionUnit& ra_exe_unit,
    std::shared_ptr<Analyzer::Expr> replacement_target,
    bool strip_join_covered_quals) {
  return {ra_exe_unit.input_descs,
          ra_exe_unit.input_col_descs,
          ra_exe_unit.simple_quals,
          strip_join_covered_quals
              ? strip_join_covered_filter_quals(ra_exe_unit.quals, ra_exe_unit.join_quals)
              : ra_exe_unit.quals,
          ra_exe_unit.join_quals,
          {},
          {replacement_target.get()},
          nullptr,
          SortInfo{{}, SortAlgorithm::Default, 0, 0},
          0,
          ra_exe_unit.query_hint,
          ra_exe_unit.query_plan_dag,
          ra_exe_unit.hash_table_build_plan_dag,
          ra_exe_unit.table_id_to_node_map,
          false,
          ra_exe_unit.union_all};
}

ResultSetPtr reduce_estimator_results(
    const RelAlgExecutionUnit& ra_exe_unit,
    std::vector<std::pair<ResultSetPtr, std::vector<size_t>>>& results_per_device) {
  if (results_per_device.empty()) {
    return nullptr;
  }
  CHECK(dynamic_cast<const Analyzer::NDVEstimator*>(ra_exe_unit.estimator.get()));
  const auto& result_set = results_per_device.front().first;
  CHECK(result_set);
  auto estimator_buffer = result_set->getHostEstimatorBuffer();
  CHECK(estimator_buffer);
  for (size_t i = 1; i < results_per_device.size(); ++i) {
    const auto& next_result_set = results_per_device[i].first;
    const auto other_estimator_buffer = next_result_set->getHostEstimatorBuffer();
    for (size_t off = 0; off < ra_exe_unit.estimator->getBufferSize(); ++off) {
      estimator_buffer[off] |= other_estimator_buffer[off];
    }
  }
  return std::move(result_set);
}
