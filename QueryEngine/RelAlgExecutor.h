/*
 * Copyright 2017 MapD Technologies, Inc.
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

#ifndef QUERYENGINE_RELALGEXECUTOR_H
#define QUERYENGINE_RELALGEXECUTOR_H

#include "DataProvider/DataProvider.h"
#include "QueryEngine/Descriptors/RelAlgExecutionDescriptor.h"
#include "QueryEngine/Execute.h"
#include "QueryEngine/InputMetadata.h"
#include "QueryEngine/JoinFilterPushDown.h"
#include "QueryEngine/QueryRewrite.h"
#include "QueryEngine/RelAlgDagBuilder.h"
#include "QueryEngine/RelAlgSchemaProvider.h"
#include "QueryEngine/SpeculativeTopN.h"
#include "QueryEngine/StreamingTopN.h"
#include "Shared/scope.h"

#include <ctime>
#include <sstream>

extern bool g_skip_intermediate_count;

enum class MergeType { Union, Reduce };

struct QueryStepExecutionResult {
  ExecutionResult result;
  const MergeType merge_type;
  const unsigned node_id;
  bool is_outermost_query;
};

class RelAlgExecutor {
 public:
  using TargetInfoList = std::vector<TargetInfo>;

  RelAlgExecutor(Executor* executor,
                 SchemaProviderPtr schema_provider,
                 DataProvider* data_provider);

  RelAlgExecutor(Executor* executor,
                 SchemaProviderPtr schema_provider,
                 DataProvider* data_provider,
                 std::unique_ptr<RelAlgDag> query_dag);

  size_t getOuterFragmentCount(const CompilationOptions& co, const ExecutionOptions& eo);

  ExecutionResult executeRelAlgQuery(const CompilationOptions& co,
                                     const ExecutionOptions& eo,
                                     const bool just_explain_plan);

  // does preparational stuff and compiles kernels
  void prepareStreamingExecution(const CompilationOptions& co,
                                 const ExecutionOptions& eo);

  // process batch
  ResultSetPtr runOnBatch(const FragmentsPerTable& fragments);

  // when the last batch has been send, invoke this function.
  ResultSetPtr finishStreamingExecution();

  ExecutionResult executeRelAlgQueryWithFilterPushDown(const RaExecutionSequence& seq,
                                                       const CompilationOptions& co,
                                                       const ExecutionOptions& eo,
                                                       const int64_t queue_time_ms);

  void prepareLeafExecution(
      const AggregatedColRange& agg_col_range,
      const StringDictionaryGenerations& string_dictionary_generations,
      const TableGenerations& table_generations);

  ExecutionResult executeRelAlgSeq(const RaExecutionSequence& seq,
                                   const CompilationOptions& co,
                                   const ExecutionOptions& eo,
                                   const int64_t queue_time_ms,
                                   const bool with_existing_temp_tables = false);

  ExecutionResult executeRelAlgSubSeq(const RaExecutionSequence& seq,
                                      const std::pair<size_t, size_t> interval,
                                      const CompilationOptions& co,
                                      const ExecutionOptions& eo,
                                      const int64_t queue_time_ms);

  QueryStepExecutionResult executeRelAlgQuerySingleStep(const RaExecutionSequence& seq,
                                                        const size_t step_idx,
                                                        const CompilationOptions& co,
                                                        const ExecutionOptions& eo);

  const RelAlgNode& getRootRelAlgNode() const {
    CHECK(query_dag_);
    return query_dag_->getRootNode();
  }

  std::shared_ptr<const RelAlgNode> getRootRelAlgNodeShPtr() const {
    CHECK(query_dag_);
    return query_dag_->getRootNodeShPtr();
  }

  std::pair<std::vector<unsigned>, std::unordered_map<unsigned, JoinQualsPerNestingLevel>>
  getJoinInfo(const RelAlgNode* root_node);

  std::shared_ptr<RelAlgTranslator> getRelAlgTranslator(const RelAlgNode* root_node);

  const std::vector<std::shared_ptr<RexSubQuery>>& getSubqueries() const noexcept {
    CHECK(query_dag_);
    return query_dag_->getSubqueries();
  };

  std::optional<RegisteredQueryHint> getParsedQueryHint(const RelAlgNode* node) {
    return query_dag_ ? query_dag_->getQueryHint(node) : std::nullopt;
  }

  std::optional<std::unordered_map<size_t, RegisteredQueryHint>> getParsedQueryHints() {
    return query_dag_ ? std::make_optional(query_dag_->getQueryHints()) : std::nullopt;
  }

  ExecutionResult executeSimpleInsert(const Analyzer::Query& insert_query);

  AggregatedColRange computeColRangesCache();
  StringDictionaryGenerations computeStringDictionaryGenerations();
  TableGenerations computeTableGenerations();

  Executor* getExecutor() const;

  void cleanupPostExecution();

  static std::string getErrorMessageFromCode(const int32_t error_code);

  void executePostExecutionCallback();

 private:
  ExecutionResult executeRelAlgQueryNoRetry(const CompilationOptions& co,
                                            const ExecutionOptions& eo,
                                            const bool just_explain_plan);

  void executeRelAlgStep(const RaExecutionSequence& seq,
                         const size_t step_idx,
                         const CompilationOptions&,
                         const ExecutionOptions&,
                         const int64_t queue_time_ms);

  ExecutionResult executeCompound(const RelCompound*,
                                  const CompilationOptions&,
                                  const ExecutionOptions&,
                                  const int64_t queue_time_ms);

  ExecutionResult executeAggregate(const RelAggregate* aggregate,
                                   const CompilationOptions& co,
                                   const ExecutionOptions& eo,
                                   const int64_t queue_time_ms);

  ExecutionResult executeProject(const RelProject*,
                                 const CompilationOptions&,
                                 const ExecutionOptions&,
                                 const int64_t queue_time_ms,
                                 const std::optional<size_t> previous_count);

  ExecutionResult executeTableFunction(const RelTableFunction*,
                                       const CompilationOptions&,
                                       const ExecutionOptions&,
                                       const int64_t queue_time_ms);

  // Computes the window function results to be used by the query.
  void computeWindow(const RelAlgExecutionUnit& ra_exe_unit,
                     const CompilationOptions& co,
                     const ExecutionOptions& eo,
                     ColumnCacheMap& column_cache_map,
                     const int64_t queue_time_ms);

  // Creates the window context for the given window function.
  std::unique_ptr<WindowFunctionContext> createWindowFunctionContext(
      const Analyzer::WindowFunction* window_func,
      const std::shared_ptr<Analyzer::BinOper>& partition_key_cond,
      const RelAlgExecutionUnit& ra_exe_unit,
      const std::vector<InputTableInfo>& query_infos,
      const CompilationOptions& co,
      ColumnCacheMap& column_cache_map,
      std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner);

  ExecutionResult executeFilter(const RelFilter*,
                                const CompilationOptions&,
                                const ExecutionOptions&,
                                const int64_t queue_time_ms);

  ExecutionResult executeSort(const RelSort*,
                              const CompilationOptions&,
                              const ExecutionOptions&,
                              const int64_t queue_time_ms);

  ExecutionResult executeLogicalValues(const RelLogicalValues*, const ExecutionOptions&);

  ExecutionResult executeUnion(const RelLogicalUnion*,
                               const RaExecutionSequence&,
                               const CompilationOptions&,
                               const ExecutionOptions&,
                               const int64_t queue_time_ms);

  // TODO(alex): just move max_groups_buffer_entry_guess to RelAlgExecutionUnit once
  //             we deprecate the plan-based executor paths and remove WorkUnit
  struct WorkUnit {
    RelAlgExecutionUnit exe_unit;
    const RelAlgNode* body;
    const size_t max_groups_buffer_entry_guess;
    std::unique_ptr<QueryRewriter> query_rewriter;
    const std::vector<size_t> input_permutation;
    const std::vector<size_t> left_deep_join_input_sizes;
  };

  WorkUnit createWorkUnitForStreaming(const RelAlgNode* body,
                                      const CompilationOptions& co,
                                      const ExecutionOptions& eo);

  std::pair<CompilationOptions, ExecutionOptions> handle_hint(
      const CompilationOptions& co,
      const ExecutionOptions& eo,
      const RelAlgNode* body);

  struct TableFunctionWorkUnit {
    TableFunctionExecutionUnit exe_unit;
    const RelAlgNode* body;
  };

  WorkUnit createSortInputWorkUnit(const RelSort*, const ExecutionOptions& eo);

  ExecutionResult executeWorkUnit(
      const WorkUnit& work_unit,
      const std::vector<TargetMetaInfo>& targets_meta,
      const bool is_agg,
      const CompilationOptions& co_in,
      const ExecutionOptions& eo_in,
      const int64_t queue_time_ms,
      const std::optional<size_t> previous_count = std::nullopt);

  size_t getNDVEstimation(const WorkUnit& work_unit,
                          const int64_t range,
                          const bool is_agg,
                          const CompilationOptions& co,
                          const ExecutionOptions& eo);

  std::optional<size_t> getFilteredCountAll(const WorkUnit& work_unit,
                                            const bool is_agg,
                                            const CompilationOptions& co,
                                            const ExecutionOptions& eo);

  FilterSelectivity getFilterSelectivity(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& filter_expressions,
      const CompilationOptions& co,
      const ExecutionOptions& eo);

  std::vector<PushedDownFilterInfo> selectFiltersToBePushedDown(
      const RelAlgExecutor::WorkUnit& work_unit,
      const CompilationOptions& co,
      const ExecutionOptions& eo);

  bool isRowidLookup(const WorkUnit& work_unit);

  ExecutionResult handleOutOfMemoryRetry(const RelAlgExecutor::WorkUnit& work_unit,
                                         const std::vector<TargetMetaInfo>& targets_meta,
                                         const bool is_agg,
                                         const CompilationOptions& co,
                                         const ExecutionOptions& eo,
                                         const bool was_multifrag_kernel_launch,
                                         const int64_t queue_time_ms);

  // Allows an out of memory error through if CPU retry is enabled. Otherwise, throws an
  // appropriate exception corresponding to the query error code.
  static void handlePersistentError(const int32_t error_code);

  WorkUnit createWorkUnit(const RelAlgNode*, const SortInfo&, const ExecutionOptions& eo);

  WorkUnit createCompoundWorkUnit(const RelCompound*,
                                  const SortInfo&,
                                  const ExecutionOptions& eo);

  WorkUnit createAggregateWorkUnit(const RelAggregate*,
                                   const SortInfo&,
                                   const bool just_explain);

  WorkUnit createProjectWorkUnit(const RelProject*,
                                 const SortInfo&,
                                 const ExecutionOptions& eo);

  WorkUnit createFilterWorkUnit(const RelFilter*,
                                const SortInfo&,
                                const bool just_explain);

  WorkUnit createJoinWorkUnit(const RelJoin*, const SortInfo&, const bool just_explain);

  WorkUnit createUnionWorkUnit(const RelLogicalUnion*,
                               const SortInfo&,
                               const ExecutionOptions& eo);

  TableFunctionWorkUnit createTableFunctionWorkUnit(const RelTableFunction* table_func,
                                                    const bool just_explain,
                                                    const bool is_gpu);

  void addTemporaryTable(const int table_id, const ResultSetPtr& result) {
    CHECK_LT(size_t(0), result->colCount());
    CHECK_LT(table_id, 0);
    const auto it_ok = temporary_tables_.emplace(table_id, result);
    CHECK(it_ok.second);
  }

  void addTemporaryTable(const int table_id, const TemporaryTable& table) {
    CHECK_LT(table_id, 0);
    const auto it_ok = temporary_tables_.emplace(table_id, table);
    CHECK(it_ok.second);
  }

  void eraseFromTemporaryTables(const int table_id) { temporary_tables_.erase(table_id); }

  void handleNop(RaExecutionDesc& ed);

  std::unordered_map<unsigned, JoinQualsPerNestingLevel>& getLeftDeepJoinTreesInfo() {
    return left_deep_join_info_;
  }

  JoinQualsPerNestingLevel translateLeftDeepJoinFilter(
      const RelLeftDeepInnerJoin* join,
      const std::vector<InputDescriptor>& input_descs,
      const std::unordered_map<const RelAlgNode*, int>& input_to_nest_level,
      const bool just_explain);

  // Transform the provided `join_condition` to conjunctive form, find composite
  // key opportunities and finally translate it to an Analyzer expression.
  std::list<std::shared_ptr<Analyzer::Expr>> makeJoinQuals(
      const RexScalar* join_condition,
      const std::vector<JoinType>& join_types,
      const std::unordered_map<const RelAlgNode*, int>& input_to_nest_level,
      const bool just_explain) const;

  Executor* executor_;
  std::unique_ptr<RelAlgDag> query_dag_;
  std::shared_ptr<SchemaProvider> schema_provider_;
  DataProvider* data_provider_;
  const Config& config_;
  TemporaryTables temporary_tables_;
  time_t now_;
  std::unordered_map<unsigned, JoinQualsPerNestingLevel> left_deep_join_info_;
  std::vector<std::shared_ptr<Analyzer::Expr>> target_exprs_owned_;  // TODO(alex): remove
  int64_t queue_time_ms_;
  static SpeculativeTopNBlacklist speculative_topn_blacklist_;

  std::optional<std::function<void()>> post_execution_callback_;

  std::shared_ptr<StreamExecutionContext> stream_execution_context_;

  friend class PendingExecutionClosure;
};

#endif  // QUERYENGINE_RELALGEXECUTOR_H
