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

#include "QueryCompilationDescriptor.h"

#include "QueryEngine/Execute.h"

extern bool g_enable_lazy_fetch;

std::unique_ptr<QueryMemoryDescriptor> QueryCompilationDescriptor::compile(
    const size_t max_groups_buffer_entry_guess,
    const int8_t crt_min_byte_width,
    const bool has_cardinality_estimation,
    const RelAlgExecutionUnit& ra_exe_unit,
    const std::vector<InputTableInfo>& table_infos,
    const ColumnFetcher& column_fetcher,
    const CompilationOptions& co,
    const ExecutionOptions& eo,
    Executor* executor) {
  compilation_device_type_ = co.device_type;
  hoist_literals_ = co.hoist_literals;
  CHECK(executor);
  std::unique_ptr<QueryMemoryDescriptor> query_mem_desc;
  try {
    std::tie(compilation_result_, query_mem_desc) = executor->compileWorkUnit(
        table_infos,
        ra_exe_unit,
        co,
        eo,
        executor->getDataMgr()->getCudaMgr(),
        g_enable_lazy_fetch &&
            co.allow_lazy_fetch,  // TODO(adb): remove param and just read from CO
        executor->row_set_mem_owner_,
        max_groups_buffer_entry_guess,
        crt_min_byte_width,
        has_cardinality_estimation,
        column_fetcher.getDataProvider(),
        column_fetcher.columnarized_table_cache_);
  } catch (const CompilationRetryNoLazyFetch&) {
    if (executor->cgen_state_->module_) {
      delete executor->cgen_state_->module_;
    }
    std::tie(compilation_result_, query_mem_desc) =
        executor->compileWorkUnit(table_infos,
                                  ra_exe_unit,
                                  co,
                                  eo,
                                  executor->getDataMgr()->getCudaMgr(),
                                  false,
                                  executor->row_set_mem_owner_,
                                  max_groups_buffer_entry_guess,
                                  crt_min_byte_width,
                                  has_cardinality_estimation,
                                  column_fetcher.getDataProvider(),
                                  column_fetcher.columnarized_table_cache_);
  }
  actual_min_byte_width_ =
      std::max(query_mem_desc->updateActualMinByteWidth(MAX_BYTE_WIDTH_SUPPORTED),
               crt_min_byte_width);
  return query_mem_desc;
}
