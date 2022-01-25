#pragma once

#include "QueryEngine/ExecutionKernel.h"  // SharedKernelContext, LegacyExecutionKernel for running queries for now

struct ExecutionDevice {
  ExecutorDeviceType type;
  int id;
};

class SharedKernelContext;

struct Kernel {
  const RelAlgExecutionUnit& ra_exe_unit_;  // compat with legacy kernels
  ExecutionDevice target_device;
  const ExecutionOptions& eo;
  const ColumnFetcher& column_fetcher;
  FragmentsList frag_list;

  const QueryCompilationDescriptor& query_comp_desc;
  const QueryMemoryDescriptor& query_mem_desc;

  ResultSetPtr results;

  LegacyExecutionKernel legacy_kernel;

  Kernel(const RelAlgExecutionUnit& ra_exe_unit,
         const ExecutorDeviceType chosen_device_type,
         int chosen_device_id,
         const ExecutionOptions& eo,
         const ColumnFetcher& column_fetcher,
         const QueryCompilationDescriptor& query_comp_desc,
         const QueryMemoryDescriptor& query_mem_desc,
         const FragmentsList& frag_list,
         const ExecutorDispatchMode kernel_dispatch_mode,
         RenderInfo* render_info,
         const int64_t rowid_lookup_key)
      : legacy_kernel(ra_exe_unit,
                      chosen_device_type,
                      chosen_device_id,
                      eo,
                      column_fetcher,
                      query_comp_desc,
                      query_mem_desc,
                      frag_list,
                      kernel_dispatch_mode,
                      render_info,
                      rowid_lookup_key)
      , ra_exe_unit_(ra_exe_unit)
      , target_device(ExecutionDevice{chosen_device_type, chosen_device_id})
      , eo(eo)
      , column_fetcher(column_fetcher)
      , query_comp_desc(query_comp_desc)
      , query_mem_desc(query_mem_desc)
      , frag_list(frag_list) {}

  void run(Executor* executor,
           const size_t thread_idx,
           SharedKernelContext& shared_context);
};
