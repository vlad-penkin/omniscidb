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

#include "DBEngine.h"
#include <boost/filesystem.hpp>
#include <stdexcept>
#include "DataMgr/ForeignStorage/ArrowForeignStorage.h"
#include "DataMgr/ForeignStorage/ForeignStorageInterface.h"
#include "Fragmenter/FragmentDefaultValues.h"
#include "Parser/ParserWrapper.h"
#include "Parser/parser.h"
#include "QueryEngine/ArrowResultSet.h"
#include "QueryEngine/Execute.h"
#include "QueryEngine/ExtensionFunctionsWhitelist.h"
#include "QueryEngine/TableFunctions/TableFunctionsFactory.h"
#include "ThriftHandler/CommandLineOptions.h"
#include "ThriftHandler/DBHandler.h"

extern bool g_enable_union;

namespace EmbeddedDatabase {

class DBEngineImpl;

/**
 * Cursor internal implementation
 */
class CursorImpl : public Cursor {
 public:
  CursorImpl(std::shared_ptr<ResultSet> result_set, std::vector<std::string> col_names)
      : result_set_(result_set), col_names_(col_names) {}

  ~CursorImpl() {
    col_names_.clear();
    record_batch_.reset();
    result_set_.reset();
  }

  size_t getColCount() { return result_set_ ? result_set_->colCount() : 0; }

  size_t getRowCount() { return result_set_ ? result_set_->rowCount() : 0; }

  Row getNextRow() {
    if (result_set_) {
      auto row = result_set_->getNextRow(true, false);
      return row.empty() ? Row() : Row(row);
    }
    return Row();
  }

  ColumnType getColType(uint32_t col_num) {
    if (col_num < getColCount()) {
      SQLTypeInfo type_info = result_set_->getColType(col_num);
      return sqlToColumnType(type_info.get_type());
    }
    return ColumnType::UNKNOWN;
  }

  std::shared_ptr<arrow::RecordBatch> getArrowRecordBatch() {
    if (record_batch_) {
      return record_batch_;
    }
    auto col_count = getColCount();
    if (col_count > 0) {
      auto row_count = getRowCount();
      if (row_count > 0) {
        auto converter =
            std::make_unique<ArrowResultSetConverter>(result_set_, col_names_, -1);
        record_batch_ = converter->convertToArrow();
        return record_batch_;
      }
    }
    return nullptr;
  }

 private:
  std::shared_ptr<ResultSet> result_set_;
  std::vector<std::string> col_names_;
  std::shared_ptr<arrow::RecordBatch> record_batch_;
};

class DBEHandler : public DBHandler {
 public:
  std::shared_ptr<ExecutionResult> sql_execute(const TSessionId& session,
                                               const std::string& query_str,
                                               const bool column_format,
                                               const std::string& nonce,
                                               const int32_t first_n,
                                               const int32_t at_most_n) {
    data_.reset();
    auto session_ptr = get_session_ptr(session);
    auto query_state = create_query_state(session_ptr, query_str);
    auto stdlog = STDLOG(session_ptr, query_state);
    stdlog.appendNameValuePairs("client", getConnectionInfo().toString());
    stdlog.appendNameValuePairs("nonce", nonce);
    auto timer = DEBUG_TIMER(__func__);

    try {
      ScopeGuard reset_was_geo_copy_from = [this, &session_ptr] {
        geo_copy_from_sessions.remove(session_ptr->get_session_id());
      };

      if (first_n >= 0 && at_most_n >= 0) {
        THROW_MAPD_EXCEPTION(
            std::string("At most one of first_n and at_most_n can be set"));
      }

      if (leaf_aggregator_.leafCount() > 0) {
        if (!agg_handler_) {
          THROW_MAPD_EXCEPTION("Distributed support is disabled.");
        }
        result_.total_time_ms = measure<>::execution([&]() {
          agg_handler_->cluster_execute(result_,
                                        query_state->createQueryStateProxy(),
                                        query_state->getQueryStr(),
                                        column_format,
                                        nonce,
                                        first_n,
                                        at_most_n,
                                        system_parameters_);
        });
        result_.nonce = nonce;
      } else {
        result_.total_time_ms = measure<>::execution([&]() {
          sql_execute_impl(query_state->createQueryStateProxy(),
                           column_format,
                           nonce,
                           session_ptr->get_executor_device_type(),
                           first_n,
                           at_most_n);
        });
      }

      // if the SQL statement we just executed was a geo COPY FROM, the import
      // parameters were captured, and this flag set, so we do the actual import here
      if (auto geo_copy_from_state =
              geo_copy_from_sessions(session_ptr->get_session_id())) {
        // import_geo_table() calls create_table() which calls this function to
        // do the work, so reset the flag now to avoid executing this part a
        // second time at the end of that, which would fail as the table was
        // already created! Also reset the flag with a ScopeGuard on exiting
        // this function any other way, such as an exception from the code above!
        geo_copy_from_sessions.remove(session_ptr->get_session_id());

        // create table as replicated?
        TCreateParams create_params;
        if (geo_copy_from_state->geo_copy_from_partitions == "REPLICATED") {
          create_params.is_replicated = true;
        }

        // now do (and time) the import
        result_.total_time_ms = measure<>::execution([&]() {
          import_geo_table(
            session,
            geo_copy_from_state->geo_copy_from_table,
            geo_copy_from_state->geo_copy_from_file_name,
            copyparams_to_thrift(geo_copy_from_state->geo_copy_from_copy_params),
            TRowDescriptor(),
            create_params);
        });
      }
      std::string debug_json = timer.stopAndGetJson();
      if (!debug_json.empty()) {
        result_.__set_debug(std::move(debug_json));
      }
      stdlog.appendNameValuePairs(
          "execution_time_ms",
          result_.execution_time_ms,
          "total_time_ms",  // BE-3420 - Redundant with duration field
          stdlog.duration<std::chrono::milliseconds>());
      VLOG(1) << "Table Schema Locks:\n" << lockmgr::TableSchemaLockMgr::instance();
      VLOG(1) << "Table Data Locks:\n" << lockmgr::TableDataLockMgr::instance();
    } catch (const std::exception& e) {
      if (strstr(e.what(), "java.lang.NullPointerException")) {
        THROW_MAPD_EXCEPTION(std::string("Exception: ") +
                           "query failed from broken view or other schema related issue");
      } else if (strstr(e.what(), "Parse failed: Encountered \";\"")) {
        THROW_MAPD_EXCEPTION("multiple SQL statements not allowed");
      } else if (strstr(e.what(),
                      "Parse failed: Encountered \"<EOF>\" at line 0, column 0")) {
        THROW_MAPD_EXCEPTION("empty SQL statment not allowed");
      } else {
        THROW_MAPD_EXCEPTION(std::string("Exception: ") + e.what());
      }
    }
    return data_;
  }

 private:
  std::vector<PushedDownFilterInfo> execute_rel_alg(QueryStateProxy query_state_proxy,
                                                    const std::string& query_ra,
                                                    const bool column_format,
                                                    const ExecutorDeviceType executor_device_type,
                                                    const int32_t first_n,
                                                    const int32_t at_most_n,
                                                    const bool just_validate,
                                                    const bool find_push_down_candidates,
                                                    const ExplainInfo& explain_info,
                                                    const std::optional<size_t> executor_index) const {
    query_state::Timer timer = query_state_proxy.createTimer(__func__);
    VLOG(1) << "Table Schema Locks:\n" << lockmgr::TableSchemaLockMgr::instance();
    VLOG(1) << "Table Data Locks:\n" << lockmgr::TableDataLockMgr::instance();

    const auto& cat = query_state_proxy.getQueryState().getConstSessionInfo()->getCatalog();
    auto executor = Executor::getExecutor(
      executor_index ? *executor_index : Executor::UNITARY_EXECUTOR_ID,
      jit_debug_ ? "/tmp" : "",
      jit_debug_ ? "mapdquery" : "",
      system_parameters_);
    RelAlgExecutor ra_executor(executor.get(),
                               cat,
                               query_ra,
                               query_state_proxy.getQueryState().shared_from_this());
    // handle hints
    const auto& query_hints = ra_executor.getParsedQueryHints();
    CompilationOptions co = {
      query_hints.cpu_mode ? ExecutorDeviceType::CPU : executor_device_type,
      /*hoist_literals=*/true,
      ExecutorOptLevel::Default,
      g_enable_dynamic_watchdog,
      /*allow_lazy_fetch=*/true,
      /*filter_on_deleted_column=*/true,
      explain_info.explain_optimized ? ExecutorExplainType::Optimized
                                     : ExecutorExplainType::Default,
      intel_jit_profile_};
    ExecutionOptions eo = {g_enable_columnar_output,
                           allow_multifrag_,
                           explain_info.justExplain(),
                           allow_loop_joins_ || just_validate,
                           g_enable_watchdog,
                           jit_debug_,
                           just_validate,
                           g_enable_dynamic_watchdog,
                           g_dynamic_watchdog_time_limit,
                           find_push_down_candidates,
                           explain_info.justCalciteExplain(),
                           system_parameters_.gpu_input_mem_limit,
                           g_enable_runtime_query_interrupt,
                           g_runtime_query_interrupt_frequency};
    data_.reset();
    data_ = std::make_shared<ResultSet>(std::vector<TargetInfo>{},
                                        ExecutorDeviceType::CPU,
                                        QueryMemoryDescriptor(),
                                        nullptr,
                                        nullptr);
    result_.execution_time_ms += measure<>::execution([&]() {
      data_ = ra_executor.executeRelAlgQuery(co, eo, explain_info.explain_plan, nullptr);
    });
    // reduce execution time by the time spent during queue waiting
    result_.execution_time_ms -= data_.getRows()->getQueueTime();
    VLOG(1) << cat.getDataMgr().getSystemMemoryUsage();
    const auto& filter_push_down_info = data_.getPushedDownFilterInfo();
    if (!filter_push_down_info.empty()) {
      return filter_push_down_info;
    }
    if (explain_info.justExplain()) {
      convert_explain(result_, *data.getRows(), column_format);
    }
    return {};
  }

  void sql_execute_impl(QueryStateProxy query_state_proxy,
                        const bool column_format,
                        const std::string& nonce,
                        const ExecutorDeviceType executor_device_type,
                        const int32_t first_n,
                        const int32_t at_most_n) {
    if (leaf_handler_) {
      leaf_handler_->flush_queue();
    }

    result_.nonce = nonce;
    result_.execution_time_ms = 0;
    auto const query_str = strip(query_state_proxy.getQueryState().getQueryStr());
    auto session_ptr = query_state_proxy.getQueryState().getConstSessionInfo();
    // Call to DistributedValidate() below may change cat.
    auto& cat = session_ptr->getCatalog();

    std::list<std::unique_ptr<Parser::Stmt>> parse_trees;

    mapd_unique_lock<mapd_shared_mutex> executeWriteLock;
    mapd_shared_lock<mapd_shared_mutex> executeReadLock;

    lockmgr::LockedTableDescriptors locks;
    ParserWrapper pw{query_str};
    switch (pw.getQueryType()) {
      case ParserWrapper::QueryType::Read: {
        result_.query_type = TQueryType::READ;
        VLOG(1) << "query type: READ";
        break;
      }
      case ParserWrapper::QueryType::Write: {
        result_.query_type = TQueryType::WRITE;
        VLOG(1) << "query type: WRITE";
        break;
      }
      case ParserWrapper::QueryType::SchemaRead: {
        result_.query_type = TQueryType::SCHEMA_READ;
        VLOG(1) << "query type: SCHEMA READ";
        break;
      }
      case ParserWrapper::QueryType::SchemaWrite: {
        result_.query_type = TQueryType::SCHEMA_WRITE;
        VLOG(1) << "query type: SCHEMA WRITE";
        break;
      }
      default: {
        result_.query_type = TQueryType::UNKNOWN;
        LOG(WARNING) << "query type: UNKNOWN";
        break;
      }
    }
    if (pw.isCalcitePathPermissable(read_only_)) {
      executeReadLock = mapd_shared_lock<mapd_shared_mutex>(
          *legacylockmgr::LockMgr<mapd_shared_mutex, bool>::getMutex(
              legacylockmgr::ExecutorOuterLock, true));

      std::string query_ra;
      if (pw.is_exec_ra) {
        query_ra = pw.actual_query;
      } else {
        result_.execution_time_ms += measure<>::execution([&]() {
          TPlanResult result;
          std::tie(result, locks) =
              parse_to_ra(query_state_proxy, query_str, {}, true, system_parameters_);
          query_ra = result.plan_result;
        });
      }

      std::string query_ra_calcite_explain;
      if (pw.isCalciteExplain() && (!g_enable_filter_push_down || g_cluster)) {
        // return the ra as the result
        convert_explain(result_, ResultSet(query_ra), true);
        return;
      } else if (pw.isCalciteExplain()) {
        // removing the "explain calcite " from the beginning of the "query_str":
        std::string temp_query_str =
            query_str.substr(std::string("explain calcite ").length());

        CHECK(!locks.empty());
        query_ra_calcite_explain =
            parse_to_ra(query_state_proxy, temp_query_str, {}, false, system_parameters_)
                .first.plan_result;
      } else if (pw.isCalciteDdl()) {
        executeDdl(result_, query_ra, session_ptr);
        return;
      }
      const auto explain_info = pw.getExplainInfo();
      std::vector<PushedDownFilterInfo> filter_push_down_requests;
      auto execute_rel_alg_task = std::make_shared<QueryDispatchQueue::Task>(
        [this,
         &filter_push_down_requests,
         &result_,
         &query_state_proxy,
         &explain_info,
         &query_ra_calcite_explain,
         &query_ra,
         &query_str,
         &locks,
         column_format,
         executor_device_type,
         first_n,
         at_most_n](const size_t executor_index) {
          filter_push_down_requests = execute_rel_alg(
              query_state_proxy,
              explain_info.justCalciteExplain() ? query_ra_calcite_explain : query_ra,
              column_format,
              executor_device_type,
              first_n,
              at_most_n,
              /*just_validate=*/false,
              g_enable_filter_push_down && !g_cluster,
              explain_info,
              executor_index);
          if (explain_info.justCalciteExplain() && filter_push_down_requests.empty()) {
            // we only reach here if filter push down was enabled, but no filter
            // push down candidate was found
            convert_explain(result_, ResultSet(query_ra), true);
          } else if (!filter_push_down_requests.empty()) {
            CHECK(!locks.empty());
            execute_rel_alg_with_filter_push_down(query_state_proxy,
                                                  query_ra,
                                                  column_format,
                                                  executor_device_type,
                                                  first_n,
                                                  at_most_n,
                                                  explain_info.justExplain(),
                                                  explain_info.justCalciteExplain(),
                                                  filter_push_down_requests);
          } else if (explain_info.justCalciteExplain() &&
                     filter_push_down_requests.empty()) {
            // return the ra as the result:
            // If we reach here, the 'filter_push_down_request' turned out to be
            // empty, i.e., no filter push down so we continue with the initial
            // (unchanged) query's calcite explanation.
            CHECK(!locks.empty());
            query_ra =
                parse_to_ra(query_state_proxy, query_str, {}, false, system_parameters_)
                    .first.plan_result;
            convert_explain(result_, ResultSet(query_ra), true);
          }
        });
      CHECK(dispatch_queue_);
      dispatch_queue_->submit(execute_rel_alg_task,
                              pw.getDMLType() == ParserWrapper::DMLType::Update ||
                                  pw.getDMLType() == ParserWrapper::DMLType::Delete);
      auto result_future = execute_rel_alg_task->get_future();
      result_future.get();
      return;
    } else if (pw.is_optimize || pw.is_validate) {
      // Get the Stmt object
      DBHandler::parser_with_error_handler(query_str, parse_trees);

      if (pw.is_optimize) {
        const auto optimize_stmt =
            dynamic_cast<Parser::OptimizeTableStmt*>(parse_trees.front().get());
        CHECK(optimize_stmt);

        result_.execution_time_ms += measure<>::execution([&]() {
          const auto td_with_lock =
              lockmgr::TableSchemaLockContainer<lockmgr::WriteLock>::acquireTableDescriptor(
                  cat, optimize_stmt->getTableName());
          const auto td = td_with_lock();

          if (!td || !user_can_access_table(
                         *session_ptr, td, AccessPrivileges::DELETE_FROM_TABLE)) {
            throw std::runtime_error("Table " + optimize_stmt->getTableName() +
                                     " does not exist.");
          }
          if (td->isView) {
            throw std::runtime_error("OPTIMIZE TABLE command is not supported on views.");
          }

          // acquire write lock on table data
          auto data_lock =
              lockmgr::TableDataLockMgr::getWriteLockForTable(cat, td->tableName);

          auto executor = Executor::getExecutor(
              Executor::UNITARY_EXECUTOR_ID, "", "", system_parameters_);
          const TableOptimizer optimizer(td, executor.get(), cat);
          if (optimize_stmt->shouldVacuumDeletedRows()) {
            optimizer.vacuumDeletedRows();
          }
          optimizer.recomputeMetadata();
        });

        return;
      }
      if (pw.is_validate) {
        // check user is superuser
        if (!session_ptr->get_currentUser().isSuper) {
          throw std::runtime_error("Superuser is required to run VALIDATE");
        }
        const auto validate_stmt =
            dynamic_cast<Parser::ValidateStmt*>(parse_trees.front().get());
        CHECK(validate_stmt);

        // Prevent any other query from running while doing validate
        executeWriteLock = mapd_unique_lock<mapd_shared_mutex>(
            *legacylockmgr::LockMgr<mapd_shared_mutex, bool>::getMutex(
                legacylockmgr::ExecutorOuterLock, true));

        std::string output{"Result for validate"};
        if (g_cluster && leaf_aggregator_.leafCount()) {
          result_.execution_time_ms += measure<>::execution([&]() {
            const DistributedValidate validator(validate_stmt->getType(),
                                                validate_stmt->isRepairTypeRemove(),
                                                cat,  // tables may be dropped here
                                                leaf_aggregator_,
                                                *session_ptr,
                                                *this);
            output = validator.validate(query_state_proxy);
          });
        } else {
          output = "Not running on a cluster nothing to validate";
        }
        convert_result(result_, ResultSet(output), true);
        return;
      }
    }
    LOG(INFO) << "passing query to legacy processor";

    const auto result = apply_copy_to_shim(query_str);
    DBHandler::parser_with_error_handler(result, parse_trees);
    auto handle_ddl = [&query_state_proxy, &session_ptr, &result_, &locks, this](
                          Parser::DDLStmt* ddl) -> bool {
      if (!ddl) {
        return false;
      }
      const auto show_create_stmt = dynamic_cast<Parser::ShowCreateTableStmt*>(ddl);
      if (show_create_stmt) {
        result_.execution_time_ms +=
            measure<>::execution([&]() { ddl->execute(*session_ptr); });
        const auto create_string = show_create_stmt->getCreateStmt();
        convert_result(result_, ResultSet(create_string), true);
        return true;
      }

      const auto import_stmt = dynamic_cast<Parser::CopyTableStmt*>(ddl);
      if (import_stmt) {
        if (g_cluster && !leaf_aggregator_.leafCount()) {
          // Don't allow copy from imports directly on a leaf node
          throw std::runtime_error(
            "Cannot import on an individual leaf. Please import from the Aggregator.");
        } else if (leaf_aggregator_.leafCount() > 0) {
          result_.execution_time_ms += measure<>::execution(
            [&]() { execute_distributed_copy_statement(import_stmt, *session_ptr); });
        } else {
          result_.execution_time_ms +=
            measure<>::execution([&]() { ddl->execute(*session_ptr); });
        }

        // Read response message
        convert_result(result_, ResultSet(*import_stmt->return_message.get()), true);
        result_.success = import_stmt->get_success();

        // get geo_copy_from info
        if (import_stmt->was_geo_copy_from()) {
          GeoCopyFromState geo_copy_from_state;
          import_stmt->get_geo_copy_from_payload(
              geo_copy_from_state.geo_copy_from_table,
              geo_copy_from_state.geo_copy_from_file_name,
              geo_copy_from_state.geo_copy_from_copy_params,
              geo_copy_from_state.geo_copy_from_partitions);
          geo_copy_from_sessions.add(session_ptr->get_session_id(), geo_copy_from_state);
         }
        return true;
      }

      // Check for DDL statements requiring locking and get locks
      auto export_stmt = dynamic_cast<Parser::ExportQueryStmt*>(ddl);
      if (export_stmt) {
        const auto query_string = export_stmt->get_select_stmt();
        TPlanResult result;
        CHECK(locks.empty());
        std::tie(result, locks) =
          parse_to_ra(query_state_proxy, query_string, {}, true, system_parameters_);
      }
      result_.execution_time_ms += measure<>::execution([&]() {
        ddl->execute(*session_ptr);
        check_and_invalidate_sessions(ddl);
      });
      return true;
    };

    for (const auto& stmt : parse_trees) {
      auto select_stmt = dynamic_cast<Parser::SelectStmt*>(stmt.get());
      if (!select_stmt) {
        check_read_only("Non-SELECT statements");
      }
      auto ddl = dynamic_cast<Parser::DDLStmt*>(stmt.get());
      if (!handle_ddl(ddl)) {
        auto stmtp = dynamic_cast<Parser::InsertValuesStmt*>(stmt.get());
          CHECK(stmtp);  // no other statements supported

          if (parse_trees.size() != 1) {
            throw std::runtime_error("Can only run one INSERT INTO query at a time.");
          }

          result_.execution_time_ms +=
              measure<>::execution([&]() { stmtp->execute(*session_ptr); });
        }
      }
    }
  }

  void execute_rel_alg_with_filter_push_down(
    QueryStateProxy query_state_proxy,
    std::string& query_ra,
    const bool column_format,
    const ExecutorDeviceType executor_device_type,
    const int32_t first_n,
    const int32_t at_most_n,
    const bool just_explain,
    const bool just_calcite_explain,
    const std::vector<PushedDownFilterInfo>& filter_push_down_requests) {
  //TODO: Implement
  }

 private:
  TQueryResult result_;
  std::shared_ptr<ExecutionResult> data_;
};

/**
 * DBEngine internal implementation
 */
class DBEngineImpl : public DBEngine {
 public:
  DBEngineImpl() : is_temp_db_(false) {}

  ~DBEngineImpl() { reset(); }

  bool init(const CommandLineOptions& prog_config_opts)
    static bool initialized{false};
    if (initialized) {
      throw std::runtime_error("Database engine already initialized");
    }
    std::string base_path = prog_config_opts.base_path;
    bool is_new_db = base_path.empty() || !catalogExists(base_path);
    if (is_new_db) {
      base_path = createCatalog(base_path);
      if (base_path.empty()) {
        throw std::runtime_error("Database directory could not be created");
      }
    }
    try {
      dbe_handler_ =
        mapd::make_shared<DBEHandler>(prog_config_opts.db_leaves,
                                     prog_config_opts.string_leaves,
                                     prog_config_opts.base_path,
                                     prog_config_opts.cpu_only,
                                     prog_config_opts.allow_multifrag,
                                     prog_config_opts.jit_debug,
                                     prog_config_opts.intel_jit_profile,
                                     prog_config_opts.read_only,
                                     prog_config_opts.allow_loop_joins,
                                     prog_config_opts.enable_rendering,
                                     prog_config_opts.renderer_use_vulkan_driver,
                                     prog_config_opts.enable_auto_clear_render_mem,
                                     prog_config_opts.render_oom_retry_threshold,
                                     prog_config_opts.render_mem_bytes,
                                     prog_config_opts.max_concurrent_render_sessions,
                                     prog_config_opts.num_gpus,
                                     prog_config_opts.start_gpu,
                                     prog_config_opts.reserved_gpu_mem,
                                     prog_config_opts.render_compositor_use_last_gpu,
                                     prog_config_opts.num_reader_threads,
                                     prog_config_opts.authMetadata,
                                     prog_config_opts.system_parameters,
                                     prog_config_opts.enable_legacy_syntax,
                                     prog_config_opts.idle_session_duration,
                                     prog_config_opts.max_session_duration,
                                     prog_config_opts.enable_runtime_udf,
                                     prog_config_opts.udf_file_name,
                                     prog_config_opts.udf_compiler_path,
                                     prog_config_opts.udf_compiler_options,
#ifdef ENABLE_GEOS
                                     prog_config_opts.libgeos_so_filename,
#endif
                                     prog_config_opts.disk_cache_config);
    } catch (const std::exception& e) {
      LOG(FATAL) << "Failed to initialize database handler: " << e.what();
    }
    dbe_handler_->connect(session_id_, OMNISCI_ROOT_USER, OMNISCI_ROOT_PASSWD_DEFAULT, OMNISCI_DEFAULT_DB);
    base_path_ = base_path;
    initialized = true;
    return true;
  }

  void executeDDL(const std::string& query) {
    dbe_handler_->sql_execute(session_id_, query, false, "", -1, -1);
  }

  void importArrowTable(const std::string& name,
                        std::shared_ptr<arrow::Table>& table,
                        uint64_t fragment_size) {
    setArrowTable(name, table);
    try {
      auto session = dbe_handler_->get_session_copy(session_id_);
      TableDescriptor td;
      td.tableName = name;
      td.userId = session->get_currentUser().userId;
      td.storageType = "ARROW:" + name;
      td.persistenceLevel = Data_Namespace::MemoryLevel::CPU_LEVEL;
      td.isView = false;
      td.fragmenter = nullptr;
      td.fragType = Fragmenter_Namespace::FragmenterType::INSERT_ORDER;
      td.maxFragRows = fragment_size > 0 ? fragment_size : DEFAULT_FRAGMENT_ROWS;
      td.maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;
      td.fragPageSize = DEFAULT_PAGE_SIZE;
      td.maxRows = DEFAULT_MAX_ROWS;
      td.keyMetainfo = "[]";

      std::list<ColumnDescriptor> cols;
      std::vector<Parser::SharedDictionaryDef> dictionaries;
      auto catalog = session->get_catalog_ptr();
      // nColumns
      catalog->createTable(td, cols, dictionaries, false);
      Catalog_Namespace::SysCatalog::instance().createDBObject(
          session->get_currentUser(), td.tableName, TableDBObjectType, *catalog);
    } catch (...) {
      releaseArrowTable(name);
      throw;
    }
    releaseArrowTable(name);
  }

  std::shared_ptr<CursorImpl> executeDML(const std::string& query) {
    ParserWrapper pw{query};
    if (pw.isCalcitePathPermissable()) {
      const auto execution_result = dbe_handler_->sql_execute(session_id_, query, false, "", -1, -1);
      auto targets = execution_result.getTargetsMeta();
      std::vector<std::string> col_names;
      for (const auto target : targets) {
        col_names.push_back(target.get_resname());
      }
      return std::make_shared<CursorImpl>(execution_result.getRows(), col_names);
    }

    auto session_info = dbe_handler_->get_session_copy(session_id_);
    auto query_state = dbe_handler_->create_query_state(session_info, query);
    auto stdlog = STDLOG(query_state);

    SQLParser parser;
    std::list<std::unique_ptr<Parser::Stmt>> parse_trees;
    std::string last_parsed;
    CHECK_EQ(parser.parse(query, parse_trees, last_parsed), 0) << query;
    CHECK_EQ(parse_trees.size(), size_t(1));
    auto stmt = parse_trees.front().get();
    auto insert_values_stmt = dynamic_cast<InsertValuesStmt*>(stmt);
    CHECK(insert_values_stmt);
    insert_values_stmt->execute(*session_info);
    return std::shared_ptr<CursorImpl>();
  }

  std::shared_ptr<CursorImpl> executeRA(const std::string& query) {
//    if (boost::starts_with(query, "execute calcite")) {
      return executeDML(query);
//    }
// TODO: Implement
//    const auto execution_result =
//        QR::get()->runSelectQueryRA(query, ExecutorDeviceType::CPU, true, true);
//    auto targets = execution_result.getTargetsMeta();
//    std::vector<std::string> col_names;
//    for (const auto target : targets) {
//      col_names.push_back(target.get_resname());
//    }
//    return std::make_shared<CursorImpl>(execution_result.getRows(), col_names);
  }

  std::vector<std::string> getTables() {
    std::vector<std::string> table_names;
    auto catalog = dbe_handler_->get_session_copy(session_id_)->get_catalog_ptr();
    if (catalog) {
      const auto tables = catalog->getAllTableMetadata();
      for (const auto td : tables) {
        if (td->shard >= 0) {
          // skip shards, they're not standalone tables
          continue;
        }
        table_names.push_back(td->tableName);
      }
    } else {
      throw std::runtime_error("System catalog uninitialized");
    }
    return table_names;
  }

  std::vector<ColumnDetails> getTableDetails(const std::string& table_name) {
    std::vector<ColumnDetails> result;
    auto catalog = dbe_handler_->get_session_copy(session_id_)->get_catalog_ptr();
    if (catalog) {
      auto metadata = catalog->getMetadataForTable(table_name, false);
      if (metadata) {
        const auto col_descriptors =
            catalog->getAllColumnMetadataForTable(metadata->tableId, false, true, false);
        const auto deleted_cd = catalog->getDeletedColumn(metadata);
        for (const auto cd : col_descriptors) {
          if (cd == deleted_cd) {
            continue;
          }
          ColumnDetails col_details;
          col_details.col_name = cd->columnName;
          auto ct = cd->columnType;
          SQLTypes sql_type = ct.get_type();
          EncodingType sql_enc = ct.get_compression();
          col_details.col_type = sqlToColumnType(sql_type);
          col_details.encoding = sqlToColumnEncoding(sql_enc);
          col_details.nullable = !ct.get_notnull();
          col_details.is_array = (sql_type == kARRAY);
          if (IS_GEO(sql_type)) {
            col_details.precision = static_cast<int>(ct.get_subtype());
            col_details.scale = ct.get_output_srid();
          } else {
            col_details.precision = ct.get_precision();
            col_details.scale = ct.get_scale();
          }
          if (col_details.encoding == ColumnEncoding::DICT) {
            // have to get the actual size of the encoding from the dictionary
            // definition
            const int dict_id = ct.get_comp_param();
            auto dd = catalog->getMetadataForDict(dict_id, false);
            if (dd) {
              col_details.comp_param = dd->dictNBits;
            } else {
              throw std::runtime_error("Dictionary definition for column doesn't exist");
            }
          } else {
            col_details.comp_param = ct.get_comp_param();
            if (ct.is_date_in_days() && col_details.comp_param == 0) {
              col_details.comp_param = 32;
            }
          }
          result.push_back(col_details);
        }
      }
    }
    return result;
  }

  void createUser(const std::string& user_name, const std::string& password) {
    Catalog_Namespace::UserMetadata user;
    auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
    if (!sys_cat.getMetadataForUser(user_name, user)) {
      sys_cat.createUser(user_name, password, false, "", true);
    }
  }

  void dropUser(const std::string& user_name) {
    Catalog_Namespace::UserMetadata user;
    auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
    if (!sys_cat.getMetadataForUser(user_name, user)) {
      sys_cat.dropUser(user_name);
    }
  }

  void createDatabase(const std::string& db_name) {
    Catalog_Namespace::DBMetadata db;
    auto user = dbe_handler_->get_session_copy(session_id_)->get_currentUser();
    auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
    if (!sys_cat.getMetadataForDB(db_name, db)) {
      sys_cat.createDatabase(db_name, user.userId);
    }
  }

  void dropDatabase(const std::string& db_name) {
    Catalog_Namespace::DBMetadata db;
    auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
    if (sys_cat.getMetadataForDB(db_name, db)) {
      sys_cat.dropDatabase(db);
    }
  }

  bool setDatabase(std::string& db_name) {
    auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
    auto user = dbe_handler_->get_session_copy(session_id_)->get_currentUser();
    auto catalog = sys_cat.switchDatabase(db_name, user.userName);
    updateSession(catalog);
    return true;
  }

  bool login(std::string& db_name, std::string& user_name, const std::string& password) {
    dbe_handler_->disconnect(session_id_);
    dbe_handler_->connect(session_id_, user_name, password, db_name);
    return true;
  }

 protected:
  void reset() {
    dbe_handler_->disconnect(session_id_);
    if (is_temp_db_) {
      boost::filesystem::remove_all(base_path_);
    }
    base_path_.clear();
  }

  bool catalogExists(const std::string& base_path) {
    if (!boost::filesystem::exists(base_path)) {
      return false;
    }
    for (auto& subdir : system_folders_) {
      std::string path = base_path + "/" + subdir;
      if (!boost::filesystem::exists(path)) {
        return false;
      }
    }
    return true;
  }

  void cleanCatalog(const std::string& base_path) {
    if (boost::filesystem::exists(base_path)) {
      for (auto& subdir : system_folders_) {
        std::string path = base_path + "/" + subdir;
        if (boost::filesystem::exists(path)) {
          boost::filesystem::remove_all(path);
        }
      }
    }
  }

  std::string createCatalog(const std::string& base_path) {
    std::string root_dir = base_path;
    if (base_path.empty()) {
      boost::system::error_code error;
      auto tmp_path = boost::filesystem::temp_directory_path(error);
      if (boost::system::errc::success != error.value()) {
        std::cerr << error.message() << std::endl;
        return "";
      }
      tmp_path /= "omnidbe_%%%%-%%%%-%%%%";
      auto uniq_path = boost::filesystem::unique_path(tmp_path, error);
      if (boost::system::errc::success != error.value()) {
        std::cerr << error.message() << std::endl;
        return "";
      }
      root_dir = uniq_path.string();
      is_temp_db_ = true;
    }
    if (!boost::filesystem::exists(root_dir)) {
      if (!boost::filesystem::create_directory(root_dir)) {
        std::cerr << "Cannot create database directory: " << root_dir << std::endl;
        return "";
      }
    }
    size_t absent_count = 0;
    for (auto& sub_dir : system_folders_) {
      std::string path = root_dir + "/" + sub_dir;
      if (!boost::filesystem::exists(path)) {
        if (!boost::filesystem::create_directory(path)) {
          std::cerr << "Cannot create database subdirectory: " << path << std::endl;
          return "";
        }
        ++absent_count;
      }
    }
    if ((absent_count > 0) && (absent_count < system_folders_.size())) {
      std::cerr << "Database directory structure is broken: " << root_dir << std::endl;
      return "";
    }
    return root_dir;
  }

 private:
  std::string base_path_;
  std::string session_id_;
  CommandLineOptions options_;
  mapd::shared_ptr<DBEHandler> dbe_handler_;
  bool is_temp_db_;
  std::string udf_filename_;

  std::vector<std::string> system_folders_ = {"mapd_catalogs",
                                              "mapd_data",
                                              "mapd_export"};
};

namespace {
std::mutex engine_create_mutex;
}

std::shared_ptr<DBEngine> DBEngine::create(const std::string& parameters) {
  const std::lock_guard<std::mutex> lock(engine_create_mutex);
  CommandLineOptions options(parameters);
  auto engine = std::make_shared<DBEngineImpl>();

  if (!engine->init(options)) {
    throw std::runtime_error("DBE initialization failed");
  }

  return engine;
}

/** DBEngine downcasting methods */

inline DBEngineImpl* getImpl(DBEngine* ptr) {
  return (DBEngineImpl*)ptr;
}

inline const DBEngineImpl* getImpl(const DBEngine* ptr) {
  return (const DBEngineImpl*)ptr;
}

/** DBEngine external methods */

void DBEngine::executeDDL(const std::string& query) {
  DBEngineImpl* engine = getImpl(this);
  engine->executeDDL(query);
}

std::shared_ptr<Cursor> DBEngine::executeDML(const std::string& query) {
  DBEngineImpl* engine = getImpl(this);
  return engine->executeDML(query);
}

std::shared_ptr<Cursor> DBEngine::executeRA(const std::string& query) {
  DBEngineImpl* engine = getImpl(this);
  return engine->executeRA(query);
}

void DBEngine::importArrowTable(const std::string& name,
                                std::shared_ptr<arrow::Table>& table,
                                uint64_t fragment_size) {
  DBEngineImpl* engine = getImpl(this);
  return engine->importArrowTable(name, table, fragment_size);
}

std::vector<std::string> DBEngine::getTables() {
  DBEngineImpl* engine = getImpl(this);
  return engine->getTables();
}

std::vector<ColumnDetails> DBEngine::getTableDetails(const std::string& table_name) {
  DBEngineImpl* engine = getImpl(this);
  return engine->getTableDetails(table_name);
}

void DBEngine::createUser(const std::string& user_name, const std::string& password) {
  DBEngineImpl* engine = getImpl(this);
  engine->createUser(user_name, password);
}

void DBEngine::dropUser(const std::string& user_name) {
  DBEngineImpl* engine = getImpl(this);
  engine->dropUser(user_name);
}

void DBEngine::createDatabase(const std::string& db_name) {
  DBEngineImpl* engine = getImpl(this);
  engine->createDatabase(db_name);
}

void DBEngine::dropDatabase(const std::string& db_name) {
  DBEngineImpl* engine = getImpl(this);
  engine->dropDatabase(db_name);
}

bool DBEngine::setDatabase(std::string& db_name) {
  DBEngineImpl* engine = getImpl(this);
  return engine->setDatabase(db_name);
}

bool DBEngine::login(std::string& db_name,
                     std::string& user_name,
                     const std::string& password) {
  DBEngineImpl* engine = getImpl(this);
  return engine->login(db_name, user_name, password);
}

/** Cursor downcasting methods */

inline CursorImpl* getImpl(Cursor* ptr) {
  return (CursorImpl*)ptr;
}

inline const CursorImpl* getImpl(const Cursor* ptr) {
  return (const CursorImpl*)ptr;
}

/** Cursor external methods */

size_t Cursor::getColCount() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getColCount();
}

size_t Cursor::getRowCount() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getRowCount();
}

Row Cursor::getNextRow() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getNextRow();
}

ColumnType Cursor::getColType(uint32_t col_num) {
  CursorImpl* cursor = getImpl(this);
  return cursor->getColType(col_num);
}

std::shared_ptr<arrow::RecordBatch> Cursor::getArrowRecordBatch() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getArrowRecordBatch();
}
}  // namespace EmbeddedDatabase
