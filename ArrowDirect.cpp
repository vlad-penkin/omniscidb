#include "Logger/Logger.h"
#include "Shared/measure.h"

#include <arrow/api.h>
#include <arrow/csv/reader.h>
#include <arrow/io/file.h>
#include <boost/program_options.hpp>

#include "QueryEngine/Execute.h"
#include "QueryEngine/InputMetadata.h"
#include "QueryEngine/RelAlgExecutionUnit.h"
#include "Shared/ArrowUtil.h"

std::shared_ptr<arrow::Table> arrow_read_csv(const std::string& csv_filename,
                                             const bool has_header) {
  std::shared_ptr<arrow::io::ReadableFile> inp;
  auto file_result = arrow::io::ReadableFile::Open(csv_filename.c_str());
  ARROW_THROW_NOT_OK(file_result.status());
  inp = file_result.ValueOrDie();

  auto arrow_parse_options = arrow::csv::ParseOptions::Defaults();
  arrow_parse_options.quoting = true;
  arrow_parse_options.escaping = false;
  arrow_parse_options.newlines_in_values = false;

  auto arrow_read_options = arrow::csv::ReadOptions::Defaults();
  arrow_read_options.use_threads = true;

  arrow_read_options.block_size = 20 * 1024 * 1024;
  arrow_read_options.autogenerate_column_names = false;
  arrow_read_options.skip_rows = has_header ? 1 : 0;

  auto arrow_convert_options = arrow::csv::ConvertOptions::Defaults();
  arrow_convert_options.check_utf8 = false;
  arrow_convert_options.include_columns = arrow_read_options.column_names;
  arrow_convert_options.strings_can_be_null = true;

  auto io_context = arrow::io::default_io_context();
  auto table_reader_result = arrow::csv::TableReader::Make(
      io_context, inp, arrow_read_options, arrow_parse_options, arrow_convert_options);
  ARROW_THROW_NOT_OK(table_reader_result.status());
  auto table_reader = table_reader_result.ValueOrDie();

  std::shared_ptr<arrow::Table> arrow_table;
  auto time = measure<>::execution([&]() {
    auto arrow_table_result = table_reader->Read();
    ARROW_THROW_NOT_OK(arrow_table_result.status());
    arrow_table = arrow_table_result.ValueOrDie();
  });

  VLOG(1) << "Read Arrow CSV file " << csv_filename << " in " << time << "ms";

  return arrow_table;
}

RelAlgExecutionUnit build_ra_exe_unit(
    const std::shared_ptr<const InputColDescriptor> input_col_desc,
    const std::vector<Analyzer::Expr*>& target_exprs) {
  return RelAlgExecutionUnit{{input_col_desc->getScanDesc()},
                             {input_col_desc},
                             {},
                             {},
                             {},
                             {},
                             target_exprs,
                             nullptr,
                             SortInfo{{}, SortAlgorithm::Default, 0, 0},
                             0};
}

inline CompilationOptions get_compilation_options(const ExecutorDeviceType& device_type) {
  return CompilationOptions{device_type, false, ExecutorOptLevel::Default, false};
}

inline ExecutionOptions get_execution_options() {
  return ExecutionOptions{
      false, false, false, false, false, false, false, false, 0, false, false, 0, false};
}

int main(int argc, char** argv) {
  namespace po = boost::program_options;

  po::options_description desc("Options");

  std::string csv_filename;
  desc.add_options()(
      "csv", po::value<std::string>(&csv_filename)->required(), "Input CSV filename");

  bool has_header = true;
  desc.add_options()(
      "header",
      po::value<bool>(&has_header)->default_value(has_header)->implicit_value(true),
      "Does the CSV file have a header?");

  std::string data_dir;
  desc.add_options()("data",
                     po::value<std::string>(&data_dir)->required(),
                     "Empty data dir -- not used, but required to init DataMgr");

  logger::LogOptions log_options(argv[0]);
  log_options.severity_ = logger::Severity::FATAL;
  log_options.set_options();  // update default values
  desc.add(log_options.get_options());

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  po::notify(vm);

  logger::init(log_options);

  auto arrow_table = arrow_read_csv(csv_filename, has_header);
  auto& columns = arrow_table->columns();

  LOG(INFO) << "Have " << columns.size() << " columns.";

  // initialize catalog and executor, and populate data
  auto data_mgr =
      std::make_shared<Data_Namespace::DataMgr>(data_dir,
                                                SystemParameters{},
                                                nullptr,
                                                /*usesGpus=*/false,
                                                0,
                                                0,
                                                File_Namespace::DiskCacheConfig{});
  auto catalog = Catalog_Namespace::Catalog(data_mgr);  // TODO

  auto executor = std::make_shared<Executor>(/*executor_id=*/0,
                                             data_mgr.get(),
                                             /*cuda_block_size=*/0,
                                             /*cuda_grid_size=*/0,
                                             /*max_gpu_slab_size=*/0,
                                             /*debug_dir=*/"",
                                             /*debug_file=*/"");

  executor->setCatalog(&catalog);
  const auto table_id = executor->arrow_handler_.addArrowTable(arrow_table);

  // try column 2, should be the budget / numeric
  auto& column = columns[2];
  // make a type for the column
  auto sql_type = ArrowDataHandler::getQueryEngineType(*column->type());

  // let's build a ResultSet and inject it into TemporaryTables
  TargetInfo target{/*is_agg=*/false,
                    /*agg_kind=*/SQLAgg::kAVG,
                    /*sql_type=*/sql_type,
                    /*agg_arg_type=*/SQLTypeInfo(kNULLT),
                    /*skip_null_val=*/false,
                    /*is_distinct=*/false,
                    /*is_varlen_projection=*/false};

  auto arr_chunk = column->chunk(0);
  CHECK(arr_chunk);

  auto qmd = QueryMemoryDescriptor(executor.get(),
                                   arr_chunk->length(),
                                   QueryDescriptionType::Projection,
                                   /*is_table_function=*/false);
  qmd.setOutputColumnar(true);

  auto i64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(arr_chunk);
  CHECK(i64_arr);
  auto values_buffer = i64_arr->values();
  CHECK(values_buffer);
  int8_t* arr_data_ptr = reinterpret_cast<int8_t*>(values_buffer->mutable_data());
  CHECK(arr_data_ptr);
  std::vector<TargetInfo> targets{target};
  auto column_result_set_storage = std::make_unique<ResultSetStorage>(
      targets, qmd, arr_data_ptr, /*buff_is_provided=*/true);
  auto rs = std::make_shared<ResultSet>(
      targets, ExecutorDeviceType::CPU, qmd, std::move(column_result_set_storage));
  auto temporary_table = TemporaryTable(rs);
  TemporaryTables temp_tables;
  temp_tables.insert({table_id, temporary_table});
  executor->setTemporaryTables(&temp_tables);
  executor->setRowSetMemoryOwner(
      std::make_shared<RowSetMemoryOwner>(Executor::getArenaBlockSize(), cpu_threads()));

  // run a count over one column
  const auto arrow_input_desc = std::make_shared<const InputDescriptor>(
      /*table_id=*/table_id, /*nest_level=*/0, /*is_arrow=*/true);
  const auto input_col_desc =
      std::make_shared<const InputColDescriptor>(*arrow_input_desc, 0);
  const auto col_expr =
      makeExpr<Analyzer::ColumnVar>(sql_type, table_id, /*column_id=*/0, 0);
  auto max_expr = makeExpr<Analyzer::AggExpr>(sql_type, kMAX, col_expr, false, nullptr);
  auto min_expr = makeExpr<Analyzer::AggExpr>(sql_type, kMIN, col_expr, false, nullptr);
  auto count_expr =
      makeExpr<Analyzer::AggExpr>(sql_type, kCOUNT, col_expr, false, nullptr);

  const auto ra_exe_unit = build_ra_exe_unit(
      input_col_desc, {count_expr.get(), min_expr.get(), max_expr.get()});
  const auto table_infos = get_table_infos(ra_exe_unit, executor.get());
  CHECK_EQ(table_infos.size(), size_t(1));

  const auto co = get_compilation_options(ExecutorDeviceType::CPU);
  const auto eo = get_execution_options();

  Executor::PerFragmentCallBack computation_callback =
      [](ResultSetPtr results, const Fragmenter_Namespace::FragmentInfo& fragment_info) {
        // just log the row count for now
        LOG(INFO) << "Row count for fragment " << results->rowCount();
        auto tv = results->getNextRow(false, false);
        CHECK_EQ(tv.size(), size_t(3));
        std::vector<int64_t> outputs(3);
        for (size_t i = 0; i < 3; i++) {
          auto stv = boost::get<ScalarTargetValue>(tv[i]);
          auto cnt = boost::get<int64_t>(stv);
          outputs[i] = cnt;
        }

        LOG(INFO) << "Counted " << outputs[0] << " rows. Min: " << outputs[1]
                  << " , Max: " << outputs[2];
      };

  // only one fragment right now
  executor->executeWorkUnitPerFragment(
      ra_exe_unit, table_infos[0], co, eo, catalog, computation_callback, {0});

  return 0;
}