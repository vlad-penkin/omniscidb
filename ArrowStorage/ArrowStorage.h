/*
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

#pragma once

#include "DataMgr/AbstractDataProvider.h"
#include "DataProvider/DictDescriptor.h"
#include "SchemaMgr/SimpleSchemaProvider.h"
#include "Shared/mapd_shared_mutex.h"

#include <arrow/api.h>

class ArrowStorage : public SimpleSchemaProvider, public AbstractDataProvider {
 public:
  struct ColumnDescription {
    std::string name;
    SQLTypeInfo type;
  };

  struct TableOptions {
    TableOptions(){};
    TableOptions(size_t fragment_size_) : fragment_size(fragment_size_){};

    size_t fragment_size = 32'000'000;
  };

  struct CsvParseOptions {
    CsvParseOptions(){};

    char delimiter = ',';
    bool header = true;
    size_t skip_rows = 0;
    size_t block_size = 20 << 20;  // Default block size is 20MB
  };

  struct JsonParseOptions {
    JsonParseOptions(){};

    size_t skip_rows = 0;
    size_t block_size = 1 << 20;  // Default block size is 1MB
  };

  ArrowStorage(int schema_id, const std::string& schema_name, int db_id)
      : SimpleSchemaProvider(schema_id, schema_name)
      , db_id_(db_id)
      , schema_id_(getSchemaId(db_id)) {}

  void fetchBuffer(const ChunkKey& key,
                   Data_Namespace::AbstractBuffer* dest,
                   const size_t num_bytes = 0) override;

  std::unique_ptr<Data_Namespace::AbstractDataToken> getZeroCopyBufferMemory(
      const ChunkKey& key,
      size_t num_bytes) override;

  TableFragmentsInfo getTableMetadata(int db_id, int table_id) const override;

  const DictDescriptor* getDictMetadata(int dict_id, bool load_dict = true) override;

  TableInfoPtr createTable(const std::string& table_name,
                           const std::vector<ColumnDescription>& columns,
                           const TableOptions& options = TableOptions());

  TableInfoPtr importArrowTable(std::shared_ptr<arrow::Table> at,
                                const std::string& table_name,
                                const std::vector<ColumnDescription>& columns,
                                const TableOptions& options = TableOptions());
  TableInfoPtr importArrowTable(std::shared_ptr<arrow::Table> at,
                                const std::string& table_name,
                                const TableOptions& options = TableOptions());

  void appendArrowTable(std::shared_ptr<arrow::Table> at, const std::string& table_name);
  void appendArrowTable(std::shared_ptr<arrow::Table> at, int table_id);

  TableInfoPtr importCsvFile(const std::string& file_name,
                             const std::string& table_name,
                             const std::vector<ColumnDescription>& columns,
                             const TableOptions& options = TableOptions(),
                             const CsvParseOptions parse_options = CsvParseOptions());
  TableInfoPtr importCsvFile(const std::string& file_name,
                             const std::string& table_name,
                             const TableOptions& options = TableOptions(),
                             const CsvParseOptions parse_options = CsvParseOptions());

  void appendCsvFile(const std::string& file_name,
                     const std::string& table_name,
                     const CsvParseOptions parse_options = CsvParseOptions());
  void appendCsvFile(const std::string& file_name,
                     int table_id,
                     const CsvParseOptions parse_options = CsvParseOptions());

  void appendCsvData(const std::string& csv_data,
                     const std::string& table_name,
                     const CsvParseOptions parse_options = CsvParseOptions());
  void appendCsvData(const std::string& csv_data,
                     int table_id,
                     const CsvParseOptions parse_options = CsvParseOptions());

  void appendJsonData(const std::string& json_data,
                      const std::string& table_name,
                      const JsonParseOptions parse_options = JsonParseOptions());
  void appendJsonData(const std::string& json_data,
                      int table_id,
                      const JsonParseOptions parse_options = JsonParseOptions());

  TableInfoPtr importParquetFile(const std::string& file_name,
                                 const std::string& table_name,
                                 const TableOptions& options = TableOptions());

  void appendParquetFile(const std::string& file_name, const std::string& table_name);
  void appendParquetFile(const std::string& file_name, int table_id);

  void dropTable(const std::string& table_name, bool throw_if_not_exist = false);
  void dropTable(int table_id, bool throw_if_not_exist = false);

 private:
  struct DataFragment {
    size_t offset = 0;
    size_t row_count = 0;
    std::vector<std::shared_ptr<ChunkMetadata>> metadata;
  };

  struct TableData {
    mapd_shared_mutex mutex;
    size_t fragment_size = 32'000'000;
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> col_data;
    std::vector<DataFragment> fragments;
    size_t row_count = 0;
  };

  class ArrowChunkDataToken : public Data_Namespace::AbstractDataToken {
   public:
    ArrowChunkDataToken(std::shared_ptr<arrow::Array> chunk,
                        const int8_t* ptr,
                        size_t size)
        : chunk_(std::move(chunk)), ptr_(ptr), size_(size) {}

    const int8_t* getMemoryPtr() const override { return ptr_; }
    size_t getSize() const override { return size_; }

   private:
    std::shared_ptr<arrow::Array> chunk_;
    const int8_t* ptr_;
    size_t size_;
  };

  void checkNewTableParams(const std::string& table_name,
                           const std::vector<ColumnDescription>& columns,
                           const TableOptions& options) const;
  void compareSchemas(std::shared_ptr<arrow::Schema> lhs,
                      std::shared_ptr<arrow::Schema> rhs);
  void computeStats(std::shared_ptr<arrow::ChunkedArray> arr,
                    SQLTypeInfo type,
                    ChunkStats& stats);
  std::shared_ptr<arrow::Table> parseCsvFile(const std::string& file_name,
                                             const CsvParseOptions parse_options,
                                             const ColumnInfoList& col_infos = {});
  std::shared_ptr<arrow::Table> parseCsvData(const std::string& csv_data,
                                             const CsvParseOptions parse_options,
                                             const ColumnInfoList& col_infos = {});
  std::shared_ptr<arrow::Table> parseCsv(std::shared_ptr<arrow::io::InputStream> input,
                                         const CsvParseOptions parse_options,
                                         const ColumnInfoList& col_infos = {});
  std::shared_ptr<arrow::Table> parseJsonData(const std::string& json_data,
                                              const JsonParseOptions parse_options,
                                              const ColumnInfoList& col_infos = {});
  std::shared_ptr<arrow::Table> parseJson(std::shared_ptr<arrow::io::InputStream> input,
                                          const JsonParseOptions parse_options,
                                          const ColumnInfoList& col_infos = {});
  std::shared_ptr<arrow::Table> parseParquetFile(const std::string& file_name);
  TableFragmentsInfo getEmptyTableMetadata(int table_id) const;
  void fetchFixedLenData(const TableData& table,
                         size_t frag_idx,
                         size_t col_idx,
                         Data_Namespace::AbstractBuffer* dest,
                         size_t num_bytes,
                         size_t elem_size) const;
  void fetchVarLenOffsets(const TableData& table,
                          size_t frag_idx,
                          size_t col_idx,
                          Data_Namespace::AbstractBuffer* dest,
                          size_t num_bytes) const;
  void fetchVarLenData(const TableData& table,
                       size_t frag_idx,
                       size_t col_idx,
                       Data_Namespace::AbstractBuffer* dest,
                       size_t num_bytes) const;
  void fetchVarLenArrayData(const TableData& table,
                            size_t frag_idx,
                            size_t col_idx,
                            Data_Namespace::AbstractBuffer* dest,
                            size_t elem_size,
                            size_t num_bytes) const;

  int db_id_;
  int schema_id_;
  int next_table_id_ = 1;
  int next_dict_id_ = 1;
  std::unordered_map<int, std::unique_ptr<TableData>> tables_;
  std::unordered_map<int, std::unique_ptr<DictDescriptor>> dicts_;
  mutable mapd_shared_mutex data_mutex_;
  mutable mapd_shared_mutex dict_mutex_;
};
