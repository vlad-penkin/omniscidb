#pragma once

#include <arrow/array.h>
#include <arrow/table.h>

#include <memory>
#include <vector>

#include "Fragmenter/Fragmenter.h"

class ArrowDataHandler {
 public:
  ArrowDataHandler() {}

  int addArrowTable(std::shared_ptr<arrow::Table> table) {
    arrow_tables_.push_back(table);
    return arrow_tables_.size() * -1;
  }

  std::shared_ptr<arrow::Table> getArrowTable(const int table_id) const {
    CHECK_LT(table_id, 0);
    const size_t normalized_table_id = static_cast<size_t>(table_id * -1) - 1;
    CHECK_LT(normalized_table_id, arrow_tables_.size());
    return arrow_tables_[normalized_table_id];
  }

  Fragmenter_Namespace::TableInfo getArrowTableInfo(const int table_id) const {
    auto arrow_table = getArrowTable(table_id);
    CHECK(arrow_table);
    Fragmenter_Namespace::TableInfo table_info;
    table_info.setPhysicalNumTuples(arrow_table->num_rows());

    table_info.fragments.emplace_back();
    auto& fragment_info = table_info.fragments.back();
    // build a chunk metadata map
    ChunkMetadataMap metadata_map;
    for (size_t i = 0; i < arrow_table->num_columns(); i++) {
      auto column = arrow_table->column(i);
      CHECK(column);

      ChunkMetadata column_metadata;
      column_metadata.sqlType = getQueryEngineType(*column->type());
      column_metadata.numBytes = column->length();
      CHECK_EQ(column->num_chunks(), int(1));
      auto chunk = column->chunk(0);
      CHECK(chunk);
      column_metadata.numElements = chunk->length();

      // just make stats invalid
#if 0
      if (i == 2) {
        auto i64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(chunk);
        CHECK(i64_arr);
        CHECK(false);
      } else {
        // punt on the stats
      }
#endif
    }

    // set basic tuple counts
    fragment_info.setPhysicalNumTuples(arrow_table->num_rows());

    return table_info;
  }

  static SQLTypeInfo getQueryEngineType(const arrow::DataType& type) {
    using namespace arrow;
    switch (type.id()) {
      case Type::INT8:
        return SQLTypeInfo(kTINYINT, false);
      case Type::INT16:
        return SQLTypeInfo(kSMALLINT, false);
      case Type::INT32:
        return SQLTypeInfo(kINT, false);
      case Type::INT64:
        return SQLTypeInfo(kBIGINT, false);
      case Type::BOOL:
        return SQLTypeInfo(kBOOLEAN, false);
      case Type::FLOAT:
        return SQLTypeInfo(kFLOAT, false);
      case Type::DATE32:
      case Type::DATE64:
        return SQLTypeInfo(kDATE, false);
      case Type::DOUBLE:
        return SQLTypeInfo(kDOUBLE, false);
        // uncomment when arrow 2.0 will be released and modin support for dictionary
        // types in read_csv would be implemented

        // case Type::DICTIONARY: {
        //   auto type = SQLTypeInfo(kTEXT, false, kENCODING_DICT);
        //   // this is needed because createTable forces type.size to be equal to
        //   // comp_param / 8, no matter what type.size you set here
        //   type.set_comp_param(sizeof(uint32_t) * 8);
        //   return type;
        // }
        // case Type::STRING:
        //   return SQLTypeInfo(kTEXT, false, kENCODING_NONE);

      case Type::STRING: {
        auto type = SQLTypeInfo(kTEXT, false, kENCODING_DICT);
        // this is needed because createTable forces type.size to be equal to
        // comp_param / 8, no matter what type.size you set here
        type.set_comp_param(sizeof(uint32_t) * 8);
        return type;
      }
      case Type::DECIMAL: {
        const auto& decimal_type = static_cast<const arrow::DecimalType&>(type);
        return SQLTypeInfo(
            kDECIMAL, decimal_type.precision(), decimal_type.scale(), false);
      }
      case Type::TIME32:
        return SQLTypeInfo(kTIME, false);
      case Type::TIMESTAMP:
        switch (static_cast<const arrow::TimestampType&>(type).unit()) {
          case TimeUnit::SECOND:
            return SQLTypeInfo(kTIMESTAMP, 0, 0);
          case TimeUnit::MILLI:
            return SQLTypeInfo(kTIMESTAMP, 3, 0);
          case TimeUnit::MICRO:
            return SQLTypeInfo(kTIMESTAMP, 6, 0);
          case TimeUnit::NANO:
            return SQLTypeInfo(kTIMESTAMP, 9, 0);
        }
      default:
        throw std::runtime_error(type.ToString() + " is not yet supported.");
    }
  }

 private:
  std::vector<std::shared_ptr<arrow::Table>> arrow_tables_;
};