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

/*
 * @file    QueryPhysicalInputsCollector.h
 * @author  Alex Suhan <alex@mapd.com>
 * @brief   Find out all the physical inputs (columns) a query is using.
 *
 * Copyright (c) 2016 MapD Technologies, Inc.  All rights reserved.
 */

#ifndef QUERYENGINE_QUERYPHYSICALINPUTSCOLLECTOR_H
#define QUERYENGINE_QUERYPHYSICALINPUTSCOLLECTOR_H

#include "QueryEngine/Descriptors/InputDescriptors.h"
#include "SchemaMgr/ColumnInfo.h"
#include "SchemaMgr/TableInfo.h"

#include <ostream>
#include <unordered_set>

class RelAlgNode;

struct PhysicalInput {
  int col_id;
  int table_id;

  bool operator==(const PhysicalInput& that) const {
    return col_id == that.col_id && table_id == that.table_id;
  }
};

std::ostream& operator<<(std::ostream&, PhysicalInput const&);

namespace std {

template <>
struct hash<PhysicalInput> {
  size_t operator()(const PhysicalInput& phys_input) const {
    return phys_input.col_id ^ phys_input.table_id;
  }
};

}  // namespace std

std::unordered_set<InputColDescriptor> get_physical_inputs(const RelAlgNode*);
std::unordered_set<std::pair<int, int>> get_physical_table_inputs(const RelAlgNode*);

TableInfoMap get_physical_table_infos(const RelAlgNode*);
ColumnInfoMap get_physical_column_infos(const RelAlgNode*);

#endif  // QUERYENGINE_QUERYPHYSICALINPUTSCOLLECTOR_H
