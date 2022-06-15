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

#include "PersistentStorageMgr.h"

#include "SchemaMgr/SchemaProvider.h"

PersistentStorageMgr::PersistentStorageMgr(const std::string& data_dir,
                                           const size_t num_reader_threads)
    : AbstractBufferMgr(0) {
  if (data_dir != "") {
    UNREACHABLE();
  }
}

AbstractBuffer* PersistentStorageMgr::createBuffer(const ChunkKey& chunk_key,
                                                   const size_t page_size,
                                                   const size_t initial_size) {
  return getStorageMgrForTableKey(chunk_key)->createBuffer(
      chunk_key, page_size, initial_size);
}

AbstractBuffer* PersistentStorageMgr::createZeroCopyBuffer(
    const ChunkKey& key,
    std::unique_ptr<AbstractDataToken> token) {
  return getStorageMgrForTableKey(key)->createZeroCopyBuffer(key, std::move(token));
}

void PersistentStorageMgr::deleteBuffer(const ChunkKey& chunk_key, const bool purge) {
  getStorageMgrForTableKey(chunk_key)->deleteBuffer(chunk_key, purge);
}

void PersistentStorageMgr::deleteBuffersWithPrefix(const ChunkKey& chunk_key_prefix,
                                                   const bool purge) {
  getStorageMgrForTableKey(chunk_key_prefix)
      ->deleteBuffersWithPrefix(chunk_key_prefix, purge);
}

AbstractBuffer* PersistentStorageMgr::getBuffer(const ChunkKey& chunk_key,
                                                const size_t num_bytes) {
  return getStorageMgrForTableKey(chunk_key)->getBuffer(chunk_key, num_bytes);
}

std::unique_ptr<AbstractDataToken> PersistentStorageMgr::getZeroCopyBufferMemory(
    const ChunkKey& key,
    size_t numBytes) {
  return getStorageMgrForTableKey(key)->getZeroCopyBufferMemory(key, numBytes);
}

void PersistentStorageMgr::fetchBuffer(const ChunkKey& chunk_key,
                                       AbstractBuffer* destination_buffer,
                                       const size_t num_bytes) {
  getStorageMgrForTableKey(chunk_key)->fetchBuffer(
      chunk_key, destination_buffer, num_bytes);
}

void PersistentStorageMgr::getChunkMetadataVecForKeyPrefix(
    ChunkMetadataVector& chunk_metadata,
    const ChunkKey& key_prefix) {
  getStorageMgrForTableKey(key_prefix)
      ->getChunkMetadataVecForKeyPrefix(chunk_metadata, key_prefix);
}

bool PersistentStorageMgr::isBufferOnDevice(const ChunkKey& chunk_key) {
  UNREACHABLE();
  return false;
}

std::string PersistentStorageMgr::printSlabs() {
  UNREACHABLE();
  return "";
}

size_t PersistentStorageMgr::getMaxSize() {
  UNREACHABLE();
  return 0;
}

size_t PersistentStorageMgr::getInUseSize() {
  UNREACHABLE();
  return 0;
}

size_t PersistentStorageMgr::getAllocated() {
  UNREACHABLE();
  return 0;
}

bool PersistentStorageMgr::isAllocationCapped() {
  UNREACHABLE();
  return false;
}

AbstractBuffer* PersistentStorageMgr::alloc(const size_t num_bytes) {
  UNREACHABLE();
  return nullptr;
}

void PersistentStorageMgr::free(AbstractBuffer* buffer) {
  UNREACHABLE();
}

MgrType PersistentStorageMgr::getMgrType() {
  return PERSISTENT_STORAGE_MGR;
}

std::string PersistentStorageMgr::getStringMgrType() {
  return ToString(PERSISTENT_STORAGE_MGR);
}

size_t PersistentStorageMgr::getNumChunks() {
  UNREACHABLE();
  return 0;
}

const DictDescriptor* PersistentStorageMgr::getDictMetadata(int dict_id, bool load_dict) {
  if (hasStorageMgr(dict_id)) {
    return getStorageMgr(dict_id)->getDictMetadata(dict_id, load_dict);
  }
  return nullptr;
}

TableFragmentsInfo PersistentStorageMgr::getTableMetadata(int db_id, int table_id) const {
  return getStorageMgrForTableKey({db_id, table_id})->getTableMetadata(db_id, table_id);
}

AbstractBufferMgr* PersistentStorageMgr::getStorageMgrForTableKey(
    const ChunkKey& table_key) const {
  return mgr_by_schema_id_.at(table_key[CHUNK_KEY_DB_IDX] >> 24).get();
}

AbstractBufferMgr* PersistentStorageMgr::getStorageMgr(int db_id) const {
  return mgr_by_schema_id_.at(getSchemaId(db_id)).get();
}

bool PersistentStorageMgr::hasStorageMgr(int db_id) const {
  return mgr_by_schema_id_.count(getSchemaId(db_id));
}

void PersistentStorageMgr::registerDataProvider(
    int schema_id,
    std::shared_ptr<AbstractBufferMgr> provider) {
  CHECK_EQ(mgr_by_schema_id_.count(schema_id), (size_t)0);
  mgr_by_schema_id_[schema_id] = provider;
}

std::shared_ptr<AbstractBufferMgr> PersistentStorageMgr::getDataProvider(
    int schema_id) const {
  CHECK_EQ(mgr_by_schema_id_.count(schema_id), (size_t)1);
  return mgr_by_schema_id_.at(schema_id);
}
