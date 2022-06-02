// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

/// table_cache缓存Table对象，每个DB一个。
/// table_cache的格式：
/// +------------------------------------------+
/// | file_number(key) | TableAfterFile(value) |
/// +------------------------------------------+

namespace leveldb {

struct TableAndFile {
  /// 每个table有一个TableAndFile，由于SST很大，一般不回全部加载进内存。
  /// 只会加载data_index_block 和 meta_block，实际的data_block需要等到操作
  /// 时才会加载进内存。
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  /// 将file_number构建成slice形式，通过slice查找TableAndFile。
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    /// 通过 (db name + file_number) 构建file name。
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    //  TODO ???----------------------
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }
    //  TODO ???----------------------

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

/**
 * 返回file number指定的iter，并将tableptr指向Table。tableptr owned by cache。
 * @param options 一些用户指定选项。
 * @param file_number 要迭代的file编号。
 * @param file_size 要迭代的file大小。
 * @param tableptr 塞入一个指向table的指针。
 */
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  /// 找到TableAndFile。
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  /// 找到SST。
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  /// 构建two_level_iter。
  Iterator* result = table->NewIterator(options);
  /// 这个iter如何释放？
  /// 由于是为cache所有，销毁是调用cache的Release。
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

/**
 * 通过file_number找到SST，然后通过SST找到data_block中k对应的value，存放在arg中。
 * @param options
 * @param file_number
 * @param file_size
 * @param k
 * @param arg
 * @param handle_result
 */
Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    /// 找到SST。
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    /// 通过SST获取entry。
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
