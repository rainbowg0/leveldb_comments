// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

/// leveldb采用的是redo-log。
/// log block的存储格式：每个block大小为32KB，里面有一条/多条record，每个record
/// 带有一个checksum，来应对fault-tolerance。
/// 这种存储方式有利有弊：
/// (1) 利：如果磁盘损坏，直接跳过损坏的block就行。
///         容易找到边界。
///         单条大数据不需要分配很大的内存来做buffer。
/// (2) 弊：小record存在浪费空间。
///         没有压缩。

namespace leveldb {
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
