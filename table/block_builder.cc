// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

/// data block中的内容：
/// +------------------+ <---------------- data_
/// |      entry1      | <-+
/// +------------------+   ｜
/// |      entry2      |   ｜
/// +------------------+   ｜
/// |       ...        | <-｜-+
/// +------------------+   ｜ ｜
/// |      entryN      |   ｜ ｜
/// +------------------+ <-｜-｜---------- data_ + restart_offset_
/// |    restart[0]    | --+  ｜
/// +------------------+      ｜
/// |    restart[1]    | -----+
/// +------------------+
/// |   num_restart    | <--------------- num_restarts_
/// +------------------+ <--------------- data_ + size_

/// 每个entry的内容：
/// +---------------------------------------------------------------------------------+
/// |  shared_bytes  | unshared_bytes | value_length | key非共享内容   |   value内容    |
/// +---------------------------------------------------------------------------------+
/// |    (varint)    |    (varint)    |   (varint)   | (non_shared)  | (value_length) |
/// +---------------------------------------------------------------------------------+

/// BlockBuilder完成的是data block的build工作。
/// builder的主要步骤：
/// (1) 生成一个builder对象。
/// (2) 向对象中添加kv等。
/// (3) Finish()构建完成。
/// (4) 构建完成后的block放在内存中，按照SST文件指定格式。

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  /// buffer_的size为所有entry所占内存大小。
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

Slice BlockBuilder::Finish() {
  // Append restart array
  /// entry块的末尾添加每一个restart分区的start point（也就是offset）。
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  /// 最后添加一个数字表示有多少个restart分区。
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  /// restart分区大小设置为block_restart_interval。
  assert(counter_ <= options_->block_restart_interval);
  /// key一定是有序的，因为在memtable中通过skiplist保证了有序性。
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    /// 当前restart分区还没满，可以继续插入entry。
    /// 获取上一个entry的key与当前要插入的entry的key的较小值。
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    /// 找到前缀长度。
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    /// 如果当前restart分区已经满了，那就再次分配一个新的restart分区。
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  /// 取得和前一个key非共享的长度。
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  /// 放入entry的前三个属性。
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  /// 放入key的非共享部分和value。
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  /// 制作新的last_key_
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
