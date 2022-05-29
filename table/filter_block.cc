// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

/// SST文件中的meta block。
/// builder主要功能就是将memtable输出在内存中整理成SST文件的格式，
/// 真正写入到磁盘是依靠writer。
/// meta block存储一些data block的信息。

/// meta block结构
/// +---------------------+ <----------------------- result_
/// |       filter1       | <---+
/// +---------------------+     ｜
/// |       filter2       | <---｜--+
/// +---------------------+     ｜  ｜
/// |         ...         |     ｜  ｜
/// +---------------------+     ｜  ｜
/// |       filterN       | <---｜--｜--+
/// +---------------------+ <---｜--｜--｜---+------- filter_offsets_
/// |   filter1 offset    | ----+   ｜  ｜  ｜
/// +---------------------+         ｜  ｜  ｜
/// |   filter2 offset    | --------+   ｜  ｜
/// +---------------------+             ｜  ｜
/// |         ...         |             ｜  ｜
/// +---------------------+             ｜  ｜
/// |   filterN offset    |-------------+   ｜
/// +---------------------+                 ｜
/// |   offset of filter  | ----------------+
/// +---------------------+
/// |       g(base)       |
/// +---------------------+

/// 一般来说，只有一个meta block，data block每次达到2KB创建一个新filter。

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

/// block_offset指的是sstable文件的offset。
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  /// 每隔2K创建一个新的filter，filter_index代表了当前filter总数。
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  /// 由于所需filter总数已经超过了当前filter总数，补齐filter。
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  /// 直接在keys_长string末尾添加key，并更新索引数组。
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    /// 虽然block没满2K，还是要给剩余的构建一个新的filter。
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    /// result尾部添加所有的filter的offset。
    PutFixed32(&result_, filter_offsets_[i]);
  }

  /// 再在最后添加filter数量以及每隔多少大小的size构建一个filter。
  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  /// 当前key的总数。
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    /// 需要新生成一个filter。
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    /// 遍历获取num_keys个key。
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    /// 将每个key保存在tmp_keys_中。
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  /// 生成了一个新的filter。
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  /// 上一个filter中的信息清空。
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  /// 只适用于读取meta block的内容。
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  /// 通过base_lg_得知每个多少byte分配一个filter。
  base_lg_ = contents[n - 1];
  /// 获取filter总数。
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  /// 每隔多少index一个filter。
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    /// 当前entry的limit就是下一个entry的开头。
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    /// limit也要是合法的（不然可能跑到offset_去）
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      /// 取出filter。
      Slice filter = Slice(data_ + start, limit - start);
      /// 通过filter获取key是否存在。
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
