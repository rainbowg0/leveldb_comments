// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

/// sstable是leveldb在持久化数据时的文件格式，sstable由数据data和元信息meta组成。
/// data和meta都存储在以block为单位的单元中。
/// sstable的生成时机：在将immutable memtable内存flush时，或者在做major compaction时。

/// sstable格式：
/// +-------------+
/// | data_block1 |
/// +-------------+
/// | data_block2 |
/// +-------------+
/// |     ...     |
/// +-------------+
/// | data_blockN |
/// +-------------+
/// | meta_block  |
/// +-------------+
/// |meta_idx_blk |
/// +-------------+
/// |  idx_block  |
/// +-------------+
/// |    footer   |
/// +-------------+

/// footer格式：
/// +----------------------------------------------------------------------+
/// | meta_idx_blk_handle | idx_blk_handle | padding_bytes | magic(uint64) |
/// +----------------------------------------------------------------------+
///           ^                    ^
///           ｜                   ｜
///     offset + size        offset + size

/// (1) data_block：实际存储kv。
/// (2) meta_block & meta_idx_blk：当前版本的两者并没有完全实现，而是以Options可选
///     配置的方式填充filter_block信息，并将filter_block_handle编码后放入
///     meta_idx_blk，所以meta_idx_blk仅包含filter_meta_block对应的索引信息。
/// (3) idx_block：data_block的last_key_及其在sstable文件中的索引。block中entry
///     的key即为last_key_，value即为data_block的BlockHandler(offset/size)。
/// (4) footer：文件末尾固定长度的数据(48B)，保存着meta_idx_block和idx_block的索引信息。
///     为了达到固定长度，需要padding_bytes。

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr /// meta block
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  /// 写出到哪个文件。
  WritableFile* file;
  /// 当前文件的offset。
  uint64_t offset;
  Status status;
  /// 存放kv的地方。
  BlockBuilder data_block;
  /// 记录所有block的idx的block。
  BlockBuilder index_block;
  /// 最后添加进来的key，之后要添加进来key，就需要和last_key进行比较，
  /// 进而保证整体顺序有效。
  std::string last_key;
  /// 总共kv数。
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  /// meta block。
  /// 一般情况下只有一个meta block，所以写完meta block后立刻写入meta idx block。
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  /// data block index中存放一个key用于将前后两个data block分隔开，分隔的时候
  /// 可以使用一个长度较短的中间值。
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  /// comparator不能变，因为comparator规定了sstable的kv存储顺序，不能一会，这样一会那样。
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  /// options被更新，block_builder中的options自然随着更新。
  rep_->options = options;
  rep_->index_block_options = options;
  /// 用于索引block的meta，自然就不需要前缀压缩了。
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  /// 检测是否有序。
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  /// 每当新生成一个data block时，需要新生成一个data block index entry。
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    /// 找到一个能够分隔两个data block的最短key。
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    /// new一段内存来存放handle编码。
    std::string handle_encoding;
    /// 将pending_handle编码到内存中。
    r->pending_handle.EncodeTo(&handle_encoding);
    /// 新生成了一个data block，也就要新生成一个data block index entry指向该data block。
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    /// 当前不是新块了。
    r->pending_index_entry = false;
  }

  /// 将key添加到filter中，用于快速确认key是否存在。
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  /// last_key更新。kv数++。新data block中添加新kv。
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  /// 差不多满了，就刷盘。
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    /// 刷完盘，当前data block为空，自然开启一个新data block。
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
