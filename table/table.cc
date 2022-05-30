// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

/// sstable格式：
/// +-------------+
/// | data_block1 | <-----+
/// +-------------+       ｜
/// | data_block2 | <-----+
/// +-------------+       ｜
/// |     ...     | <-----+
/// +-------------+       ｜
/// | data_blockN | <-----+
/// +-------------+       ｜
/// | meta_block  | <--   ｜
/// +-------------+   ｜  ｜
/// |meta_idx_blk | --+   ｜
/// +-------------+       ｜
/// |  idx_block  | ------+
/// +-------------+
/// |    footer   |
/// +-------------+

/// footer格式：
/// +----------------------------------------------------------------------+
/// | meta_idx_blk_handle | idx_blk_handle | padding_bytes | magic(uint64) |
/// +----------------------------------------------------------------------+
/// 保存着meta_idx_blk和idx_blk的索引信息以及magic校验。

/// table模块是对SST进行读取，从磁盘读取到内存，但不是整个SST全读，而是：
/// - 读data index block，节省内存并且可以快速查找。
/// - 读filter block，进行过滤。

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  /// 对文件的随机访问。
  RandomAccessFile* file;
  uint64_t cache_id;
  /// meta block reader。
  FilterBlockReader* filter;
  /// meta block read时所需数据。
  const char* filter_data;

  /// 索引meta block。
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  /// data index block。
  Block* index_block;
};

/**
 * Open会读取文件中的data index block和meta block。
 * @param options 构建内存SST的一些选项。
 * @param file SST文件。
 * @param size SST文件大小。
 * @param table 传入Table**，将构建好的Table*放入。
 * @return
 */
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  /// 读取SST中的footer，固定长度。
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  /// 从file的 (size-Footer::kEncodedLength) 处开始读取 (Footer::kEncodedLength)
  /// 字节到footer_space中，并且footer_input指向footer_space第一个字节。
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  /// footer = meta_idx_blk_handle + idx_blk_handle + padding_bytes + magic
  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  /// 通过idx_blk_handle从SST文件读取index_block_contents到内存。
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    /// 通过index_block_contents在内存中构建index_block。
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

/// 通过footer读取meta index block的index，取出meta block index，读出meta block。
void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  /// 是否设置了保存错误状态，如果设置了，回对读取的数据校验。
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  /// 通过meta_idx_blk_handle，从meta_index_block_index处读取meta index block
  /// 放入contents。
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  /// 通过contents构建meta index block。
  /// meta index block格式：
  /// | filter.name | BlockHandle |
  /// | compresstype 1 byte       |
  /// | crc32 4 byte              |
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    /// 通过key(filter.name)获取到value(BlockHandle)。
    /// 通过BlockHandle构建meta block。
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  /// 生成filter_handle
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  /// 通过filter_handle，从SST文件读取meta block到内存。
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  /// caller负责删除。
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  /// 通过meta block，构建一个meta block reader。
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

/// 三个与Block相关的清理工作：对于Block::Iter来说，当一个Iter不再使用Block时：
/// - 如果Block不在cache，直接额销毁。
/// - 如果Block在cache，先从cache移除，删除cache中的key，最后再释放Block内存。

/// 从内存直接删除block。
static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

/// 删除cache中block的内存。（cache中item删除时自动调用）。
static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

/// 从cache移除block。
static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  /// 对index_value重新编码，生成BlockHandle。
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      /// 需要对Block进行cache。
      char cache_key_buffer[16];
      /// cache_key_buffer = cache_id + offset
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      /// block是否已经在内存中了。
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        /// 如果已经在内存中了，直接取出。
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        /// 否则从SST文件读取，然后放入cache。
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          /// 通过contents构建Block。
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            /// 放入hash_table。
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      /// 没有cache就直接从文件读取。
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    /// 构建好了Block，继续构建data_iter。
    iter = block->NewIterator(table->rep_->options.comparator);
    /// 有无cache对如何释放Block有影响。
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    /// 没构建好Block就直接返回错误。
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

/// 找到相应的key/value之后，用saver进行一次函数回调。
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  /// 二级迭代的第一级，找到对应的data block。
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    /// 取得index_block中的offset + size。
    Slice input = index_iter->value();
    /// 编码为BlockHandle。
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      /// 找到offset。
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      /// 找不到返回的就是meta index block的index。
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    /// 找不到返回的就是meta index block的index。
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
