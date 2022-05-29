// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

/// arena的逻辑很简单，用一个vector<char *>来维护所有分配的内存。
/// alloc_ptr_指向当前内存池中可以分配内存的起始点。
/// 当一个内存分配请求x字节的内存时，
/// 先看alloc_ptr_到该块末尾还有多少内存(这个记录在alloc_bytes_remaining_中)，
/// 如果该块内存够，新申请的内存首地址就是现在的alloc_ptr_，
/// 然后把alloc_ptr_朝后面移动x字节，并重新计算alloc_bytes_remaining_。
/// 如果该块剩下的内存不够了，就看x是否大于1/4 page，如果不大于1/4page，
/// 说明当前块剩下的不足1/4了，那么就直接分配一个新的block(1 page)，
/// 把alloc_ptr_的置为新block的首地址，再按之前的逻辑进行分配，旧的块剩余的内存就被浪费掉了。
/// 新申请的block被pushback到blocks_中，以后在arena被析构的时候统一释放。
/// 如果内存分配请求大于1/4个pagesize，为了避免浪费当前块剩下的内存，
/// 就直接向操作系统申请x字节的内存，作为一个单独的block返回，
/// 这个block同样也会被pushback到vector之中。

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

char* Arena::AllocateFallback(size_t bytes) {
  /// 只有当Allocate中alloc_bytes_remaining_无法满足要分配的bytes时，
  /// AllocateFallback才会被调用。
  /// 如果新分配的块大于kBlockSize/4，那说明alloc_bytes_remaining_也有概率大于
  /// kBlockSize/4，那么alloc_bytes_remaining_就不能直接丢弃。
  /// 所以另起一块（bytes长度）分配给新块。原先remaining保留。
  if (bytes > kBlockSize / 4) {
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  /// 否则的话，alloc_bytes_remaining_一定小于1/4，这就可以直接抛弃
  /// 并且可以直接另起一个新block
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

char* Arena::AllocateAligned(size_t bytes) {
  /// sizeof(void*)由编译器决定
  /// 至少0b1000对齐。
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  /// 位操作：
  /// 比如说0b1000与上0b0111就为0。
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  /// slop就是计算出当前alloc_ptr_离对齐还差几个字节，然后给bytes加上
  /// 从而将之前alloc_ptr_缺少的补齐，这样result头部就能对齐。
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    /// AllocateFallback总返回对齐的块。why?
    /// AllocateFallback调用AllocateNewBlock，AllocateNewBlock直接调用new
    /// 进行内存分配，new对于小于128byte的内存分配来说，内存池中按照8, 16,...,128
    /// 维护free list，对于大于128byte的内存分配来说，调用的是calloc，所以new返回的
    /// 地址一定是8byte对齐的。
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  /// 分配一个block_bytes大小的块
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  /// 将当前值原子性的替换为当前值加上arg的结果：
  /// 也就是memory_usage_ += block_bytes + sizeof(char*)
  /// 为什么要加上sizeof(char*)?
  /// 因为blocks_中会保存新分配的块的信息，这也需要内存。
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
