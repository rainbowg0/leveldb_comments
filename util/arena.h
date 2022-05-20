// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb {

class Arena {
 public:
  Arena();

  // 无拷贝赋值
  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  ~Arena();

  // 返回一个指向新分配的内存大小为bytes的块的指针。
  char* Allocate(size_t bytes);

  // 分配的内存自然对齐。
  char* AllocateAligned(size_t bytes);

  // 返回分配的数据的总内存使用量的估计值。
  size_t MemoryUsage() const {
    // 原子性的加载并返回当前值。值在内存的顺序不要紧。
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // 分配状态：
  char* alloc_ptr_;  // 指第一个待分配的指针
  // 如果分配了一个大块，而没有用完，下次分配会先查看
  // 大块中没有用完的部分，符合就直接从大块取，不用再分配新块了。
  size_t alloc_bytes_remaining_;

  // 通过new[]分配的内存块数组。
  std::vector<char*> blocks_;

  // arena的所有内存使用状况。
  //
  // TODO(costan): 这个成员确保原子操作的，但其他
  //               成员不是原子操作，同时也没有锁，是否合理？
  std::atomic<size_t> memory_usage_;
};

inline char* Arena::Allocate(size_t bytes) {
  // 如果返回0-byte，那语义就很奇怪，所以就干脆在调用Allocate时保证非0。
  assert(bytes > 0);
  // 如果当前分配的块还有剩余，并且剩余满足要分配的大小，就直接从当前的块分配内存。
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  // 如果当前分配的块没有剩余或者剩余不满足要分配的大小，分配一个新块。
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
