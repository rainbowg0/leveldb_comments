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

  /// 无拷贝赋值
  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc.
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    /// memory_order_relaxed只保证内部有序，不保证外部有序。
    /// 也就是说对于memory_usage_变量的操作是原子性的，但是对于memory_usage_的多条语句
    /// 不保证前后顺序一致性。
    /// 举个例子：
    /// Thread 1:
    /// r1 = y.load(std::memory_order_relaxed); // A
    /// x.store(r1, std::memory_order_relaxed); // B
    /// Thread 2:
    /// r2 = x.load(std::memory_order_relaxed); // C
    /// y.store(42, std::memory_order_relaxed); // D
    /// 有可能出现r1 == r2 == 42的情况。也就是出现先运行D，再运行A，即使A在D之前。
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  char* alloc_ptr_;
  /// 如果分配了一个大块，而没有用完，下次分配会先查看
  /// 大块中没有用完的部分，符合就直接从大块取，不用再分配新块了。
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  //
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?
  std::atomic<size_t> memory_usage_;
};

/// Allocate流程：
/// (1) 如果要分配的bytes小于等于上次分配1 page但还没用完的alloc_bytes_remaining
///     那就从剩余中取，并更新alloc_ptr_和alloc_bytes_remaining_。
/// (2) 否则，剩余不能满足bytes，通过AllocateFallback分配。
inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use). assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  /// 如果当前分配的块没有剩余或者剩余不满足要分配的大小，分配一个新块。
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
