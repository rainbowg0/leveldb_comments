// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

/// 确保线程安全，写入需要外部同步（mutex）
/// 读取要求在读取过程中SkipList不被销毁，除此以外，读取不需要内部锁以及同步。
/// 不变式：
/// (1) SkipList销毁时才会销毁节点。
/// (2) 节点被链接到SkipList后除了next/prev，其他属性不能被修改。
///     如果要修改，只能通过Insert()。

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

/// 基于以下两个特点，skiplist中操作不需要锁或者node的引用计数：
/// (1) skiplist中node内保存的是InternalKey与相应的value组成的数据，SequenceNumber
///     的全局唯一性保证了不会有相同的node出现，也就保证了不会有node被更新。
/// (2) delete操作等同于put，所以不需要引用计数记录node的存活周期。
/// (3) node只会在skiplist销毁时才会销毁。

template <typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;  // Arena used for allocations of nodes

  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int> max_height_;  // Height of the entire list

  // Read/written only by Insert().
  Random rnd_;
};

// Implementation details follow
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k) : key(k) {}

  Key const key;


  /// memory_order指定了memory access的方式：
  /// 包括非原子的memory access会被原子操作进行排序。
  /// 在多个系统上没有限制，当多个线程同时read/write几个变量时，
  /// 一个线程可以观察到与另一个线程写入它们的顺序不同的值的变化顺序。
  /// 通过给定一个memory_order可以指定操作。

  // Accessors/mutators for links.  Wrapped in methods, so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    /// acquire：在这次load之前，当前线程的读写不能被reorder
    /// 所有其他线程的写入如果release了相同的原子变量的话，在当前线程就可见。
    /// release-acquire ordering：线程A有一个release，线程B有一个acquire，
    /// 在load前，B可以看到所有A对内存做的改变
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    /// release：在这次store之后，当前线程的读写不能被reorder
    /// 所有在当前线程的写入被其他acquire了相同的原子变量的线程可见。（release-acquire）
    next_[n].store(x, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  /// relax：：没有对其他读取或写入施加同步或排序约束，仅保证此操作的原子性。
  /// relax-ordering：标记为 memory_order_relaxed 的原子操作不是同步操作。
  /// 它们不会在并发内存访问之间强加顺序。它们只保证原子性和修改顺序的一致性。
  /// 例如：
  /// Thread 1:
  /// r1 = y.load(std::memory_order_relaxed); // A
  /// x.store(r1, std::memory_order_relaxed); // B
  /// Thread 2:
  /// r2 = x.load(std::memory_order_relaxed); // C
  /// y.store(42, std::memory_order_relaxed); // D
  /// 可能会出现r1 == r2 == 42的结果。
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {
  /// 常量指针
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  return new (node_memory) Node(key);
}

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  /// 确保height小于等于kMaxHeight
  while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

/// 如果key大于n中的值，返回true
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

/// 返回第一个大于等于key的节点（如果没找到，返回nullptr）
/// 当prev非空时，在prev中保存所有[0..max_height_-1]层的指向返回节点的节点。
/// 流程：
/// (1) 从当前最高level开始，一直next，直到找到n->key >= key。
/// (2) 找到后，将当前level的节点加入到prev中。然后判定：
///     a. 如果当前level已经为0，直接返回next节点。
///     b. 否则下降一层，继续while循环。
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  /// 从head_开始遍历所有层。
  Node* x = head_;
  int level = GetMaxHeight() - 1;

  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      /// 如果next非空并且next->key < key，前往下一个当前层的节点。
      x = next;
    } else {
      /// 否则要降一层了，先将当前层的指向待返回节点的指针保存下来。
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        /// 已经到底了，直接返回。
        return next;
      } else {
        // Switch to next list
        /// 下降至level0。
        level--;
      }
    }
  }
}

/// 返回小于等于key的最大节点（如果没找到返回nullptr）
/// 流程：
/// (1) 从当前最高level开始，一直next，直到找到n->key >= key。
/// (2) 找到后，判定：
///     a. 如果当前level已经为0，直接返回next节点。
///     b. 否则下降一层，继续while循环。
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;  /// 从头部开始
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    /// 当x->key < key时，获取下一个node
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      /// 如果下一个node为表尾，或者下一个node的key大于等于key
      if (level == 0) {
        /// 如果当前已经是level0，说明找到了大于等于key的最小node
        /// 直接返回
        return x;
      } else {
        /// 否则往下降一层，继续遍历。
        // Switch to next list
        level--;
      }
    } else {
      /// 如果下一个node既不是表尾并且node的key小于key
      /// 那么从跳到下一个node，继续遍历。
      x = next;
    }
  }
}

/// 返回list中的最后一个节点。如果list为空，返回head。
/// (1) 从当前最高level开始，一直next。
/// (2) 发现next为nullptr：
///     a. 如果此时已经到level0了，说明找到了尾节点，直接返回。
///     b. 否则进入下一层找。
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {
  /// 从head_开始找并且遍历[max_height_-1, ..., 0]层。
  Node* x = head_;
  int level = GetMaxHeight() - 1;

  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      /// 下一个节点为空，有两种可能：
      /// (1) level != 0 说明level0上层跳过last node直接指向nullptr
      /// (2) level == 0 说明已经到达level0，作为level0的最后一个节点，必然指向nullptr。
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      /// 下一个节点不为空，那就一直往下跳。
      x = next;
    }
  }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

/// 插入流程：
/// (1) 先找到大于等于key的第一个节点。并将所有指向该节点的指针保存下来。
/// (2) 随机产生一个高度作为新节点的level，如果新生成的level大于当前的max_height_，
///     超出的部分体现在prev中之前保存的node，将这些超出部分填入head。并更新max_height_。
///     更新时，可以memory_order_relaxed，why？
///     并发reader读取max_height_只会有两种可能：
///     a. max_height_更新了，但head_中next指针还没更新：
///        next指向nullptr，自然结束。
///     b. head_中next已经更新，那就使用那个新插入的节点。
/// (3) 构造一个新节点，更新prev和next。
template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  /// prev中最多保存kMaxHeight层的指向待返回的节点的指针。
  /// 先找到大于等于key的节点x。
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  /// 检查一下重复插入相同key。
  assert(x == nullptr || !Equal(key, x->key));

  /// roll一个level
  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      /// 如果roll的随即高度大于max_height_
      /// 待插入节点的从大于部分开始的prev就是head_（因为之前这些高度没有节点）
      prev[i] = head_;
    }
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.store(height, std::memory_order_relaxed);
  }

  /// 构建新节点，通过prev更新新节点指向的节点以及指向新节点的节点的指针。
  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
