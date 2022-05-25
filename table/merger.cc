// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),  // 为n个child_iter分配一块内存。
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  ~MergingIterator() override { delete[] children_; }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    /// 所有child_iter都移动各自的到开头位置。
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    /// 找到最小的child_iter。
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override {
    /// 所有child_iter共同向SeekToFirst。
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    /// 找到最大的child_iter。
    FindLargest();
    direction_ = kReverse;
  }

  void Seek(const Slice& target) override {
    /// 所有child_iter同时Seek。
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    /// Seek找到的是大于等于target的最小key，
    /// 通过找到最小child_iter，如果target不存在，那么最小child_iter也会大于target，
    /// 如果target存在，那最小child_iter就等于target。
    FindSmallest();
    direction_ = kForward;
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    /// 这边的iter只能从小往大迭代，一旦调用了Next，那么迭代器就会往后迭代。
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        /// 找到non_current_中，找到一个所有大于等于current_->key的key的iter，
        /// 如果相等，那就将所有的这些child_iter往后移。
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    /// 如果是向后扫描，那么当前current_直接取next就行。
    /// 每次next后，当前current_不一定符合条件了，因为key的值被改变了。
    /// 所以需要重新FindSmallest，刷新current_状态。
    current_->Next();
    FindSmallest();
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    /// 所有操作与Next相反。
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  /// 两个扫描的移动方向。
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  /// 基本和Iterator差不多，除了对key和value进行缓存提高效率。
  IteratorWrapper* children_;
  /// children_中iter的长度。
  int n_;
  /// 当前iter。
  IteratorWrapper* current_;
  /// 当前扫描的移动方向。
  Direction direction_;
};

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  /// 从n个child_iter中找到当前key最小的，将current_指针移动到那个child_iter上。
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
