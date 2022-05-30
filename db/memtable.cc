// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

/// key_data:
/// +---------------------------------------+
/// | raw_key | seqno(7byte) | type(1byte)  |
/// +---------------------------------------+
/// - raw_key：用户定义的key。
/// - seqno：每次写操作都有一个sequence number，表示写入先后顺序。通过raw_key + seqno
///          确保了不会产生两个相同的key。
/// - type：更新(插入)/删除。
/// InternalKey的排序顺序：先按照raw_key升序排序，再按照seqno降序排序。

/// memtable中entry格式：
/// +------------------------––––––––---------------------+
/// |  key_size  |  key_data  | value_size |  value_data  |
/// +------------------------––––––––---------------------+
/// | (varint32) | (key_size) | (varint32) | (value_size) |
/// +------------------------–––––––----------------------+
/// - key_size = raw_key's size + 8(seqno's size + type's size)

/// 所有内存中的kv都存储在memtable中，
/// memtable到达阈值(Options.write_buffer_size)后，memtable转换为immutable memtable，
/// 然后再生成一个新的memtable，并在后台将immutable memtable写入SST。
/// 所以同时会存在两个memtable，一个不可修改，一个正在写入。

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  /// 这边的varint最多占5byte。len用于返回key_data的长度。
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

/// 初始引用计数为0，caller会调用至少一次Ref()。
/// 同时创建一个SkipList（table_）。
MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

/// 返回MemTable使用的大约的内存大小。读取时被修改没问题（因为是大约）。
size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  /// 获得aptr和bptr的key_data(raw_key + seqno + type)。
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  /// 将size转变为char放入string的开始位置，其中对size通过varint进行压缩。
  PutVarint32(scratch, target.size());
  /// 之后将key_data继续放入key_size之后。
  scratch->append(target.data(), target.size());
  /// string->data() 返回的是字符串中指向char数组的指针。
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

/// 返回一个memtable上的迭代器。
/// caller需要确保在返回迭代器时，底层的memtable存在。
/// 迭代器返回的key是被编码过的。
Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]

  /// memtable的数据存储格式：
  /// key_size    |   key_data  |   tag   | value_size  | value_data
  /// (varint32)  |  (key_size) |(uint64) | (varint32)  | (value_size)

  /// 获取key和value的大小（用作通过索引找到key和value的位置）
  size_t key_size = key.size();
  size_t val_size = value.size();
  /// internal_key = key + tag(8bytes)
  size_t internal_key_size = key_size + 8;
  /// encoded_len: 要分配的内存大小（byte为单位）=
  /// 保存internal_key_size所需byte + 保存val_size所需byte +
  /// key_data + tag所需byte + value_data所需byte
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  /// 分配encoded_len大小的一块内存。
  char* buf = arena_.Allocate(encoded_len);
  /// 将internal_key_size保存在buf的前几位中
  char* p = EncodeVarint32(buf, internal_key_size);
  /// 直接将key放入分配的内存。
  std::memcpy(p, key.data(), key_size);
  /// 后移，为下次放入作准备。
  p += key_size;
  /// 制作64bit的tag，并压缩成byte，放入p。
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  /// 将bit压缩成byte，放入p，之后放入value。
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  /// 在skiplist插入buf块。保证唯一性。
  table_.Insert(buf);
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  /// lookupkey = varint32 + key + tag
  /// 通过调用memtable_key，保留varint32 + key + tag。
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  /// Seek == FindGreaterOrEqual（memkey.data())
  /// 由于tag具有唯一性，找到的kv一定是唯一的。
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    /// 获取memkey的开始与结束位置。
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    /// 如果memkey在table_中存在：
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      /// 通过tag取出获取key的类型，由于删除不会直接删除key
      /// 通过tag可以查找到是否已经被删除。
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
