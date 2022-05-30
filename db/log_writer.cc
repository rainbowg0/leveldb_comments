// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

/// 一个block中可能有多个record，但并不是以block为最小单位进行I/O的，而是以record进行I/O.
/// log block的组成：
/// +---------+
/// | record0 |
/// +---------+
/// | record1 |
/// +---------+
/// | ....... |
/// +---------+
/// | recordN |
/// +---------+
/// | trailer |
/// +---------+
/// trailer：如果block最后部分小于record的kHeaderSize，剩余部分为trailer，填0不用。

/// record组成：
/// +------------------------------–––––––––––-----------------------+
/// | checksum(uint32) | length(uint16) | type(uint8) | data(length) |
/// +------------------------------–––––––––––-----------------------+
/// + checksum： 记录的是type和data的crc校验
/// + length：是record内部保存的data长度（小端）
/// + 共5种record types：
/// (1) kZeroType：为preallocated文件保存的。
/// (2) kFullType：该record是完整的。
/// (3) kFirstType：碎片的第一块。
/// (4) kMiddleType：碎片的中间块。
/// (5) kLastType：碎片的最后一块。
/// 其中 (checksum+length+type) 共同组成了header，大小为kHeaderSize。

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    /// crc32c返回的是crc形式的数据n。
    /// 初始化所有的5种record type。
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

/// 从block的0开始写入。
Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

/// 从block的dest_length % kBlockSize开始写入。
Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  /// 如果record塞不下当前block，就将其分片。
  Status s;
  bool begin = true;
  do {
    /// block还剩多大
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {
      /// 如果剩余size比header还小，那就重新分配一个块。
      // Switch to a new block
      /// 把block剩余的填充。
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      /// 从新block的offset = 0开始。
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    /// avail：分配给header后，当前block还剩的size。
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    /// 如果还剩的size比left还大，那就划出left大小的块，否则将block剩余全部分配。
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    /// 如果left < avail，那么要么full，要么last。
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  /// 4 5代表length
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  /// 6代表type
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  /// 0 1 2 3 代表checksum
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  /// 写入header
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    /// 写入data。
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      /// 以record为单位进行刷盘。
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
