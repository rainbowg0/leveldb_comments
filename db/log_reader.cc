// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

/// 区分逻辑record和物理record：
/// (1) 逻辑record：用户输出的一个完整slice。
/// (2) 物理record：一个完整slice可能不能完全在log block里放下，所以需要物理record
///     将逻辑record分为几个物理record，存储在block中。

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

/// 创建一个reader，从file读取record（reader使用过程中，file remain live）
/// 如果reporter非空，在发生错误时报告情况。
/// 如果checksum为true，检验读取是否正确。
/// 从initial_offset开始读。
Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

/// 流程：
/// (1) 通过 initial_offset % kBlockSize 找到文件中block的偏移量，initial_offset减去
///     偏移量获得当前块的起始位置。
/// (2) 如果偏移量在后6字节处，起始位置往下移一个block，读取下一个block。
/// (3) Skip block_start_location 字节，跳跃到指定位置读。
bool Reader::SkipToInitialBlock() {
  /// offset_in_block：从要读的block的偏移量开始读。
  /// block_start_location：要读的block的在文件中的偏移量。
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  /// 如果从当前的block开始读，发现剩余size小于header，
  /// 说明当前block不可能存储有record信息或者存储的信息无法读全。
  /// 那就进入下一个块开始读。
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    /// 跳到对应的block开始读。
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      /// 如果碰到eof，报道错误
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  /// 通过initial_offset_跳跃到文件中的指定block。
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  /// 从当前block的第一个record开始读。
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    /// 获取要读取的block。
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    /// 找到record在文件中的偏移量。
    /// end_of_buffer_offset_就是当前block末尾在文件中的偏移量。
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    /// 如果是在重启中
    /// 跳过所有middle tye，直到遇到下一个逻辑record。
    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          /// 上一次读取到的是first/middle block，这一次应该读取的是middle/last block，
          /// 但是读到了full block，逻辑有误。
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        /// scratch是负责将所有<first, middle, last> record收集并拼接。
        /// 如果读到了full record，自然就不用拼接了。
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            /// 当前first record，前一个要么是碎片，要么是full
            /// 不管哪一个，都已经是完整的record了，自然scratch是空的。
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        /// 将当前first record放入到scratch进行拼接。
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        /// 将整个block放入buffer_来为这一次读作准备。
        /// 而backing_store_指向buffer_的起点。
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size();
        if (!status.ok()) {
          /// 从文件读取失败，终止读操作。
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          /// 读取成功了，但是并没有读取到整块，说明：
          /// (1) 读取的size大于等于kHeaderSize，这是最后一个要读取的块
          /// (2) 读取的size小于kHeaderSize，又一次发生错误。
          /// 不管哪种情况，都eof了。
          eof_ = true;
        }
        /// 进入下一轮循环。
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        /// 如果buffer_非空并且小于kHeaderSize，说明write时发生了错误，直接返回。
        /// 当然，如果buffer_非空，并且eof，但是大于等于kHeaderSize，
        /// 说明当前读取的是最后一个block，也不会走到这。
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    /// header[0..3]: checksum
    /// header[4..5]: length
    /// header[6]: type
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    if (kHeaderSize + length > buffer_.size()) {
      /// kHeaderSize + length 应该和buffer_.size()一样大，出现不一样大的原因有2：
      /// 1. record内容有错误。
      /// 2. writer写入时crash了。
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    /// 发生这种情况的原因：写入record到block后，可能会遇到剩余7bytes的情况，只能写一个header。
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        /// 如果没过checksum，将buffer清空并报告错误块大小。
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    /// 移动到实际data处
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
