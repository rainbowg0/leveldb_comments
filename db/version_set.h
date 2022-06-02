// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

/// 每次compact后的最新数据状态定义为version，就是当前db的元信息以及
/// 每个level上最新数据状态的SST集合。compact会在某个level上新加入/删除一些
/// SST，如果删除时，SST正在被读，由于SST一旦生成就不会被改动，每个version加入引用计数，
/// 这样通过链表将多个version串联起来，当version的引用计数为0并且不是当前最新version，
/// 会从链表移除。该version的SST就可以删除了（删除的SST会在下一次compact被清理）。
class Version {
 public:
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };

  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  VersionSet* vset_;  // VersionSet to which this Version belongs
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version

  // List of files per level
  /// 每个level的所有SST的元信息。
  /// files_[i]中的FileMetaData按照FileMetadata::smallest排序。
  /// 每次更新都确保有序（VersionSet::Builder::Save()）。
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Next file to compact based on seek stats.
  /// 需要compact的文件。（allowed_seeks用完）。
  FileMetaData* file_to_compact_;
  /// file_to_compact_的level。
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  /// 当前最大compact权重以及对应level。
  double compaction_score_;
  int compaction_level_;

  /**
   * 对和compact相关的成员变量着重说明：
   * (1) compaction_score_:
   *     leveldb中分level对SST进行管理，对于读来说，各个level的SST的count，size以及
   *     range的分布会影响读的效率。理想情况下，level-0只有一个SST，level-1以及
   *     以上key-range均匀分布。更多查找可以遍历更少level可以定位到。
   *     这种理想情况可以定义为：level处于均衡状态。通过compaction_score_可以量化
   *     level的不均衡比重，score越大，该level越不均衡，需要优先compact。
   *     每个level具体均衡参数及其比重计算策略如下：
   *     (a) level-0的SST的range可能会overlap，所以如果level-0上有过多的SST，
   *         在做查找时会严重影响效率，同时，因为level-0中的SST由memtable直接dump
   *         得到，不受kTargetFileSize（SST的最大size）控制，所以SST的count更有意义，
   *         所以对于level-0来说，均衡状态需要满足：SST的count < kL0_CompactionTrigger
   *         score = SST的count / KL0_CompactionTrigger
   *     (b) 对于level-1及以上的level来说，SST均由compact过程产生，生成的SST大小
   *         由kTargetFileSize控制，所以可以限定SST总的size。当前的策略是设置初始值
   *         kBaseLevelSize，然后以10的指数级按level增长。每个level可以容纳
   *         quota_size = kBaseLevelSize * 10^(level_num-1)。
   *         所以对于level-1及以上的level，均衡状态需要满足：SST的size < quota_size
   *         score = SST's size / quota_size
   *
   *         每次compact完成，生成新的version(VersionSet::Finalize()), 都会
   *         根据上述策略，计算出每个level的score，取最大值作为
   *         当前version的compaction_score_，同时记录对应的level(compaction_level_)。
   * (2) file_to_compact_:
   *     leveldb对单个SST文件的IO做了细化的优化。
   *     首先，一次查找如果对于多个SST进行查找（对SST查找看作对于data_block的寻道），
   *     说明处于低level上的SST没有提供高hit的比率，可以认为处在不最优的情况。
   *     而compact后应该处于均衡状态，所以在一个SST的seek次数达到一定阈值之后，
   *     主动对其进行compact，具体由allowed_seeks决定。
   *     (a) 一次磁盘寻道seek约耗费10ms。
   *     (b) 读/写1MB数据约耗费10ms
   *     (c) compact 1MB数据约25M的IO：从level-n读取1M数据，从level-n+1读10～12M数据，
   *         写入level-n+1中10～12M数据。 所以compact 1M数据相当于做了25次磁盘seek，
   *         一次seek相当于compact 40k数据，那么seek阈值allowed_seeks = SST size / 40k。
   *         保守设置，当前实际的allowed_seeks = SST size / 16k。每次compact完成，
   *         构造新的version(Builder::Apply())，每个SST的allowed_seeks会计算出
   *         保存在FileMetaData。在每次get操作，如果由超过一个SST文件进行了查找，
   *         会讲第一个进行查找的SST的allowed_seeks减1，并检查其是否已经用完了allowed_seeks，
   *         如果是，则将SST记录成当前version的file_to_compact_，并记录其所在的
   *         level（file_to_compact_level_）。
   */
};

/// 整个db的当前状态被VersionSet管理着，其中有当前最新的version以及其他正在服务的
/// version链表。全局的SequenceNumber，FileNumber；当前的manifest_file_number，
/// 以及封装的SST的TableCache，每个level中下一次compact要选取的start_key等。
class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  Env* const env_;
  /// db的数据路径。
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  /// 下一个可用的FileNumber。
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  /// 最后用过的SequenceNumber。
  uint64_t last_sequence_;
  /// log文件的file_number。
  uint64_t log_number_;
  /// 辅助log文件的file_number，在compact memtable时，设置为0。
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  /// manifest文件的封装。
  WritableFile* descriptor_file_;
  /// manifest文件的writer。
  log::Writer* descriptor_log_;
  /// 正在服务的Version链表。
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  /// 当前最新的Version。
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  /// 为了尽量均匀compact每个level，将这一次compact的end-key作为下一次compact的start-key。
  /// compactor_pointer_就保存着每个level下一次compact的start-key。
  /// 除了current_外的version，并不会compact，所以这个值并不保存在version中。
  std::string compact_pointer_[config::kNumLevels];
};

// A Compaction encapsulates information about a compaction.
/// db中有一个compact后台进程，负责将memtable持久化成SST，以及
/// 均衡整个db中各level的SST。compact进程会优先将已经写满的
/// memtable dump成level-0的SST（不会合并相同的key/清理已经删除的key）
/// 然后根据设计的策略选取level-n以及level-n+1中有key-range overlap的几个SST
/// 进行merge（期间会有合并相同的key以及清理删除的key），最后生成若干个
/// level-n+1的SST。随着不断写入和compact，低level的SST向高level迁移。
/// level-0中的SST因为是由memtable直接dump得到，所以key-range可能overlap。
/// 但是level-1以及更高的SST都是由merge产生，保证了位于同level的SST之间，
/// key-range不会overlap，有利于read。
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  /// 要compact的level。
  int level_;
  /// 生成的SST的最大size。（kTargetFileSize）
  uint64_t max_output_file_size_;
  /// compact时的当前version。
  Version* input_version_;
  /// 记录compact过程的操作。
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  /// inputs_[0]为level-n的SST文件信息。
  /// inputs_[1]为level-n+1的SST文件信息。
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  /// 位于level-n+2，并且与compact的key-range有overlap的SST。
  /// 保存grandparents_是因为compact最终会生成一系列level-n+1的SST。
  /// 如果生成的SST与level-n+2中有过多overlap，当compact level-n+1时，
  /// 会产生过多的merge，为了尽量避免这种情况，compact过程中需要检测
  /// 与level-n+2中产生overlap的size，并且与阈值kMaxGrandParentOverlapBytes
  /// 做比较，来提前终止compact。
  std::vector<FileMetaData*> grandparents_;
  /// 记录compact时，grandparents_中已经overlap的index。
  size_t grandparent_index_;  // Index in grandparent_starts_
  /// 记录是否有key检查overlap
  /// 如果是第一次检查，发现有overlap，也不会增加overlapped_bytes_。
  bool seen_key_;             // Some output key has been seen
  /// 记录overlap的累计size。
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  /// compact时，当key的ValueType是kTypeDeletion时，
  /// 检查其在levle-n+1以上是否存在（isBaseLevelForKey()）
  /// 从而决定是否丢弃该key。因为compact时，key的遍历是顺序的，
  /// 所以每次检查从上一次检查结束的地方开始。
  /// level_ptrs_[i]记录了input_version_->levels_[i]，上一次比较结束的SST下标。
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
