//
// Created by j on 2022/5/19.
//

#include <cassert>
#include "leveldb/db.h"

int main() {
  leveldb::DB *db;
  leveldb::Options options;

  options.create_if_missing = true;
  options.error_if_exists = true;
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());

  return 0;
}