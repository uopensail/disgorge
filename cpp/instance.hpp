//
// `swallow` - 'trace log storage for recommender system'
// Copyright (C) 2019 - present timepi <timepi123@gmail.com>
// LuBan is provided under: GNU Affero General Public License (AGPL3.0)
// https://www.gnu.org/licenses/agpl-3.0.html unless stated otherwise.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be usefulType,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//

#ifndef SWALLOW_INSTANCE_HPP
#define SWALLOW_INSTANCE_HPP

#pragma once

#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/write_batch.h>

#include <iostream>
#include <string>
#include <vector>

#include "query.hpp"

namespace swallow {

const size_t max_count = 1000;

class Instance;

class Response {
 public:
  Response() : more_(false), lastkey_("") {}
  ~Response() = default;
  bool more() { return more_; }
  size_t size() { return data_.size(); }
  const std::string &lastkey() { return lastkey_; }
  const std::string &operator[](size_t i) const { return data_[i]; }

 private:
  bool more_;
  std::string lastkey_;
  std::vector<std::string> data_;
  friend class Instance;
};

class Instance {
 public:
  Instance() = delete;
  Instance(std::string data_dir, std::string secondary = "") : db_(nullptr) {
    rocksdb::Status status;
    if (secondary != "") {
      status = rocksdb::DB::OpenAsSecondary(rocksdb::Options(), data_dir,
                                            secondary, &db_);
    } else {
      status = rocksdb::DB::OpenForReadOnly(rocksdb::Options(), data_dir, &db_);
    }
    if (!status.ok()) {
      std::cerr << "open leveldb error: " << status.ToString() << std::endl;
      throw std::runtime_error("open rocksdb error");
    }
    assert(db_ != nullptr);
  }
  ~Instance() {
    db_->Close();
    delete db_;
  }

  Response *scan(rocksdb::Slice query, rocksdb::Slice start,
                 rocksdb::Slice end) {
    auto expr = swallow_query::parse(query.data(), query.size());
    Response *resp = new Response();
    const rocksdb::Snapshot *sp = db_->GetSnapshot();
    rocksdb::ReadOptions options;
    options.snapshot = sp;
    if (start.size() > 0) {
      options.iterate_lower_bound = &start;
    }

    if (end.size() > 0) {
      options.iterate_upper_bound = &end;
    }

    rocksdb::Iterator *it = db_->NewIterator(options);
    it->SeekToFirst();

    if (it->Valid()) {
      if (it->key() == start) {
        it->Next();
      }
    }
    size_t count = 0;
    for (; it->Valid(); it->Next()) {
      rapidjson::Document doc;
      doc.Parse(it->value().data(), it->value().size());
      if (expr->Exec(doc)) {
        resp->data_.emplace_back(
            std::string{it->value().data(), it->value().size()});
        count++;
        if (count >= max_count) {
          resp->more_ = true;
          resp->lastkey_ = std::string{it->key().data(), it->key().size()};
          break;
        }
      }
    }
    delete it;
    db_->ReleaseSnapshot(sp);
    return resp;
  }

 private:
  rocksdb::DB *db_;
};
}  // namespace swallow

#endif  // SWALLOW_INSTANCE_HPP