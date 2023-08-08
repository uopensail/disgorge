//
// `disgorge` - 'trace log querier for recommender system'
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
#ifndef DISGORGE_QUERY_HPP
#define DISGORGE_QUERY_HPP

#include <rapidjson/document.h>

#include <string>
#include <type_traits>

namespace disgorge {
enum Type : int {
  kNilType,
  kBetweenIntType,
  kBetweenFloatType,
  kBetweenStrType,
  kRightCompareIntType,
  kRightCompareFloatType,
  kRightCompareStrType,
  kLeftCompareIntType,
  kLeftCompareFloatType,
  kLeftCompareStrType,
  kRightLikeType,
  kLeftLikeType,
  kBinaryLikeType,
  kInArrayIntType,
  kInArrayFloatType,
  kInArrayStrType,
  kAndType,
  kOrType
};

enum Cmp : int {
  kError,
  kEqual,
  kNotEqual,
  kGreaterThan,
  kGreaterThanEqual,
  kLessThan,
  kLessThanEqual
};

template <class T>
static bool cmp(T a, T b, Cmp op) {
  switch (op) {
    case kEqual:
      return a == b;
    case kNotEqual:
      return a != b;
    case kGreaterThan:
      return a > b;
    case kGreaterThanEqual:
      return a >= b;
    case kLessThan:
      return a < b;
    case kLessThanEqual:
      return a <= b;
    default:
      return false;
  }
}

static bool cmp(const char *a, const char *b, Cmp op) {
  switch (op) {
    case kEqual:
      return strcmp(a, b) == 0;
    case kNotEqual:
      return strcmp(a, b) != 0;
    case kGreaterThan:
      return strcmp(a, b) > 0;
    case kGreaterThanEqual:
      return strcmp(a, b) >= 0;
    case kLessThan:
      return strcmp(a, b) < 0;
    case kLessThanEqual:
      return strcmp(a, b) <= 0;
    default:
      return false;
  }
}

static Cmp str2cmp(const char *s) {
  if (strcmp(s, "=") == 0 || strcmp(s, "==") == 0) {
    return kEqual;
  } else if (strcmp(s, "!=") || strcmp(s, "<>")) {
    return kNotEqual;
  } else if (strcmp(s, ">")) {
    return kGreaterThan;
  } else if (strcmp(s, ">=")) {
    return kGreaterThanEqual;
  } else if (strcmp(s, "<=")) {
    return kLessThanEqual;
  } else if (strcmp(s, "<")) {
    return kLessThan;
  }
  return kError;
}

template <class T>
static std::vector<T> parse_array(rapidjson::Value &array) {
  std::vector<T> result;
  if constexpr (std::is_same_v<T, int64_t>) {
    for (rapidjson::SizeType i = 0; i < array.Size(); i++) {
      result.push_back(array[i].GetInt64());
    }
  } else if constexpr (std::is_same_v<T, float>) {
    for (rapidjson::SizeType i = 0; i < array.Size(); i++) {
      result.push_back(array[i].GetFloat());
    }
  } else if constexpr (std::is_same_v<T, std::string>) {
    for (rapidjson::SizeType i = 0; i < array.Size(); i++) {
      result.emplace_back(array[i].GetString());
    }
  }
  return result;
}

class Boolean {
 public:
  Boolean() = default;
  virtual Type type() { return kNilType; }
  virtual ~Boolean() = default;
  virtual bool Exec(rapidjson::Document &d) = 0;
};

template <class T>
class Between : public Boolean {
 public:
  Between() = delete;
  Between(T lower, T upper, const char *col)
      : lower_(lower), upper_(upper), col_(col) {}
  virtual ~Between() = default;

  virtual Type type() {
    if constexpr (std::is_same_v<T, int64_t>) {
      return kBetweenIntType;
    } else if constexpr (std::is_same_v<T, float>) {
      return kBetweenFloatType;
    } else if constexpr (std::is_same_v<T, std::string>) {
      return kBetweenStrType;
    }
    return kNilType;
  }

  virtual bool Exec(rapidjson::Document &d) {
    if (!d.HasMember(col_.c_str())) {
      return false;
    }
    auto &c = d[col_.c_str()];

    if constexpr (std::is_same_v<T, int64_t>) {
      if (!c.IsInt64()) {
        return false;
      }
      auto v = c.GetInt64();
      return lower_ <= v && v <= upper_;
    } else if constexpr (std::is_same_v<T, float>) {
      if (!c.IsFloat()) {
        return false;
      }
      auto v = c.GetFloat();
      return lower_ <= v && v <= upper_;
    } else if constexpr (std::is_same_v<T, std::string>) {
      if (!c.IsString()) {
        return false;
      }
      auto v = c.GetString();
      return strcmp(lower_.c_str(), v) <= 0 && strcmp(v, upper_.c_str()) <= 0;
    }
    return false;
  }

 private:
  T lower_;
  T upper_;
  std::string col_;
};

template <class T>
class RightCompare : public Boolean {
 public:
  RightCompare() = delete;
  RightCompare(T left, const char *col, Cmp op)
      : left_(left), col_(col), op_(op) {}
  virtual ~RightCompare() = default;

  virtual Type type() {
    if constexpr (std::is_same_v<T, int64_t>) {
      return kRightCompareIntType;
    } else if constexpr (std::is_same_v<T, float>) {
      return kRightCompareFloatType;
    } else if constexpr (std::is_same_v<T, std::string>) {
      return kRightCompareStrType;
    }
    return kNilType;
  }

  virtual bool Exec(rapidjson::Document &d) {
    if (!d.HasMember(col_.c_str())) {
      return false;
    }
    auto &c = d[col_.c_str()];

    if constexpr (std::is_same_v<T, int64_t>) {
      if (!c.IsInt64()) {
        return false;
      }
      auto v = c.GetInt64();
      return cmp(left_, v, op_);
    } else if constexpr (std::is_same_v<T, float>) {
      if (!c.IsFloat()) {
        return false;
      }
      auto v = c.GetFloat();
      return cmp(left_, v, op_);
    } else if constexpr (std::is_same_v<T, std::string>) {
      if (!c.IsString()) {
        return false;
      }
      auto v = c.GetString();
      return cmp(left_.c_str(), v, op_);
    }
    return false;
  }

 private:
  T left_;
  std::string col_;
  Cmp op_;
};

template <class T>
class LeftCompare : public Boolean {
 public:
  LeftCompare() = delete;
  LeftCompare(T right, const char *col, Cmp op)
      : right_(right), col_(col), op_(op) {}
  virtual ~LeftCompare() = default;

  virtual Type type() {
    if constexpr (std::is_same_v<T, int64_t>) {
      return kLeftCompareIntType;
    } else if constexpr (std::is_same_v<T, float>) {
      return kLeftCompareFloatType;
    } else if constexpr (std::is_same_v<T, std::string>) {
      return kLeftCompareStrType;
    }
    return kNilType;
  }

  virtual bool Exec(rapidjson::Document &d) {
    if (!d.HasMember(col_.c_str())) {
      return false;
    }
    auto &c = d[col_.c_str()];

    if constexpr (std::is_same_v<T, int64_t>) {
      if (!c.IsInt64()) {
        return false;
      }
      auto v = c.GetInt64();
      return cmp(v, right_, op_);
    } else if constexpr (std::is_same_v<T, float>) {
      if (!c.IsFloat()) {
        return false;
      }
      auto v = c.GetFloat();
      return cmp(v, right_, op_);
    } else if constexpr (std::is_same_v<T, std::string>) {
      if (!c.IsString()) {
        return false;
      }
      auto v = c.GetString();
      return cmp(v, right_.c_str(), op_);
    }
    return false;
  }

 private:
  T right_;
  std::string col_;
  Cmp op_;
};

template <class T>
class InArray : public Boolean {
 public:
  InArray() = delete;
  InArray(std::vector<T> array, const char *col) : array_(array), col_(col) {}
  virtual ~InArray() = default;

  virtual Type type() {
    if constexpr (std::is_same_v<T, int64_t>) {
      return kInArrayIntType;
    } else if constexpr (std::is_same_v<T, float>) {
      return kInArrayFloatType;
    } else if constexpr (std::is_same_v<T, std::string>) {
      return kInArrayStrType;
    }
    return kNilType;
  }

  virtual bool Exec(rapidjson::Document &d) {
    if (!d.HasMember(col_.c_str())) {
      return false;
    }
    auto &c = d[col_.c_str()];

    if constexpr (std::is_same_v<T, int64_t>) {
      if (!c.IsInt64()) {
        return false;
      }
      auto v = c.GetInt64();
      for (size_t i = 0; i < array_.size(); i++) {
        if (array_[i] == v) {
          return true;
        }
      }
      return false;
    } else if constexpr (std::is_same_v<T, float>) {
      if (!c.IsFloat()) {
        return false;
      }
      auto v = c.GetFloat();
      for (size_t i = 0; i < array_.size(); i++) {
        if (array_[i] == v) {
          return true;
        }
      }
      return false;
    } else if constexpr (std::is_same_v<T, std::string>) {
      if (!c.IsString()) {
        return false;
      }
      auto v = c.GetString();
      for (size_t i = 0; i < array_.size(); i++) {
        if (array_[i] == v) {
          return true;
        }
      }
      return false;
    }
    return false;
  }

 private:
  std::vector<T> array_;
  std::string col_;
};

class LeftLike : public Boolean {
 public:
  LeftLike() = delete;
  LeftLike(const char *value, const char *col) : value_(value), col_(col) {}
  virtual ~LeftLike() = default;

  virtual Type type() { return kLeftLikeType; }

  virtual bool Exec(rapidjson::Document &d) {
    if (!d.HasMember(col_.c_str())) {
      return false;
    }
    auto &c = d[col_.c_str()];
    if (!c.IsString()) {
      return false;
    }
    std::string v = c.GetString();
    if (v.size() < value_.size()) {
      return false;
    }
    return v.substr(v.size() - value_.size()) == value_;
  }

 private:
  std::string value_;
  std::string col_;
};

class RightLike : public Boolean {
 public:
  RightLike() = delete;
  RightLike(const char *value, const char *col) : value_(value), col_(col) {}
  virtual ~RightLike() = default;

  virtual Type type() { return kRightLikeType; }

  virtual bool Exec(rapidjson::Document &d) {
    if (!d.HasMember(col_.c_str())) {
      return false;
    }
    auto &c = d[col_.c_str()];
    if (!c.IsString()) {
      return false;
    }
    std::string v = c.GetString();
    if (v.size() < value_.size()) {
      return false;
    }
    return v.substr(0, value_.size()) == value_;
  }

 private:
  std::string value_;
  std::string col_;
};

class BinaryLike : public Boolean {
 public:
  BinaryLike() = delete;
  BinaryLike(const char *value, const char *col) : value_(value), col_(col) {}
  virtual ~BinaryLike() = default;

  virtual Type type() { return kBinaryLikeType; }

  virtual bool Exec(rapidjson::Document &d) {
    if (!d.HasMember(col_.c_str())) {
      return false;
    }
    auto &c = d[col_.c_str()];
    if (!c.IsString()) {
      return false;
    }
    std::string v = c.GetString();
    if (v.size() < value_.size()) {
      return false;
    }
    return v.find(value_) != std::string::npos;
  }

 private:
  std::string value_;
  std::string col_;
};

class AndBoolean : public Boolean {
 public:
  AndBoolean() = delete;
  AndBoolean(std::shared_ptr<Boolean> left, std::shared_ptr<Boolean> right)
      : left_(left), right_(right) {}
  virtual ~AndBoolean() = default;

  virtual Type type() { return kAndType; }

  virtual bool Exec(rapidjson::Document &d) {
    return left_->Exec(d) && right_->Exec(d);
  }

 private:
  std::shared_ptr<Boolean> left_;
  std::shared_ptr<Boolean> right_;
};

class OrBoolean : public Boolean {
 public:
  OrBoolean() = delete;
  OrBoolean(std::shared_ptr<Boolean> left, std::shared_ptr<Boolean> right)
      : left_(left), right_(right) {}
  virtual ~OrBoolean() = default;

  virtual Type type() { return kOrType; }

  virtual bool Exec(rapidjson::Document &d) {
    return left_->Exec(d) || right_->Exec(d);
  }

 private:
  std::shared_ptr<Boolean> left_;
  std::shared_ptr<Boolean> right_;
};

static std::shared_ptr<Boolean> parse_from_value(rapidjson::Value &document) {
  if (!document.HasMember("type")) {
    return nullptr;
  }
  rapidjson::Value &v = document["type"];

  int type = v.GetInt();
  switch (type) {
    case kBetweenIntType:
      return std::make_shared<Between<int64_t>>(document["lower"].GetInt64(),
                                                document["upper"].GetInt64(),
                                                document["column"].GetString());

    case kBetweenFloatType:
      return std::make_shared<Between<float>>(document["lower"].GetFloat(),
                                              document["upper"].GetFloat(),
                                              document["column"].GetString());
    case kBetweenStrType:
      return std::make_shared<Between<std::string>>(
          document["lower"].GetString(), document["upper"].GetString(),
          document["column"].GetString());

    case kRightCompareIntType:
      return std::make_shared<RightCompare<int64_t>>(
          document["left"].GetInt64(), document["column"].GetString(),
          str2cmp(document["op"].GetString()));
    case kRightCompareFloatType:
      return std::make_shared<RightCompare<float>>(
          document["left"].GetFloat(), document["column"].GetString(),
          str2cmp(document["op"].GetString()));
    case kRightCompareStrType:
      return std::make_shared<RightCompare<std::string>>(
          document["left"].GetString(), document["column"].GetString(),
          str2cmp(document["op"].GetString()));
    case kLeftCompareIntType:
      return std::make_shared<LeftCompare<int64_t>>(
          document["right"].GetInt64(), document["column"].GetString(),
          str2cmp(document["op"].GetString()));
    case kLeftCompareFloatType:
      return std::make_shared<LeftCompare<float>>(
          document["right"].GetFloat(), document["column"].GetString(),
          str2cmp(document["op"].GetString()));
    case kLeftCompareStrType:
      return std::make_shared<LeftCompare<std::string>>(
          document["right"].GetString(), document["column"].GetString(),
          str2cmp(document["op"].GetString()));
    case kRightLikeType:
      return std::make_shared<RightLike>(document["value"].GetString(),
                                         document["column"].GetString());
    case kLeftLikeType:
      return std::make_shared<LeftLike>(document["value"].GetString(),
                                        document["column"].GetString());
    case kBinaryLikeType:
      return std::make_shared<BinaryLike>(document["value"].GetString(),
                                          document["column"].GetString());
    case kInArrayIntType:
      return std::make_shared<InArray<int64_t>>(
          parse_array<int64_t>(document["array"]),
          document["column"].GetString());
    case kInArrayFloatType:
      return std::make_shared<InArray<float>>(
          parse_array<float>(document["array"]),
          document["column"].GetString());
    case kInArrayStrType:
      return std::make_shared<InArray<std::string>>(
          parse_array<std::string>(document["array"]),
          document["column"].GetString());
    case kAndType:

      return std::make_shared<AndBoolean>(parse_from_value(document["left"]),
                                          parse_from_value(document["right"]));
    case kOrType:
      std::make_shared<OrBoolean>(parse_from_value(document["left"]),
                                  parse_from_value(document["right"]));
    default:
      return nullptr;
  }
}

static std::shared_ptr<Boolean> parse(const char *json, size_t len) {
  rapidjson::Document document;
  document.Parse(json, len);
  return parse_from_value(document);
}

};  // namespace disgorge

#endif  // DISGORGE_QUERY_HPP