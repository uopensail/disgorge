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

#include <string>
#include <type_traits>
#include <variant>

#include "json.hpp"
using json = nlohmann::json;

namespace query {
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

using Field = std::variant<int, std::string>;

static void check_empty(const std::string &s) {
  if (s.size() == 0) {
    throw std::runtime_error("syntax error: empty string");
  }
}

static std::shared_ptr<std::vector<Field>> extract_fields(
    const std::string &path) {
  auto ret = std::make_shared<std::vector<Field>>();
  std::string tmp = "";
  bool isint = false;
  size_t i = 0, size = path.size();

  while (i < size) {
    if (path[i] == '\\') {
      ++i;
      if (i == size) {
        tmp.push_back('\\');
        break;
      }

      if (path[i] == '.' || path[i] == '"' || path[i] == '\'' ||
          path[i] == '#') {
        tmp.push_back(path[i]);
        ++i;
      } else {
        throw std::runtime_error(
            "escape character only support: `\"`, `'`, `.`, `\\`, `#`");
      }
    } else if (path[i] == '.') {
      check_empty(tmp);
      if (isint) {
        ret->push_back(std::stoi(tmp));
        isint = false;
      } else {
        ret->push_back(tmp);
      }
      tmp = "";
      ++i;
    } else if (path[i] == '#') {
      if (tmp.size() > 0) {
        tmp.push_back(path[i]);
      } else {
        if (!isint) {
          isint = true;
        } else {
          throw std::runtime_error("syntax error");
        }
      }
      ++i;
    } else {
      tmp.push_back(path[i]);
      ++i;
    }
  }
  check_empty(tmp);

  if (isint) {
    ret->push_back(std::stoi(tmp));
  } else {
    ret->push_back(tmp);
  }

  return ret;
}

template <typename T>
bool cmp(T a, T b, Cmp op) {
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

template <>
bool cmp(const std::string &a, const std::string &b, Cmp op) {
  switch (op) {
    case kEqual:
      return strcmp(a.c_str(), b.c_str()) == 0;
    case kNotEqual:
      return strcmp(a.c_str(), b.c_str()) != 0;
    case kGreaterThan:
      return strcmp(a.c_str(), b.c_str()) > 0;
    case kGreaterThanEqual:
      return strcmp(a.c_str(), b.c_str()) >= 0;
    case kLessThan:
      return strcmp(a.c_str(), b.c_str()) < 0;
    case kLessThanEqual:
      return strcmp(a.c_str(), b.c_str()) <= 0;
    default:
      return false;
  }
}

static Cmp str2cmp(const std::string &s) {
  if (strcmp(s.c_str(), "=") == 0 || strcmp(s.c_str(), "==") == 0) {
    return kEqual;
  } else if (strcmp(s.c_str(), "!=") || strcmp(s.c_str(), "<>")) {
    return kNotEqual;
  } else if (strcmp(s.c_str(), ">")) {
    return kGreaterThan;
  } else if (strcmp(s.c_str(), ">=")) {
    return kGreaterThanEqual;
  } else if (strcmp(s.c_str(), "<=")) {
    return kLessThanEqual;
  } else if (strcmp(s.c_str(), "<")) {
    return kLessThan;
  }
  return kError;
}

const json *get(const json &d,
                const std::shared_ptr<std::vector<Field>> &fields) {
  const json *tmp = &d;
  for (auto &field : *fields) {
    if (int *p = std::get_if<int>(&field)) {
      if (!tmp->is_array()) {
        return nullptr;
      }
      size_t size = tmp->size();
      if (*p < 0 || size <= *p) {
        return nullptr;
      }
      tmp = &(tmp->at(*p));
    } else if (std::string *p = std::get_if<std::string>(&field)) {
      if (!tmp->contains(*p)) {
        return nullptr;
      }
      tmp = &((*tmp)[*p]);
    } else {
      return nullptr;
    }
  }
  return tmp;
}

class Boolean {
 public:
  Boolean() = default;
  virtual Type type() { return kNilType; }
  virtual ~Boolean() = default;
  virtual bool Exec(const json &d) = 0;
};

template <class T>
class Between : public Boolean {
 public:
  Between() = delete;
  Between(T lower, T upper, const std::string &col)
      : lower_(lower), upper_(upper), col_(col) {
    fields_ = extract_fields(col_);
  }
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

  virtual bool Exec(const json &d) {
    const json *ptr = get(d, fields_);
    if (ptr == nullptr) {
      return false;
    }
    const json &c = *ptr;
    if constexpr (std::is_same_v<T, int64_t>) {
      if (!(c.type() == json::value_t::number_integer)) {
        return false;
      }
      const int64_t &v = c.get<int64_t>();
      return lower_ <= v && v <= upper_;
    } else if constexpr (std::is_same_v<T, float>) {
      if (!(c.type() == json::value_t::number_float)) {
        return false;
      }
      const float &v = c.get<float>();
      return lower_ <= v && v <= upper_;
    } else if constexpr (std::is_same_v<T, std::string>) {
      if (!(c.type() == json::value_t::string)) {
        return false;
      }
      const std::string &v = c.get<std::string>();
      return strcmp(lower_.c_str(), v.c_str()) <= 0 &&
             strcmp(v.c_str(), upper_.c_str()) <= 0;
    }
    return false;
  }

 private:
  T lower_;
  T upper_;
  std::string col_;
  std::shared_ptr<std::vector<Field>> fields_;
};

template <class T>
class RightCompare : public Boolean {
 public:
  RightCompare() = delete;
  RightCompare(T left, const std::string &col, Cmp op)
      : left_(left), col_(col), op_(op) {
    fields_ = extract_fields(col_);
  }
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

  virtual bool Exec(const json &d) {
    const json *ptr = get(d, fields_);
    if (ptr == nullptr) {
      return false;
    }
    const json &c = *ptr;
    if constexpr (std::is_same_v<T, int64_t>) {
      if (!(c.type() == json::value_t::number_integer)) {
        return false;
      }
      auto v = c.get<int64_t>();
      return cmp(left_, v, op_);
    } else if constexpr (std::is_same_v<T, float>) {
      if (!(c.type() == json::value_t::number_float)) {
        return false;
      }
      auto v = c.get<float>();
      return cmp(left_, v, op_);
    } else if constexpr (std::is_same_v<T, std::string>) {
      if (!(c.type() == json::value_t::string)) {
        return false;
      }
      const std::string &v = c.get<std::string>();
      return cmp(left_, v, op_);
    }
    return false;
  }

 private:
  T left_;
  std::string col_;
  Cmp op_;
  std::shared_ptr<std::vector<Field>> fields_;
};

template <class T>
class LeftCompare : public Boolean {
 public:
  LeftCompare() = delete;
  LeftCompare(T right, const std::string &col, Cmp op)
      : right_(right), col_(col), op_(op) {
    fields_ = extract_fields(col_);
  }
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

  virtual bool Exec(const json &d) {
    const json *ptr = get(d, fields_);
    if (ptr == nullptr) {
      return false;
    }
    const json &c = *ptr;
    if constexpr (std::is_same_v<T, int64_t>) {
      if (!(c.type() == json::value_t::number_integer)) {
        return false;
      }
      auto v = c.get<int64_t>();
      return cmp(v, right_, op_);
    } else if constexpr (std::is_same_v<T, float>) {
      if (!(c.type() == json::value_t::number_float)) {
        return false;
      }
      auto v = c.get<float>();
      return cmp(v, right_, op_);
    } else if constexpr (std::is_same_v<T, std::string>) {
      if (!(c.type() == json::value_t::string)) {
        return false;
      }
      const std::string &v = c.get<std::string>();
      return cmp(v, right_, op_);
    }
    return false;
  }

 private:
  T right_;
  std::string col_;
  Cmp op_;
  std::shared_ptr<std::vector<Field>> fields_;
};

template <class T>
class InArray : public Boolean {
 public:
  InArray() = delete;
  InArray(std::vector<T> array, const std::string &col)
      : array_(array), col_(col) {
    fields_ = extract_fields(col_);
  }
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

  virtual bool Exec(const json &d) {
    const json *ptr = get(d, fields_);
    if (ptr == nullptr) {
      return false;
    }
    const json &c = *ptr;
    if constexpr (std::is_same_v<T, int64_t>) {
      if (!(c.type() == json::value_t::number_integer)) {
        return false;
      }
      auto v = c.get<int64_t>();
      for (size_t i = 0; i < array_.size(); i++) {
        if (array_[i] == v) {
          return true;
        }
      }
      return false;
    } else if constexpr (std::is_same_v<T, float>) {
      if (!(c.type() == json::value_t::number_float)) {
        return false;
      }
      auto v = c.get<float>();
      for (size_t i = 0; i < array_.size(); i++) {
        if (array_[i] == v) {
          return true;
        }
      }
      return false;
    } else if constexpr (std::is_same_v<T, std::string>) {
      if (!(c.type() == json::value_t::string)) {
        return false;
      }
      const std::string &v = c.get<std::string>();
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
  std::shared_ptr<std::vector<Field>> fields_;
};

class LeftLike : public Boolean {
 public:
  LeftLike() = delete;
  LeftLike(const std::string &value, const std::string &col)
      : value_(value), col_(col) {
    fields_ = extract_fields(col_);
  }
  virtual ~LeftLike() = default;

  virtual Type type() { return kLeftLikeType; }

  virtual bool Exec(const json &d) {
    const json *ptr = get(d, fields_);
    if (ptr == nullptr) {
      return false;
    }
    const json &c = *ptr;
    if (!(c.type() == json::value_t::string)) {
      return false;
    }
    const std::string &v = c.get<std::string>();
    if (v.size() < value_.size()) {
      return false;
    }
    return v.substr(v.size() - value_.size()) == value_;
  }

 private:
  std::string value_;
  std::string col_;
  std::shared_ptr<std::vector<Field>> fields_;
};

class RightLike : public Boolean {
 public:
  RightLike() = delete;
  RightLike(const std::string &value, const std::string &col)
      : value_(value), col_(col) {
    fields_ = extract_fields(col_);
  }
  virtual ~RightLike() = default;

  virtual Type type() { return kRightLikeType; }

  virtual bool Exec(const json &d) {
    const json *ptr = get(d, fields_);
    if (ptr == nullptr) {
      return false;
    }
    const json &c = *ptr;
    if (!(c.type() == json::value_t::string)) {
      return false;
    }
    const std::string &v = c.get<std::string>();
    if (v.size() < value_.size()) {
      return false;
    }
    return v.substr(0, value_.size()) == value_;
  }

 private:
  std::string value_;
  std::string col_;
  std::shared_ptr<std::vector<Field>> fields_;
};

class BinaryLike : public Boolean {
 public:
  BinaryLike() = delete;
  BinaryLike(const std::string &value, const std::string &col)
      : value_(value), col_(col) {
    fields_ = extract_fields(col_);
  }
  virtual ~BinaryLike() = default;

  virtual Type type() { return kBinaryLikeType; }

  virtual bool Exec(const json &d) {
    const json *ptr = get(d, fields_);
    if (ptr == nullptr) {
      return false;
    }
    const json &c = *ptr;

    if (!(c.type() == json::value_t::string)) {
      return false;
    }
    const std::string &v = c.get<std::string>();
    if (v.size() < value_.size()) {
      return false;
    }
    return v.find(value_) != std::string::npos;
  }

 private:
  std::string value_;
  std::string col_;
  std::shared_ptr<std::vector<Field>> fields_;
};

class AndBoolean : public Boolean {
 public:
  AndBoolean() = delete;
  AndBoolean(std::shared_ptr<Boolean> left, std::shared_ptr<Boolean> right)
      : left_(left), right_(right) {}
  virtual ~AndBoolean() = default;
  virtual Type type() { return kAndType; }
  virtual bool Exec(const json &d) { return left_->Exec(d) && right_->Exec(d); }

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
  virtual bool Exec(const json &d) { return left_->Exec(d) || right_->Exec(d); }

 private:
  std::shared_ptr<Boolean> left_;
  std::shared_ptr<Boolean> right_;
};

static std::shared_ptr<Boolean> parse_from_value(const json &document) {
  if (!document.contains("type")) {
    return nullptr;
  }
  auto &v = document["type"];
  int type = v.get<int>();
  switch (type) {
    case kBetweenIntType:
      return std::make_shared<Between<int64_t>>(
          document["lower"].get<int64_t>(), document["upper"].get<int64_t>(),
          document["column"].get<std::string>());

    case kBetweenFloatType:
      return std::make_shared<Between<float>>(
          document["lower"].get<float>(), document["upper"].get<float>(),
          document["column"].get<std::string>());
    case kBetweenStrType:
      return std::make_shared<Between<std::string>>(
          document["lower"].get<std::string>(),
          document["upper"].get<std::string>(),
          document["column"].get<std::string>());
    case kRightCompareIntType:
      return std::make_shared<RightCompare<int64_t>>(
          document["left"].get<int64_t>(),
          document["column"].get<std::string>(),
          str2cmp(document["op"].get<std::string>()));
    case kRightCompareFloatType:
      return std::make_shared<RightCompare<float>>(
          document["left"].get<float>(), document["column"].get<std::string>(),
          str2cmp(document["op"].get<std::string>()));
    case kRightCompareStrType:
      return std::make_shared<RightCompare<std::string>>(
          document["left"].get<std::string>(),
          document["column"].get<std::string>(),
          str2cmp(document["op"].get<std::string>()));
    case kLeftCompareIntType:
      return std::make_shared<LeftCompare<int64_t>>(
          document["right"].get<int64_t>(),
          document["column"].get<std::string>(),
          str2cmp(document["op"].get<std::string>()));
    case kLeftCompareFloatType:
      return std::make_shared<LeftCompare<float>>(
          document["right"].get<float>(), document["column"].get<std::string>(),
          str2cmp(document["op"].get<std::string>()));
    case kLeftCompareStrType:
      return std::make_shared<LeftCompare<std::string>>(
          document["right"].get<std::string>(),
          document["column"].get<std::string>(),
          str2cmp(document["op"].get<std::string>()));
    case kRightLikeType:
      return std::make_shared<RightLike>(document["value"].get<std::string>(),
                                         document["column"].get<std::string>());
    case kLeftLikeType:
      return std::make_shared<LeftLike>(document["value"].get<std::string>(),
                                        document["column"].get<std::string>());
    case kBinaryLikeType:
      return std::make_shared<BinaryLike>(
          document["value"].get<std::string>(),
          document["column"].get<std::string>());
    case kInArrayIntType:
      return std::make_shared<InArray<int64_t>>(
          document["array"].get<std::vector<int64_t>>(),
          document["column"].get<std::string>());
    case kInArrayFloatType:
      return std::make_shared<InArray<float>>(
          document["array"].get<std::vector<float>>(),
          document["column"].get<std::string>());
    case kInArrayStrType:
      return std::make_shared<InArray<std::string>>(
          document["array"].get<std::vector<std::string>>(),
          document["column"].get<std::string>());
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

static std::shared_ptr<Boolean> parse(const char *data, size_t len) {
  const json &document = json::parse(std::string_view{data, len});
  return parse_from_value(document);
}

};  // namespace query

#endif  // DISGORGE_QUERY_HPP