//
// `swallow` - 'trace log querier for recommender system'
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

#include <iostream>

#include "query.hpp"

void test_query() {
  std::string str =
      "{\"type\": 1, \"lower\": 5, \"upper\": 9, \"column\": \"val\"}";
  auto expr = query::parse(str.c_str(), str.size());
  std::string doc = "{\"val\": 4}";
  std::string doc1 = "{\"val\": {\"key\":[1,2,3]}}";
  json d1 = json::parse(doc1);
  json d = json::parse(doc);
  auto fields = query::extract_fields("val.key.#2");
  std::cout << query::get(d1, fields)->get<int>() << std::endl;
  std::cout << expr->Exec(d) << std::endl;
}

void print(std::shared_ptr<std::vector<query::Field>> fields) {
  for (auto field : *fields) {
    std::visit([](auto &&arg) { std::cout << arg << std::endl; }, field);
  }
}

void test_extract_field() {
  auto e = query::extract_fields("#123");
  print(e);

  e = query::extract_fields("aaaaa.#123.456");
  print(e);

  e = query::extract_fields("aaa\"aa.#123.ab#c\\.6");
  print(e);

  e = query::extract_fields("\\#abc");
  print(e);

  e = query::extract_fields("123");
  print(e);
}

int main() {
  test_query();
  test_extract_field();
  return 0;
}