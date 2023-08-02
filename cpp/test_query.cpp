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

#include <iostream>

#include "query.hpp"

void test_query() {
  std::string str =
      "{\"type\": 1, \"lower\": 5, \"upper\": 9, \"column\": \"val\"}";
  auto expr = swallow_query::parse(str.c_str(), str.size());
  std::string doc = "{\"val\": 4}";
  rapidjson::Document d;
  d.Parse(doc.c_str());
  std::cout << d["val"].GetInt64() << std::endl;
  std::cout << expr->Exec(d) << std::endl;
}

int main() {
  test_query();
  return 0;
}