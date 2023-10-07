#include "disgorge.h"

#include "instance.hpp"

void *disgorge_open(const char* dir, const char* secondary_dir) {
  disgorge::Instance *instance = nullptr;
  try {
    if (secondary_dir == nullptr || strlen(secondary_dir) == 0) {
      instance = new disgorge::Instance(std::string((char *)dir, strlen(dir)));
    } else {
      instance = new disgorge::Instance(std::string((char *)dir, strlen(dir)),
                                        std::string((char *)secondary_dir, strlen(secondary_dir)));
    }
    return instance;
  } catch (...) {
    return nullptr;
  }
}

void disgorge_close(void *ins) {
  if (ins == nullptr) {
    return;
  }
  disgorge::Instance *instance = (disgorge::Instance *)ins;
  delete instance;
}

void *disgorge_scan(void *ins, void *query, unsigned long long qlen,
                    void *start, unsigned long long slen, void *end,
                    unsigned long long elen, long long max_count) {
  if (ins == nullptr) {
    return nullptr;
  }
  disgorge::Instance *instance = (disgorge::Instance *)ins;
  return instance->scan({(char *)query, qlen}, {(char *)start, slen},
                        {(char *)end, elen}, max_count);
}

unsigned long long disgorge_response_size(void *resp) {
  if (resp == nullptr) {
    return 0;
  }
  disgorge::Response *r = (disgorge::Response *)resp;
  return r->size();
}

int disgorge_response_more(void *resp) {
  if (resp == nullptr) {
    return 0;
  }
  disgorge::Response *r = (disgorge::Response *)resp;
  return r->more();
}

int disgorge_check_query(void *query, unsigned long long len) {
  if (query == nullptr || len == 0) {
    return 0;
  }
  auto expr = query::parse((const char *)query, len);
  if (expr == nullptr) {
    return 0;
  }
  return 1;
}

const char *disgorge_response_lastkey(void *resp) {
  if (resp == nullptr) {
    return 0;
  }
  disgorge::Response *r = (disgorge::Response *)resp;
  return r->lastkey().c_str();
}
const char *disgorge_response_value(void *resp, unsigned long long index) {
  if (resp == nullptr) {
    return nullptr;
  }
  disgorge::Response *r = (disgorge::Response *)resp;
  return r->operator[](index).c_str();
}

void disgorge_del_response(void *resp) {
  if (resp == nullptr) {
    return;
  }
  disgorge::Response *r = (disgorge::Response *)resp;
  delete r;
}