#include "swallow.h"

#include "instance.hpp"

void *swallow_open(void *dir, unsigned long long len, void *secondary,
                   unsigned long long slen) {
  swallow::Instance *instance = nullptr;
  try {
    if (secondary == nullptr || slen == 0) {
      instance = new swallow::Instance(std::string((char *)dir, len));
    } else {
      instance = new swallow::Instance(std::string((char *)dir, len),
                                       std::string((char *)secondary, slen));
    }
    return instance;
  } catch (...) {
    return nullptr;
  }
}

void swallow_close(void *ins) {
  if (ins == nullptr) {
    return;
  }
  swallow::Instance *instance = (swallow::Instance *)ins;
  delete instance;
}

void *swallow_scan(void *ins, void *query, unsigned long long qlen, void *start,
                   unsigned long long slen, void *end,
                   unsigned long long elen) {
  if (ins == nullptr) {
    return nullptr;
  }
  swallow::Instance *instance = (swallow::Instance *)ins;
  return instance->scan({(char *)query, qlen}, {(char *)start, slen},
                        {(char *)end, elen});
}

unsigned long long swallow_response_size(void *resp) {
  if (resp == nullptr) {
    return 0;
  }
  swallow::Response *r = (swallow::Response *)resp;
  return r->size();
}

bool swallow_response_more(void *resp) {
  if (resp == nullptr) {
    return 0;
  }
  swallow::Response *r = (swallow::Response *)resp;
  return r->more();
}

const char *swallow_response_lastkey(void *resp) {
  if (resp == nullptr) {
    return 0;
  }
  swallow::Response *r = (swallow::Response *)resp;
  return r->lastkey().c_str();
}
const char *swallow_response_value(void *resp, unsigned long index) {
  if (resp == nullptr) {
    return nullptr;
  }
  swallow::Response *r = (swallow::Response *)resp;
  return r->operator[](index).c_str();
}

void swallow_del_response(void *resp) {
  if (resp == nullptr) {
    return;
  }
  swallow::Response *r = (swallow::Response *)resp;
  delete r;
}