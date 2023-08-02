//
// `swallow` - 'trace log storage for recommender system'
// Copyright (C) 2019 - present timepi <timepi123@gmail.com>
// `Damo-Embedding` is provided under: GNU Affero General Public License
// (AGPL3.0) https://www.gnu.org/licenses/agpl-3.0.html unless stated otherwise.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//

#ifndef SWALLOW_H
#define SWALLOW_H

#ifdef __cplusplus
extern "C" {
#endif

void *swallow_open(void *dir, unsigned long long len, void *secondary,
                   unsigned long long slen);
void swallow_close(void *ins);

void *swallow_scan(void *ins, void *quyery, unsigned long long qlen,
                   void *start, unsigned long long slen, void *end,
                   unsigned long long elen);

unsigned long long swallow_response_size(void *resp);
bool swallow_response_more(void *resp);
const char *swallow_response_lastkey(void *resp);
const char *swallow_response_value(void *resp, unsigned long index);
void swallow_del_response(void *resp);

#ifdef __cplusplus
} /* end extern "C"*/
#endif

#endif  // SWALLOW_H