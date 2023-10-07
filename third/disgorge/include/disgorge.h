//
// `disgorge` - 'trace log querier for recommender system'
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

#ifndef DISGORGE_H
#define DISGORGE_H

#ifdef __cplusplus
extern "C" {
#endif

void *disgorge_open(const char* dir, const char* secondary_dir);
void disgorge_close(void *ins);

void *disgorge_scan(void *ins, void *query, unsigned long long qlen,
                    void *start, unsigned long long slen, void *end,
                    unsigned long long elen, long long max_count);

int disgorge_check_query(void *query, unsigned long long len);

unsigned long long disgorge_response_size(void *resp);
int disgorge_response_more(void *resp);
const char *disgorge_response_lastkey(void *resp);
const char *disgorge_response_value(void *resp, unsigned long long index);
void disgorge_del_response(void *resp);

#ifdef __cplusplus
} /* end extern "C"*/
#endif

#endif  // DISGORGE_H