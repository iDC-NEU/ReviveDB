#pragma once
#include <cstdint>
extern "C" {
struct Dash;
void init(const char *key);
void *dash_create(void);
int dash_insert(void *dash, uint64_t key, uint64_t value);
int dash_update(void *dash, uint64_t key, uint64_t value);
uint64_t dash_remove(void *dash, uint64_t key);
uint64_t dash_find(void *dash, uint64_t key);
}