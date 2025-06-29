#pragma once

#include "parallel_hashmap/phmap.h"

namespace NVMDB {
template<class K, class V, class Mutex=phmap::NullMutex, size_t N=4>
using MyFlatHashMap = phmap::parallel_flat_hash_map<K, V,
                                                    phmap::priv::hash_default_hash<K>,
                                                    phmap::priv::hash_default_eq<K>,
                                                    phmap::priv::Allocator<phmap::priv::Pair<const K, V>>,
                                                    N, Mutex>;

template<class K, class V, class Mutex=phmap::NullMutex, size_t N=4>
using MyNodeHashMap = phmap::parallel_node_hash_map<K, V,
                                                    phmap::priv::hash_default_hash<K>,
                                                    phmap::priv::hash_default_eq<K>,
                                                    phmap::priv::Allocator<phmap::priv::Pair<const K, V>>,
                                                    N, Mutex>;
}

