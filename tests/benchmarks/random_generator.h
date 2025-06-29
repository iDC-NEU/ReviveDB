#ifndef NVMDB_RANDOM_GENERATOR_H
#define NVMDB_RANDOM_GENERATOR_H

#include "common/nvm_utils.h"

namespace NVMDB {
class RandomGenerator {
    static constexpr int MAX_SEED_LEN = 3;

    unsigned short seed[MAX_SEED_LEN];
    unsigned short seed2[MAX_SEED_LEN];
    unsigned short inital[MAX_SEED_LEN];
    unsigned short inital2[MAX_SEED_LEN];

public:
    RandomGenerator() {
        for (int i = 0; i < MAX_SEED_LEN; i++) {
            inital[i] = seed[i] = random();
            inital2[i] = seed2[i] = random();
        }
    }

    int randomInt() {
        return nrand48(seed) ^ nrand48(seed2);
    }

    double randomDouble() {
        return erand48(seed) * erand48(seed2);
    }

    void setSeed(unsigned short newseed[MAX_SEED_LEN]) {
        constexpr size_t memLen = sizeof(unsigned short) * MAX_SEED_LEN;
        int ret = memcpy_s(seed, memLen, newseed, memLen);
        SecureRetCheck(ret);
    }

    void reset() {
        constexpr size_t memLen = sizeof(unsigned short) * MAX_SEED_LEN;
        int ret = memcpy_s(seed, memLen, inital, memLen);
        SecureRetCheck(ret);
        ret = memcpy_s(seed2, memLen, inital2, memLen);
        SecureRetCheck(ret);
    }

    long long Next() {
        return randomInt();
    }
} __attribute__((aligned(64)));

}

#endif  // OPENGAUSS_RANDOM_GENERATOR_H
