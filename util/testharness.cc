// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/testharness.h"

#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <string>

namespace leveldb {
namespace test {

namespace {
struct Test {
    const char* base;
    const char* name;
    void (*func)();
};
std::vector<Test>* tests;
} // namespace

bool RegisterTest(const char* base, const char* name, void (*func)()) {
    if (tests == NULL) {
        tests = new std::vector<Test>;
    }
    Test t;
    t.base = base;
    t.name = name;
    t.func = func;
    tests->push_back(t);
    return true;
}

int RunAllTests() {
    const char* matcher = getenv("LEVELDB_TESTS");
    int num = 0;
    // std::vector<std::string> target = {"MTReorder_1","MTReorder_2","MTReorder_3"};
    // std::vector<std::string> target = {"MTMW_1", "MTMW_2", "MTMW_3", "MTMW_4", "MTMW_5", "MTMW_6"};
    // std::vector<std::string> target = {"MTMW_5"};
    std::vector<std::string> target = {"MTMW_5"};
    if (tests != NULL) {
        for (size_t i = 0; i < tests->size(); i++) {
            const Test& t = (*tests)[i];
            if (matcher != NULL) {
                std::string name = t.base;
                name.push_back('.');
                name.append(t.name);
                if (strstr(name.c_str(), matcher) == NULL) {
                    continue;
                }
            }
            for (size_t j = 0; j < target.size(); j++) {
                if (strcmp(t.name, target[j].c_str()) == 0) {
                    std::cout << "test name " << t.name << "\n";
                    (*t.func)();
                    ++num;
                    fprintf(stderr, "%lu ==== Test %s.%s Success!\n", i, t.base, t.name);
                }
            }
        }
    }
    fprintf(stderr, "==== PASSED %d tests\n", num);
    return 0;
}

std::string TmpDir() {
    std::string dir;
    Status s = Env::Default()->GetTestDirectory(&dir);
    ASSERT_TRUE(s.ok()) << s.ToString();
    return dir;
}

int RandomSeed() {
    const char* env = getenv("TEST_RANDOM_SEED");
    int result = (env != NULL ? atoi(env) : 301);
    if (result <= 0) {
        result = 301;
    }
    return result;
}

} // namespace test
} // namespace leveldb
