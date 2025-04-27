#ifndef TEST_H_
#define TEST_H_

#include "input.h"

#include "debug.h"

#include <string>
#include <map>
#include <set>
#include <algorithm>

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_COPPER_NODE

namespace UT {
template<typename UTClass>
class TestBase {
public:
    TestBase(int argc, char *argv[], const std::set<std::string>& default_black_list) : _argc(argc), _argv(argv) {
        num_runs = Parse_Args(_argc, _argv, test.all_tests, test.test_priority, tests_to_run, default_black_list);
        
        std::sort(tests_to_run.begin(), tests_to_run.end(), [this](const std::string& a, const std::string& b) {
            return test.test_priority[a] < test.test_priority[b];
        });
    }

    int Run() {
        
        if (num_runs == 0) {
            return 1;
        }

        for (size_t i = 0; i < num_runs; ++i) {
            fprintf(stderr, "Starting round %lu/%lu...\n\n", i+1, num_runs);
            test.Init(i + 1);
            fprintf(stderr, "Inited the tests.\n\n");
            
            for (std::string test_name : tests_to_run) {
                fprintf(stderr, "Searching for Test " _COLORF_CYAN "%s" _COLORF_RESET "...\n", test_name.c_str());
                auto it = test.tests.find(test_name);
                if (it != test.tests.end()) {
                    fprintf(stderr, "Test found.\n");
                    if ((test.*(it->second))()) {
                        fprintf(stderr, _COLORF_GREEN "Test " _COLORF_RESET _COLORF_CYAN "%s" _COLORF_RESET _COLORF_GREEN
                                " ran successfully!" _COLORF_RESET "\n\n", test_name.c_str());
                    }
                    else {
                        fprintf(stderr, _COLORF_RED "Test " _COLORF_RESET _COLORF_CYAN " %s " _COLORF_RESET _COLORF_RED 
                                " failed!" _COLORF_RESET "\n\n", test_name.c_str());
                    }
                }  
                else {
                    fprintf(stderr, _COLORF_RED "Error: Test " _COLORF_RESET _COLORF_CYAN "%s" _COLORF_RESET _COLORF_RED 
                            " not found!\n\n" _COLORF_RESET, test_name.c_str());
                }
            }

            test.Destroy();
            fprintf(stderr, "End of round %lu/%lu\n\n", i+1, num_runs);
        }

        return 0;
    }
protected:
    UTClass test;
    int _argc;
    char **_argv;
    std::vector<std::string> tests_to_run;
    size_t try_count = 0;
    size_t num_runs = 0;
};

};

// Todo update run_ut so that we can use the same regexs and white/black lists across all ut files

#endif