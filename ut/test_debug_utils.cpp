#include "debug.h"

#include "input.h"

#include <string>
#include <map>
#include <set>
#include <algorithm>

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_COPPER_NODE

class Test {
public:
    Test() {
        tests["test_debug_util::level_test"] = Test::level_test;
        tests["test_debug_util::tag_test"] = Test::tag_test;
        tests["test_debug_util::level_tag_comb_test"] = Test::level_tag_comb_test;
        tests["test_debug_util::error_assert_test"] = Test::error_assert_test;
        tests["test_debug_util::error_assert_tag_test"] = Test::error_assert_tag_test;
        tests["test_debug_util::fatal_assert_tag_test"] = Test::fatal_assert_tag_test;
        tests["test_debug_util::fatal_assert_test"] = Test::fatal_assert_test;
        tests["test_debug_util::panic_tag_comb_test"] = Test::panic_tag_comb_test;
        tests["test_debug_util::panic_test"] = Test::panic_test;

        test_priority["test_debug_util::level_test"] = 0;
        test_priority["test_debug_util::tag_test"] = 1;
        test_priority["test_debug_util::level_tag_comb_test"] = 2;
        test_priority["test_debug_util::error_assert_test"] = 3;
        test_priority["test_debug_util::error_assert_tag_test"] = 4;
        test_priority["test_debug_util::fatal_assert_tag_test"] = 5;
        test_priority["test_debug_util::fatal_assert_test"] = 6;
        test_priority["test_debug_util::panic_tag_comb_test"] = 7;
        test_priority["test_debug_util::panic_test"] = 8;

        all_tests.insert("test_debug_util::level_test");
        all_tests.insert("test_debug_util::tag_test");
        all_tests.insert("test_debug_util::level_tag_comb_test");
        all_tests.insert("test_debug_util::error_assert_test");
        all_tests.insert("test_debug_util::error_assert_tag_test");
        all_tests.insert("test_debug_util::fatal_assert_tag_test");
        all_tests.insert("test_debug_util::fatal_assert_test");
        all_tests.insert("test_debug_util::panic_tag_comb_test");
        all_tests.insert("test_debug_util::panic_test");
    }

    ~Test() {

    }

    bool level_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::level_test for %luth time...", try_count);

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_ANY, "Hello World! This is a DEBUG."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_ANY, "Hello World! This is a LOG."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_WARNING, LOG_TAG_ANY, "Hello World! This is a WARNING."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_ERROR, LOG_TAG_ANY, "Hello World! This is a ERROR."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::level_test.");

        return true;
    }

    bool tag_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::tag_test for %luth time...", try_count);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_BASIC, "Hello World! This is a basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_COPPER_NODE, "Hello World! This is a copper tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "Hello World! This is a not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Hello World! This is a test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_ANY, "Hello World! This is any tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
            
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::tag_test.");

        return true;
    }

    bool level_tag_comb_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::level_tag_comb_test for %luth time...", try_count);

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_BASIC, "Hello World! This is a DEBUG with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_BASIC, "Hello World! This is a LOG with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_WARNING, LOG_TAG_BASIC, "Hello World! This is a WARNING with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_ERROR, LOG_TAG_BASIC, "Hello World! This is a ERROR with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_COPPER_NODE, "Hello World! This is a DEBUG with copper tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_COPPER_NODE, "Hello World! This is a LOG with copper tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_WARNING, LOG_TAG_COPPER_NODE, "Hello World! This is a WARNING with copper tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_ERROR, LOG_TAG_COPPER_NODE, "Hello World! This is a ERROR with copper tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_NOT_IMPLEMENTED, "Hello World! This is a DEBUG with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "Hello World! This is a LOG with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_WARNING, LOG_TAG_NOT_IMPLEMENTED, "Hello World! This is a WARNING with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_ERROR, LOG_TAG_NOT_IMPLEMENTED, "Hello World! This is a ERROR with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_TEST, "Hello World! This is a DEBUG with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Hello World! This is a LOG with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_WARNING, LOG_TAG_TEST, "Hello World! This is a WARNING with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Hello World! This is a ERROR with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_ANY, "Hello World! This is a DEBUG with any tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_ANY, "Hello World! This is a LOG with any tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_WARNING, LOG_TAG_ANY, "Hello World! This is a WARNING with any tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_ERROR, LOG_TAG_ANY, "Hello World! This is a ERROR with any tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
            
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::level_tag_comb_test.");

        return true;
    }

    bool error_assert_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::error_assert_test for %luth time...", try_count);

        ErrorAssert(true, LOG_TAG_ANY, "This is a true assert error."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(false, LOG_TAG_ANY, "This is a false assert error."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
            
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::error_assert_test.");

        return true;
    }

    bool error_assert_tag_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::error_assert_tag_test for %luth time...", try_count);

        ErrorAssert(true, LOG_TAG_BASIC, "This is a true assert error with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(true, LOG_TAG_COPPER_NODE, "This is a true assert error with copper tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(true, LOG_TAG_NOT_IMPLEMENTED, "This is a true assert error with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(true, LOG_TAG_TEST, "This is a true assert error with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(true, LOG_TAG_ANY, "This is a true assert error with any tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        
        ErrorAssert(false, LOG_TAG_BASIC, "This is a false assert error with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(false, LOG_TAG_COPPER_NODE, "This is a false assert error with copper tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(false, LOG_TAG_NOT_IMPLEMENTED, "This is a false assert error with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(false, LOG_TAG_TEST, "This is a false assert error with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(false, LOG_TAG_ANY, "This is a false assert error with any tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
            
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::error_assert_tag_test.");

        return true;
    }

    bool fatal_assert_tag_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::fatal_assert_tag_test for %luth time...", try_count);

        FatalAssert(true, LOG_TAG_BASIC, "This is a true fatal assert with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(true, LOG_TAG_COPPER_NODE, "This is a true fatal assert with copper tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(true, LOG_TAG_NOT_IMPLEMENTED, "This is a true fatal assert with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(true, LOG_TAG_TEST, "This is a true fatal assert with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(true, LOG_TAG_ANY, "This is a true fatal assert with any tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        
        FatalAssert(false, LOG_TAG_BASIC, "This is a false fatal assert with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(false, LOG_TAG_COPPER_NODE, "This is a false fatal assert with copper tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "This is a false fatal assert with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(false, LOG_TAG_TEST, "This is a false fatal assert with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(false, LOG_TAG_ANY, "This is a false fatal assert with any tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
            
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::fatal_assert_tag_test.");

        return true;
    }

    bool fatal_assert_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::fatal_assert_test for %luth time...", try_count);

        FatalAssert(true, LOG_TAG_ANY, "This is a true fatal assert."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(false, LOG_TAG_ANY, "This is a false fatal assert."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
            
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::fatal_assert_test.");

        return true;
    }
    
    bool panic_tag_comb_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::panic_tag_comb_test for %luth time...", try_count);

        CLOG(LOG_LEVEL_PANIC, LOG_TAG_BASIC, "Hello World! This is a PANIC with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_COPPER_NODE, "Hello World! This is a PANIC with copper tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "Hello World! This is a PANIC with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_TEST, "Hello World! This is a PANIC with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_ANY, "Hello World! This is a PANIC with any tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
            
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::panic_tag_comb_test.");

        return true;
    }

    bool panic_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::panic_test for %luth time...", try_count);

        CLOG(LOG_LEVEL_PANIC, LOG_TAG_ANY, "Hello World! This is a PANIC."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
            
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::panic_test.");

        return true;
    }

    void Init(size_t t_count) {
        try_count = t_count;
    }

    void Destroy() {
        try_count = 0;
    }

    void Run(const std::vector<std::string>& tests_to_run) {
        std::sort(tests_to_run.begin(), tests_to_run.end(), [this](const std::string& a, const std::string& b) {
            return test_priority[a] < test_priority[b];
        });
        for (std::string test_name : tests_to_run) {
            fprintf(stderr, "Searching for Test %s...\n", test_name.c_str());
            auto it = tests.find(test_name);
            if (it != tests.end()) {
                fprintf(stderr, "Test found.\n");
                bool (Test::*test)() = it->second;
                if ((this->*test)()) {
                    fprintf(stderr, COLORF_GREEN "Test %s was ran successfully!" COLORF_RESET "\n", test_name.c_str());
                }
                else {
                    fprintf(stderr, COLORF_RED "Test %s failed!" COLORF_RESET "\n", test_name.c_str());
                }
            }  
            else {
                fprintf(stderr, "Error: Test %s not found!\n", test_name.c_str());
            }
        }
    }

    std::set<std::string> all_tests;
protected:
    std::map<std::string, bool (Test::*)()> tests;
    std::map<std::string, int> test_priority;
    size_t try_count = 0;
};

int main(int argc, char *argv[]) {
    Test test;
    std::vector<std::string> tests_to_run;
    std::set<std::string> default_black_list = {"test_debug_util::fatal_assert_tag_test", "test_debug_util::fatal_assert_test", 
                                                "test_debug_util::panic_tag_comb_test", "test_debug_util::panic_test"};

    size_t num_runs = Parse_Args(argc, argv, test.all_tests, tests_to_run, default_black_list);
    if (num_runs == 0) {
        return 1;
    }

    for (size_t i = 0; i < num_runs; ++i) {
        fprintf(stderr, "Starting round %lu/%lu...\n", i+1, num_runs);
        test.Init(i + 1);
        fprintf(stderr, "Inited the tests.");
        test.Run(tests_to_run);
        test.Destroy();
        fprintf(stderr, "End of round %lu/%lu\n", i+1, num_runs);
    }
}

// Todo update run_ut so that we can use the same regexs and white/black lists across all ut files