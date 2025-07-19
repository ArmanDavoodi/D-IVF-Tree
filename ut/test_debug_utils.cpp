#include "test.h"

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_DIVFTREE_VERTEX

namespace UT {

class Test {
public:
    Test() {
        tests["test_debug_util::level_test"] = &Test::level_test;
        tests["test_debug_util::tag_test"] = &Test::tag_test;
        tests["test_debug_util::level_tag_comb_test"] = &Test::level_tag_comb_test;
        tests["test_debug_util::conditional_level_test"] = &Test::conditional_level_test;
        tests["test_debug_util::conditional_tag_test"] = &Test::conditional_tag_test;
        tests["test_debug_util::conditional_level_tag_comb_test"] = &Test::conditional_level_tag_comb_test;
        tests["test_debug_util::conditional_false_panic_test"] = &Test::conditional_false_panic_test;
        tests["test_debug_util::conditional_false_panic_tag_comb_test"] = &Test::conditional_false_panic_tag_comb_test;
        tests["test_debug_util::error_assert_test"] = &Test::error_assert_test;
        tests["test_debug_util::error_assert_tag_test"] = &Test::error_assert_tag_test;
        tests["test_debug_util::fatal_assert_tag_test"] = &Test::fatal_assert_tag_test;
        tests["test_debug_util::fatal_assert_test"] = &Test::fatal_assert_test;
        tests["test_debug_util::conditional_true_true_panic_tag_comb_test"] = &Test::conditional_true_true_panic_tag_comb_test;
        tests["test_debug_util::conditional_false_false_panic_tag_comb_test"] = &Test::conditional_false_false_panic_tag_comb_test;
        tests["test_debug_util::conditional_true_true_panic_test"] = &Test::conditional_true_true_panic_test;
        tests["test_debug_util::conditional_false_false_panic_test"] = &Test::conditional_false_false_panic_test;
        tests["test_debug_util::panic_tag_comb_test"] = &Test::panic_tag_comb_test;
        tests["test_debug_util::panic_test"] = &Test::panic_test;

        test_priority["test_debug_util::level_test"] = 0;
        test_priority["test_debug_util::tag_test"] = 1;
        test_priority["test_debug_util::level_tag_comb_test"] = 2;
        test_priority["test_debug_util::conditional_level_test"] = 3;
        test_priority["test_debug_util::conditional_tag_test"] = 4;
        test_priority["test_debug_util::conditional_level_tag_comb_test"] = 5;
        test_priority["test_debug_util::conditional_false_panic_test"] = 6;
        test_priority["test_debug_util::conditional_false_panic_tag_comb_test"] = 7;
        test_priority["test_debug_util::error_assert_test"] = 8;
        test_priority["test_debug_util::error_assert_tag_test"] = 9;
        test_priority["test_debug_util::fatal_assert_tag_test"] = 10;
        test_priority["test_debug_util::fatal_assert_test"] = 11;
        test_priority["test_debug_util::conditional_true_true_panic_tag_comb_test"] = 12;
        test_priority["test_debug_util::conditional_false_false_panic_tag_comb_test"] = 13;
        test_priority["test_debug_util::conditional_true_true_panic_test"] = 14;
        test_priority["test_debug_util::conditional_false_false_panic_test"] = 15;
        test_priority["test_debug_util::panic_tag_comb_test"] = 16;
        test_priority["test_debug_util::panic_test"] = 17;

        all_tests.insert("test_debug_util::level_test");
        all_tests.insert("test_debug_util::tag_test");
        all_tests.insert("test_debug_util::conditional_level_tag_comb_test");
        all_tests.insert("test_debug_util::conditional_level_test");
        all_tests.insert("test_debug_util::conditional_tag_test");
        all_tests.insert("test_debug_util::conditional_false_panic_test");
        all_tests.insert("test_debug_util::conditional_false_panic_tag_comb_test");
        all_tests.insert("test_debug_util::level_tag_comb_test");
        all_tests.insert("test_debug_util::error_assert_test");
        all_tests.insert("test_debug_util::error_assert_tag_test");
        all_tests.insert("test_debug_util::fatal_assert_tag_test");
        all_tests.insert("test_debug_util::fatal_assert_test");
        all_tests.insert("test_debug_util::conditional_true_true_panic_tag_comb_test");
        all_tests.insert("test_debug_util::conditional_false_false_panic_tag_comb_test");
        all_tests.insert("test_debug_util::conditional_true_true_panic_test");
        all_tests.insert("test_debug_util::conditional_false_false_panic_test");
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
        CLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE_VERTEX, "Hello World! This is a divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "Hello World! This is a not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Hello World! This is a test tag."
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

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Hello World! This is a DEBUG with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE_VERTEX, "Hello World! This is a LOG with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE_VERTEX, "Hello World! This is a WARNING with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE_VERTEX, "Hello World! This is a ERROR with divftree tag."
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

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::level_tag_comb_test.");

        return true;
    }

    bool conditional_level_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::conditional_level_test for %luth time...", try_count);

        CLOG_IF_TRUE(true, LOG_LEVEL_DEBUG, LOG_TAG_ANY, "logged if true(true)! This is a DEBUG."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_LOG, LOG_TAG_ANY, "logged if true(true)! This is a LOG."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_WARNING, LOG_TAG_ANY, "logged if true(true)! This is a WARNING."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_ERROR, LOG_TAG_ANY, "logged if true(true)! This is a ERROR."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_TRUE(false, LOG_LEVEL_DEBUG, LOG_TAG_ANY, "logged if true(false)! This is a DEBUG."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_LOG, LOG_TAG_ANY, "logged if true(false)! This is a LOG."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_WARNING, LOG_TAG_ANY, "logged if true(false)! This is a WARNING."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_ERROR, LOG_TAG_ANY, "logged if true(false)! This is a ERROR."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(true, LOG_LEVEL_DEBUG, LOG_TAG_ANY, "logged if false(true)! This is a DEBUG."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_LOG, LOG_TAG_ANY, "logged if false(true)! This is a LOG."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_WARNING, LOG_TAG_ANY, "logged if false(true)! This is a WARNING."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_ERROR, LOG_TAG_ANY, "logged if false(true)! This is a ERROR."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(false, LOG_LEVEL_DEBUG, LOG_TAG_ANY, "logged if false(false)! This is a DEBUG."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_LOG, LOG_TAG_ANY, "logged if false(false)! This is a LOG."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_WARNING, LOG_TAG_ANY, "logged if false(false)! This is a WARNING."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_ERROR, LOG_TAG_ANY, "logged if false(false)! This is a ERROR."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::conditional_level_test.");

        return true;
    }

    bool conditional_tag_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::conditional_tag_test for %luth time...", try_count);

        CLOG_IF_TRUE(true, LOG_LEVEL_LOG, LOG_TAG_BASIC, "logged if true(true)! This is a basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_LOG, LOG_TAG_DIVFTREE_VERTEX, "logged if true(true)! This is a divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "logged if true(true)! This is a not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_LOG, LOG_TAG_TEST, "logged if true(true)! This is a test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_TRUE(false, LOG_LEVEL_LOG, LOG_TAG_BASIC, "logged if true(false)! This is a basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_LOG, LOG_TAG_DIVFTREE_VERTEX, "logged if true(false)! This is a divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "logged if true(false)! This is a not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_LOG, LOG_TAG_TEST, "logged if true(false)! This is a test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(true, LOG_LEVEL_LOG, LOG_TAG_BASIC, "logged if false(true)! This is a basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_LOG, LOG_TAG_DIVFTREE_VERTEX, "logged if false(true)! This is a divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "logged if false(true)! This is a not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_LOG, LOG_TAG_TEST, "logged if false(true)! This is a test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(false, LOG_LEVEL_LOG, LOG_TAG_BASIC, "logged if false(false)! This is a basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_LOG, LOG_TAG_DIVFTREE_VERTEX, "logged if false(false)! This is a divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "logged if false(false)! This is a not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_LOG, LOG_TAG_TEST, "logged if false(false)! This is a test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::conditional_tag_test.");

        return true;
    }

    bool conditional_level_tag_comb_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::conditional_level_tag_comb_test for %luth time...", try_count);

        CLOG_IF_TRUE(true, LOG_LEVEL_DEBUG, LOG_TAG_BASIC, "logged if true(true)! This is a DEBUG with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_LOG, LOG_TAG_BASIC, "logged if true(true)! This is a LOG with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_WARNING, LOG_TAG_BASIC, "logged if true(true)! This is a WARNING with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_ERROR, LOG_TAG_BASIC, "logged if true(true)! This is a ERROR with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_TRUE(true, LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "logged if true(true)! This is a DEBUG with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_LOG, LOG_TAG_DIVFTREE_VERTEX, "logged if true(true)! This is a LOG with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE_VERTEX, "logged if true(true)! This is a WARNING with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE_VERTEX, "logged if true(true)! This is a ERROR with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_TRUE(true, LOG_LEVEL_DEBUG, LOG_TAG_NOT_IMPLEMENTED, "logged if true(true)! This is a DEBUG with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "logged if true(true)! This is a LOG with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_WARNING, LOG_TAG_NOT_IMPLEMENTED, "logged if true(true)! This is a WARNING with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_ERROR, LOG_TAG_NOT_IMPLEMENTED, "logged if true(true)! This is a ERROR with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_TRUE(true, LOG_LEVEL_DEBUG, LOG_TAG_TEST, "logged if true(true)! This is a DEBUG with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_LOG, LOG_TAG_TEST, "logged if true(true)! This is a LOG with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_WARNING, LOG_TAG_TEST, "logged if true(true)! This is a WARNING with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_ERROR, LOG_TAG_TEST, "logged if true(true)! This is a ERROR with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_TRUE(false, LOG_LEVEL_DEBUG, LOG_TAG_BASIC, "logged if true(false)! This is a DEBUG with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_LOG, LOG_TAG_BASIC, "logged if true(false)! This is a LOG with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_WARNING, LOG_TAG_BASIC, "logged if true(false)! This is a WARNING with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_ERROR, LOG_TAG_BASIC, "logged if true(false)! This is a ERROR with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_TRUE(false, LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "logged if true(false)! This is a DEBUG with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_LOG, LOG_TAG_DIVFTREE_VERTEX, "logged if true(false)! This is a LOG with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE_VERTEX, "logged if true(false)! This is a WARNING with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE_VERTEX, "logged if true(false)! This is a ERROR with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_TRUE(false, LOG_LEVEL_DEBUG, LOG_TAG_NOT_IMPLEMENTED, "logged if true(false)! This is a DEBUG with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "logged if true(false)! This is a LOG with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_WARNING, LOG_TAG_NOT_IMPLEMENTED, "logged if true(false)! This is a WARNING with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_ERROR, LOG_TAG_NOT_IMPLEMENTED, "logged if true(false)! This is a ERROR with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_TRUE(false, LOG_LEVEL_DEBUG, LOG_TAG_TEST, "logged if true(false)! This is a DEBUG with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_LOG, LOG_TAG_TEST, "logged if true(false)! This is a LOG with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_WARNING, LOG_TAG_TEST, "logged if true(false)! This is a WARNING with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_ERROR, LOG_TAG_TEST, "logged if true(false)! This is a ERROR with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);


        CLOG_IF_FALSE(true, LOG_LEVEL_DEBUG, LOG_TAG_BASIC, "logged if false(true)! This is a DEBUG with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_LOG, LOG_TAG_BASIC, "logged if false(true)! This is a LOG with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_WARNING, LOG_TAG_BASIC, "logged if false(true)! This is a WARNING with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_ERROR, LOG_TAG_BASIC, "logged if false(true)! This is a ERROR with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(true, LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "logged if false(true)! This is a DEBUG with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_LOG, LOG_TAG_DIVFTREE_VERTEX, "logged if false(true)! This is a LOG with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE_VERTEX, "logged if false(true)! This is a WARNING with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE_VERTEX, "logged if false(true)! This is a ERROR with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(true, LOG_LEVEL_DEBUG, LOG_TAG_NOT_IMPLEMENTED, "logged if false(true)! This is a DEBUG with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "logged if false(true)! This is a LOG with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_WARNING, LOG_TAG_NOT_IMPLEMENTED, "logged if false(true)! This is a WARNING with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_ERROR, LOG_TAG_NOT_IMPLEMENTED, "logged if false(true)! This is a ERROR with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(true, LOG_LEVEL_DEBUG, LOG_TAG_TEST, "logged if false(true)! This is a DEBUG with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_LOG, LOG_TAG_TEST, "logged if false(true)! This is a LOG with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_WARNING, LOG_TAG_TEST, "logged if false(true)! This is a WARNING with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_ERROR, LOG_TAG_TEST, "logged if false(true)! This is a ERROR with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(false, LOG_LEVEL_DEBUG, LOG_TAG_BASIC, "logged if false(false)! This is a DEBUG with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_LOG, LOG_TAG_BASIC, "logged if false(false)! This is a LOG with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_WARNING, LOG_TAG_BASIC, "logged if false(false)! This is a WARNING with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_ERROR, LOG_TAG_BASIC, "logged if false(false)! This is a ERROR with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(false, LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "logged if false(false)! This is a DEBUG with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_LOG, LOG_TAG_DIVFTREE_VERTEX, "logged if false(false)! This is a LOG with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE_VERTEX, "logged if false(false)! This is a WARNING with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE_VERTEX, "logged if false(false)! This is a ERROR with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(false, LOG_LEVEL_DEBUG, LOG_TAG_NOT_IMPLEMENTED, "logged if false(false)! This is a DEBUG with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_LOG, LOG_TAG_NOT_IMPLEMENTED, "logged if false(false)! This is a LOG with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_WARNING, LOG_TAG_NOT_IMPLEMENTED, "logged if false(false)! This is a WARNING with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_ERROR, LOG_TAG_NOT_IMPLEMENTED, "logged if false(false)! This is a ERROR with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(false, LOG_LEVEL_DEBUG, LOG_TAG_TEST, "logged if false(false)! This is a DEBUG with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_LOG, LOG_TAG_TEST, "logged if false(false)! This is a LOG with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_WARNING, LOG_TAG_TEST, "logged if false(false)! This is a WARNING with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_ERROR, LOG_TAG_TEST, "logged if false(false)! This is a ERROR with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::conditional_level_tag_comb_test.");

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
        ErrorAssert(true, LOG_TAG_DIVFTREE_VERTEX, "This is a true assert error with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(true, LOG_TAG_NOT_IMPLEMENTED, "This is a true assert error with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(true, LOG_TAG_TEST, "This is a true assert error with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        ErrorAssert(false, LOG_TAG_BASIC, "This is a false assert error with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(false, LOG_TAG_DIVFTREE_VERTEX, "This is a false assert error with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(false, LOG_TAG_NOT_IMPLEMENTED, "This is a false assert error with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        ErrorAssert(false, LOG_TAG_TEST, "This is a false assert error with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::error_assert_tag_test.");

        return true;
    }

    bool fatal_assert_tag_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::fatal_assert_tag_test for %luth time...", try_count);

        FatalAssert(true, LOG_TAG_BASIC, "This is a true fatal assert with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(true, LOG_TAG_DIVFTREE_VERTEX, "This is a true fatal assert with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(true, LOG_TAG_NOT_IMPLEMENTED, "This is a true fatal assert with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(true, LOG_TAG_TEST, "This is a true fatal assert with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        FatalAssert(false, LOG_TAG_BASIC, "This is a false fatal assert with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(false, LOG_TAG_DIVFTREE_VERTEX, "This is a false fatal assert with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "This is a false fatal assert with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        FatalAssert(false, LOG_TAG_TEST, "This is a false fatal assert with test tag."
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

    bool conditional_false_panic_tag_comb_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::conditional_false_panic_tag_comb_test for %luth time...", try_count);

        CLOG_IF_TRUE(false, LOG_LEVEL_PANIC, LOG_TAG_BASIC, "logged if true(false)! This is a PANIC with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE_VERTEX, "logged if true(false)! This is a PANIC with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "logged if true(false)! This is a PANIC with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(false, LOG_LEVEL_PANIC, LOG_TAG_TEST, "logged if true(false)! This is a PANIC with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG_IF_FALSE(true, LOG_LEVEL_PANIC, LOG_TAG_BASIC, "logged if false(true)! This is a PANIC with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE_VERTEX, "logged if false(true)! This is a PANIC with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "logged if false(true)! This is a PANIC with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_PANIC, LOG_TAG_TEST, "logged if false(true)! This is a PANIC with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::conditional_false_panic_tag_comb_test.");

        return true;
    }

    bool conditional_true_true_panic_tag_comb_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::conditional_true_true_panic_tag_comb_test for %luth time...", try_count);

        CLOG_IF_TRUE(true, LOG_LEVEL_PANIC, LOG_TAG_BASIC, "logged if true(true)! This is a PANIC with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE_VERTEX, "logged if true(true)! This is a PANIC with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "logged if true(true)! This is a PANIC with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_TRUE(true, LOG_LEVEL_PANIC, LOG_TAG_TEST, "logged if true(true)! This is a PANIC with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::conditional_true_true_panic_tag_comb_test.");

        return true;
    }

    bool conditional_false_false_panic_tag_comb_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::conditional_false_false_panic_tag_comb_test for %luth time...", try_count);

        CLOG_IF_FALSE(false, LOG_LEVEL_PANIC, LOG_TAG_BASIC, "logged if false(false)! This is a PANIC with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE_VERTEX, "logged if false(false)! This is a PANIC with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "logged if false(false)! This is a PANIC with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(false, LOG_LEVEL_PANIC, LOG_TAG_TEST, "logged if false(false)! This is a PANIC with test tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::conditional_false_false_panic_tag_comb_test.");

        return true;
    }

    bool conditional_false_panic_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::conditional_false_panic_test for %luth time...", try_count);

        CLOG_IF_TRUE(false, LOG_LEVEL_PANIC, LOG_TAG_ANY, "logged if true(false)! This is a PANIC."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG_IF_FALSE(true, LOG_LEVEL_PANIC, LOG_TAG_ANY, "logged if false(true)! This is a PANIC."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::conditional_false_panic_test.");

        return true;
    }

    bool conditional_true_true_panic_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::conditional_true_true_panic_test for %luth time...", try_count);

        CLOG_IF_TRUE(true, LOG_LEVEL_PANIC, LOG_TAG_ANY, "logged if true(true)! This is a PANIC."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::conditional_true_true_panic_test.");

        return true;
    }

    bool conditional_false_false_panic_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::conditional_false_false_panic_test for %luth time...", try_count);

        CLOG_IF_FALSE(false, LOG_LEVEL_PANIC, LOG_TAG_ANY, "logged if false(false)! This is a PANIC."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_debug_util::conditional_false_false_panic_test.");

        return true;
    }

    bool panic_tag_comb_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_debug_util::panic_tag_comb_test for %luth time...", try_count);

        CLOG(LOG_LEVEL_PANIC, LOG_TAG_BASIC, "Hello World! This is a PANIC with basic tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE_VERTEX, "Hello World! This is a PANIC with divftree tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "Hello World! This is a PANIC with not implemented tag."
            " str:%s, int:%d, uint8%hhu, uint64:%lu.", "test", -5, (uint8_t)12, 12655486lu);
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_TEST, "Hello World! This is a PANIC with test tag."
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

    std::set<std::string> all_tests;
    std::map<std::string, int> test_priority;
protected:
    std::map<std::string, bool (Test::*)()> tests;
    size_t try_count = 0;

friend class TestBase<Test>;
};
}

int main(int argc, char *argv[]) {
    std::set<std::string> default_black_list = {"test_debug_util::fatal_assert_tag_test", "test_debug_util::fatal_assert_test",
                                                "test_debug_util::panic_tag_comb_test", "test_debug_util::panic_test",
                                                "test_debug_util::conditional_true_true_panic_tag_comb_test", "test_debug_util::conditional_false_false_panic_tag_comb_test",
                                                "test_debug_util::conditional_true_true_panic_test", "test_debug_util::conditional_false_false_panic_test"};
    UT::TestBase<UT::Test> test(argc, argv, default_black_list);
    return test.Run();
}

// Todo update run_ut so that we can use the same regexs and white/black lists across all ut files