#include "test.h"

#include "buffer.h"
#include "dummy_copper.h"

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_COPPER_NODE
namespace UT {
class Test {
public:
    Test() {
        tests["test_buffer::vector_test1"] = &Test::vector_test1;

        test_priority["test_buffer::vector_test1"] = 0;

        all_tests.insert("test_buffer::vector_test1");

    }

    ~Test() {}

    bool vector_test1() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_buffer::vector_test1 for %luth time...", try_count);
        bool status = true;

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_buffer::vector_test1.");
        return status;
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

    static constexpr uint16_t dim = 8;
    static constexpr uint16_t size = 3;

    const uint16_t _data16[size][dim] = {{1, 2, 3, 4, 5, 6, 7, 8},
                                         {9, 10, 11, 12, 13, 14, 15, 16},
                                         {17, 18, 19, 20, 21, 22, 23, 24}};
    const uint64_t _ids16[size] = {1ul, 2ul, 3ul};

    const float _dataf[size][dim] = {{0.2f, 25.6f, -12.2f, 1.112f, 36.0f, 7.5f, -3.3f, 8.8f},
                                     {9.1f, -4.6f, 5.5f, 2.2f, 3.3f, -1.1f, 6.6f, 7.7f},
                                     {8.8f, 9.9f, -10.1f, 11.2f, 12.3f, -13.4f, 14.5f, 15.6f}};
    const uint64_t _idsf[size] = {4ul, 5ul, 6ul};

friend class TestBase<Test>;
};
};

int main(int argc, char *argv[]) {
    std::set<std::string> default_black_list = {};
    UT::TestBase<UT::Test> test(argc, argv, default_black_list);
    return test.Run();
}

// Todo update run_ut so that we can use the same regexs and white/black lists across all ut files
