#include "debug.h"
// #include "copper.h"

// #include "input.h"

// #include <string>
// #include <map>
// #include <set>
// #include <algorithm>

// // build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=(LOG_TAG_COPPER_NODE|LOG_TAG_TEST)
// // Todo: add file_name before test names
// // Todo: make tests return bool
// class Test {
//     public:
//         Test() {
//             tests["level_test"] = Test::level_test;

//             test_priority["level_test"] = 0;

//             all_tests.insert("level_test");
//         }

//         ~Test() {

//         }

//         void level_test() {
//             CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running level_test for %luth time...", try_count);


//             CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of level_test.");
//         }

//         void Init(size_t t_count) {
//             try_count = t_count;
//         }

//         void Destroy() {
//             try_count = 0;
//         }

//         void Run(const std::vector<std::string>& tests_to_run) {
//             std::sort(tests_to_run.begin(), tests_to_run.end(), [this](const std::string& a, const std::string& b) {
//                 return test_priority[a] < test_priority[b];
//             });
//             for (std::string test_name : tests_to_run) {
//                 fprintf(stderr, "Searching for Test %s...\n", test_name.c_str());
//                 auto it = tests.find(test_name);
//                 if (it != tests.end()) {
//                     fprintf(stderr, "Test found.\n");
//                     void (Test::*test)() = it->second;
//                     (this->*test)();
//                 }
//                 else {
//                     fprintf(stderr, "Error: Test %s not found!\n", test_name.c_str());
//                 }
//             }
//         }

//         std::set<std::string> all_tests;
//     protected:
//         std::map<std::string, void (Test::*)()> tests;
//         std::map<std::string, int> test_priority;
//         size_t try_count = 0;
//     };

//     int main(int argc, char *argv[]) {
//         Test test;
//         std::vector<std::string> tests_to_run;
//         size_t num_runs = Parse_Args(argc, argv, test.all_tests, tests_to_run);
//         if (num_runs == 0) {
//             return 1;
//         }

//         for (size_t i = 0; i < num_runs; ++i) {
//             fprintf(stderr, "Starting round %lu/%lu...\n", i+1, num_runs);
//             test.Init(i + 1);
//             fprintf(stderr, "Inited the tests.");
//             test.Run(tests_to_run);
//             test.Destroy();
//             fprintf(stderr, "End of round %lu/%lu\n", i+1, num_runs);
//         }
//     }

int main() {

}