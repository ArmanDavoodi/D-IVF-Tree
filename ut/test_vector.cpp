// #define TESTING

#include "debug.h"
// #include "vector_utils.h"

// #include "input.h"

// #include <string>
// #include <map>
// #include <set>
// #include <algorithm>
// #include <queue>


// // int main() {
//     // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Test Vector_Utils START.");

    
//     // copper::VectorID* ids16 = static_cast<copper::VectorID*>((void*)_ids16);

    
//     // copper::VectorID* idsf = static_cast<copper::VectorID*>((void*)_idsf);

//     // copper::Vector vec = _data16;
//     // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Uint Vector: %s", copper::to_string<uint16_t>(vec, dim).c_str());

//     // vec = _dataf;
//     // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Float Vector: %s", copper::to_string<float>(vec, dim).c_str());

    
//     // copper::VectorSet uset{_data16, ids16, size};
//     // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Uint VectorSet: %s", uset.to_string<uint16_t>(dim).c_str());

//     // copper::VectorSet fset{_dataf, idsf, size};
//     // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Float VectorSet: %s", fset.to_string<float>(dim).c_str());
    
//     // copper::Distance<uint16_t> *_du = new copper::L2_Distance<uint16_t>;
//     // copper::Distance<float> *_df = new copper::L2_Distance<float>;
    
//     // double udist[size][size], fdist[size][size];

//     // double b_udist[size][size] = {{0.0, 512.0, 2048.0}, {512.0, 0.0, 512.0}, {2048.0, 512.0, 0.0}};
//     // // I used the python dist to calculate these then I powered each by 2 so they may not be the correct values
//     // double b_fdist[size][size] = {{0.0, 2548.193744, 1788.207744}, {2548.193744, 0.0, 891.81}, {1788.207744, 891.81, 0.0}};
//     // std::priority_queue<std::pair<double, copper::VectorID>
//     //         , std::vector<std::pair<double, copper::VectorID>>, copper::Similarity<uint16_t>> 
//     //             udist_pq{_du};
//     // std::priority_queue<std::pair<double, copper::VectorID>
//     //         , std::vector<std::pair<double, copper::VectorID>>, copper::Similarity<float>> 
//     //             fdist_pq{_df};

//     // for (uint16_t i = 0; i < size; ++i) {
//     //     for (uint16_t j = 0; j < size; ++j) {
//     //         if (j > i)
//     //             continue;

//     //         udist[i][j] = ((*_du)(uset.Get_Vector<uint16_t>(i, dim), uset.Get_Vector<uint16_t>(j, dim), dim));
//     //         ErrorAssert(udist[i][j] == b_udist[i][j], LOG_TAG_TEST, 
//     //                     "uint16 distance calculation err: calc=%f, ground_truth=%f", udist[i][j], b_udist[i][j]);
//     //         udist_pq.push({udist[i][j], uset.Get_Index(i)});
//     //         fdist[i][j] = ((*_df)(fset.Get_Vector<float>(i, dim), fset.Get_Vector<float>(j, dim), dim));
//     //         ErrorAssert(fdist[i][j] == b_fdist[i][j], LOG_TAG_TEST, 
//     //             "float distance calculation err: calc=%f, ground_truth=%f", fdist[i][j], b_fdist[i][j]);
//     //         fdist_pq.push({fdist[i][j], fset.Get_Index(i)});
//     //     }
//     // }

//     // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "udist_pq: size=%lu", udist_pq.size());
//     // while(!udist_pq.empty()) {
//     //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "udist_pq: <%f, %lu>", udist_pq.top().first, udist_pq.top().second);
//     //     double p = udist_pq.top().first;
//     //     udist_pq.pop();
//     //     if (!udist_pq.empty()) {
//     //         ErrorAssert(p >= udist_pq.top().first, LOG_TAG_TEST, "udist_pq: Distance should be more than the next element.(top should be the least similar)")
//     //         ErrorAssert((*_du)(udist_pq.top().first, p), LOG_TAG_TEST, "udist_pq: distance comparator should show that top is less similar");
//     //     }
//     // }

//     // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fdist_pq: size=%lu", fdist_pq.size());
//     // while(!fdist_pq.empty()) {
//     //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fdist_pq: <%f, %lu>", fdist_pq.top().first, fdist_pq.top().second);
//     //     double p = fdist_pq.top().first;
//     //     fdist_pq.pop();
//     //     if (!fdist_pq.empty()) {
//     //         ErrorAssert(p >= fdist_pq.top().first, LOG_TAG_TEST, "fdist_pq: Distance should be more than the next element.(top should be the least similar)")
//     //         ErrorAssert((*_df)(fdist_pq.top().first, p), LOG_TAG_TEST, "fdist_pq: distance comparator should show that top is less similar");
//     //     }
//     // }

//     // for (uint16_t i = 0; i < uset._size; ++i) {
//     //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "uset[%hu] get_vec_by_id: ID=%lu, idx=%hu, data=%s", 
//     //                     i, uset.Get_VectorID(i)._id, i, copper::to_string<uint16_t>(uset.Get_Vector_By_ID<uint16_t>(uset.Get_VectorID(i), dim), dim));
//     //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "uset[%hu] get_vec: ID=%lu, idx=%hu, data=%s", 
//     //         i, uset.Get_VectorID(i)._id, i, copper::to_string<uint16_t>(uset.Get_Vector<uint16_t>(i, dim), dim));
//     // }

//     // for (uint16_t i = 0; i < fset._size; ++i) {
//     //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fset[%hu] get_vec_by_id: ID=%lu, idx=%hu, data=%s", 
//     //                     i, fset.Get_VectorID(i)._id, i, copper::to_string<float>(fset.Get_Vector_By_ID<float>(fset.Get_VectorID(i), dim), dim));
//     //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fset[%hu] get_vec: ID=%lu, idx=%hu, data=%s", 
//     //         i, fset.Get_VectorID(i)._id, i, copper::to_string<float>(fset.Get_Vector<float>(i, dim), dim));
//     // }
    
//     // copper::Vector sv;
//     // copper::VectorID svid(copper::INVALID_VECTOR_ID);

//     // copper::VectorID last_vec = 

//     // uset.Delete<uint16_t>(2, dim, svid, sv);// delete now returns an update instead

//     // ErrorAssert(svid == )
//     // //  todo check VectorSet functions
    
//     // //  todo check VectorIndex operations
    
//     // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Test Vector_Utils END.");
// // }

// // build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_COPPER_NODE
// class Test {
// public:
//     Test() {
//         tests["test_debug_util::level_test"] = Test::level_test;

//         test_priority["test_debug_util::level_test"] = 0;

//         all_tests.insert("test_debug_util::level_test");
//     }

//     ~Test() {

//     }

//     bool default_constructor_test() {
//         bool status = true;
//         copper::Vector<uint16_t, dim> vec;

//         if (vec._delete_on_destroy)

//         return status;
//     }

//     bool level_test() {

//         // copper::Vector<uint16_t, dim> vec(_data16, );
//         // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Uint Vector: %s", copper::to_string<uint16_t>(vec, dim).c_str());

//         // vec = _dataf;
//         // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Float Vector: %s", copper::to_string<float>(vec, dim).c_str());

//         // return true;
//     }

//     void Init(size_t t_count) {
//         try_count = t_count;
//     }

//     void Destroy() {
//         try_count = 0;
//     }

//     void Run(const std::vector<std::string>& tests_to_run) {
//         std::sort(tests_to_run.begin(), tests_to_run.end(), [this](const std::string& a, const std::string& b) {
//             return test_priority[a] < test_priority[b];
//         });
//         for (std::string test_name : tests_to_run) {
//             fprintf(stderr, "Searching for Test " COLORF_CYAN "%s" COLORF_RESET "...\n", test_name.c_str());
//             auto it = tests.find(test_name);
//             if (it != tests.end()) {
//                 fprintf(stderr, "Test found.\n");
//                 bool (Test::*test)() = it->second;
//                 if ((this->*test)()) {
//                     fprintf(stderr, COLORF_GREEN "Test " COLORF_RESET COLORF_CYAN "%s" COLORF_RESET COLORF_GREEN
//                             " ran successfully!" COLORF_RESET "\n", test_name.c_str());
//                 }
//                 else {
//                     fprintf(stderr, COLORF_RED "Test " COLORF_RESET COLORF_CYAN " %s " COLORF_RESET COLORF_RED 
//                             " failed!" COLORF_RESET "\n", test_name.c_str());
//                 }
//             }  
//             else {
//                 fprintf(stderr, COLORF_RED "Error: Test " COLORF_RESET COLORF_CYAN "%s" COLORF_RESET COLORF_RED 
//                         " not found!\n" COLORF_RESET, test_name.c_str());
//             }
//         }
//     }

//     std::set<std::string> all_tests;
// protected:
//     std::map<std::string, bool (Test::*)()> tests;
//     std::map<std::string, int> test_priority;
//     size_t try_count = 0;

//     static constexpr uint16_t dim = 8;
//     static constexpr uint16_t size = 3;

//     static constexpr uint16_t _data16[dim * size] = {1, 2, 3, 4, 5, 6, 7, 8, 
//                                     9, 10, 11, 12, 13, 14, 15, 16, 
//                                     17, 18, 19, 20, 21, 22, 23, 24};
//     static constexpr uint64_t _ids16[dim * size] = {1ul, 2ul, 3ul};

//     static constexpr float _dataf[dim * size] = {0.2f, 25.6f, -12.2f, 1.112f, 36.0f, 7.5f, -3.3f, 8.8f, 
//         9.1f, -4.6f, 5.5f, 2.2f, 3.3f, -1.1f, 6.6f, 7.7f, 
//         8.8f, 9.9f, -10.1f, 11.2f, 12.3f, -13.4f, 14.5f, 15.6f};
//     static constexpr uint64_t _idsf[dim * size] = {4ul, 5ul, 6ul};
// };

// int main(int argc, char *argv[]) {
//     Test test;
//     std::vector<std::string> tests_to_run;
//     std::set<std::string> default_black_list = {"test_debug_util::fatal_assert_tag_test", "test_debug_util::fatal_assert_test", 
//                                                 "test_debug_util::panic_tag_comb_test", "test_debug_util::panic_test"};

//     size_t num_runs = Parse_Args(argc, argv, test.all_tests, tests_to_run, default_black_list);
//     if (num_runs == 0) {
//         return 1;
//     }

//     for (size_t i = 0; i < num_runs; ++i) {
//         fprintf(stderr, "Starting round %lu/%lu...\n", i+1, num_runs);
//         test.Init(i + 1);
//         fprintf(stderr, "Inited the tests.");
//         test.Run(tests_to_run);
//         test.Destroy();
//         fprintf(stderr, "End of round %lu/%lu\n_________________________\n", i+1, num_runs);
//     }
// }

// // Todo update run_ut so that we can use the same regexs and white/black lists across all ut files

int main() {

}