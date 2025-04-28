#include "test.h"

#include "vector_utils.h"

#include <queue>

// int main() {
    // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Test Vector_Utils START.");

    
    // copper::VectorID* ids16 = static_cast<copper::VectorID*>((void*)_ids16);

    
    // copper::VectorID* idsf = static_cast<copper::VectorID*>((void*)_idsf);

    // copper::Vector vec = _data16;
    // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Uint Vector: %s", copper::to_string<uint16_t>(vec, dim).c_str());

    // vec = _dataf;
    // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Float Vector: %s", copper::to_string<float>(vec, dim).c_str());

    
    // copper::VectorSet uset{_data16, ids16, size};
    // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Uint VectorSet: %s", uset.to_string<uint16_t>(dim).c_str());

    // copper::VectorSet fset{_dataf, idsf, size};
    // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Float VectorSet: %s", fset.to_string<float>(dim).c_str());
    
    // copper::Distance<uint16_t> *_du = new copper::L2_Distance<uint16_t>;
    // copper::Distance<float> *_df = new copper::L2_Distance<float>;
    
    // double udist[size][size], fdist[size][size];

    // double b_udist[size][size] = {{0.0, 512.0, 2048.0}, {512.0, 0.0, 512.0}, {2048.0, 512.0, 0.0}};
    // // I used the python dist to calculate these then I powered each by 2 so they may not be the correct values
    // double b_fdist[size][size] = {{0.0, 2548.193744, 1788.207744}, {2548.193744, 0.0, 891.81}, {1788.207744, 891.81, 0.0}};
    // std::priority_queue<std::pair<double, copper::VectorID>
    //         , std::vector<std::pair<double, copper::VectorID>>, copper::Similarity<uint16_t>> 
    //             udist_pq{_du};
    // std::priority_queue<std::pair<double, copper::VectorID>
    //         , std::vector<std::pair<double, copper::VectorID>>, copper::Similarity<float>> 
    //             fdist_pq{_df};

    // for (uint16_t i = 0; i < size; ++i) {
    //     for (uint16_t j = 0; j < size; ++j) {
    //         if (j > i)
    //             continue;

    //         udist[i][j] = ((*_du)(uset.Get_Vector<uint16_t>(i, dim), uset.Get_Vector<uint16_t>(j, dim), dim));
    //         ErrorAssert(udist[i][j] == b_udist[i][j], LOG_TAG_TEST, 
    //                     "uint16 distance calculation err: calc=%f, ground_truth=%f", udist[i][j], b_udist[i][j]);
    //         udist_pq.push({udist[i][j], uset.Get_Index(i)});
    //         fdist[i][j] = ((*_df)(fset.Get_Vector<float>(i, dim), fset.Get_Vector<float>(j, dim), dim));
    //         ErrorAssert(fdist[i][j] == b_fdist[i][j], LOG_TAG_TEST, 
    //             "float distance calculation err: calc=%f, ground_truth=%f", fdist[i][j], b_fdist[i][j]);
    //         fdist_pq.push({fdist[i][j], fset.Get_Index(i)});
    //     }
    // }

    // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "udist_pq: size=%lu", udist_pq.size());
    // while(!udist_pq.empty()) {
    //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "udist_pq: <%f, %lu>", udist_pq.top().first, udist_pq.top().second);
    //     double p = udist_pq.top().first;
    //     udist_pq.pop();
    //     if (!udist_pq.empty()) {
    //         ErrorAssert(p >= udist_pq.top().first, LOG_TAG_TEST, "udist_pq: Distance should be more than the next element.(top should be the least similar)")
    //         ErrorAssert((*_du)(udist_pq.top().first, p), LOG_TAG_TEST, "udist_pq: distance comparator should show that top is less similar");
    //     }
    // }

    // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fdist_pq: size=%lu", fdist_pq.size());
    // while(!fdist_pq.empty()) {
    //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fdist_pq: <%f, %lu>", fdist_pq.top().first, fdist_pq.top().second);
    //     double p = fdist_pq.top().first;
    //     fdist_pq.pop();
    //     if (!fdist_pq.empty()) {
    //         ErrorAssert(p >= fdist_pq.top().first, LOG_TAG_TEST, "fdist_pq: Distance should be more than the next element.(top should be the least similar)")
    //         ErrorAssert((*_df)(fdist_pq.top().first, p), LOG_TAG_TEST, "fdist_pq: distance comparator should show that top is less similar");
    //     }
    // }

    // for (uint16_t i = 0; i < uset._size; ++i) {
    //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "uset[%hu] get_vec_by_id: ID=%lu, idx=%hu, data=%s", 
    //                     i, uset.Get_VectorID(i)._id, i, copper::to_string<uint16_t>(uset.Get_Vector_By_ID<uint16_t>(uset.Get_VectorID(i), dim), dim));
    //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "uset[%hu] get_vec: ID=%lu, idx=%hu, data=%s", 
    //         i, uset.Get_VectorID(i)._id, i, copper::to_string<uint16_t>(uset.Get_Vector<uint16_t>(i, dim), dim));
    // }

    // for (uint16_t i = 0; i < fset._size; ++i) {
    //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fset[%hu] get_vec_by_id: ID=%lu, idx=%hu, data=%s", 
    //                     i, fset.Get_VectorID(i)._id, i, copper::to_string<float>(fset.Get_Vector_By_ID<float>(fset.Get_VectorID(i), dim), dim));
    //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fset[%hu] get_vec: ID=%lu, idx=%hu, data=%s", 
    //         i, fset.Get_VectorID(i)._id, i, copper::to_string<float>(fset.Get_Vector<float>(i, dim), dim));
    // }
    
    // copper::Vector sv;
    // copper::VectorID svid(copper::INVALID_VECTOR_ID);

    // copper::VectorID last_vec = 

    // uset.Delete<uint16_t>(2, dim, svid, sv);// delete now returns an update instead

    // ErrorAssert(svid == )
    // //  todo check VectorSet functions
    
    // //  todo check VectorIndex operations
    
    // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Test Vector_Utils END.");
// }

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_COPPER_NODE
namespace UT {
class Test {
public:
    Test() {
        tests["test_vector::vector_constructor_and_assignment_test"] = &Test::vector_constructor_and_assignment_test;

        test_priority["test_vector::vector_constructor_and_assignment_test"] = 0;

        all_tests.insert("test_vector::vector_constructor_and_assignment_test");
    }

    ~Test() {

    }

    bool vector_constructor_and_assignment_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_vector::vector_constructor_and_assignment_test for %luth time...", try_count);
        bool status = true;
        {
            copper::Vector<uint16_t, dim> vec1;
            status = status && vec1._delete_on_destroy;
            ErrorAssert(vec1._delete_on_destroy, LOG_TAG_TEST, "case 1) Defult constructor did not set _delete_on_destroy to true.");

            for (uint16_t i = 0; i < dim; ++i) {
                vec1[i] = _data16[1][i];
            }

            copper::Vector<uint16_t, dim> vec2(_data16[0]);
            status = status && vec2._delete_on_destroy;
            ErrorAssert(vec2._delete_on_destroy, LOG_TAG_TEST, "case 2) public constructor did not set _delete_on_destroy to true.");

            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec2[i] == _data16[0][i]);
                ErrorAssert(vec2[i] == _data16[0][i], LOG_TAG_TEST, "");
            }

            copper::Vector<uint16_t, dim> vec3(vec1);
            status = status && vec3._delete_on_destroy;
            ErrorAssert(vec3._delete_on_destroy, LOG_TAG_TEST, "case 3) public constructor did not set _delete_on_destroy to true.");

            copper::Vector<uint16_t, dim> vec4(std::move(vec3));
            status = status && vec4._delete_on_destroy;
            ErrorAssert(vec4._delete_on_destroy, LOG_TAG_TEST, "case 4) public constructor did not set _delete_on_destroy to true.");
            status = status && (!vec3.Is_Valid());
            ErrorAssert(!vec3.Is_Valid(), LOG_TAG_TEST, "case 4) moved vector should no longer be valid.");
            status = status && (!vec3._delete_on_destroy);
            ErrorAssert(!vec3._delete_on_destroy, LOG_TAG_TEST, "case 4) moved vector should no longer be deleted.");

            /* we should see 2 constructor logs for case 5 */
            copper::Vector<uint16_t, dim> vec5 = copper::Vector<uint16_t, dim>::NEW_INVALID();
            status = status && !(vec5._delete_on_destroy);
            ErrorAssert(!(vec5._delete_on_destroy), LOG_TAG_TEST, "case 5) Invalid vector should not be destoyed");
            status = status && !(vec5.Is_Valid());
            ErrorAssert(!(vec5.Is_Valid()), LOG_TAG_TEST, "case 5) Invalid vector should not be valid");

            copper::Vector<uint16_t, dim> vec6 = vec2;
            status = status && vec2._delete_on_destroy;
            ErrorAssert(vec2._delete_on_destroy, LOG_TAG_TEST, "case 6) public constructor did not set _delete_on_destroy to true.");
            
            /* Since vec6 is not invalid, we should copy the data of vec1 and not use its pointer. */
            vec6 = std::move(vec1);
            status = status && (vec1.Is_Valid());
            ErrorAssert(vec1.Is_Valid(), LOG_TAG_TEST, "right vector should remain valid.");
            status = status && (vec6.Is_Valid());
            ErrorAssert(vec6.Is_Valid(), LOG_TAG_TEST, "left vector should remain valid.");
            status = status && (vec1._delete_on_destroy);
            ErrorAssert(vec1._delete_on_destroy, LOG_TAG_TEST, "right vector should be destroyed.");
            status = status && (vec6._delete_on_destroy);
            ErrorAssert(vec6._delete_on_destroy, LOG_TAG_TEST, "left vector should be destroyed.");
            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec6[i] == vec1[i]);
                ErrorAssert(vec6[i] == vec1[i], LOG_TAG_TEST, "vectors should have the same data.");
            }
            status = status && (!vec6.Are_The_Same(vec1));
            ErrorAssert((!vec6.Are_The_Same(vec1)), LOG_TAG_TEST, "vectors should not be the same.");

            vec5 = std::move(vec1);
            status = status && (!vec1.Is_Valid());
            ErrorAssert(!vec1.Is_Valid(), LOG_TAG_TEST, "right vector should not remain valid.");
            status = status && (vec5.Is_Valid());
            ErrorAssert(vec5.Is_Valid(), LOG_TAG_TEST, "left vector should remain valid.");
            status = status && (!vec1._delete_on_destroy);
            ErrorAssert(!vec1._delete_on_destroy, LOG_TAG_TEST, "right vector should not be destroyed.");
            status = status && (vec5._delete_on_destroy);
            ErrorAssert(vec5._delete_on_destroy, LOG_TAG_TEST, "left vector should be destroyed.");
            status = status && (!vec5.Are_The_Same(vec1));
            ErrorAssert((!vec5.Are_The_Same(vec1)), LOG_TAG_TEST, "vectors should not be the same.");

            vec4 = vec2;
            status = status && (vec2.Is_Valid());
            ErrorAssert(vec2.Is_Valid(), LOG_TAG_TEST, "right vector should remain valid.");
            status = status && (vec4.Is_Valid());
            ErrorAssert(vec4.Is_Valid(), LOG_TAG_TEST, "left vector should remain valid.");
            status = status && (vec2._delete_on_destroy);
            ErrorAssert(vec2._delete_on_destroy, LOG_TAG_TEST, "right vector should be destroyed.");
            status = status && (vec4._delete_on_destroy);
            ErrorAssert(vec4._delete_on_destroy, LOG_TAG_TEST, "left vector should be destroyed.");
            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec2[i] == vec4[i]);
                ErrorAssert(vec2[i] == vec4[i], LOG_TAG_TEST, "vectors should have the same data.");
            }
            status = status && (!vec4.Are_The_Same(vec2));
            ErrorAssert((!vec4.Are_The_Same(vec2)), LOG_TAG_TEST, "vectors should not be the same.");

            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector1=%s", vec1.to_string().c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector2=%s", vec2.to_string().c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector3=%s", vec3.to_string().c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector4=%s", vec4.to_string().c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector5=%s", vec5.to_string().c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector6=%s", vec6.to_string().c_str());
        }

        /* todo test for the private constructors to check for memory leaks and stuff */

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_vector::vector_constructor_and_assignment_test.");
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

    uint16_t _data16[size][dim] = {{1, 2, 3, 4, 5, 6, 7, 8}, 
                                    {9, 10, 11, 12, 13, 14, 15, 16}, 
                                    {17, 18, 19, 20, 21, 22, 23, 24}};
    uint64_t _ids16[size] = {1ul, 2ul, 3ul};

    float _dataf[size][dim] = {{0.2f, 25.6f, -12.2f, 1.112f, 36.0f, 7.5f, -3.3f, 8.8f}, 
                                {9.1f, -4.6f, 5.5f, 2.2f, 3.3f, -1.1f, 6.6f, 7.7f}, 
                                {8.8f, 9.9f, -10.1f, 11.2f, 12.3f, -13.4f, 14.5f, 15.6f}};
    uint64_t _idsf[size] = {4ul, 5ul, 6ul};

friend class TestBase<Test>;
};
};

int main(int argc, char *argv[]) {
    std::set<std::string> default_black_list = {};
    UT::TestBase<UT::Test> test(argc, argv, default_black_list);
    return test.Run();
}

// Todo update run_ut so that we can use the same regexs and white/black lists across all ut files
