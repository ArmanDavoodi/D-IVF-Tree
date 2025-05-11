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
        tests["test_vector::vector_test1"] = &Test::vector_test1;
        tests["test_vector::vector_test2"] = &Test::vector_test2;
        tests["test_vector::distance_L2"] = &Test::distance_L2;

        test_priority["test_vector::vector_test1"] = 0;
        test_priority["test_vector::vector_test2"] = 1;
        test_priority["test_vector::distance_L2"] = 2;

        all_tests.insert("test_vector::vector_test1");
        all_tests.insert("test_vector::vector_test2");
        all_tests.insert("test_vector::distance_L2");
    }

    ~Test() {}

    bool vector_test1() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_vector::vector_test1 for %luth time...", try_count);
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
            status = status && vec1._delete_on_destroy;
            ErrorAssert(vec1._delete_on_destroy, LOG_TAG_TEST, "case 3) public constructor changed _delete_on_destroy of input.");

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
            status = status && vec6._delete_on_destroy;
            ErrorAssert(vec6._delete_on_destroy, LOG_TAG_TEST, "case 6) public constructor did not set _delete_on_destroy to true.");
            status = status && vec2._delete_on_destroy;
            ErrorAssert(vec2._delete_on_destroy, LOG_TAG_TEST, "case 6) public constructor changed _delete_on_destroy of input.");

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

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_vector::vector_test1.");
        return status;
    }

    bool vector_test2() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_vector::vector_test2 for %luth time...", try_count);
        bool status = true;
        {
            uint16_t _tmp_data16[size][dim];
            for (uint16_t i = 0; i < size; ++i) {
                for (uint16_t j = 0; j < dim; ++j) {
                    _tmp_data16[i][j] = _data16[i][j];
                }
            }

            copper::Vector<uint16_t, dim> vec1(_tmp_data16[0], false);
            status = status && !vec1._delete_on_destroy;
            ErrorAssert(!vec1._delete_on_destroy, LOG_TAG_TEST, "case 7) private constructor did not set _delete_on_destroy to false.");
            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec1[i] == _tmp_data16[0][i]);
                ErrorAssert(vec1[i] == _tmp_data16[0][i], LOG_TAG_TEST, "");
            }

            copper::Vector<uint16_t, dim> vec2(vec1);
            status = status && vec2._delete_on_destroy;
            ErrorAssert(vec2._delete_on_destroy, LOG_TAG_TEST, "case 8) public constructor did not set _delete_on_destroy to true.");
            status = status && !vec1._delete_on_destroy;
            ErrorAssert(!vec1._delete_on_destroy, LOG_TAG_TEST, "case 8) public constructor changed _delete_on_destroy of input.");
            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec1[i] == vec2[i]);
                ErrorAssert(vec1[i] == vec2[i], LOG_TAG_TEST, "");
            }

            copper::Vector<uint16_t, dim> vec3(_tmp_data16[1], false);
            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec3[i] == _tmp_data16[1][i]);
                ErrorAssert(vec3[i] == _tmp_data16[1][i], LOG_TAG_TEST, "");
            }

            copper::Vector<uint16_t, dim> vec4(std::move(vec3));
            status = status && !vec4._delete_on_destroy;
            ErrorAssert(!vec4._delete_on_destroy, LOG_TAG_TEST, "case 9) public constructor did not set _delete_on_destroy to false.");
            status = status && (!vec3.Is_Valid());
            ErrorAssert(!vec3.Is_Valid(), LOG_TAG_TEST, "case 9) moved vector should no longer be valid.");
            status = status && (!vec3._delete_on_destroy);
            ErrorAssert(!vec3._delete_on_destroy, LOG_TAG_TEST, "case 9) moved vector should no longer be deleted.");

            copper::Vector<uint16_t, dim> vec5 = copper::Vector<uint16_t, dim>::NEW_INVALID();

            copper::Vector<uint16_t, dim> vec6 = vec1;
            status = status && vec6._delete_on_destroy;
            ErrorAssert(vec6._delete_on_destroy, LOG_TAG_TEST, "case 10) public constructor did not set _delete_on_destroy to true.");
            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec1[i] == vec6[i]);
                ErrorAssert(vec1[i] == vec6[i], LOG_TAG_TEST, "");
            }

            /* Since vec6 is not invalid, we should copy the data of vec1 and not use its pointer. */
            vec6 = std::move(vec1);
            status = status && (vec1.Is_Valid());
            ErrorAssert(vec1.Is_Valid(), LOG_TAG_TEST, "right vector should remain valid.");
            status = status && (vec6.Is_Valid());
            ErrorAssert(vec6.Is_Valid(), LOG_TAG_TEST, "left vector should remain valid.");
            status = status && (!vec1._delete_on_destroy);
            ErrorAssert(!vec1._delete_on_destroy, LOG_TAG_TEST, "right vector should not be destroyed.");
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
            status = status && (!vec5._delete_on_destroy);
            ErrorAssert(!vec5._delete_on_destroy, LOG_TAG_TEST, "left vector should not be destroyed.");
            status = status && (!vec5.Are_The_Same(vec1));
            ErrorAssert((!vec5.Are_The_Same(vec1)), LOG_TAG_TEST, "vectors should not be the same.");

            vec4 = vec5;
            status = status && (vec5.Is_Valid());
            ErrorAssert(vec5.Is_Valid(), LOG_TAG_TEST, "right vector should remain valid.");
            status = status && (vec4.Is_Valid());
            ErrorAssert(vec4.Is_Valid(), LOG_TAG_TEST, "left vector should remain valid.");
            status = status && (!vec5._delete_on_destroy);
            ErrorAssert(!vec5._delete_on_destroy, LOG_TAG_TEST, "right vector should not be destroyed.");
            status = status && (!vec4._delete_on_destroy);
            ErrorAssert(!vec4._delete_on_destroy, LOG_TAG_TEST, "left vector should be destroyed.");
            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec5[i] == vec4[i]);
                ErrorAssert(vec5[i] == vec4[i], LOG_TAG_TEST, "vectors should have the same data.");
            }
            status = status && (!vec4.Are_The_Same(vec5));
            ErrorAssert((!vec4.Are_The_Same(vec5)), LOG_TAG_TEST, "vectors should not be the same.");

            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector1=%s", vec1.to_string().c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector2=%s", vec2.to_string().c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector3=%s", vec3.to_string().c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector4=%s", vec4.to_string().c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector5=%s", vec5.to_string().c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector6=%s", vec6.to_string().c_str());

            std::string data0 = "", data1 = "", data2 = "";
            for (uint16_t i = 0; i < dim; ++i) {
                data0 += std::to_string(_tmp_data16[0][i]);
                data1 += std::to_string(_tmp_data16[1][i]);
                data2 += std::to_string(_tmp_data16[2][i]);
                if (i < dim - 1) {
                    data0 += ", ";
                    data1 += ", ";
                    data2 += ", ";
                }
            }
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Data1=%s", data0.c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Data2=%s", data1.c_str());
            CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Data3=%s", data2.c_str());
        }

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_vector::vector_test2.");
        return status;
    }

    bool vector_set() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_vector::vector_set for %luth time...", try_count);
        bool status = true;

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_vector::vector_set.");
        return status;
    }

    bool distance_L2() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_vector::distance_L2 for %luth time...", try_count);
        bool status = true;
        std::string data0 = "{", data1 = "{", data2 = "{";
        for (uint16_t i = 0; i < dim; ++i) {
            data0 += std::to_string(_data16[0][i]);
            data1 += std::to_string(_data16[1][i]);
            data2 += std::to_string(_data16[2][i]);
            if (i < dim - 1) {
                data0 += ", ";
                data1 += ", ";
                data2 += ", ";
            }
            else {
                data0 += "}";
                data1 += "}";
                data2 += "}";
            }
        }
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Data1=%s", data0.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Data2=%s", data1.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Data3=%s", data2.c_str());

        std::string dataf0 = "{", dataf1 = "{", dataf2 = "{";
        for (uint16_t i = 0; i < dim; ++i) {
            dataf0 += std::to_string(_dataf[0][i]);
            dataf1 += std::to_string(_dataf[1][i]);
            dataf2 += std::to_string(_dataf[2][i]);
            if (i < dim - 1) {
                dataf0 += ", ";
                dataf1 += ", ";
                dataf2 += ", ";
            }
            else {
                dataf0 += "}";
                dataf1 += "}";
                dataf2 += "}";
            }
        }
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Dataf1=%s", dataf0.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Dataf2=%s", dataf1.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Dataf3=%s", dataf2.c_str());

        copper::L2_Distance<uint16_t, dim, double> _du;
        copper::L2_Distance<float, dim, double> _df;

        double udist[size][size], fdist[size][size];
        double b_udist[size][size], b_fdist[size][size];

        std::string udist_str = "";
        std::string fdist_str = "";
        std::string b_udist_str = "";
        std::string b_fdist_str = "";

        for (uint16_t i = 0; i < size; ++i) {
            for (uint16_t j = 0; j < size; ++j) {
                if (j > i)
                    continue;

                b_udist[i][j] = 0;
                b_fdist[i][j] = 0;
                for (uint16_t k = 0; k < dim; ++k) {
                    b_udist[i][j] += (((double)_data16[i][k] - (double)_data16[j][k]) *
                                      ((double)_data16[i][k] - (double)_data16[j][k]));
                    b_fdist[i][j] += (((double)_dataf[i][k] - (double)_dataf[j][k]) *
                                      ((double)_dataf[i][k] - (double)_dataf[j][k]));
                }
                udist[i][j] = _du(copper::Vector<uint16_t, dim>(_data16[i]), copper::Vector<uint16_t, dim>(_data16[j]));
                fdist[i][j] = _df(copper::Vector<float, dim>(_dataf[i]), copper::Vector<float, dim>(_dataf[j]));

                status = status && (udist[i][j] == b_udist[i][j]);
                ErrorAssert(udist[i][j] == b_udist[i][j], LOG_TAG_TEST,
                            "uint16 distance calculation err: calc=%f, ground_truth=%f", udist[i][j], b_udist[i][j]);

                status = status && (fdist[i][j] == b_fdist[i][j]);
                ErrorAssert(fdist[i][j] == b_fdist[i][j], LOG_TAG_TEST,
                            "float distance calculation err: calc=%f, ground_truth=%f", fdist[i][j], b_fdist[i][j]);

                udist_str += "<" + std::to_string(i) + ", " + std::to_string(j) + ">" + std::to_string(udist[i][j]);
                fdist_str += "<" + std::to_string(i) + ", " + std::to_string(j) + ">" + std::to_string(fdist[i][j]);
                b_udist_str += "<" + std::to_string(i) + ", " + std::to_string(j) + ">" + std::to_string(b_udist[i][j]);
                b_fdist_str += "<" + std::to_string(i) + ", " + std::to_string(j) + ">" + std::to_string(b_fdist[i][j]);
                if (j < size - 1) {
                    udist_str += ", ";
                    fdist_str += ", ";
                    b_udist_str += ", ";
                    b_fdist_str += ", ";
                }
            }
        }
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "udist: %s", udist_str.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_udist: %s", b_udist_str.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fdist: %s", fdist_str.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_fdist: %s", b_fdist_str.c_str());

        const uint16_t uquery[dim] = {2, 1, 5, 3, 2, 8, 15, 16};
        const float fquery[dim] = {0.15f, 20.4f, -9.0f, 1.1f, 10.0f, 6.0f, 5.1f, 9.55f};

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "uquery: %s", copper::Vector<uint16_t, dim>(uquery).to_string().c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fquery: %s", copper::Vector<float, dim>(fquery).to_string().c_str());

        double uquery_dist[size], fquery_dist[size], b_uquery_dist[size], b_fquery_dist[size];
        std::string uquery_dist_str = "";
        std::string fquery_dist_str = "";
        std::string b_uquery_dist_str = "";
        std::string b_fquery_dist_str = "";
        for (uint16_t i = 0; i < size; ++i) {
            b_uquery_dist[i] = 0;
            b_fquery_dist[i] = 0;
            for (uint16_t k = 0; k < dim; ++k) {
                b_uquery_dist[i] += (((double)uquery[k] - (double)_data16[i][k]) *
                                     ((double)uquery[k] - (double)_data16[i][k]));
                b_fquery_dist[i] += (((double)fquery[k] - (double)_dataf[i][k]) *
                                     ((double)fquery[k] - (double)_dataf[i][k]));
            }
            uquery_dist[i] = _du(copper::Vector<uint16_t, dim>(uquery), copper::Vector<uint16_t, dim>(_data16[i]));
            fquery_dist[i] = _df(copper::Vector<float, dim>(fquery), copper::Vector<float, dim>(_dataf[i]));

            status = status && (uquery_dist[i] == b_uquery_dist[i]);
            ErrorAssert(uquery_dist[i] == b_uquery_dist[i], LOG_TAG_TEST,
                        "uint16 distance calculation err: calc=%f, ground_truth=%f", uquery_dist[i], b_uquery_dist[i]);

            status = status && (fquery_dist[i] == b_fquery_dist[i]);
            ErrorAssert(fquery_dist[i] == b_fquery_dist[i], LOG_TAG_TEST,
                        "float distance calculation err: calc=%f, ground_truth=%f", fquery_dist[i], b_fquery_dist[i]);

            uquery_dist_str += std::to_string(uquery_dist[i]);
            fquery_dist_str += std::to_string(fquery_dist[i]);
            b_uquery_dist_str += std::to_string(b_uquery_dist[i]);
            b_fquery_dist_str += std::to_string(b_fquery_dist[i]);
            if (i < size - 1) {
                uquery_dist_str += ", ";
                fquery_dist_str += ", ";
                b_uquery_dist_str += ", ";
                b_fquery_dist_str += ", ";
            }
        }

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "uquery_dist: %s", uquery_dist_str.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_uquery_dist: %s", b_uquery_dist_str.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fquery_dist: %s", fquery_dist_str.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_fquery_dist: %s", b_fquery_dist_str.c_str());

        /* The most similar vector to query should be first -> for L2 that is the minimum distance */
        std::sort(uquery_dist, uquery_dist + size, _du);
        std::sort(fquery_dist, fquery_dist + size, _df);
        for (uint16_t i = 0; i < size - 1; ++i) {
            status = status && (uquery_dist[i] <= uquery_dist[i + 1]);
            ErrorAssert(uquery_dist[i] <= uquery_dist[i + 1], LOG_TAG_TEST,
                        "uint16 l2 similarity err: %f should be less than %f", uquery_dist[i], uquery_dist[i + 1]);
            status = status && (fquery_dist[i] <= fquery_dist[i + 1]);
            ErrorAssert(fquery_dist[i] <= fquery_dist[i + 1], LOG_TAG_TEST,
                        "float l2 similarity err: %f should be less than %f", fquery_dist[i], fquery_dist[i + 1]);
        }
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "uquery_dist_sorted: %s", copper::Vector<double, size>(uquery_dist).to_string().c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fquery_dist_sorted: %s", copper::Vector<double, size>(fquery_dist).to_string().c_str());

        copper::Vector<uint16_t, dim> ucent = _du.Compute_Centroid((const uint16_t*)_data16, size);
        copper::Vector<float, dim> fcent = _df.Compute_Centroid((const float*)_dataf, size);

        copper::Vector<uint16_t, dim> b_ucent;
        copper::Vector<float, dim> b_fcent;
        for (uint16_t i = 0; i < dim; ++i) {
            b_ucent[i] = 0;
            b_fcent[i] = 0;
            for (uint16_t j = 0; j < size; ++j) {
                b_ucent[i] += _data16[j][i];
                b_fcent[i] += _dataf[j][i];
            }
            b_ucent[i] /= size;
            b_fcent[i] /= size;
        }

        status = status && (ucent == b_ucent);
        ErrorAssert(ucent == b_ucent, LOG_TAG_TEST,
                    "uint16 centroid calculation err: calc=%s, ground_truth=%s", ucent.to_string().c_str(),
                    b_ucent.to_string().c_str());
        status = status && (fcent == b_fcent);
        ErrorAssert(fcent == b_fcent, LOG_TAG_TEST,
                    "float centroid calculation err: calc=%s, ground_truth=%s", fcent.to_string().c_str(),
                    b_fcent.to_string().c_str());

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "ucent: %s", ucent.to_string().c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_ucent: %s", b_ucent.to_string().c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fcent: %s", fcent.to_string().c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_fcent: %s", b_fcent.to_string().c_str());

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_vector::distance_L2.");
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
