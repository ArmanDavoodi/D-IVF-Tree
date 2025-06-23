#include "test.h"

#include "vector_utils.h"

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_COPPER_NODE
namespace UT {
class Test {
public:
    Test() {
        tests["test_vector::vector_test1"] = &Test::vector_test1;
        tests["test_vector::vector_test2"] = &Test::vector_test2;
        tests["test_vector::vector_set"] = &Test::vector_set;

        test_priority["test_vector::vector_test1"] = 0;
        test_priority["test_vector::vector_test2"] = 1;
        test_priority["test_vector::vector_set"] = 2;

        all_tests.insert("test_vector::vector_test1");
        all_tests.insert("test_vector::vector_test2");
        all_tests.insert("test_vector::vector_set");
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

        copper::VectorSet<uint16_t, dim, 3> uset;
        copper::VectorSet<float, dim, 3> fset;

        for (uint16_t i = 0; i < size; ++i) {
            copper::Address uva = uset.Insert(_data16[i], _ids16[i]);
            copper::Address fva = fset.Insert(copper::Vector<float, dim>(_dataf[i]), _idsf[i]);

            status = status && (uva == uset.Get_Typed_Address() + (i * dim));
            ErrorAssert(uva == uset.Get_Typed_Address() + (i * dim), LOG_TAG_TEST,
                        "uint16 vector set returned wrong address. Expected=%p, Actual=%p",
                        uva, uset.Get_Typed_Address() + (i * dim));
            status = status && (fva == fset.Get_Typed_Address() + (i * dim));
            ErrorAssert(fva == fset.Get_Typed_Address() + (i * dim), LOG_TAG_TEST,
                        "float vector set returned wrong address. Expected=%p, Actual=%p",
                        fva, fset.Get_Typed_Address() + (i * dim));
        }

        status = status && (uset.Size() == size);
        ErrorAssert(uset.Size() == size, LOG_TAG_TEST,
                    "uint16 vector set size mismatch. Expected=%hu, Actual=%hu",
                    size, uset.Size());
        status = status && (fset.Size() == size);
        ErrorAssert(fset.Size() == size, LOG_TAG_TEST,
                    "float vector set size mismatch. Expected=%hu, Actual=%hu",
                    size, fset.Size());

        for (uint16_t i = 0; i < size; ++i) {
            copper::VectorPair<uint16_t, dim> vecp = uset[i];
            copper::VectorID id = uset.Get_VectorID(i);
            copper::Vector<uint16_t, dim> vec1 = uset.Get_Vector(i);

            status = status && (vecp.id == id);
            ErrorAssert(vecp.id == id, LOG_TAG_TEST,
                        "uint16 vector set returned wrong vector id. Expected=%lu, Actual=%lu",
                        id._id, vecp.id._id);
            status = status && (vecp.vector.Are_The_Same(vec1));
            ErrorAssert(vecp.vector.Are_The_Same(vec1), LOG_TAG_TEST,
                        "uint16 vector set returned wrong vector. Expected=%s, Actual=%s",
                        vec1.to_string().c_str(), vecp.vector.to_string().c_str());

            status = status && (uset.Contains(id));
            ErrorAssert(uset.Contains(id), LOG_TAG_TEST,
                        "uint16 vector set does not contain vector id %lu", id._id);
            status = status && (uset.Get_Index(id) == i);
            ErrorAssert(uset.Get_Index(id) == i, LOG_TAG_TEST,
                        "uint16 vector set returned wrong index for vector id %lu. Expected=%hu, Actual=%hu",
                        id._id, i, uset.Get_Index(id));
            status = status && (uset.Get_Vector_By_ID(id).Are_The_Same(vec1));
            ErrorAssert(uset.Get_Vector_By_ID(id).Are_The_Same(vec1), LOG_TAG_TEST,
                        "uint16 vector set returned wrong vector. Expected=%s, Actual=%s",
                        vec1.to_string().c_str(), uset.Get_Vector_By_ID(id).to_string().c_str());
        }

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Uint VectorSet: %s", uset.to_string().c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Float VectorSet: %s", fset.to_string().c_str());

        uset.Delete_Last();
        status = status && (uset.Size() == size - 1);
        ErrorAssert(uset.Size() == size - 1, LOG_TAG_TEST,
                    "uint16 vector set size mismatch after delete. Expected=%hu, Actual=%hu",
                    size - 1, uset.Size());
        status = status && !(uset.Contains(_ids16[size - 1]));
        ErrorAssert(!(uset.Contains(_ids16[size - 1])), LOG_TAG_TEST,
                    "uint16 vector set should not contain vector id %lu", _ids16[size - 1]);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Uint VectorSet after deletion: %s", uset.to_string().c_str());

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_vector::vector_set.");
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
