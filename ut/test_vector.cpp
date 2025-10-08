#include "test.h"

#define VECTOR_TYPE float
#define VTYPE_FMT "%f"
#define DISTANCE_TYPE double
#define DTYPE_FMT "%lf"

#include "vector_utils.h"

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_DIVFTREE_VERTEX
namespace UT {
class Test {
public:
    Test() {
        tests["test_vector::vector_test"] = &Test::vector_test;
        tests["test_vector::vertex_cluster"] = &Test::vertex_cluster;

        test_priority["test_vector::vector_test"] = 0;
        test_priority["test_vector::vertex_cluster"] = 1;

        all_tests.insert("test_vector::vector_test");
        all_tests.insert("test_vector::vertex_cluster");
    }

    ~Test() {}

    divftree::Cluster* CreateCluster(uint16_t dim, uint16_t capacity) {
        divftree::Cluster* cluster = static_cast<divftree::Cluster*>(
            malloc(sizeof(divftree::Cluster) + divftree::Cluster::DataBytes(dim, capacity)));
        FatalAssert(cluster != nullptr, LOG_TAG_TEST, "Failed to allocate memory for Cluster.");
        new (cluster) divftree::Cluster(dim, capacity);
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_TEST, "Created Cluster: this=%p, dim=%hu, capacity=%hu",
             cluster, dim, capacity);
        return cluster;
    }

    void DestroyCluster(divftree::Cluster*& cluster) {
        FatalAssert(cluster != nullptr, LOG_TAG_TEST, "Cannot destroy a null Cluster.");
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_TEST, "Destroying Cluster: this=%p", cluster);
        cluster->~Cluster();
        free(cluster);
        cluster = nullptr;
    }

    bool vector_test() {
        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_vector::vector_test for %luth time...", try_count);
        bool status = true;
        {
            bool pre_delete_on_destroy;
            divftree::Address pre_address;

            divftree::Vector vec1;
            status = status && !vec1.IsValid();
            ErrorAssert(!vec1.IsValid(), LOG_TAG_TEST, "Vector should not be valid after default constructor.");
            status = status && !vec1._delete_on_destroy;
            ErrorAssert(!vec1._delete_on_destroy, LOG_TAG_TEST, "Vector should not be deleted after default constructor.");
            FaultAssert(vec1.Destroy(), status, LOG_TAG_TEST, "Invalid vector should not be destroyed.");
            FaultAssert(vec1.Unlink(), status, LOG_TAG_TEST, "Invalid vector can not be unlinked.");
            FaultAssert(vec1.CopyFrom(_data[0], dim), status, LOG_TAG_TEST,
                        "Invalid vector should not copy from data.");

            vec1.Create(dim);
            status = status && vec1.IsValid();
            ErrorAssert(vec1.IsValid(), LOG_TAG_TEST, "Vector should be valid after Create.");
            status = status && vec1._delete_on_destroy;
            ErrorAssert(vec1._delete_on_destroy, LOG_TAG_TEST, "Vector should be deleted after Create.");
            FaultAssert(vec1.Unlink(), status, LOG_TAG_TEST,
                        "Copied vector cannot be unlinked.");

            vec1.CopyFrom(_data[1], dim);
            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec1[i] == _data[1][i]);
                ErrorAssert(vec1[i] == _data[1][i], LOG_TAG_TEST, "");
            }

            pre_delete_on_destroy = vec1._delete_on_destroy;
            pre_address = vec1.GetData();

            divftree::Vector vec2(static_cast<divftree::Address>(_data[0]));
            status = status && !vec2._delete_on_destroy;
            ErrorAssert(!vec2._delete_on_destroy, LOG_TAG_TEST, "Vector should not be deleted after link constructor.");

            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec2[i] == _data[0][i]);
                ErrorAssert(vec2[i] == _data[0][i], LOG_TAG_TEST, "");
            }

            FaultAssert(vec2.Create(dim), status, LOG_TAG_TEST, "Valid linked vector should not be created.");
            FaultAssert(vec2.Link(vec1), status, LOG_TAG_TEST, "Valid linked vector should not be linked.");
            FaultAssert(vec2.Link(static_cast<divftree::Address>(_data[1])), status, LOG_TAG_TEST,
                        "Valid linked vector should not be linked.");
            FaultAssert(vec2.Destroy(), status, LOG_TAG_TEST,
                        "linked vector cannot be destroyed.");
            FaultAssert(vec2 = std::move(vec1), status, LOG_TAG_TEST, "Valid linked vector should not be assigned.");
            status = status && (vec1.GetData() == pre_address);
            ErrorAssert(vec1.GetData() == pre_address, LOG_TAG_TEST,
                        "Vector should not change address after faulty move assignment.");
            status = status && (vec1._delete_on_destroy == pre_delete_on_destroy);
            ErrorAssert(vec1._delete_on_destroy == pre_delete_on_destroy, LOG_TAG_TEST,
                        "Vector should not change delete_on_destroy after faulty move assignment.");
            status = status && (vec2.GetData() == static_cast<divftree::Address>(_data[0]));
            ErrorAssert(vec2.GetData() == static_cast<divftree::Address>(_data[0]), LOG_TAG_TEST,
                        "Vector should not change address after faulty operations.");
            status = status && (!vec2._delete_on_destroy);
            ErrorAssert(!vec2._delete_on_destroy, LOG_TAG_TEST,
                        "Vector should not change delete_on_destroy after faulty operations.");

            FaultAssert(vec1.Create(dim), status, LOG_TAG_TEST, "Valid vector should not be created.");
            FaultAssert(vec1.Link(vec2), status, LOG_TAG_TEST, "Valid vector should not be linked.");
            FaultAssert(vec1.Link(static_cast<divftree::Address>(_data[1])), status, LOG_TAG_TEST,
                        "Valid vector should not be linked.");
            FaultAssert(vec1 = std::move(vec2), status, LOG_TAG_TEST, "Valid vector should not be assigned.");
            status = status && (vec2.GetData() == static_cast<divftree::Address>(_data[0]));
            ErrorAssert(vec2.GetData() == static_cast<divftree::Address>(_data[0]), LOG_TAG_TEST,
                        "Vector should not change address after faulty move assignment.");
            status = status && !(vec2._delete_on_destroy);
            ErrorAssert(!(vec2._delete_on_destroy), LOG_TAG_TEST,
                        "Vector should not change delete_on_destroy after faulty move assignment.");
            status = status && (vec1.GetData() == pre_address);
            ErrorAssert(vec1.GetData() == pre_address, LOG_TAG_TEST,
                        "Vector should not change address after faulty operations.");
            status = status && (vec1._delete_on_destroy == pre_delete_on_destroy);
            ErrorAssert(vec1._delete_on_destroy == pre_delete_on_destroy, LOG_TAG_TEST,
                        "Vector should not change delete_on_destroy after faulty operations.");

            divftree::Vector vec3(vec1, dim);
            status = status && vec3._delete_on_destroy;
            ErrorAssert(vec3._delete_on_destroy, LOG_TAG_TEST, "Vector should be deleted after copy constructor.");

            status = status && (vec3.Similar(vec1, dim));
            ErrorAssert(vec3.Similar(vec1, dim), LOG_TAG_TEST,
                        "Vectors should be similar after copy constructor.");
            status = status && (vec3 != vec1);
            ErrorAssert(vec3 != vec1, LOG_TAG_TEST,
                        "Vectors should not be the same after copy constructor.");

            pre_delete_on_destroy = vec3._delete_on_destroy;
            pre_address = vec3.GetData();

            divftree::Vector vec4(std::move(vec3));
            status = status && (vec4._delete_on_destroy == pre_delete_on_destroy);
            ErrorAssert(vec4._delete_on_destroy == pre_delete_on_destroy, LOG_TAG_TEST,
                        "vector constructed using move, should inherit the delete_on_destroy of it's input.");
            status = status && (vec4.GetData() == pre_address);
            ErrorAssert(vec4.GetData() == pre_address, LOG_TAG_TEST,
                        "vector constructed using move, should inherit the address of it's input.");

            status = status && !(vec3.IsValid());
            ErrorAssert(!vec3.IsValid(), LOG_TAG_TEST, "case 4) moved vector should no longer be valid.");
            status = status && (!vec3._delete_on_destroy);
            ErrorAssert(!vec3._delete_on_destroy, LOG_TAG_TEST, "case 4) moved vector should no longer be deleted.");

            vec3 = std::move(vec4);
            status = status && (vec3._delete_on_destroy == pre_delete_on_destroy);
            ErrorAssert(vec3._delete_on_destroy == pre_delete_on_destroy, LOG_TAG_TEST,
                        "vector assignment, should inherit the delete_on_destroy of it's input.");
            status = status && (vec3.GetData() == pre_address);
            ErrorAssert(vec3.GetData() == pre_address, LOG_TAG_TEST,
                        "vector assignment, should inherit the address of it's input.");

            status = status && !(vec4.IsValid());
            ErrorAssert(!vec4.IsValid(), LOG_TAG_TEST, "case 4) moved vector should no longer be valid.");
            status = status && (!vec4._delete_on_destroy);
            ErrorAssert(!vec4._delete_on_destroy, LOG_TAG_TEST, "case 4) moved vector should no longer be deleted.");

            FaultAssert(vec3 = std::move(vec2), status, LOG_TAG_TEST,
                        "Valid vector should not be assigned to an other vector.");

            pre_delete_on_destroy = vec2._delete_on_destroy;
            pre_address = vec2.GetData();
            vec2.CopyFrom(vec3, dim);
            status = status && (vec2._delete_on_destroy == pre_delete_on_destroy);
            ErrorAssert(vec2._delete_on_destroy == pre_delete_on_destroy, LOG_TAG_TEST,
                        "vector copy, should not change the delete_on_destroy.");
            status = status && (vec2.GetData() == pre_address);
            ErrorAssert(vec2.GetData() == pre_address, LOG_TAG_TEST,
                        "vector copy, should not change the address.");
            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec2[i] == vec3[i]);
                ErrorAssert(vec2[i] == vec3[i], LOG_TAG_TEST, "Vectors should have the same data.");
            }

            pre_delete_on_destroy = vec1._delete_on_destroy;
            pre_address = vec1.GetData();
            vec1.CopyFrom(_data[2], dim);
            status = status && (vec1._delete_on_destroy == pre_delete_on_destroy);
            ErrorAssert(vec1._delete_on_destroy == pre_delete_on_destroy, LOG_TAG_TEST,
                        "vector copy, should not change the delete_on_destroy.");
            status = status && (vec1.GetData() == pre_address);
            ErrorAssert(vec1.GetData() == pre_address, LOG_TAG_TEST,
                        "vector copy, should not change the address.");
            for (uint16_t i = 0; i < dim; ++i) {
                status = status && (vec1[i] == _data[2][i]);
                ErrorAssert(vec1[i] == _data[2][i], LOG_TAG_TEST, "Vectors should have the same data.");
            }

            divftree::Vector vec5(vec2.GetData());
            status = status && (vec2.Similar(vec5, dim));
            ErrorAssert(vec2.Similar(vec5, dim), LOG_TAG_TEST,
                        "Vectors should be similar after link constructor.");
            status = status && (vec2 == vec5);
            ErrorAssert(vec2 == vec5, LOG_TAG_TEST,
                        "Vectors should be the same after link constructor.");

            vec4.Link(vec1);
            status = status && (vec4.Similar(vec1, dim));
            ErrorAssert(vec4.Similar(vec1, dim), LOG_TAG_TEST,
                        "Vectors should be similar after link.");
            status = status && (vec4 == vec1);
            ErrorAssert(vec4 == vec1, LOG_TAG_TEST,
                        "Vectors should be the same after link.");
#ifdef MEMORY_DEBUG
            FaultAssert(vec1.Destroy(), status, LOG_TAG_TEST,
                        "vector cannot be destroyed while others are linked to it.");
#endif
            vec4.Unlink();
            status = status && !vec4.IsValid();
            ErrorAssert(!vec4.IsValid(), LOG_TAG_TEST, "Vector should not be valid after Unlink.");
            status = status && !vec4._delete_on_destroy;
            ErrorAssert(!vec4._delete_on_destroy, LOG_TAG_TEST, "Vector should not be deleted after Unlink.");

            /* Remember not to destroy a vector if other vectors are linked to it! */
            vec1.Destroy();
            status = status && !vec1.IsValid();
            ErrorAssert(!vec1.IsValid(), LOG_TAG_TEST, "Vector should not be valid after Destroy.");
            status = status && !vec1._delete_on_destroy;
            ErrorAssert(!vec1._delete_on_destroy, LOG_TAG_TEST, "Vector should not be deleted after Destroy.");

            DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector1=%s", vec1.ToString(dim).ToCStr());
            DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector2=%s", vec2.ToString(dim).ToCStr());
            DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector3=%s", vec3.ToString(dim).ToCStr());
            DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector4=%s", vec4.ToString(dim).ToCStr());
            DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Vector5=%s", vec5.ToString(dim).ToCStr());
        }

        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_vector::vector_test.");
        return status;
    }

    bool vertex_cluster() {
        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_vector::vertex_cluster for %luth time...", try_count);
        bool status = true;

        divftree::Cluster* set1p = CreateCluster(dim, size);
        divftree::Cluster* set2p = CreateCluster(dim, size+5);
        divftree::Cluster& set1 = *set1p;
        divftree::Cluster& set2 = *set2p;

        for (uint16_t i = 0; i < size; ++i) {
            divftree::Address va1 = set1.Insert(_data[i], _ids[i]);
            divftree::Address va2 = set2.Insert(divftree::Vector(_data[i]), _ids[i]);

            status = status && (va1 == set1.GetVectors() + (i * dim * sizeof(divftree::VTYPE)));
            ErrorAssert(va1 == set1.GetVectors() + (i * dim * sizeof(divftree::VTYPE)), LOG_TAG_TEST,
                        "vector set returned wrong address. Expected=%p, Actual=%p",
                        va1, set1.GetVectors() + (i * dim * sizeof(divftree::VTYPE)));
            status = status && (va2 == set2.GetVectors() + (i * dim * sizeof(divftree::VTYPE)));
            ErrorAssert(va2 == set2.GetVectors() + (i * dim * sizeof(divftree::VTYPE)), LOG_TAG_TEST,
                        "vector set returned wrong address. Expected=%p, Actual=%p",
                        va2, set1.GetVectors() + (i * dim * sizeof(divftree::VTYPE)));

            status = status && (set1[i].id == _ids[i]);
            ErrorAssert(set1[i].id == _ids[i], LOG_TAG_TEST,
                        "vector set returned wrong vector id. Expected=%lu, Actual=%lu",
                        _ids[i], set1[i].id._id);
            status = status && (set2[i].id == _ids[i]);
            ErrorAssert(set2[i].id == _ids[i], LOG_TAG_TEST,
                        "vector set returned wrong vector id. Expected=%lu, Actual=%lu",
                        _ids[i], set2[i].id._id);
            status = status && (set1[i].vec.IsValid());
            ErrorAssert(set1[i].vec.IsValid(), LOG_TAG_TEST,
                        "vector set returned invalid vector at index %hu", i);
            status = status && (set2[i].vec.IsValid());
            ErrorAssert(set2[i].vec.IsValid(), LOG_TAG_TEST,
                        "vector set returned invalid vector at index %hu", i);
            for (uint16_t j = 0; j < dim; ++j) {
                status = status && (set1[i].vec[j] == _data[i][j]);
                ErrorAssert(set1[i].vec[j] == _data[i][j], LOG_TAG_TEST,
                            "vector set returned wrong vector data at index %hu, dim %hu. Expected=%f, Actual=%f",
                            i, j, _data[i][j], set1[i].vec[j]);
                status = status && (set2[i].vec[j] == _data[i][j]);
                ErrorAssert(set2[i].vec[j] == _data[i][j], LOG_TAG_TEST,
                            "vector set returned wrong vector data at index %hu, dim %hu. Expected=%f, Actual=%f",
                            i, j, _data[i][j], set2[i].vec[j]);
            }
        }
        FaultAssert(set1.Insert(_data[0], _ids[0]), status, LOG_TAG_TEST,
                    "Cluster should not insert when full.");
        FaultAssert(set1.Insert(divftree::Vector(_data[0]), _ids[0]), status, LOG_TAG_TEST,
                    "Cluster should not insert when full.");

        status = status && (set1.Size() == size);
        ErrorAssert(set1.Size() == size, LOG_TAG_TEST,
                    "vector set size mismatch. Expected=%hu, Actual=%hu",
                    size, set1.Size());
        status = status && (set2.Size() == size);
        ErrorAssert(set2.Size() == size, LOG_TAG_TEST,
                    "vector set size mismatch. Expected=%hu, Actual=%hu",
                    size, set2.Size());

        for (uint16_t i = 0; i < size; ++i) {
            divftree::VectorPair vecp = set1[i];
            divftree::VectorID id = set1.GetVectorID(i);
            divftree::Vector vec1 = set1.GetVector(i);

            status = status && (vecp.id == id);
            ErrorAssert(vecp.id == id, LOG_TAG_TEST,
                        "uint16 vector set returned wrong vector id. Expected=%lu, Actual=%lu",
                        id._id, vecp.id._id);
            status = status && (vecp.vec == vec1);
            ErrorAssert(vecp.vec == vec1, LOG_TAG_TEST,
                        "uint16 vector set returned wrong vector. Expected=%s, Actual=%s",
                        vec1.ToString(dim).ToCStr(), vecp.vec.ToString(dim).ToCStr());

            status = status && (set1.Contains(id));
            ErrorAssert(set1.Contains(id), LOG_TAG_TEST,
                        "vector set does not contain vector id %lu", id._id);
            status = status && (set1.GetIndex(id) == i);
            ErrorAssert(set1.GetIndex(id) == i, LOG_TAG_TEST,
                        "vector set returned wrong index for vector id %lu. Expected=%hu, Actual=%hu",
                        id._id, i, set1.GetIndex(id));
            status = status && (set1.GetVectorByID(id) == vec1);
            ErrorAssert(set1.GetVectorByID(id) == vec1, LOG_TAG_TEST,
                        "vector set returned wrong vector. Expected=%s, Actual=%s",
                        vec1.ToString(dim).ToCStr(), set1.GetVectorByID(id).ToString(dim).ToCStr());
        }

        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Cluster1: %s", set1.ToString().ToCStr());
        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Cluster2: %s", set2.ToString().ToCStr());

        set1.DeleteLast();
        status = status && (set1.Size() == size - 1);
        ErrorAssert(set1.Size() == size - 1, LOG_TAG_TEST,
                    "vector set size mismatch after delete. Expected=%hu, Actual=%hu",
                    size - 1, set1.Size());
        status = status && !(set1.Contains(_ids[size - 1]));
        ErrorAssert(!(set1.Contains(_ids[size - 1])), LOG_TAG_TEST,
                    "uint16 vector set should not contain vector id %lu", _ids[size - 1]);

        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Cluster1 after deletion: %s", set1.ToString().ToCStr());

        DestroyCluster(set1p);
        DestroyCluster(set2p);

        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_vector::vertex_cluster.");
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

    divftree::VTYPE _data[size][dim] = {{0.2f, 25.6f, -12.2f, 1.112f, 36.0f, 7.5f, -3.3f, 8.8f},
                                     {9.1f, -4.6f, 5.5f, 2.2f, 3.3f, -1.1f, 6.6f, 7.7f},
                                     {8.8f, 9.9f, -10.1f, 11.2f, 12.3f, -13.4f, 14.5f, 15.6f}};
    uint64_t _ids[size] = {1ul, 2ul, 3ul};

friend class TestBase<Test>;
};
};

int main(int argc, char *argv[]) {
    std::set<std::string> default_black_list = {};
    UT::TestBase<UT::Test> test(argc, argv, default_black_list);
    return test.Run();
}

// Todo update run_ut so that we can use the same regexs and white/black lists across all ut files
