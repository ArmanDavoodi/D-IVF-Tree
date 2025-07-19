#include "test.h"

#define VECTOR_TYPE float
#define VTYPE_FMT "%f"
#define DISTANCE_TYPE double
#define DTYPE_FMT "%lf"

#include "distance.h"

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_DIVFTREE_VERTEX
namespace UT {
class Test {
public:
    Test() {
        tests["test_distance::distance_L2"] = &Test::distance_L2;

        test_priority["test_distance::distance_L2"] = 0;

        all_tests.insert("test_distance::distance_L2");
    }

    ~Test() {}

    bool distance_L2() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_distance::distance_L2 for %luth time...", try_count);
        bool status = true;
        std::string data0 = "{", data1 = "{", data2 = "{";
        for (uint16_t i = 0; i < dim; ++i) {
            data0 += std::to_string(_data[0][i]);
            data1 += std::to_string(_data[1][i]);
            data2 += std::to_string(_data[2][i]);
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

        divftree::DTYPE dist[size][size], base_dist[size][size];

        std::string dist_str = "";
        std::string base_dist_str = "";

        for (uint16_t i = 0; i < size; ++i) {
            for (uint16_t j = 0; j < size; ++j) {
                if (j > i)
                    continue;

                base_dist[i][j] = 0;
                for (uint16_t k = 0; k < dim; ++k) {
                    base_dist[i][j] += (((divftree::DTYPE)_data[i][k] - (divftree::DTYPE)_data[j][k]) *
                                        ((divftree::DTYPE)_data[i][k] - (divftree::DTYPE)_data[j][k]));
                }
                dist[i][j] = divftree::L2::Distance(divftree::Vector(_data[i], dim), divftree::Vector(_data[j], dim), dim);

                status = status && (dist[i][j] == base_dist[i][j]);
                ErrorAssert(dist[i][j] == base_dist[i][j], LOG_TAG_TEST,
                            "Distance calculation err: calc=" DTYPE_FMT ", ground_truth=" DTYPE_FMT,
                            dist[i][j], base_dist[i][j]);

                dist_str += "<" + std::to_string(i) + ", " + std::to_string(j) + ">" + std::to_string(dist[i][j]);
                base_dist_str += "<" + std::to_string(i) + ", " + std::to_string(j) + ">" + std::to_string(base_dist[i][j]);
                if (j < size - 1) {
                    dist_str += ", ";
                    base_dist_str += ", ";
                }
            }
        }
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "dist: %s", dist_str.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "base_dist: %s", base_dist_str.c_str());

        const divftree::VTYPE query[dim] = {0.15f, 20.4f, -9.0f, 1.1f, 10.0f, 6.0f, 5.1f, 9.55f};

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "query: %s", divftree::Vector(query, dim).ToString(dim).ToCStr());

        divftree::DTYPE query_dist[size], base_query_dist[size];
        std::string query_dist_str = "";
        std::string base_query_dist_str = "";
        for (uint16_t i = 0; i < size; ++i) {
            base_query_dist[i] = 0;
            for (uint16_t k = 0; k < dim; ++k) {
                base_query_dist[i] += (((divftree::DTYPE)query[k] - (divftree::DTYPE)_data[i][k]) *
                                       ((divftree::DTYPE)query[k] - (divftree::DTYPE)_data[i][k]));
            }
            query_dist[i] = divftree::L2::Distance(divftree::Vector(query, dim), divftree::Vector(_data[i], dim), dim);

            status = status && (query_dist[i] == base_query_dist[i]);
            ErrorAssert(query_dist[i] == base_query_dist[i], LOG_TAG_TEST,
                        "Distance calculation err: calc=" DTYPE_FMT ", ground_truth=" DTYPE_FMT,
                        query_dist[i], base_query_dist[i]);

            query_dist_str += std::to_string(query_dist[i]);
            base_query_dist_str += std::to_string(base_query_dist[i]);
            if (i < size - 1) {
                query_dist_str += ", ";
                base_query_dist_str += ", ";
            }
        }

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "query_dist_str: %s", query_dist_str.c_str());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "base_query_dist_str: %s", base_query_dist_str.c_str());

        /* The most similar vector to query should be first -> for L2 that is the minimum distance */
        std::sort(query_dist, query_dist + size, divftree::L2::MoreSimilar);
        for (uint16_t i = 0; i < size - 1; ++i) {
            status = status && (query_dist[i] <= query_dist[i + 1]);
            ErrorAssert(query_dist[i] <= query_dist[i + 1], LOG_TAG_TEST,
                        "uint16 l2 similarity err: " DTYPE_FMT " should be less than " DTYPE_FMT,
                        query_dist[i], query_dist[i + 1]);
        }
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "query_dist_sorted: %s", divftree::Vector(query_dist, size).
            ToString(size).ToCStr());

        divftree::Vector cent = divftree::L2::ComputeCentroid((const divftree::VTYPE*)_data, size, dim);

        divftree::Vector base_cent;
        base_cent.Create(dim);
        for (uint16_t i = 0; i < dim; ++i) {
            base_cent[i] = 0;
            for (uint16_t j = 0; j < size; ++j) {
                base_cent[i] += _data[j][i];
            }
            base_cent[i] /= size;
        }

        status = status && (cent.Similar(base_cent, dim));
        ErrorAssert(cent.Similar(base_cent, dim), LOG_TAG_TEST,
                    "Centroid calculation err: calc=%s, ground_truth=%s", cent.ToString(dim).ToCStr(),
                    base_cent.ToString(dim).ToCStr());

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "cent: %s", cent.ToString(dim).ToCStr());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "base_cent: %s", base_cent.ToString(dim).ToCStr());

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_distance::distance_L2.");
        return status;
    }

    // bool clustering() {
    //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_distance::clustering for %luth time...", try_count);
    //     bool status = true;
    //     constexpr uint16_t DIM = 3;
    //     constexpr uint16_t MIN_SIZE = 2;
    //     constexpr uint16_t MAX_SIZE = 8;
    //     const uint16_t data[MAX_SIZE][DIM] = {{1, 2, 3},
    //                                           {4, 5, 6},
    //                                           {7, 8, 9},
    //                                           {10, 11, 12},
    //                                           {13, 14, 15},
    //                                           {16, 17, 18},
    //                                           {19, 20, 21},
    //                                           {22, 23, 24}};
    //     divftree::VectorID ids[MAX_SIZE];
    //     divftree::BufferManager<uint16_t, DIM, MIN_SIZE, MAX_SIZE, MIN_SIZE, MAX_SIZE,
    //                            double, divftree::Simple_Divide_L2> _bufmgr;
    //     using Vertex = divftree::DIVFTreeVertex<uint16_t, DIM, MIN_SIZE, MAX_SIZE,
    //                                      double, divftree::Simple_Divide_L2>;

    //     _bufmgr.Init();
    //     divftree::VectorID vertex_id = _bufmgr.Record_Root();
    //     divftree::RetStatus rs = _bufmgr.UpdateClusterAddress(vertex_id, new Vertex(vertex_id));
    //     status = status && rs.IsOK();
    //         ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to update cluster address for vertex_id: " VECTORID_LOG_FMT, VECTORID_LOG(vertex_id));
    //     uint16_t level = 2;

    //     divftree::DIVFTreeVertex<uint16_t, DIM, MIN_SIZE, MAX_SIZE, double, divftree::Simple_Divide_L2> *vertex =
    //         _bufmgr.template Get_Vertex<Vertex>(vertex_id);

    //     status = status && (vertex != nullptr);
    //     ErrorAssert(vertex != nullptr, LOG_TAG_VECTOR_INDEX, "Vertex should not be nullptr for vertex_id: " VECTORID_LOG_FMT, VECTORID_LOG(vertex_id));

    //     for (uint16_t i = 0; i < MAX_SIZE; ++i) {
    //         ids[i] = _bufmgr.Record_Vector(0);
    //         divftree::Address vec_add = vertex->Insert(divftree::Vector<uint16_t, DIM>(data[i]), ids[i]);
    //         status = status && (vec_add != divftree::INVALID_ADDRESS);
    //         ErrorAssert(vec_add != divftree::INVALID_ADDRESS, LOG_TAG_VECTOR_INDEX,
    //                     "Failed to insert vector with id: " VECTORID_LOG_FMT, VECTORID_LOG(ids[i]));
    //         rs = _bufmgr.UpdateVectorAddress(ids[i], vec_add);
    //         status = status && rs.IsOK();
    //         ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to insert vector with id: " VECTORID_LOG_FMT, VECTORID_LOG(ids[i]));
    //     }

    //     divftree::VectorID _root_id = _bufmgr.Record_Root();
    //     status = status && (_root_id.Is_Valid());
    //     ErrorAssert(_root_id.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Root ID should"
    //                 "not be invalid: " VECTORID_LOG_FMT, VECTORID_LOG(_root_id));
    //     Vertex* new_root = new Vertex(_root_id);
    //     status = status && (new_root != nullptr);
    //     ErrorAssert(new_root != nullptr, LOG_TAG_VECTOR_INDEX, "Failed to create new root vertex for root_id: " VECTORID_LOG_FMT, VECTORID_LOG(_root_id));
    //     rs = _bufmgr.UpdateClusterAddress(_root_id, new_root);
    //     status = status && rs.IsOK();
    //     ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to update cluster address for root_id: " VECTORID_LOG_FMT, VECTORID_LOG(_root_id));
    //     rs = _bufmgr.UpdateVectorAddress(_root_id, new_root->Insert(vertex->Compute_Current_Centroid(), vertex_id));
    //     status = status && rs.IsOK();
    //     ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to update vector address for root_id: " VECTORID_LOG_FMT, VECTORID_LOG(_root_id));
    //     rs = vertex->Assign_Parent(_root_id);
    //     status = status && rs.IsOK();
    //     ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to assign parent for vertex_id: " VECTORID_LOG_FMT, VECTORID_LOG(vertex_id));
    //     ++level;


    //     std::vector<Vertex*> vertices;
    //     vertices.push_back(vertex);
    //     std::vector<divftree::Vector<uint16_t, DIM>> centroids;
    //     divftree::Simple_Divide_L2<uint16_t, DIM> _core;
    //     rs = _core.template Cluster<Vertex, MIN_SIZE, MAX_SIZE, MIN_SIZE, MAX_SIZE>(vertices, 0, centroids, 2, _bufmgr);
    //     status = status && rs.IsOK();
    //     ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Clustering failed with error: %s", rs.Msg());

    //     for (uint16_t i = 0; i < centroids.size(); ++i) {
    //         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Centroid %u: %s", i, centroids[i].to_string().c_str());
    //     }

    //     CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_distance::clustering.");
    //     return status;
    // }

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

    const divftree::VTYPE _data[size][dim] = {{0.2f, 25.6f, -12.2f, 1.112f, 36.0f, 7.5f, -3.3f, 8.8f},
                                            {9.1f, -4.6f, 5.5f, 2.2f, 3.3f, -1.1f, 6.6f, 7.7f},
                                            {8.8f, 9.9f, -10.1f, 11.2f, 12.3f, -13.4f, 14.5f, 15.6f}};
    const uint64_t _ids[size] = {1ul, 2ul, 3ul};

friend class TestBase<Test>;
};
};

int main(int argc, char *argv[]) {
    std::set<std::string> default_black_list = {};
    UT::TestBase<UT::Test> test(argc, argv, default_black_list);
    return test.Run();
}

// Todo update run_ut so that we can use the same regexs and white/black lists across all ut files
