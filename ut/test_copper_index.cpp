#define PRINT_BUCKET true

#define VECTOR_TYPE uint16_t
#define VTYPE_FMT "%hu"
#define DISTANCE_TYPE double
#define DTYPE_FMT "%lf"

#include "test.h"

#include "copper.h"

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_COPPER_NODE
namespace UT {
class Test {
public:
    Test() {
        tests["test_copper_index::copper_node_test"] = &Test::copper_node_test;
        tests["test_copper_index::copper_index_simple_test"] = &Test::copper_index_simple_test;

        test_priority["test_copper_index::copper_node_test"] = 0;
        test_priority["test_copper_index::copper_index_simple_test"] = 1;

        all_tests.insert("test_copper_index::copper_node_test");
        all_tests.insert("test_copper_index::copper_index_simple_test");

    }

    ~Test() {}

    bool copper_node_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_copper_index::copper_node_test for %luth time...", try_count);
        bool status = true;
        copper::RetStatus rs = copper::RetStatus::Success();
        copper::VectorID vec_id = copper::INVALID_VECTOR_ID;
        vec_id._id = 0;
        vec_id._level = 1;
        static constexpr uint16_t size = 8;
        const uint64_t _ids[size] = {0ul, 1ul, 2ul, 3ul,
                                       4ul, 5ul, 6ul, 7ul};
        copper::CopperAttributes attr;
        attr.core.dimention = dim;
        attr.core.distanceAlg = copper::DistanceType::L2Distance;
        attr.core.clusteringAlg = copper::ClusteringType::SimpleDivide;
        attr.internal_max_size = KI_MAX;
        attr.internal_min_size = KI_MIN;
        attr.leaf_max_size = KL_MAX;
        attr.leaf_min_size = KL_MIN;
        attr.split_internal = KI_MAX / 2;
        attr.split_leaf = KL_MAX / 2;
        copper::VectorIndex _tree(attr);


        copper::CopperNode *node = (copper::CopperNode *)_tree.CreateNewNode(vec_id);
        for(uint16_t i = 0; i < size; ++i) {
            copper::Vector vec(_data[i], dim);
            copper::Address res = node->Insert(vec, _ids[i]);
            status = status && (res != copper::INVALID_ADDRESS);
            ErrorAssert(res != copper::INVALID_ADDRESS, LOG_TAG_TEST,
                        "Insert failed for vector id %lu", _ids[i]);
            status = status && (node->Size() == i + 1);
            ErrorAssert(node->Size() == i + 1, LOG_TAG_TEST,
                        "Node size mismatch. Expected=%hu, Actual=%hu", i + 1, node->Size());
            status = status && (node->Contains(_ids[i]));
            ErrorAssert(node->Contains(_ids[i]), LOG_TAG_TEST,
                        "Node should contain vector id %lu", _ids[i]);
            copper::Vector back_vec = node->_bucket.GetLastVector();
            copper::VectorID back_id = node->_bucket.GetLastVectorID();
            copper::VectorPair last_vec = node->_bucket[i];
            status = status && (back_vec == last_vec.vec);
            ErrorAssert(back_vec == last_vec.vec, LOG_TAG_TEST,
                        "Last vector should be same as the current vector. i=%hu, back_vec=%s, last_vec=%s", i,
                        back_vec.ToString(dim).ToCStr(), last_vec.vec.ToString(dim).ToCStr());
            status = status && (back_id == last_vec.id);
            ErrorAssert(back_id == last_vec.id, LOG_TAG_TEST,
                        "Last vector id should be same as the current vector id. i=%hu, back_id=" VECTORID_LOG_FMT
                        ", last_vec.id=" VECTORID_LOG_FMT, i, VECTORID_LOG(back_id), VECTORID_LOG(last_vec.id));
            status = status && (back_id == _ids[i]);
            ErrorAssert(back_id == _ids[i], LOG_TAG_TEST,
                        "Last vector id should be same as the current vector id. i=%hu, back_id=" VECTORID_LOG_FMT
                        ", _ids[i]=%lu", i, VECTORID_LOG(back_id), _ids[i]);
            status = status && (back_vec != vec);
            ErrorAssert(back_vec != vec, LOG_TAG_TEST,
                        "Last vector should not be same as the current vector. i=%hu, back_vec=%s, vec=%s", i,
                        back_vec.ToString(dim).ToCStr(), vec.ToString(dim).ToCStr());
            status = status && (back_vec.Similar(vec, dim));
            ErrorAssert(back_vec.Similar(vec, dim), LOG_TAG_TEST,
                        "Last vector should be a copy of the current vector. i=%hu, back_vec=%s, vec=%s", i,
                        back_vec.ToString(dim).ToCStr(), vec.ToString(dim).ToCStr());
        }

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Node:" NODE_LOG_FMT, NODE_PTR_LOG(node));
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Node Bucket: %s", node->_bucket.ToString().ToCStr());

        vec_id._val += 1;
        copper::CopperNode *other_node = (copper::CopperNode *)_tree.CreateNewNode(vec_id);
        copper::VectorUpdate update = node->MigrateLastVectorTo(other_node);
        status = status && (update.vector_id == other_node->_bucket.GetLastVectorID());
        ErrorAssert(update.vector_id == other_node->_bucket.GetLastVectorID(), LOG_TAG_TEST,
                    "Last vector id should be same as the current vector id. update.vector_id=" VECTORID_LOG_FMT
                    ", other_node->_bucket.Get_Last_VectorID()=" VECTORID_LOG_FMT, VECTORID_LOG(update.vector_id),
                    VECTORID_LOG(other_node->_bucket.GetLastVectorID()));
        status = status && (update.vector_data == other_node->_bucket.GetLastVector().GetData());
        ErrorAssert(update.vector_data == other_node->_bucket.GetLastVector().GetData(), LOG_TAG_TEST,
                    "Last vector address should be same as the current vector address. update.vector_data=%p, "
                    "other_node->_bucket.GetLastVector().GetData()=%p", update.vector_data,
                    other_node->_bucket.GetLastVector().GetData());
        status = status && (node->Size() == size - 1);
        ErrorAssert(node->Size() == size - 1, LOG_TAG_TEST,
                    "Node size mismatch. Expected=%hu, Actual=%hu", size - 1, node->Size());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Node:" NODE_LOG_FMT, NODE_PTR_LOG(node));
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Node Bucket: %s", node->_bucket.ToString().ToCStr());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Other Node:" NODE_LOG_FMT, NODE_PTR_LOG(other_node));
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Other Node Bucket: %s", other_node->_bucket.ToString().ToCStr());

        std::vector<std::pair<copper::VectorID, copper::DTYPE>> neighbours;
        copper::Vector query(_data[3], dim);
        query[0] = 12;
        rs = node->Search(query, 3, neighbours);
        status = status && (rs.IsOK());
        ErrorAssert(rs.IsOK(), LOG_TAG_TEST, "ApproximateKNearestNeighbours failed with status %s.", rs.Msg());
        status = status && (neighbours.size() == 3);
        ErrorAssert(neighbours.size() == 3, LOG_TAG_TEST,
                    "ApproximateKNearestNeighbours should return 3 neighbours. size=%lu", neighbours.size());
        std::sort_heap(neighbours.begin(), neighbours.end(), node->GetSimilarityComparator(true));

        PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_TEST,
                                "ApproximateKNearestNeighbours: Batch After sort ");

        status = status && (neighbours[0].first == _ids[3]);
        ErrorAssert(neighbours[0].first == _ids[3], LOG_TAG_TEST,
                    "First neighbour id should be same as the current vector id. neighbours[0].first=" VECTORID_LOG_FMT
                    ", _ids[3]=%lu", VECTORID_LOG(neighbours[0].first), _ids[3]);

        status = status && (neighbours[1].first == _ids[2]);
        ErrorAssert(neighbours[1].first == _ids[2], LOG_TAG_TEST,
                    "Second neighbour id should be same as the current vector id. neighbours[1].first=" VECTORID_LOG_FMT
                    ", _ids[2]=%lu", VECTORID_LOG(neighbours[1].first), _ids[2]);

        status = status && (neighbours[2].first == _ids[4]);
        ErrorAssert(neighbours[2].first == _ids[4], LOG_TAG_TEST,
                    "Third neighbour id should be same as the current vector id. neighbours[2].first=" VECTORID_LOG_FMT
                    ", _ids[4]=%lu", VECTORID_LOG(neighbours[2].first), _ids[4]);

        neighbours.clear();

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_copper_index::copper_node_test.");
        return status;
    }

    bool copper_index_simple_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_copper_index::copper_index_simple_test for %luth time...", try_count);
        bool status = true;
        copper::RetStatus rs = copper::RetStatus::Success();

        copper::CopperAttributes attr;
        attr.core.dimention = dim;
        attr.core.distanceAlg = copper::DistanceType::L2Distance;
        attr.core.clusteringAlg = copper::ClusteringType::SimpleDivide;
        attr.internal_max_size = KI_MAX;
        attr.internal_min_size = KI_MIN;
        attr.leaf_max_size = KL_MAX;
        attr.leaf_min_size = KL_MIN;
        attr.split_internal = KI_MAX / 2;
        attr.split_leaf = KL_MAX / 2;
        copper::VectorIndex _tree(attr);

        size_t num_nodes = 4;
        std::vector<std::pair<copper::VectorID, copper::DTYPE>> neighbours;
        std::vector<std::pair<copper::VectorID, copper::Vector>> vectors;
        for (size_t i = 0; i < num_nodes; ++i) {
            for (size_t j = i; j < max_size; j += num_nodes) {
                copper::VectorID vec_id = copper::INVALID_VECTOR_ID;
                rs = _tree.Insert(copper::Vector(_data[j], dim), vec_id, 1);
                status = status && rs.IsOK();
                ErrorAssert(rs.IsOK(), LOG_TAG_TEST, "Insert failed with status %s.", rs.Msg());
                status = status && vec_id.IsValid();
                ErrorAssert(vec_id.IsValid(), LOG_TAG_TEST, "Vector ID should be valid after insertion. vec_id="
                            VECTORID_LOG_FMT, VECTORID_LOG(vec_id));

                vectors.emplace_back(vec_id, copper::Vector(_data[j], dim));
                CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Inserted vector %lu: " VECTORID_LOG_FMT ", data=%s",
                     j, VECTORID_LOG(vec_id), vectors.back().second.ToString(dim).ToCStr());
            }
        }

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Index:%s", _tree.ToString().ToCStr());

        copper::Vector target;
        target.Create(dim);
        target[0] = 5;
        target[1] = 15;
        target[2] = 6;
        target[3] = 17;
        size_t k = 6;
        std::vector<std::pair<copper::VectorID, copper::DTYPE>> knn, ann;

        /* This test case has 10 leaves so by checkning 10 nodes at each layer, we should get the exact KNN */
        rs = _tree.ApproximateKNearestNeighbours(target, k, 10, 10, ann, true, true);
        status = status && rs.IsOK();
        ErrorAssert(rs.IsOK(), LOG_TAG_TEST, "ApproximateKNearestNeighbours failed with status %s.", rs.Msg());
        status = status && (ann.size() == k);
        ErrorAssert(ann.size() == k, LOG_TAG_TEST,
                    "ApproximateKNearestNeighbours should return %lu neighbours. size=%lu", k, ann.size());
        PRINT_VECTOR_PAIR_BATCH(ann, LOG_TAG_TEST,
                                "ApproximateKNearestNeighbours: Batch After sort ");

        rs = copper::VectorIndexInterface::KNearestNeighbours(target, k, dim, vectors, knn,
                                                              copper::DistanceType::L2Distance, true, true);
        status = status && rs.IsOK();
        ErrorAssert(rs.IsOK(), LOG_TAG_TEST, "KNearestNeighbours failed with status %s.", rs.Msg());
        status = status && (knn.size() == k);
        ErrorAssert(knn.size() == k, LOG_TAG_TEST,
                    "KNearestNeighbours should return %lu neighbours. size=%lu", k, knn.size());

        PRINT_VECTOR_PAIR_BATCH(knn, LOG_TAG_TEST,
                                "KNearestNeighbours: Batch After sort ");

        for (size_t i = 0; i < k; ++i) {
            status = status && (ann[i].first == knn[i].first);
            ErrorAssert(ann[i].first == knn[i].first, LOG_TAG_TEST,
                        "Neighbour %lu should have the same id. ann[%lu].first=" VECTORID_LOG_FMT
                        ", knn[%lu].first=" VECTORID_LOG_FMT, i, i, VECTORID_LOG(ann[i].first),
                        i, VECTORID_LOG(knn[i].first));
            status = status && (ann[i].second == knn[i].second);
            ErrorAssert(ann[i].second == knn[i].second, LOG_TAG_TEST,
                        "Neighbour %lu should have the same vector. ann[%lu].second=" DTYPE_FMT
                        ", knn[%lu].second=" DTYPE_FMT, i, i,
                        ann[i].second, i, knn[i].second);
        }

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_copper_index::copper_index_simple_test.");
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

    static constexpr uint16_t dim = 4;
    static constexpr uint16_t max_size = 32;

    static constexpr uint16_t KI_MAX = 4, KI_MIN = 2;
    static constexpr uint16_t KL_MAX = 8, KL_MIN = 2;

    const copper::VTYPE _data[max_size][dim] = {
        {1, 2, 3, 4},
        {5, 6, 7, 8},
        {9, 10, 11, 12},
        {13, 14, 15, 16},
        {17, 18, 19, 20},
        {21, 22, 23, 24},
        {25, 26, 27, 28},
        {29, 30, 31, 32},
        {33, 34, 35, 36},
        {37, 38, 39, 40},
        {41, 42, 43, 44},
        {45, 46, 47, 48},
        {49, 50, 51, 52},
        {53, 54, 55, 56},
        {57, 58, 59, 60},
        {61, 62, 63, 64},
        {65, 66, 67, 68},
        {69, 70, 71, 72},
        {73, 74, 75, 76},
        {77, 78, 79, 80},
        {81, 82, 83, 84},
        {85, 86, 87, 88},
        {89, 90, 91, 92},
        {93, 94, 95, 96},
        {97, 98, 99,100},
        {101,102,103,104},
        {105,106,107,108},
        {109,110,111,112},
        {113,114,115,116},
        {117,118,119,120},
        {121,122,123,124},
        {125,126,127,128},
    };
friend class TestBase<Test>;
};
};

int main(int argc, char *argv[]) {
    std::set<std::string> default_black_list = {};
    UT::TestBase<UT::Test> test(argc, argv, default_black_list);
    return test.Run();
}

// Todo update run_ut so that we can use the same regexs and white/black lists across all ut files
