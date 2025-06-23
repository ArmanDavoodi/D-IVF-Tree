#include "test.h"

#include "copper.h"

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_COPPER_NODE
namespace UT {
class Test {
public:
    Test() {
        tests["test_copper_index::copper_node_test"] = &Test::copper_node_test;

        test_priority["test_copper_index::copper_node_test"] = 0;

        all_tests.insert("test_copper_index::copper_node_test");

    }

    ~Test() {}

    bool copper_node_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_copper_index::copper_node_test for %luth time...", try_count);
        bool status = true;
        // copper::RetStatus rs = copper::RetStatus::Success();
        // copper::VectorID vec_id = copper::INVALID_VECTOR_ID;
        // vec_id._id = 0;
        // vec_id._level = 1;
        // const uint64_t _ids16[size] = {0ul, 1ul, 2ul, 3ul,
        //                                4ul, 5ul, 6ul, 7ul};


        // LeafNode node(vec_id);
        // for(uint16_t i = 0; i < size; ++i) {
        //     copper::Vector<uint16_t, dim> vec(_data16[i]);
        //     copper::Address res = node.Insert(vec, _ids16[i]);
        //     status = status && (res != copper::INVALID_ADDRESS);
        //     ErrorAssert(res != copper::INVALID_ADDRESS, LOG_TAG_TEST,
        //                 "Insert failed for vector id %lu", _ids16[i]);
        //     status = status && (node.Size() == i + 1);
        //     ErrorAssert(node.Size() == i + 1, LOG_TAG_TEST,
        //                 "Node size mismatch. Expected=%hu, Actual=%hu", i + 1, node.Size());
        //     status = status && (node.Contains(_ids16[i]));
        //     ErrorAssert(node.Contains(_ids16[i]), LOG_TAG_TEST,
        //                 "Node should contain vector id %lu", _ids16[i]);
        //     copper::Vector<uint16_t, dim> back_vec = node._bucket.Get_Last_Vector();
        //     copper::VectorID back_id = node._bucket.Get_Last_VectorID();
        //     copper::VectorPair<uint16_t, dim> last_vec = node._bucket[i];
        //     status = status && (back_vec.Are_The_Same(last_vec.vector));
        //     ErrorAssert(back_vec.Are_The_Same(last_vec.vector), LOG_TAG_TEST,
        //                 "Last vector should be same as the current vector. i=%hu, back_vec=%s, last_vec=%s", i,
        //                 back_vec.to_string().c_str(), last_vec.vector.to_string().c_str());
        //     status = status && (back_id == last_vec.id);
        //     ErrorAssert(back_id == last_vec.id, LOG_TAG_TEST,
        //                 "Last vector id should be same as the current vector id. i=%hu, back_id=" VECTORID_LOG_FMT
        //                 ", last_vec.id=" VECTORID_LOG_FMT, i, VECTORID_LOG(back_id), VECTORID_LOG(last_vec.id));
        //     status = status && (back_id == _ids16[i]);
        //     ErrorAssert(back_id == _ids16[i], LOG_TAG_TEST,
        //                 "Last vector id should be same as the current vector id. i=%hu, back_id=" VECTORID_LOG_FMT
        //                 ", _ids16[i]=%lu", i, VECTORID_LOG(back_id), _ids16[i]);
        //     status = status && (!back_vec.Are_The_Same(vec));
        //     ErrorAssert(!back_vec.Are_The_Same(vec), LOG_TAG_TEST,
        //                 "Last vector should not be same as the current vector. i=%hu, back_vec=%s, vec=%s", i,
        //                 back_vec.to_string().c_str(), vec.to_string().c_str());
        //     status = status && (back_vec == vec);
        //     ErrorAssert(back_vec == vec, LOG_TAG_TEST,
        //                 "Last vector should be a copy of the current vector. i=%hu, back_vec=%s, vec=%s", i,
        //                 back_vec.to_string().c_str(), vec.to_string().c_str());
        // }

        // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Node:" NODE_LOG_FMT, NODE_PTR_LOG(&node, false));
        // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Node Bucket: %s", node._bucket.to_string().c_str());

        // vec_id._val += 1;
        // LeafNode other_node(vec_id);
        // copper::VectorUpdate update = node.MigrateLastVectorTo(&other_node);
        // status = status && (update.vector_id == other_node._bucket.Get_Last_VectorID());
        // ErrorAssert(update.vector_id == other_node._bucket.Get_Last_VectorID(), LOG_TAG_TEST,
        //             "Last vector id should be same as the current vector id. update.vector_id=" VECTORID_LOG_FMT
        //             ", other_node._bucket.Get_Last_VectorID()=" VECTORID_LOG_FMT, VECTORID_LOG(update.vector_id),
        //             VECTORID_LOG(other_node._bucket.Get_Last_VectorID()));
        // status = status && (update.vector_data == other_node._bucket.Get_Last_Vector().Get_Address());
        // ErrorAssert(update.vector_data == other_node._bucket.Get_Last_Vector().Get_Address(), LOG_TAG_TEST,
        //             "Last vector address should be same as the current vector address. update.vector_data=%p, "
        //             "other_node._bucket.Get_Last_Vector().Get_Address()=%p", update.vector_data,
        //             other_node._bucket.Get_Last_Vector().Get_Address());
        // status = status && (node.Size() == size - 1);
        // ErrorAssert(node.Size() == size - 1, LOG_TAG_TEST,
        //             "Node size mismatch. Expected=%hu, Actual=%hu", size - 1, node.Size());
        // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Node:" NODE_LOG_FMT, NODE_PTR_LOG(&node, false));
        // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Node Bucket: %s", node._bucket.to_string().c_str());
        // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Other Node:" NODE_LOG_FMT, NODE_PTR_LOG(&other_node, false));
        // CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Other Node Bucket: %s", other_node._bucket.to_string().c_str());

        // const uint16_t target[dim] = {18, 19, 23, 14};
        // copper::Vector<uint16_t, dim> target_vec(_data16[1]);
        // // copper::VectorID nearest = node.Find_Nearest(target_vec);
        // // status = status && (nearest == _ids16[1]);
        // // ErrorAssert(nearest == _ids16[1], LOG_TAG_TEST,
        // //             "Nearest vector id should be same as the current vector id. nearest=" VECTORID_LOG_FMT
        // //             ", _ids16[1]=%lu", VECTORID_LOG(nearest), _ids16[1]);

        // // nearest = node.Find_Nearest(copper::Vector<uint16_t, dim>(target));
        // // status = status && (nearest == _ids16[4]);
        // // ErrorAssert(nearest == _ids16[4], LOG_TAG_TEST,
        // //             "Nearest vector id should be same as the current vector id. nearest=" VECTORID_LOG_FMT
        // //             ", _ids16[4]=%lu", VECTORID_LOG(nearest), _ids16[4]);

        // std::vector<std::pair<copper::VectorID, double>> neighbours;
        // copper::Vector<uint16_t, dim> query(_data16[3]);
        // query[0] = 12;
        // rs = node.ApproximateKNearestNeighbours(query, 3, 7, neighbours);
        // status = status && (rs.Is_OK());
        // ErrorAssert(rs.Is_OK(), LOG_TAG_TEST, "ApproximateKNearestNeighbours failed with status %s.", rs.Msg());
        // status = status && (neighbours.size() == 3);
        // ErrorAssert(neighbours.size() == 3, LOG_TAG_TEST,
        //             "ApproximateKNearestNeighbours should return 3 neighbours. size=%lu", neighbours.size());
        // std::sort_heap(neighbours.begin(), neighbours.end(), _more_similar);

        // status = status && (neighbours[0].first == _ids16[3]);
        // ErrorAssert(neighbours[0].first == _ids16[3], LOG_TAG_TEST,
        //             "First neighbour id should be same as the current vector id. neighbours[0].first=" VECTORID_LOG_FMT
        //             ", _ids16[3]=%lu", VECTORID_LOG(neighbours[0].first), _ids16[3]);

        // status = status && (neighbours[1].first == _ids16[2]);
        // ErrorAssert(neighbours[1].first == _ids16[2], LOG_TAG_TEST,
        //             "Second neighbour id should be same as the current vector id. neighbours[1].first=" VECTORID_LOG_FMT
        //             ", _ids16[2]=%lu", VECTORID_LOG(neighbours[1].first), _ids16[2]);

        // status = status && (neighbours[2].first == _ids16[4]);
        // ErrorAssert(neighbours[2].first == _ids16[4], LOG_TAG_TEST,
        //             "Third neighbour id should be same as the current vector id. neighbours[2].first=" VECTORID_LOG_FMT
        //             ", _ids16[4]=%lu", VECTORID_LOG(neighbours[2].first), _ids16[4]);

        // neighbours.clear();

        // /* Todo: change when a better sampling alg is used */
        // rs = node.ApproximateKNearestNeighbours(query, 3, 4, neighbours);
        // status = status && (rs.Is_OK());
        // ErrorAssert(rs.Is_OK(), LOG_TAG_TEST, "ApproximateKNearestNeighbours failed with status %s.", rs.Msg());
        // status = status && (neighbours.size() == 3);
        // ErrorAssert(neighbours.size() == 3, LOG_TAG_TEST,
        //             "ApproximateKNearestNeighbours should return 3 neighbours. size=%lu", neighbours.size());
        // std::sort_heap(neighbours.begin(), neighbours.end(), _more_similar);

        // status = status && (neighbours[0].first == _ids16[3]);
        // ErrorAssert(neighbours[0].first == _ids16[3], LOG_TAG_TEST,
        //             "First neighbour id should be same as the current vector id. neighbours[0].first=" VECTORID_LOG_FMT
        //             ", _ids16[3]=%lu", VECTORID_LOG(neighbours[0].first), _ids16[3]);

        // status = status && (neighbours[1].first == _ids16[2]);
        // ErrorAssert(neighbours[1].first == _ids16[2], LOG_TAG_TEST,
        //             "Second neighbour id should be same as the current vector id. neighbours[1].first=" VECTORID_LOG_FMT
        //             ", _ids16[2]=%lu", VECTORID_LOG(neighbours[1].first), _ids16[2]);

        // status = status && (neighbours[2].first == _ids16[1]);
        // ErrorAssert(neighbours[2].first == _ids16[1], LOG_TAG_TEST,
        //             "Third neighbour id should be same as the current vector id. neighbours[2].first=" VECTORID_LOG_FMT
        //             ", _ids16[1]=%lu", VECTORID_LOG(neighbours[2].first), _ids16[1]);

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_copper_index::copper_node_test.");
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
    static constexpr uint16_t size = 8;

    static constexpr uint16_t KI_MAX = 4, KI_MIN = 2;
    static constexpr uint16_t KL_MAX = 8, KL_MIN = 2;

    using LeafNode = copper::Copper_Node<uint16_t, dim, KL_MIN, KL_MAX, double, copper::Simple_Divide_L2>;
    using InternalNode = copper::Copper_Node<uint16_t, dim, KI_MIN, KI_MAX, double, copper::Simple_Divide_L2>;

    const uint16_t _data16[size][dim] = {
        {1, 2, 3, 4},
        {5, 6, 7, 8},
        {9, 10, 11, 12},
        {13, 14, 15, 16},
        {17, 18, 19, 20},
        {21, 22, 23, 24},
        {25, 26, 27, 28},
        {29, 30, 31, 32}
    };

    struct _DIST_ID_PAIR_SIMILARITY {
        copper::L2_Distance<uint16_t, dim, double> _cmp;

        _DIST_ID_PAIR_SIMILARITY(copper::L2_Distance<uint16_t, dim, double> _d) : _cmp(_d) {}
        inline bool operator()(const std::pair<copper::VectorID, double>& a, const std::pair<copper::VectorID, double>& b) const {
            return _cmp(a.second, b.second);
        }
    };

    _DIST_ID_PAIR_SIMILARITY _more_similar{copper::L2_Distance<uint16_t, dim, double>()};

friend class TestBase<Test>;
};
};

int main(int argc, char *argv[]) {
    std::set<std::string> default_black_list = {};
    UT::TestBase<UT::Test> test(argc, argv, default_black_list);
    return test.Run();
}

// Todo update run_ut so that we can use the same regexs and white/black lists across all ut files
