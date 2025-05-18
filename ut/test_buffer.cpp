#include "test.h"

#include "buffer.h"
#include "dummy_copper.h"

// build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_COPPER_NODE
namespace UT {
class Test {
public:
    Test() {
        tests["test_buffer::insert_test"] = &Test::insert_test;

        test_priority["test_buffer::insert_test"] = 0;

        all_tests.insert("test_buffer::insert_test");

    }

    ~Test() {}

    bool insert_test() {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_buffer::insert_test for %luth time...", try_count);
        bool status = true;
        constexpr uint16_t KI_MAX = 4, KI_MIN = 2;
        constexpr uint16_t KL_MAX = 8, KL_MIN = 1;
        copper::Buffer_Manager<uint16_t, dim, KI_MIN, KI_MAX, KL_MIN, KL_MAX,
            uint16_t, copper::L2_Distance<uint16_t, dim, double>> _buffer_manager;
        using LeafNode = copper::Copper_Node<uint16_t, dim, KL_MIN, KL_MAX,
            uint16_t, copper::L2_Distance<uint16_t, dim, double>>;
        using InternalNode = copper::Copper_Node<uint16_t, dim, KI_MIN, KI_MAX,
            uint16_t, copper::L2_Distance<uint16_t, dim, double>>;

        copper::RetStatus rs = _buffer_manager.Init();
        status = status && (rs.Is_OK());
        ErrorAssert(rs.Is_OK(), LOG_TAG_TEST, "Buffer manager init failed with status %s.", rs.Msg());

        copper::VectorID first_root_id = copper::INVALID_VECTOR_ID;
        first_root_id._id = 0;
        first_root_id._level = 1;

        copper::VectorID cur_root_id = _buffer_manager.Record_Root();
        status = status && (cur_root_id == first_root_id);
        ErrorAssert(cur_root_id == first_root_id, LOG_TAG_TEST, "First Root ID should be same as first ID. First ID: " VECTORID_LOG_FMT
            ", cur_root_id: " VECTORID_LOG_FMT, VECTORID_LOG(first_root_id), VECTORID_LOG(cur_root_id));
        rs = _buffer_manager.UpdateClusterAddress(cur_root_id, new LeafNode(cur_root_id));
        status = status && (rs.Is_OK());
        ErrorAssert(rs.Is_OK(), LOG_TAG_TEST, "Buffer manager update cluster address failed with status %s.", rs.Msg());

        uint16_t num_vec = 32;
        uint64_t root_level = 1;
        std::vector<uint64_t> vecs;

        copper::VectorID vec_id = _buffer_manager.Record_Vector(0);
        status = status && (vec_id == 0);
        ErrorAssert(vec_id == 0, LOG_TAG_TEST, "first Vector ID should be 0.");
        vecs.emplace_back(vec_id._id);
        vecs.emplace_back(cur_root_id._id);

        for(uint16_t i = 0; i < num_vec; ++i) {
            uint64_t level = 0, j = i;
            while (j > 0) {
                ++level;
                j = j / (level == 0 ? KL_MAX : KI_MAX);
            }
            if (level >= root_level) {
                cur_root_id = _buffer_manager.Record_Root();
                status = status && (cur_root_id._level == root_level + 1);
                ErrorAssert(cur_root_id._level == root_level + 1, LOG_TAG_TEST, "Root level should be %u.", root_level + 1);
                root_level = cur_root_id._level;
                status = status && (cur_root_id._val == 0);
                ErrorAssert(cur_root_id._val == 0, LOG_TAG_TEST, "Root val should be 0.");
                vecs.emplace_back(cur_root_id._id);
                rs = _buffer_manager.UpdateClusterAddress(cur_root_id, new InternalNode(cur_root_id));
                status = status && (rs.Is_OK());
                ErrorAssert(rs.Is_OK(), LOG_TAG_TEST, "Buffer manager update cluster address failed with status %s.", rs.Msg());
            }
            status = status && (root_level == vecs.size() - 1);
            ErrorAssert(root_level == vecs.size() - 1, LOG_TAG_TEST, "Root level should be %u.", vecs.size() - 1);

            vec_id = _buffer_manager.Record_Vector(level);
            status = status && (vec_id._level == level);
            ErrorAssert(vec_id._level == level, LOG_TAG_TEST, "Vector level should be %u.", level);
            status = status && (vec_id == (vecs[level] + 1));
            ErrorAssert(vec_id == (vecs[level]+1), LOG_TAG_TEST, "Vector ID " VECTORID_LOG_FMT
                " should be %u.", VECTORID_LOG(vec_id), (vecs[level]+1));
            vecs[level] = vec_id._id;
            if (level == 1) {
                rs = _buffer_manager.UpdateClusterAddress(vec_id, new LeafNode(vec_id));
                status = status && (rs.Is_OK());
                ErrorAssert(rs.Is_OK(), LOG_TAG_TEST, "Buffer manager update cluster address failed with status %s.", rs.Msg());
            }
            else if (level > 1) {
                rs = _buffer_manager.UpdateClusterAddress(vec_id, new InternalNode(vec_id));
                status = status && (rs.Is_OK());
                ErrorAssert(rs.Is_OK(), LOG_TAG_TEST, "Buffer manager update cluster address failed with status %s.", rs.Msg());
            }
        }

        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Buffer Manager:%s", _buffer_manager.to_string().c_str());

        rs = _buffer_manager.Shutdown();
        status = status && (rs.Is_OK());
        ErrorAssert(rs.Is_OK(), LOG_TAG_TEST, "Buffer manager shutdown failed with status %s.", rs.Msg());
        CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_buffer::insert_test.");
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
