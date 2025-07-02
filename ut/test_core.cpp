// #include "test.h"

// #include "distance.h"
// #include "buffer.h"
// #include "dummy_copper.h"

// // build using: ./build_ut.sh -DLOG_MIN_LEVEL=LOG_LEVEL_ERROR -DLOG_LEVEL=LOG_LEVEL_DEBUG -DLOG_TAG=LOG_TAG_COPPER_NODE
// namespace UT {
// class Test {
// public:
//     Test() {
//         tests["test_core::distance_L2"] = &Test::distance_L2;
//         tests["test_core::clustering"] = &Test::clustering;

//         test_priority["test_core::distance_L2"] = 0;
//         test_priority["test_core::clustering"] = 1;

//         all_tests.insert("test_core::distance_L2");
//         all_tests.insert("test_core::clustering");
//     }

//     ~Test() {}

//     bool distance_L2() {
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_core::distance_L2 for %luth time...", try_count);
//         bool status = true;
//         std::string data0 = "{", data1 = "{", data2 = "{";
//         for (uint16_t i = 0; i < dim; ++i) {
//             data0 += std::to_string(_data16[0][i]);
//             data1 += std::to_string(_data16[1][i]);
//             data2 += std::to_string(_data16[2][i]);
//             if (i < dim - 1) {
//                 data0 += ", ";
//                 data1 += ", ";
//                 data2 += ", ";
//             }
//             else {
//                 data0 += "}";
//                 data1 += "}";
//                 data2 += "}";
//             }
//         }
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Data1=%s", data0.c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Data2=%s", data1.c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Data3=%s", data2.c_str());

//         std::string dataf0 = "{", dataf1 = "{", dataf2 = "{";
//         for (uint16_t i = 0; i < dim; ++i) {
//             dataf0 += std::to_string(_dataf[0][i]);
//             dataf1 += std::to_string(_dataf[1][i]);
//             dataf2 += std::to_string(_dataf[2][i]);
//             if (i < dim - 1) {
//                 dataf0 += ", ";
//                 dataf1 += ", ";
//                 dataf2 += ", ";
//             }
//             else {
//                 dataf0 += "}";
//                 dataf1 += "}";
//                 dataf2 += "}";
//             }
//         }
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Dataf1=%s", dataf0.c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Dataf2=%s", dataf1.c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Dataf3=%s", dataf2.c_str());

//         copper::L2_Distance<uint16_t, dim, double> _du;
//         copper::L2_Distance<float, dim, double> _df;

//         double udist[size][size], fdist[size][size];
//         double b_udist[size][size], b_fdist[size][size];

//         std::string udist_str = "";
//         std::string fdist_str = "";
//         std::string b_udist_str = "";
//         std::string b_fdist_str = "";

//         for (uint16_t i = 0; i < size; ++i) {
//             for (uint16_t j = 0; j < size; ++j) {
//                 if (j > i)
//                     continue;

//                 b_udist[i][j] = 0;
//                 b_fdist[i][j] = 0;
//                 for (uint16_t k = 0; k < dim; ++k) {
//                     b_udist[i][j] += (((double)_data16[i][k] - (double)_data16[j][k]) *
//                                       ((double)_data16[i][k] - (double)_data16[j][k]));
//                     b_fdist[i][j] += (((double)_dataf[i][k] - (double)_dataf[j][k]) *
//                                       ((double)_dataf[i][k] - (double)_dataf[j][k]));
//                 }
//                 udist[i][j] = _du(copper::Vector<uint16_t, dim>(_data16[i]), copper::Vector<uint16_t, dim>(_data16[j]));
//                 fdist[i][j] = _df(copper::Vector<float, dim>(_dataf[i]), copper::Vector<float, dim>(_dataf[j]));

//                 status = status && (udist[i][j] == b_udist[i][j]);
//                 ErrorAssert(udist[i][j] == b_udist[i][j], LOG_TAG_TEST,
//                             "uint16 distance calculation err: calc=%f, ground_truth=%f", udist[i][j], b_udist[i][j]);

//                 status = status && (fdist[i][j] == b_fdist[i][j]);
//                 ErrorAssert(fdist[i][j] == b_fdist[i][j], LOG_TAG_TEST,
//                             "float distance calculation err: calc=%f, ground_truth=%f", fdist[i][j], b_fdist[i][j]);

//                 udist_str += "<" + std::to_string(i) + ", " + std::to_string(j) + ">" + std::to_string(udist[i][j]);
//                 fdist_str += "<" + std::to_string(i) + ", " + std::to_string(j) + ">" + std::to_string(fdist[i][j]);
//                 b_udist_str += "<" + std::to_string(i) + ", " + std::to_string(j) + ">" + std::to_string(b_udist[i][j]);
//                 b_fdist_str += "<" + std::to_string(i) + ", " + std::to_string(j) + ">" + std::to_string(b_fdist[i][j]);
//                 if (j < size - 1) {
//                     udist_str += ", ";
//                     fdist_str += ", ";
//                     b_udist_str += ", ";
//                     b_fdist_str += ", ";
//                 }
//             }
//         }
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "udist: %s", udist_str.c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_udist: %s", b_udist_str.c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fdist: %s", fdist_str.c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_fdist: %s", b_fdist_str.c_str());

//         const uint16_t uquery[dim] = {2, 1, 5, 3, 2, 8, 15, 16};
//         const float fquery[dim] = {0.15f, 20.4f, -9.0f, 1.1f, 10.0f, 6.0f, 5.1f, 9.55f};

//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "uquery: %s", copper::Vector<uint16_t, dim>(uquery).to_string().c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fquery: %s", copper::Vector<float, dim>(fquery).to_string().c_str());

//         double uquery_dist[size], fquery_dist[size], b_uquery_dist[size], b_fquery_dist[size];
//         std::string uquery_dist_str = "";
//         std::string fquery_dist_str = "";
//         std::string b_uquery_dist_str = "";
//         std::string b_fquery_dist_str = "";
//         for (uint16_t i = 0; i < size; ++i) {
//             b_uquery_dist[i] = 0;
//             b_fquery_dist[i] = 0;
//             for (uint16_t k = 0; k < dim; ++k) {
//                 b_uquery_dist[i] += (((double)uquery[k] - (double)_data16[i][k]) *
//                                      ((double)uquery[k] - (double)_data16[i][k]));
//                 b_fquery_dist[i] += (((double)fquery[k] - (double)_dataf[i][k]) *
//                                      ((double)fquery[k] - (double)_dataf[i][k]));
//             }
//             uquery_dist[i] = _du(copper::Vector<uint16_t, dim>(uquery), copper::Vector<uint16_t, dim>(_data16[i]));
//             fquery_dist[i] = _df(copper::Vector<float, dim>(fquery), copper::Vector<float, dim>(_dataf[i]));

//             status = status && (uquery_dist[i] == b_uquery_dist[i]);
//             ErrorAssert(uquery_dist[i] == b_uquery_dist[i], LOG_TAG_TEST,
//                         "uint16 distance calculation err: calc=%f, ground_truth=%f", uquery_dist[i], b_uquery_dist[i]);

//             status = status && (fquery_dist[i] == b_fquery_dist[i]);
//             ErrorAssert(fquery_dist[i] == b_fquery_dist[i], LOG_TAG_TEST,
//                         "float distance calculation err: calc=%f, ground_truth=%f", fquery_dist[i], b_fquery_dist[i]);

//             uquery_dist_str += std::to_string(uquery_dist[i]);
//             fquery_dist_str += std::to_string(fquery_dist[i]);
//             b_uquery_dist_str += std::to_string(b_uquery_dist[i]);
//             b_fquery_dist_str += std::to_string(b_fquery_dist[i]);
//             if (i < size - 1) {
//                 uquery_dist_str += ", ";
//                 fquery_dist_str += ", ";
//                 b_uquery_dist_str += ", ";
//                 b_fquery_dist_str += ", ";
//             }
//         }

//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "uquery_dist: %s", uquery_dist_str.c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_uquery_dist: %s", b_uquery_dist_str.c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fquery_dist: %s", fquery_dist_str.c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_fquery_dist: %s", b_fquery_dist_str.c_str());

//         /* The most similar vector to query should be first -> for L2 that is the minimum distance */
//         std::sort(uquery_dist, uquery_dist + size, _du);
//         std::sort(fquery_dist, fquery_dist + size, _df);
//         for (uint16_t i = 0; i < size - 1; ++i) {
//             status = status && (uquery_dist[i] <= uquery_dist[i + 1]);
//             ErrorAssert(uquery_dist[i] <= uquery_dist[i + 1], LOG_TAG_TEST,
//                         "uint16 l2 similarity err: %f should be less than %f", uquery_dist[i], uquery_dist[i + 1]);
//             status = status && (fquery_dist[i] <= fquery_dist[i + 1]);
//             ErrorAssert(fquery_dist[i] <= fquery_dist[i + 1], LOG_TAG_TEST,
//                         "float l2 similarity err: %f should be less than %f", fquery_dist[i], fquery_dist[i + 1]);
//         }
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "uquery_dist_sorted: %s", copper::Vector<double, size>(uquery_dist).to_string().c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fquery_dist_sorted: %s", copper::Vector<double, size>(fquery_dist).to_string().c_str());

//         copper::Vector<uint16_t, dim> ucent = _du.Compute_Centroid((const uint16_t*)_data16, size);
//         copper::Vector<float, dim> fcent = _df.Compute_Centroid((const float*)_dataf, size);

//         copper::Vector<uint16_t, dim> b_ucent;
//         copper::Vector<float, dim> b_fcent;
//         for (uint16_t i = 0; i < dim; ++i) {
//             b_ucent[i] = 0;
//             b_fcent[i] = 0;
//             for (uint16_t j = 0; j < size; ++j) {
//                 b_ucent[i] += _data16[j][i];
//                 b_fcent[i] += _dataf[j][i];
//             }
//             b_ucent[i] /= size;
//             b_fcent[i] /= size;
//         }

//         status = status && (ucent == b_ucent);
//         ErrorAssert(ucent == b_ucent, LOG_TAG_TEST,
//                     "uint16 centroid calculation err: calc=%s, ground_truth=%s", ucent.to_string().c_str(),
//                     b_ucent.to_string().c_str());
//         status = status && (fcent == b_fcent);
//         ErrorAssert(fcent == b_fcent, LOG_TAG_TEST,
//                     "float centroid calculation err: calc=%s, ground_truth=%s", fcent.to_string().c_str(),
//                     b_fcent.to_string().c_str());

//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "ucent: %s", ucent.to_string().c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_ucent: %s", b_ucent.to_string().c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "fcent: %s", fcent.to_string().c_str());
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "b_fcent: %s", b_fcent.to_string().c_str());

//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_core::distance_L2.");
//         return status;
//     }

//     bool clustering() {
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Running test_core::clustering for %luth time...", try_count);
//         bool status = true;
//         constexpr uint16_t DIM = 3;
//         constexpr uint16_t MIN_SIZE = 2;
//         constexpr uint16_t MAX_SIZE = 8;
//         const uint16_t data[MAX_SIZE][DIM] = {{1, 2, 3},
//                                               {4, 5, 6},
//                                               {7, 8, 9},
//                                               {10, 11, 12},
//                                               {13, 14, 15},
//                                               {16, 17, 18},
//                                               {19, 20, 21},
//                                               {22, 23, 24}};
//         copper::VectorID ids[MAX_SIZE];
//         copper::BufferManager<uint16_t, DIM, MIN_SIZE, MAX_SIZE, MIN_SIZE, MAX_SIZE,
//                                double, copper::Simple_Divide_L2> _bufmgr;
//         using Node = copper::CopperNode<uint16_t, DIM, MIN_SIZE, MAX_SIZE,
//                                          double, copper::Simple_Divide_L2>;

//         _bufmgr.Init();
//         copper::VectorID node_id = _bufmgr.Record_Root();
//         copper::RetStatus rs = _bufmgr.UpdateClusterAddress(node_id, new Node(node_id));
//         status = status && rs.IsOK();
//             ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to update cluster address for node_id: " VECTORID_LOG_FMT, VECTORID_LOG(node_id));
//         uint16_t level = 2;

//         copper::CopperNode<uint16_t, DIM, MIN_SIZE, MAX_SIZE, double, copper::Simple_Divide_L2> *node =
//             _bufmgr.template Get_Node<Node>(node_id);

//         status = status && (node != nullptr);
//         ErrorAssert(node != nullptr, LOG_TAG_VECTOR_INDEX, "Node should not be nullptr for node_id: " VECTORID_LOG_FMT, VECTORID_LOG(node_id));

//         for (uint16_t i = 0; i < MAX_SIZE; ++i) {
//             ids[i] = _bufmgr.Record_Vector(0);
//             copper::Address vec_add = node->Insert(copper::Vector<uint16_t, DIM>(data[i]), ids[i]);
//             status = status && (vec_add != copper::INVALID_ADDRESS);
//             ErrorAssert(vec_add != copper::INVALID_ADDRESS, LOG_TAG_VECTOR_INDEX,
//                         "Failed to insert vector with id: " VECTORID_LOG_FMT, VECTORID_LOG(ids[i]));
//             rs = _bufmgr.UpdateVectorAddress(ids[i], vec_add);
//             status = status && rs.IsOK();
//             ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to insert vector with id: " VECTORID_LOG_FMT, VECTORID_LOG(ids[i]));
//         }

//         copper::VectorID _root_id = _bufmgr.Record_Root();
//         status = status && (_root_id.Is_Valid());
//         ErrorAssert(_root_id.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Root ID should"
//                     "not be invalid: " VECTORID_LOG_FMT, VECTORID_LOG(_root_id));
//         Node* new_root = new Node(_root_id);
//         status = status && (new_root != nullptr);
//         ErrorAssert(new_root != nullptr, LOG_TAG_VECTOR_INDEX, "Failed to create new root node for root_id: " VECTORID_LOG_FMT, VECTORID_LOG(_root_id));
//         rs = _bufmgr.UpdateClusterAddress(_root_id, new_root);
//         status = status && rs.IsOK();
//         ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to update cluster address for root_id: " VECTORID_LOG_FMT, VECTORID_LOG(_root_id));
//         rs = _bufmgr.UpdateVectorAddress(_root_id, new_root->Insert(node->Compute_Current_Centroid(), node_id));
//         status = status && rs.IsOK();
//         ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to update vector address for root_id: " VECTORID_LOG_FMT, VECTORID_LOG(_root_id));
//         rs = node->Assign_Parent(_root_id);
//         status = status && rs.IsOK();
//         ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to assign parent for node_id: " VECTORID_LOG_FMT, VECTORID_LOG(node_id));
//         ++level;


//         std::vector<Node*> nodes;
//         nodes.push_back(node);
//         std::vector<copper::Vector<uint16_t, DIM>> centroids;
//         copper::Simple_Divide_L2<uint16_t, DIM> _core;
//         rs = _core.template Cluster<Node, MIN_SIZE, MAX_SIZE, MIN_SIZE, MAX_SIZE>(nodes, 0, centroids, 2, _bufmgr);
//         status = status && rs.IsOK();
//         ErrorAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Clustering failed with error: %s", rs.Msg());

//         for (uint16_t i = 0; i < centroids.size(); ++i) {
//             CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Centroid %u: %s", i, centroids[i].to_string().c_str());
//         }

//         CLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "End of test_core::clustering.");
//         return status;
//     }

//     void Init(size_t t_count) {
//         try_count = t_count;
//     }

//     void Destroy() {
//         try_count = 0;
//     }

//     std::set<std::string> all_tests;
//     std::map<std::string, int> test_priority;
// protected:
//     std::map<std::string, bool (Test::*)()> tests;
//     size_t try_count = 0;

//     static constexpr uint16_t dim = 8;
//     static constexpr uint16_t size = 3;

//     const uint16_t _data16[size][dim] = {{1, 2, 3, 4, 5, 6, 7, 8},
//                                          {9, 10, 11, 12, 13, 14, 15, 16},
//                                          {17, 18, 19, 20, 21, 22, 23, 24}};
//     const uint64_t _ids16[size] = {1ul, 2ul, 3ul};

//     const float _dataf[size][dim] = {{0.2f, 25.6f, -12.2f, 1.112f, 36.0f, 7.5f, -3.3f, 8.8f},
//                                      {9.1f, -4.6f, 5.5f, 2.2f, 3.3f, -1.1f, 6.6f, 7.7f},
//                                      {8.8f, 9.9f, -10.1f, 11.2f, 12.3f, -13.4f, 14.5f, 15.6f}};
//     const uint64_t _idsf[size] = {4ul, 5ul, 6ul};

// friend class TestBase<Test>;
// };
// };

int main(int argc, char *argv[]) {
    // std::set<std::string> default_black_list = {};
    // UT::TestBase<UT::Test> test(argc, argv, default_black_list);
    // return test.Run();
}

// // Todo update run_ut so that we can use the same regexs and white/black lists across all ut files
