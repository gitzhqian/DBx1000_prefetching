#include "global.h"
#include "mem_alloc.h"
#include "stats.h"
#include "dl_detect.h"
#include "manager.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"

mem_alloc mem_allocator;
Stats stats;
DL_detect dl_detector;
Manager * glob_manager;
Query_queue * query_queue;
Plock part_lock_man;
OptCC occ_man;
#if CC_ALG == VLL
VLLMan vll_man;
#endif 

bool volatile warmup_finish = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;
#ifndef NOGRAPHITE
carbon_barrier_t enable_barrier;
#endif

ts_t g_abort_penalty = ABORT_PENALTY;
bool g_central_man = CENTRAL_MAN;
UInt32 g_ts_alloc = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_no_dl = NO_DL;
ts_t g_timeout = TIMEOUT;
ts_t g_dl_loop_detect = DL_LOOP_DETECT;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
UInt32 g_ts_batch_num = TS_BATCH_NUM;

bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;
UInt32 g_cc_alg = CC_ALG;
ts_t g_query_intvl = QUERY_INTVL;
UInt32 g_part_per_txn = PART_PER_TXN;
double g_perc_multi_part = PERC_MULTI_PART;
double g_read_perc = READ_PERC;
double g_write_perc = WRITE_PERC;
double g_insert_perc = INSERT_PERC;
double g_scan_perc = SCAN_PERC;
double g_zipf_theta = ZIPF_THETA;
bool g_prt_lat_distr = PRT_LAT_DISTR;
UInt32 g_part_cnt = PART_CNT;
UInt32 g_virtual_part_cnt = VIRTUAL_PART_CNT;
UInt32 g_thread_cnt = THREAD_CNT;
UInt64 g_synth_table_size = SYNTH_TABLE_SIZE;
UInt32 g_req_per_query = REQ_PER_QUERY;
UInt32 g_field_per_tuple = FIELD_PER_TUPLE;
UInt32 g_init_parallelism = INIT_PARALLELISM;

std::vector<char *> secondary_keys;
tbb::concurrent_set<uint64_t> distinct_search_keys;
//std::vector<std::pair<uint64_t,uint64_t>> history_search_keys;
//tbb::concurrent_hash_map<uint64_t, uint64_t>  history_search_keys;
void *root_node;
tbb::concurrent_hash_map<uint64_t, std::vector<std::pair<void *,void *>>> keys_paths;
std::vector<void *> distance_4_paths;
std::vector<void *> distance_5_paths;
std::vector<void *> distance_6_paths;
std::vector<void *> distance_6_paths_k;
tbb::concurrent_set<void *> distance_6_paths_set;
tbb::concurrent_set<void *> distance_3_paths_set;
tbb::concurrent_set<void *> distance_4_paths_set;
tbb::concurrent_set<void *> distance_5_paths_set;
std::map<uint64_t,uint64_t> distance_6_paths_map;
//std::queue<void *> history_requests;
uint64_t total_chain_length;


UInt32 g_num_wh = NUM_WH;
double g_perc_payment = PERC_PAYMENT;
bool g_wh_update = WH_UPDATE;
char * output_file = NULL;

map<string, string> g_params;

#if TPCC_SMALL
UInt32 g_max_items = 10000;
UInt32 g_cust_per_dist = 2000;
#else 
UInt32 g_max_items = 100000;
UInt32 g_cust_per_dist = 3000;
#endif
uint64_t g_max_orderline = uint64_t(1) << 32;