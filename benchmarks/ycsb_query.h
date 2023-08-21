#ifndef _YCSB_QUERY_H_
#define _YCSB_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"
#include "btree_store.h"

class workload;
class Query_thd;
// Each ycsb_query contains several ycsb_requests, 
// each of which is a RD, WR to a single table

class ycsb_request {
public:
	access_t rtype; 
	uint64_t key;
	char *s_key = nullptr;
	char value;
	// only for (qtype == SCAN)
	UInt32 scan_len = 0;
    std::array<std::pair<void *, void *>, 10> paths;
    std::array<std::pair<uint64_t,std::array<std::pair<void *, void *>, 10>>,3> scan_paths;
//    std::vector<void *> paths;
};

class ycsb_query : public base_query {
public:
	void init(uint64_t thd_id, workload * h_wl) { assert(false); };
	void init(uint64_t thd_id, workload * h_wl, Query_thd * query_thd, std::vector<uint64_t> &insert_keys, UInt32 qid);
	static void calculateDenom();

	uint64_t request_cnt;
	ycsb_request * requests;
    INDEX *ycsb_index;

private:
	void gen_requests(uint64_t thd_id, workload * h_wl, std::vector<uint64_t> &insert_keys, UInt32 qid);
	// for Zipfian distribution
	static double zeta(uint64_t n, double theta);
	uint64_t zipf(uint64_t n, double theta);
	
	static uint64_t the_n;
	static double denom;
	double zeta_2_theta;
	Query_thd * _query_thd;
};

#endif
