#include <algorithm>
#include "query.h"
#include "ycsb_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "ycsb.h"
#include "table.h"
#include "benchmark_common.h"

uint64_t ycsb_query::the_n = 0;
double ycsb_query::denom = 0;

void ycsb_query::init(uint64_t thd_id, workload * h_wl, Query_thd * query_thd, std::vector<uint64_t> &insert_keys, UInt32 qid) {
	_query_thd = query_thd;
	requests = (ycsb_request *) 
		mem_allocator.alloc(sizeof(ycsb_request) * g_req_per_query, thd_id);
	part_to_access = (uint64_t *) 
		mem_allocator.alloc(sizeof(uint64_t) * g_part_per_txn, thd_id);
	zeta_2_theta = zeta(2, g_zipf_theta);
	assert(the_n != 0);
	assert(denom != 0);
	ycsb_index = h_wl->indexes["MAIN_INDEX"];
	gen_requests(thd_id, h_wl, insert_keys, qid);
}

void 
ycsb_query::calculateDenom()
{
	assert(the_n == 0);
	uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
	the_n = table_size - 1;
	denom = zeta(the_n, g_zipf_theta);
}

// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug. 
// The original paper says zeta(theta, 2.0). But I guess it should be 
// zeta(2.0, theta).
double ycsb_query::zeta(uint64_t n, double theta) {
	double sum = 0;
	for (uint64_t i = 1; i <= n; i++) 
		sum += pow(1.0 / i, theta);
	return sum;
}

uint64_t ycsb_query::zipf(uint64_t n, double theta) {
	assert(this->the_n == n);
	assert(theta == g_zipf_theta);
	double alpha = 1 / (1 - theta);
	double zetan = denom;
	double eta = (1 - pow(2.0 / n, 1 - theta)) / 
		(1 - zeta_2_theta / zetan);
	double u; 
	drand48_r(&_query_thd->buffer, &u);
	double uz = u * zetan;
	if (uz < 1) return 1;
	if (uz < 1 + pow(0.5, theta)) return 2;
	return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}

void ycsb_query::gen_requests(uint64_t thd_id, workload * h_wl, std::vector<uint64_t> &insert_keys, UInt32 qid) {
#if CC_ALG == HSTORE
	assert(g_virtual_part_cnt == g_part_cnt);
#endif
	int access_cnt = 0;
	set<uint64_t> all_keys;
//    std::set<ycsb_request> all_reqs;
	part_num = 0;
	double r = 0;
	int64_t rint64 = 0;
	drand48_r(&_query_thd->buffer, &r);
	lrand48_r(&_query_thd->buffer, &rint64);
	if (r < g_perc_multi_part) {
		for (UInt32 i = 0; i < g_part_per_txn; i++) {
			if (i == 0 && FIRST_PART_LOCAL)
				part_to_access[part_num] = thd_id % g_virtual_part_cnt;
			else {
				part_to_access[part_num] = rint64 % g_virtual_part_cnt;
			}
			UInt32 j;
			for (j = 0; j < part_num; j++) 
				if ( part_to_access[part_num] == part_to_access[j] )
					break;
			if (j == part_num)
				part_num ++;
		}
	} else {
		part_num = 1;
		if (FIRST_PART_LOCAL)
			part_to_access[0] = thd_id % g_part_cnt;
		else
			part_to_access[0] = rint64 % g_part_cnt;
	}


	int rid = 0;
    //auto table_size_ = h_wl->tables["MAIN_TABLE"]->get_table_size();
    //ZipfDistribution zipf_s(table_size_ - 1, g_zipf_theta);
	for (UInt32 tmp = 0; tmp < g_req_per_query; tmp ++) {
		double r;
		drand48_r(&_query_thd->buffer, &r);
		ycsb_request * req = &requests[rid];

		if (g_read_perc == 1){
            req->rtype = RO;
		}else{
            if (r < g_read_perc) {
                req->rtype = RD;
            } else if (r >= g_read_perc && r <= g_write_perc + g_read_perc) {
                req->rtype = WR;
            } else if(r >= (g_write_perc + g_read_perc) && r <= (g_write_perc + g_read_perc + g_scan_perc)) {
                req->rtype = SCAN;
                req->scan_len = SCAN_LEN;
            } else{
                req->rtype = INS;
            }
		}

		// the request will access part_id.
		uint64_t ith = tmp * part_num / g_req_per_query;
		uint64_t part_id =  part_to_access[ ith ];
		uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
		uint64_t row_id = zipf(table_size - 1, g_zipf_theta);
		assert(row_id < table_size);
		uint64_t primary_key = row_id * g_virtual_part_cnt + part_id;

		req->key = primary_key;
		int64_t rint64;
		lrand48_r(&_query_thd->buffer, &rint64);
		req->value = rint64 % (1<<8);
		// Make sure a single row is not accessed twice
		if (req->rtype == RD || req->rtype == RO || req->rtype == WR) {
#if SECONDARY_INDEX == true
            req->s_key = secondary_keys[row_id];
#else
            if (all_keys.find(req->key) == all_keys.end()) {
				all_keys.insert(req->key);
//                all_search_keys.insert(req->key);
				access_cnt ++;

				std::array<std::pair<void *, void *>, 10> paths;
                ycsb_index->GetKeyPaths(req->key, KEY_SIZE, paths);
                req->paths = paths;
			} else {
			    continue;
			}
#endif
		} else if(req->rtype == INS){
            uint64_t insert_k = insert_keys[qid];
            req->key = insert_k;
            all_keys.insert(req->key);
            access_cnt ++;
            g_key_order = false;
		} else {
			bool conflict = false;
            //row_id = zipf_s.GetNextNumber();
            if (all_keys.find(req->key) == all_keys.end()) {
                all_keys.insert(req->key);
                access_cnt += SCAN_LEN;

                std::array<std::pair<uint64_t,std::array<std::pair<void *, void *>, 10>>,3> scan_paths;
                uint64_t start_key=req->key;
                int i =0;
                int to_scan = 1;
                if (SCAN_LEN == 20){
                    to_scan = 2;
                } else if(SCAN_LEN == 30){
                    to_scan = 3;
                }
                for (int len = 0; len <SCAN_LEN&&i<to_scan;  ) {
                    std::array<std::pair<void *, void *>, 10> paths;
                    std::pair<uint64_t,int> last_key = ycsb_index->GetKeyScanPaths(start_key, KEY_SIZE, paths );
                    scan_paths[i] = std::make_pair(start_key,paths);
                    start_key = last_key.first;
                    len=len+last_key.second;
                    i++;
                }
                req->scan_paths = scan_paths;
            } else {
                continue;
            }

//			for (UInt32 i = 0; i < req->scan_len; i++) {
//				primary_key = (row_id + i) * g_part_cnt + part_id;
//				if (all_keys.find( primary_key ) != all_keys.end() ){
//                    conflict = true;
//				}
//			}
//			if (conflict) {
//			    continue;
//			} else {
//				for (UInt32 i = 0; i < req->scan_len; i++){
//                    all_keys.insert( (row_id + i) * g_part_cnt + part_id);
//				}
//
//				access_cnt += SCAN_LEN;
//			}
		}
		rid ++;
	}
	request_cnt = rid;

//    for (auto reqq = all_reqs.cbegin();  reqq != all_reqs.cend();  ++reqq  ) {
//        ycsb_request *req = *reqq;
//        std::vector<std::pair<void *,void *>> acc = req->paths;
//        if (acc.empty() || acc.size()>10) continue;
//
//    }

	// Sort the requests in key order.
	if (g_key_order) {
		for (int i = request_cnt - 1; i > 0; i--) 
			for (int j = 0; j < i; j ++)
				if (requests[j].key > requests[j + 1].key) {
					ycsb_request tmp = requests[j];
					requests[j] = requests[j + 1];
					requests[j + 1] = tmp;
				}
		for (UInt32 i = 0; i < request_cnt - 1; i++){
            assert(requests[i].key < requests[i + 1].key);
		}

	}

}


