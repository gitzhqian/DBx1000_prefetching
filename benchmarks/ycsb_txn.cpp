#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "btree_store.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"
#include "benchmark_common.h"

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (ycsb_wl *) h_wl;
}

char *ycsb_txn_man::ycsb_row_data(uint32_t tuple_size){
    char *new_data = (char *)_mm_malloc(tuple_size, 64);
    return new_data;
}
void ycsb_txn_man::copy_row_to_table(char *dest, char *src, uint32_t sz){
    memcpy(dest, src, sz);
}
RC ycsb_txn_man::run_txn(base_query * query) {
	RC rc;
	ycsb_query * m_query = (ycsb_query *) query;
	ycsb_wl * wl = (ycsb_wl *) h_wl;
	itemid_t * m_item = NULL;
    row_t * row = NULL;
    void *vd_row = NULL;
    void *vd_row_s = NULL;
    void *master_row = NULL;
    row_cnt = 0;
    Catalog * schema = wl->the_table->get_schema();
    auto request_cnt = m_query->request_cnt;
    auto tuple_size = _wl->the_table->schema->get_tuple_size();

	for (uint32_t rid = 0; rid < request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
//        printf("req key:%lu , type:%u.\n",req->key, req->rtype);
		int part_id = wl->key_to_part( req->key );
		bool finish_req = false;
		UInt32 iteration = 0;
        char *row_;
        uint32_t scan_range = req->scan_len;
        static thread_local unique_ptr<Iterator> scan_iter;
        access_t type = req->rtype;
        uint64_t record_pointer;
        row_t * row_local;
        char update_data = 'g';
        auto fild_count = schema->get_field_cnt();
        std::array<std::pair<void *, void *>,10> ptr_vector = req->paths;
        std::array<std::pair<uint64_t,std::array<std::pair<void *, void *>, 10>>,3> ptr_scan_vector = req->scan_paths;
//        history_requests.push(reinterpret_cast<void *>(req));
#if JUMP_PREFETCHING_CHAIN
        if (type == RD) {
            distinct_search_keys.insert(req->key);
        }
#endif

		while (!finish_req) {
#if SECONDARY_INDEX == true
            if (type == RO) {
                rc = _wl->the_s_index->index_read_s(req->s_key, S1_KEY_SIZE, vd_row_s);
            }else if (type == SCAN && iteration == 0) {
                scan_iter = _wl->the_s_index->RangeScanBySize(req->s_key,
                                                              S1_KEY_SIZE, scan_range);
                auto get_next_row_ = scan_iter ->GetNextS();
                if (get_next_row_ == nullptr) {
                    break;
                }else{
                    vd_row_s = get_next_row_;
                }
            }else if(type == SCAN && iteration != 0) {
                auto get_next_row_ = scan_iter ->GetNextS();
                if (get_next_row_ == nullptr) {
                    break;
                }else{
                    vd_row_s = get_next_row_;
                }
            }

            if (rc == Abort){
                goto final;
            }
            assert(rc == RCOK);
            auto row_s = reinterpret_cast<row_t *>(vd_row_s);//meta
            auto row_data = row_s->data;
            if (row_data == nullptr){
                rc = Abort;
                goto final;
            }
#if ENGINE_TYPE == PTR0 || ENGINE_TYPE == PTR1
            uint64_t pky = *reinterpret_cast<uint64_t *>(row_data+S1_KEY_SIZE);
            vd_row = index_read(_wl->the_index, pky, part_id);
            if (vd_row == nullptr){
                rc = Abort;
                goto final;
			}
#elif ENGINE_TYPE == PTR2
            record_pointer = *reinterpret_cast<uint64_t *>(row_data+S1_KEY_SIZE);
#endif
#else
///1.read the index
            if (type == RD || type == RO || type == WR) {
                vd_row = index_read(_wl->the_index, req->key, part_id, type, ptr_vector);
			}else if (type == SCAN && iteration == 0){
                uint64_t scan_start_key = req->key;
                uint64_t starttime = get_sys_clock();
                scan_iter = _wl->the_index->RangeScanBySize(reinterpret_cast<char *>(&scan_start_key),
                                                            KEY_SIZE, scan_range, ptr_scan_vector);

                auto get_next_row_ = scan_iter ->GetNext(ptr_scan_vector);
                INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
                if (get_next_row_ == nullptr) {
                    break;
                }else{
                    vd_row = get_next_row_;
                }
            }else if(type == SCAN && iteration != 0) {
                uint64_t starttime = get_sys_clock();
                auto get_next_row_ = scan_iter ->GetNext(ptr_scan_vector);
                INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
                if (get_next_row_ == nullptr) {
                    break;
                }else{
                    vd_row = get_next_row_;
                }
            }else if(type == INS){
                row_t *new_row = NULL;
                char *new_row_data = ycsb_row_data(tuple_size);
                memset(new_row_data, 'h', tuple_size);
//                for (int vid = 0; vid < tuple_size; vid ++) {
//                    new_row_data[vid] = 'h';
//                }
                uint64_t row_id = rid;
                auto part_id = key_to_part(row_id);
                uint64_t primary_key = req->key;
#if ENGINE_TYPE != PTR0
                rc = _wl->the_table->get_new_row(new_row,part_id,row_id);
                new_row->set_primary_key(primary_key);
                copy_row_to_table(new_row->data, new_row_data, tuple_size);

                insert_row(new_row, _wl->the_table);
#elif ENGINE_TYPE == PTR0
                row_t *row_insert = NULL;
                auto index_ = _wl->the_index;
                rc = index_->index_insert(primary_key, reinterpret_cast<void *&>(row_insert), new_row_data);

                if (rc == RCOK) {
                    assert(row_insert != NULL);
                    _wl->the_table->init_row(row_insert);
                }
#endif
                delete new_row_data;
                if (rc == Abort){
                    goto final;
                }

                assert(rc == RCOK);
                finish_req = true;
                continue;
            }
            //2. verify the index payload
			if (vd_row == nullptr){
                rc = Abort;
                goto final;
			}
#endif


//3. read the payload(record, record pointer)
#if ENGINE_TYPE == PTR0
            if(type == SCAN){
                // if scan/update workload
//                row_local = get_row(vd_row, RD);
                // if scan/insert workload
                row_local = reinterpret_cast<row_t *>(vd_row);
            }else{
                row_local = get_row(vd_row, type);
            }
#elif ENGINE_TYPE == PTR1
            row = reinterpret_cast<row_t *>(vd_row);// row_t meta
            if (row->data == nullptr){
                rc = Abort;
                goto final;
            }
            record_pointer = *reinterpret_cast<uint64_t *>(row->data);
            row_t *vd_row_ = reinterpret_cast<row_t *>(record_pointer);
            master_row = reinterpret_cast<void *>(vd_row_);
            if(type == SCAN){
                //if scan/update workload, type=RD
                //if scan/insert workload. type=SCAN
                row_local = get_row(master_row, RD);
            }else {
                row_local = get_row(master_row, type);
            }
#elif ENGINE_TYPE == PTR2
    #if SECONDARY_INDEX == false
            row = reinterpret_cast<row_t *>(vd_row);// row_t meta
            if (row->data == nullptr){
                rc = Abort;
                goto final;
            }
//#if JUMP_PREFETCHING
//            auto v_header = row->manager;
//            __builtin_prefetch((const void *)((char *)v_header), 0, 1);
//#endif
            record_pointer = *reinterpret_cast<uint64_t *>(row->data);
            if (record_pointer == 0){
                finish_req = true;
                continue;
            }
    #endif
            m_item = reinterpret_cast<itemid_t *>(record_pointer);
            master_row = m_item->location;
            if(type == SCAN){
               //if scan/update workload, type=RD
               //if scan/insert workload. type=SCAN
               row_local = get_row(master_row, RD);
            }else{
               row_local = get_row(master_row, type);
            }
#endif

//4. verify the payload(record,record pointer)
            if (row_local == NULL) {
//                rc = Abort;
//                goto final;
                finish_req = true;
                continue;
            }

//5. read the data of the record
			// Computation, Only do computation when there are more than 1 requests.
            if (row_local != NULL  && m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == RO || req->rtype == SCAN) {
                    char *data = row_local->data;

                    __attribute__((unused)) char * value = (&data[tuple_size]);
                } else {
                    assert(req->rtype == WR);
                    char *update_location;
                    int fid = 2;
                    auto fild_leng = schema->get_field_size(fid);

#if ENGINE_TYPE == PTR2 || ENGINE_TYPE == PTR1
                    update_location = row_local->data;
#endif

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
                    memcpy(update_location+((fid-1)*fild_leng), &update_data, fild_leng);
#elif ENGINE_TYPE == PTR0
                    row_local->manager->update_data = value_upt;
                    row_local->manager->update_length = tuple_size;
#endif
                }
            }

            iteration ++;
			if (req->rtype == RD || req->rtype == RO || req->rtype == WR || iteration == req->scan_len){
                finish_req = true;
			}
		}

//        printf("key = %ld, value = %s \n", req->key, row_.c_str());
	}

	rc = RCOK;

final:

    //insert for ptr1 and ptr2
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
    if (rc == RCOK) {
        rc = insert_row_finish(rc);
    }
#endif

    rc = finish(rc);

    //todo: the secondary indexes impact the insert and update
    //primary key: insert need to insert s_k/p_k to the secondary indexes
    //indirection: 1.insert need to insert s_k/i_ptr to the secondary indexes
    //             2.insert need to allocate the indirect location
    //record pointer: 1.insert need to insert s_k/ptr to the secondary indexes
    //                2.insert split need to update the secondary indexes

//    rc = RCOK;
	return rc;
}

