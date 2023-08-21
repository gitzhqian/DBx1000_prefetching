#include "global.h"
#include "helper.h"
#include "tpcc.h"
#include "wl.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "tpcc_const.h"
#include "catalog.h"
#include "btree_store.h"

RC tpcc_wl::init() {
	workload::init();
//	string path = "./benchmarks/";
    string path = "/home/zhangqian/code/DBx1000_engine/benchmarks/";
#if TPCC_SMALL
	path += "TPCC_short_schema.txt";
#else
	path += "TPCC_full_schema.txt";
#endif

    path = "/home/zhangqian/code/DBx1000_engine/benchmarks/TPCC_full_schema.txt";

	cout << "reading schema file: " << path << endl;
	init_schema( path.c_str() );
	cout << "TPCC schema initialized" << endl;
	init_table();
	next_tid = 0;
	return RCOK;
}

RC tpcc_wl::init_schema(const char * schema_file) {
    workload::init_schema(schema_file);
    t_warehouse = tables["WAREHOUSE"];
    t_district = tables["DISTRICT"];
    t_district_ext = tables["DISTRICT-EXT"];
    t_customer = tables["CUSTOMER"];
    t_history = tables["HISTORY"];
    t_neworder = tables["NEW-ORDER"];
    t_order = tables["ORDER"];
    t_orderline = tables["ORDER-LINE"];
    t_item = tables["ITEM"];
    t_stock = tables["STOCK"];

    t_warehouse->schema->set_tuple_size_const(WAREHOUSE_SIZE);
    t_district->schema->set_tuple_size_const(DISTRICT_SIZE);
    t_customer->schema->set_tuple_size_const(CUSTOMER_SIZE);
    t_history->schema->set_tuple_size_const(HISTORY_SIZE);
    t_neworder->schema->set_tuple_size_const(NORDER_SIZE);
    t_order->schema->set_tuple_size_const(ORDER_SIZE);
    t_orderline->schema->set_tuple_size_const(ORDERLINE_SIZE);
    t_item->schema->set_tuple_size_const(ITEM_SIZE);
    t_stock->schema->set_tuple_size_const(STOCK_SIZE);

    i_item = indexes["ITEM_IDX"];
    i_warehouse = indexes["WAREHOUSE_IDX"];
    i_district = indexes["DISTRICT_IDX"];
    i_district_ext = indexes["DISTRICT_EXT_IDX"];
    i_customer_id = indexes["CUSTOMER_ID_IDX"];
    i_customer_last = indexes["CUSTOMER_LAST_IDX"];
    i_stock = indexes["STOCK_IDX"];
    i_order = indexes["ORDER_IDX"];
    i_order_cust = indexes["ORDER_CUST_IDX"];
    i_neworder = indexes["NEW_ORDER_IDX"];
    i_orderline = indexes["ORDER_LINE_IDX"];
    i_history = indexes["HISTORY_IDX"];

    tables_[0] = t_item;
    tables_[1] = t_warehouse;
    tables_[2] = t_district;
    tables_[3] = t_district_ext;
    tables_[4] = t_customer;
    tables_[5] = t_history;
    tables_[6] = t_stock;
    tables_[7] = t_order;
    tables_[8] = t_orderline;
    tables_[9] = t_neworder;
    tables_[10] = NULL;

    indexes_[0] = i_item;
    indexes_[1] = i_warehouse;
    indexes_[2] = i_district;
    indexes_[3] = i_district_ext;
    indexes_[4] = i_customer_id;
    indexes_[5] = i_customer_last;
    indexes_[6] = i_stock;
    indexes_[7] = i_order;
    indexes_[8] = i_order_cust;
    indexes_[9] = i_neworder;
    indexes_[10] = i_orderline;
    indexes_[11] = i_history;
    index_2_table_[0] = 0;
    index_2_table_[1] = 1;
    index_2_table_[2] = 2;
    index_2_table_[3] = 3;
    index_2_table_[4] = 4;
    index_2_table_[5] = 4;
    index_2_table_[6] = 6;
    index_2_table_[7] = 7;
    index_2_table_[8] = 7;
    index_2_table_[9] = 9;
    index_2_table_[10] = 8;
    return RCOK;
}

RC tpcc_wl::init_table() {
	num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//	- stock
// 	- dist
//  	- cust
//	  	- hist
//		- order 
//		- new order
//		- order line
/**********************************/
    int buf_cnt = (num_wh > g_thread_cnt) ? num_wh : g_thread_cnt;
    tpcc_buffer = new drand48_data * [buf_cnt];
    for (uint32_t i = 0; i < buf_cnt; ++i) {
        // printf("%d\n", g_thread_cnt);
        tpcc_buffer[i] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
        srand48_r(i + 1, tpcc_buffer[i]);
    }
    pthread_t * p_thds = new pthread_t[g_num_wh - 1];
    for (uint32_t i = 0; i < g_num_wh - 1; i++)
        pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
    threadInitWarehouse(this);
    for (uint32_t i = 0; i < g_num_wh - 1; i++)
        pthread_join(p_thds[i], NULL);

	printf("TPCC Data Initialization Complete!\n");
	return RCOK;
}

RC tpcc_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
	txn_manager = (tpcc_txn_man *) _mm_malloc( sizeof(tpcc_txn_man), 64);
	new(txn_manager) tpcc_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}

// TODO ITEM table is assumed to be in partition 0
//8,int64_t,I_ID
//8,int64_t,I_IM_ID
//24,string,I_NAME
//8,int64_t,I_PRICE
//50,string,I_DATA
void tpcc_wl::init_tab_item() {
    char *data_;
	for (UInt32 i = 1; i <= g_max_items; i++) {
        row_t * row;
        uint64_t row_id;
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        t_item->get_new_row(row, 0, row_id);
        row->set_primary_key(i);
#endif

        data_ = new char[ITEM_SIZE];
        memcpy( data_, &i, 8);
        uint64_t im_id = URand(1L,10000L, 0);
        memcpy(data_+8, &im_id, 8);
        char name[24];
        MakeAlphaString(14, 24, name, 0);
        memcpy(data_+8+8, name, 24);
        uint64_t price = URand(1, 100, 0);
        memcpy(data_+8+8+24, &price, 8);
        char data[50];
        int len = MakeAlphaString(26, 50, data, 0);
        if (RAND(10, 0) == 0) {
            uint64_t startORIGINAL = URand(2, (len - 8), 0);
            strcpy(data + startORIGINAL, "original");
        }
        memcpy(data_+8+8+24+8, data, 50);

//        row->set_value(I_ID, i);
//        row->set_value(I_IM_ID, URand(1L,10000L, 0));
//        char name[24];
//        MakeAlphaString(14, 24, name, 0);
//        row->set_value(I_NAME, name);
//        row->set_value(I_PRICE, URand(1, 100, 0));
//        char data[50];
//        int len = MakeAlphaString(26, 50, data, 0);
//        // TODO in TPCC, "original" should start at a random position
//        if (RAND(10, 0) == 0) {
//            uint64_t startORIGINAL = URand(2, (len - 8), 0);
//            strcpy(data + startORIGINAL, "original");
//        }
//        row->set_value(I_DATA, data);
        //memcpy( &data[pos], ptr, datasize);
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
		memcpy(row->data, data_, ITEM_SIZE);
#endif
		index_insert(i_item, i, row, data_,0);
	}

    delete data_;
}
//8,int64_t,W_ID
//10,string,W_NAME
//20,string,W_STREET_1
//20,string,W_STREET_2
//20,string,W_CITY
//2,string,W_STATE
//9,string,W_ZIP
//8,double,W_TAX
//8,double,W_YTD
void tpcc_wl::init_tab_wh(uint32_t wid) {
    char *data_;
	assert(wid >= 1 && wid <= g_num_wh);
    row_t * row;
    uint64_t row_id;
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
    t_warehouse->get_new_row(row, 0, row_id);
    row->set_primary_key(wid);
#endif

    data_ = new char[WAREHOUSE_SIZE];
    memcpy(data_, &wid, 8);
    char name[10];
    MakeAlphaString(6, 10, name, wid-1);
    memcpy(data_+8, name, 10);
    char street[20];
    MakeAlphaString(10, 20, street, wid-1);
    memcpy(data_+8+10, street, 20);
    MakeAlphaString(10, 20, street, wid-1);
    memcpy(data_+8+10+20, street, 20);
    MakeAlphaString(10, 20, street, wid-1);
    memcpy(data_+8+10+20+20, street, 20);
    char state[2];
    MakeAlphaString(2, 2, state, wid-1); /* State */
    memcpy(data_+8+10+20+20+20, state, 2);
    char zip[9];
    MakeNumberString(9, 9, zip, wid-1); /* Zip */
    memcpy(data_+8+10+20+20+20+2, zip, 9);
    double tax = (double)URand(0L,200L,wid-1)/1000.0;
    memcpy(data_+8+10+20+20+20+2+9, &tax, 8);
    double w_ytd=300000.00;
    memcpy(data_+8+10+20+20+20+2+9+8, &w_ytd, 8);

//    row->set_value(W_ID, wid);
//    char name[10];
//    MakeAlphaString(6, 10, name, wid-1);
//    row->set_value(W_NAME, name);
//    char street[20];
//    MakeAlphaString(10, 20, street, wid-1);
//    row->set_value(W_STREET_1, street);
//    MakeAlphaString(10, 20, street, wid-1);
//    row->set_value(W_STREET_2, street);
//    MakeAlphaString(10, 20, street, wid-1);
//    row->set_value(W_CITY, street);
//    char state[2];
//    MakeAlphaString(2, 2, state, wid-1); /* State */
//    row->set_value(W_STATE, state);
//    char zip[9];
//    MakeNumberString(9, 9, zip, wid-1); /* Zip */
//    row->set_value(W_ZIP, zip);
//    double tax = (double)URand(0L,200L,wid-1)/1000.0;
//    double w_ytd=300000.00;
//    row->set_value(W_TAX, tax);
//    row->set_value(W_YTD, w_ytd);

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
    memcpy(row->data, data_, WAREHOUSE_SIZE);
#endif
    index_insert(i_warehouse, wid, row, data_, wh_to_part(wid));

    delete data_;
}
//8,int64_t,D_ID
//8,int64_t,D_W_ID
//10,string,D_NAME
//20,string,D_STREET_1
//20,string,D_STREET_2
//20,string,D_CITY
//2,string,D_STATE
//9,string,D_ZIP
//8,double,D_TAX
//8,double,D_YTD
//8,int64_t,D_NEXT_O_ID
void tpcc_wl::init_tab_dist(uint64_t wid) {
    char *data_;
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		row_t * row;
        uint64_t row_id;
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        t_district->get_new_row(row, 0, row_id);
        row->set_primary_key(distKey(did, wid));
#endif

        data_ = new char[DISTRICT_SIZE];
        memcpy(&data_[0], &did, 8);
        memcpy(&data_[8], &wid, 8);
        char name[10];
        MakeAlphaString(6, 10, name, wid-1);
        memcpy(&data_[8+8], name, 10);
        char street[20];
        MakeAlphaString(10, 20, street, wid-1);
        memcpy(&data_[8+8+10], street, 20);
        MakeAlphaString(10, 20, street, wid-1);
        memcpy(&data_[8+8+10+20], street, 20);
        MakeAlphaString(10, 20, street, wid-1);
        memcpy(&data_[8+8+10+20+20], street, 20);
        char state[2];
        MakeAlphaString(2, 2, state, wid-1); /* State */
        memcpy(&data_[8+8+10+20+20+20], state, 2);
        char zip[9];
        MakeNumberString(9, 9, zip, wid-1); /* Zip */
        memcpy(&data_[8+8+10+20+20+20+2],zip,9);
        double tax = (double)URand(0L,200L,wid-1)/1000.0;
        memcpy(&data_[8+8+10+20+20+20+2+9], &tax, 8);
        double w_ytd=30000.00;
        memcpy(&data_[8+8+10+20+20+20+2+9+8], &w_ytd, 8);
        uint64_t non = 3001;
        memcpy(&data_[8+8+10+20+20+20+2+9+8+8], &non, 8);

//        row->set_value(D_ID, did);
//        row->set_value(D_W_ID, wid);
//        char name[10];
//        MakeAlphaString(6, 10, name, wid-1);
//        row->set_value(D_NAME, name);
//        char street[20];
//        MakeAlphaString(10, 20, street, wid-1);
//        row->set_value(D_STREET_1, street);
//        MakeAlphaString(10, 20, street, wid-1);
//        row->set_value(D_STREET_2, street);
//        MakeAlphaString(10, 20, street, wid-1);
//        row->set_value(D_CITY, street);
//        char state[2];
//        MakeAlphaString(2, 2, state, wid-1); /* State */
//        row->set_value(D_STATE, state);
//        char zip[9];
//        MakeNumberString(9, 9, zip, wid-1); /* Zip */
//        row->set_value(D_ZIP, zip);
//        double tax = (double)URand(0L,200L,wid-1)/1000.0;
//        double w_ytd=30000.00;
//        row->set_value(D_TAX, tax);
//        row->set_value(D_YTD, w_ytd);
//        row->set_value(D_NEXT_O_ID, 3001);

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        memcpy(row->data, data_, DISTRICT_SIZE);
#endif
        index_insert(i_district, distKey(did, wid), row, data_, wh_to_part(wid));
	}

    delete data_;
}
//8,int64_t,S_I_ID
//8,int64_t,S_W_ID
//8,int64_t,S_QUANTITY
//24,string,S_DIST_01
//24,string,S_DIST_02
//24,string,S_DIST_03
//24,string,S_DIST_04
//24,string,S_DIST_05
//24,string,S_DIST_06
//24,string,S_DIST_07
//24,string,S_DIST_08
//24,string,S_DIST_09
//24,string,S_DIST_10
//8,int64_t,S_YTD
//8,int64_t,S_ORDER_CNT
//8,int64_t,S_REMOTE_CNT
//50,string,S_DATA
void tpcc_wl::init_tab_stock(uint64_t wid) {
    char *data_;
	for (UInt32 sid = 1; sid <= g_max_items; sid++) {
		row_t * row;
        uint64_t row_id;
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        t_stock->get_new_row(row, 0, row_id);
        row->set_primary_key(stockKey(sid, wid));
#endif

        data_ = new char[STOCK_SIZE];
        memcpy(data_, &sid, 8);
        memcpy(data_+8, &wid, 8);
        uint64_t quanty = URand(10, 100, wid-1);
        memcpy(data_+8+8, &quanty, 8);
        char s_dist[25];
        char row_name[10] = "S_DIST_";
        for (int i = 1; i <= 10; i++) {
            if (i < 10) {
                row_name[7] = '0';
                row_name[8] = i + '0';
            } else {
                row_name[7] = '1';
                row_name[8] = '0';
            }
            row_name[9] = '\0';
            MakeAlphaString(24, 24, s_dist, wid-1);
            memcpy(data_+8+8+8+(i-1)*24, s_dist, 24);
        }
        uint64_t ynt = 0;
        memcpy(data_+8+8+8+10*24, &ynt, 8);
        memcpy(data_+8+8+8+10*24+8, &ynt, 8);
        memcpy(data_+8+8+8+10*24+8+8, &ynt, 8);
        char s_data[50];
        int len = MakeAlphaString(26, 50, s_data, wid-1);
        if (rand() % 100 < 10) {
            int idx = URand(0, len - 8, wid-1);
            strcpy(&s_data[idx], "original");
        }
        memcpy(data_+8+8+8+10*24+8+8+8, s_data, 50);

//        row->set_value(S_I_ID, sid);
//        row->set_value(S_W_ID, wid);
//        row->set_value(S_QUANTITY, URand(10, 100, wid-1));
//        row->set_value(S_REMOTE_CNT, 0);
//#if !TPCC_SMALL
//        char s_dist[25];
//        char row_name[10] = "S_DIST_";
//        for (int i = 1; i <= 10; i++) {
//            if (i < 10) {
//                row_name[7] = '0';
//                row_name[8] = i + '0';
//            } else {
//                row_name[7] = '1';
//                row_name[8] = '0';
//            }
//            row_name[9] = '\0';
//            MakeAlphaString(24, 24, s_dist, wid-1);
//            row->set_value(row_name, s_dist);
//        }
//        row->set_value(S_YTD, 0);
//        row->set_value(S_ORDER_CNT, 0);
//        char s_data[50];
//        int len = MakeAlphaString(26, 50, s_data, wid-1);
//        if (rand() % 100 < 10) {
//            int idx = URand(0, len - 8, wid-1);
//            strcpy(&s_data[idx], "original");
//        }
//        row->set_value(S_DATA, s_data);
//#endif

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        memcpy(row->data, data_, STOCK_SIZE);
#endif

        index_insert(i_stock, stockKey(sid, wid), row, data_, wh_to_part(wid));
	}

    delete data_;
}
//8,int64_t,C_ID
//8,int64_t,C_D_ID
//8,int64_t,C_W_ID
//16,string,C_FIRST
//2,string,C_MIDDLE
//16,string,C_LAST
//20,string,C_STREET_1
//20,string,C_STREET_2
//20,string,C_CITY
//2,string,C_STATE
//9,string,C_ZIP
//16,string,C_PHONE
//8,int64_t,C_SINCE
//2,string,C_CREDIT
//8,int64_t,C_CREDIT_LIM
//8,int64_t,C_DISCOUNT
//8,double,C_BALANCE
//8,double,C_YTD_PAYMENT
//8,uint64_t,C_PAYMENT_CNT
//8,uint64_t,C_DELIVERY_CNT
//500,string,C_DATA
void tpcc_wl::init_tab_cust(uint64_t did, uint64_t wid) {
    char *data_;
	assert(g_cust_per_dist >= 1000);
	for (UInt32 cid = 1; cid <= g_cust_per_dist; cid++) {
        row_t * row;
        uint64_t row_id;
        char c_last[LASTNAME_LEN];
        if (cid <= 1000)
            Lastname(cid - 1, c_last);
        else
            Lastname(NURand(255,0,999,wid-1), c_last);
        uint64_t key;
//        key = custNPKey(did, wid, c_last);
        key = custKey(cid, did, wid);
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        t_customer->get_new_row(row, 0, row_id);
        row->set_primary_key(key);
#endif

        data_ = new char[CUSTOMER_SIZE];
        memcpy(data_, &cid, 8);
        memcpy(data_+8, &did, 8);
        memcpy(data_+8+8, &wid, 8);
        char c_first[FIRSTNAME_LEN];
        MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first, wid-1);
        memcpy(data_+8+8+8, c_first, 16);
        char tmp[3] = "OE";
        memcpy(data_+8+8+8+16, tmp, 2);
        memcpy(data_+8+8+8+16+2, c_last, 16);
        char street[20];
        MakeAlphaString(10, 20, street, wid-1);
        memcpy(data_+8+8+8+16+2+16, street, 20);
        MakeAlphaString(10, 20, street, wid-1);
        memcpy(data_+8+8+8+16+2+16+20, street, 20);
        MakeAlphaString(10, 20, street, wid-1);
        memcpy(data_+8+8+8+16+2+16+20+20, street, 20);
        char state[2];
        MakeAlphaString(2, 2, state, wid-1); /* State */
        memcpy(data_+8+8+8+16+2+16+20+20+20, state, 2);
        char zip[9];
        MakeNumberString(9, 9, zip, wid-1); /* Zip */
        memcpy(data_+8+8+8+16+2+16+20+20+20+2, zip, 9);
        char phone[16];
        MakeNumberString(16, 16, phone, wid-1); /* Zip */
        memcpy(data_+8+8+8+16+2+16+20+20+20+2+9, phone, 16);
        uint64_t since = 0;
        memcpy(data_+8+8+8+16+2+16+20+20+20+2+9+16, &since, 8); //since
        if (RAND(10, wid-1) == 0) {
            char tmp[3] = "GC";
            memcpy(data_+8+8+8+16+2+16+20+20+20+2+9+16+8,tmp,2);
        } else {
            char tmp[3] = "BC";
            memcpy(data_+8+8+8+16+2+16+20+20+20+2+9+16+8,tmp,2);//credit
        }
        uint64_t amout = 50000;
        uint64_t discount = (double)RAND(5000,wid-1) / 10000;
        uint64_t balance = (double)(-10.0);
        uint64_t payment =1;
        uint64_t delv = 0;
        memcpy(data_+8+8+8+16+2+16+20+20+20+2+9+16+8+2, &amout,8);//credit_limit
        memcpy(data_+8+8+8+16+2+16+20+20+20+2+9+16+8+2+8, &discount,8);//discount
        memcpy(data_+8+8+8+16+2+16+20+20+20+2+9+16+8+2+8+8,&balance,8);//balance
        memcpy(data_+8+8+8+16+2+16+20+20+20+2+9+16+8+2+8+8+8,&balance,8);//ytd_payment
        memcpy(data_+8+8+8+16+2+16+20+20+20+2+9+16+8+2+8+8+8+8,&payment,8);//payment
        memcpy(data_+8+8+8+16+2+16+20+20+20+2+9+16+8+2+8+8+8+8+8,&delv,8);//delivery
        char c_data[500];
        MakeAlphaString(300, 500, c_data, wid-1);
        memcpy(data_+8+8+8+16+2+16+20+20+20+2+9+16+8+2+8+8+8+8+8+8, c_data, 500);
//        row->set_value(C_ID, cid);
//        row->set_value(C_D_ID, did);
//        row->set_value(C_W_ID, wid);
//        char c_last[LASTNAME_LEN];
//        if (cid <= 1000)
//            Lastname(cid - 1, c_last);
//        else
//            Lastname(NURand(255,0,999,wid-1), c_last);
//        row->set_value(C_LAST, c_last);
//#if !TPCC_SMALL
//        char tmp[3] = "OE";
//        row->set_value(C_MIDDLE, tmp);
//        char c_first[FIRSTNAME_LEN];
//        MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first, wid-1);
//        row->set_value(C_FIRST, c_first);
//        char street[20];
//        MakeAlphaString(10, 20, street, wid-1);
//        row->set_value(C_STREET_1, street);
//        MakeAlphaString(10, 20, street, wid-1);
//        row->set_value(C_STREET_2, street);
//        MakeAlphaString(10, 20, street, wid-1);
//        row->set_value(C_CITY, street);
//        char state[2];
//        MakeAlphaString(2, 2, state, wid-1); /* State */
//        row->set_value(C_STATE, state);
//        char zip[9];
//        MakeNumberString(9, 9, zip, wid-1); /* Zip */
//        row->set_value(C_ZIP, zip);
//        char phone[16];
//        MakeNumberString(16, 16, phone, wid-1); /* Zip */
//        row->set_value(C_PHONE, phone);
//        row->set_value(C_SINCE, 0);
//        row->set_value(C_CREDIT_LIM, 50000);
//        row->set_value(C_DELIVERY_CNT, 0);
//        char c_data[500];
//        MakeAlphaString(300, 500, c_data, wid-1);
//        row->set_value(C_DATA, c_data);
//#endif
//        if (RAND(10, wid-1) == 0) {
//            char tmp[] = "GC";
//            row->set_value(C_CREDIT, tmp);
//        } else {
//            char tmp[] = "BC";
//            row->set_value(C_CREDIT, tmp);
//        }
//        row->set_value(C_DISCOUNT, (double)RAND(5000,wid-1) / 10000);
//        row->set_value(C_BALANCE, -10.0);
//        row->set_value(C_YTD_PAYMENT, 10.0);
//        row->set_value(C_PAYMENT_CNT, 1);
   //todo: i_customer_last
//        uint64_t key;
//        key = custNPKey(did, wid, c_last);
//        row->set_primary_key(key);
//        index_insert(i_customer_last, key, row, wh_to_part(wid));

//        key = custKey(cid, did, wid);
//        row_t *row_new;
//        t_customer->get_new_row(row_new, 0, row_id);
//        row_new->set_primary_key(key);
//        memcpy(row_new->get_data(), row->get_data(), row->get_tuple_size());
//        index_insert(i_customer_id, key, row_new, wh_to_part(wid));

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        memcpy(row->data, data_, CUSTOMER_SIZE);
#endif
        index_insert(i_customer_id, key, row, data_, wh_to_part(wid));
	}

    delete data_;
}
//8,int64_t,H_C_ID
//8,int64_t,H_C_D_ID
//8,int64_t,H_C_W_ID
//8,int64_t,H_D_ID
//8,int64_t,H_W_ID
//8,int64_t,H_DATE
//8,double,H_AMOUNT
//24,string,H_DATA
void tpcc_wl::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
    char *data_;
    row_t * row;
    uint64_t row_id;

    t_history->get_new_row(row, 0, row_id);
    row->set_primary_key(0);
    data_ = new char[HISTORY_SIZE];
    memcpy(data_, &c_id, 8);
    memcpy(data_+8, &d_id, 8);
    memcpy(data_+8+8, &w_id, 8);
    memcpy(data_+8+8+8, &d_id, 8);
    memcpy(data_+8+8+8+8, &w_id, 8);
    uint64_t amount = 10;
    uint64_t date = 0;
    memcpy(data_+8+8+8+8+8, &date, 8);
    memcpy(data_+8+8+8+8+8+8, &amount, 8);
    char h_data[24];
    MakeAlphaString(12, 24, h_data, w_id-1);
    memcpy(data_+8+8+8+8+8+8+8, h_data, 24);

//    row->set_value(H_C_ID, c_id);
//    row->set_value(H_C_D_ID, d_id);
//    row->set_value(H_D_ID, d_id);
//    row->set_value(H_C_W_ID, w_id);
//    row->set_value(H_W_ID, w_id);
//    row->set_value(H_DATE, 0);
//    row->set_value(H_AMOUNT, 10.0);
//#if !TPCC_SMALL
//    char h_data[24];
//    MakeAlphaString(12, 24, h_data, w_id-1);
//    row->set_value(H_DATA, h_data);
//#endif

    memcpy(row->data, data_, HISTORY_SIZE);

}
//8,int64_t,O_ID
//8,int64_t,O_C_ID
//8,int64_t,O_D_ID
//8,int64_t,O_W_ID
//8,int64_t,O_ENTRY_D
//8,int64_t,O_CARRIER_ID
//8,int64_t,O_OL_CNT
//8,int64_t,O_ALL_LOCAL
void tpcc_wl::init_tab_order(uint64_t did, uint64_t wid) {
    char *data_;
    uint64_t perm[g_cust_per_dist];
    init_permutation(perm, wid); /* initialize permutation of customer numbers */
    for (UInt32 oid = 1; oid <= g_cust_per_dist; oid++) {
        row_t *row;
        uint64_t row_id;
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        t_order->get_new_row(row, 0, row_id);
        row->set_primary_key(orderKey(oid, did, wid));
#endif
        uint64_t o_ol_cnt = 1;
        uint64_t cid = perm[oid - 1]; //get_permutation();

        data_ = new char[ORDER_SIZE];
        memcpy(data_, &oid, 8);
        memcpy(data_+8, &cid, 8);
        memcpy(data_+8+8, &did, 8);
        memcpy(data_+8+8+8, &wid, 8);
        uint64_t o_entry = 2013;
        memcpy(data_+8+8+8+8, &o_entry, 8);
        uint64_t carry = URand(1, 10, wid - 1);
        uint64_t carry_ = 0;
        uint64_t local = 1;
        if (oid < 2101)
            memcpy(data_+8+8+8+8+8, &carry, 8);
        else
            memcpy(data_+8+8+8+8+8, &carry_, 8);
        o_ol_cnt = URand(5, 15, wid - 1);
        memcpy(data_+8+8+8+8+8+8, &o_ol_cnt, 8);
        memcpy(data_+8+8+8+8+8+8+8, &local, 8);
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        memcpy(row->data, data_, ORDER_SIZE);
#endif
        index_insert(i_order, orderKey(oid, did, wid), row, data_, wh_to_part(wid));

//        row->set_value(O_ID, oid);
//        row->set_value(O_C_ID, cid);
//        row->set_value(O_D_ID, did);
//        row->set_value(O_W_ID, wid);
//        uint64_t o_entry = 2013;
//        row->set_value(O_ENTRY_D, o_entry);
//        if (oid < 2101)
//            row->set_value(O_CARRIER_ID, URand(1, 10, wid - 1));
//        else
//            row->set_value(O_CARRIER_ID, 0);
//        o_ol_cnt = URand(5, 15, wid - 1);
//        row->set_value(O_OL_CNT, o_ol_cnt);
//        row->set_value(O_ALL_LOCAL, 1);
        // index_insert(i_customer_id, key, row, wh_to_part(wid));
//        index_insert(i_order, orderKey(oid, did, wid), row, wh_to_part(wid));

//        row_t *row_cust;
//        t_order->get_new_row(row_cust, 0, row_id);
//        row_cust->set_primary_key(orderCustKey(oid, cid, did, wid));
//        memcpy(row_cust->get_data(), row->get_data(), row->get_tuple_size());
//        index_insert(i_order_cust, orderCustKey(oid, cid, did, wid), row_cust,
//                     wh_to_part(wid));


        // ORDER-LINE
//        8,int64_t,OL_O_ID
//        8,int64_t,OL_D_ID
//        8,int64_t,OL_W_ID
//        8,int64_t,OL_NUMBER
//        8,int64_t,OL_I_ID
//        8,int64_t,OL_SUPPLY_W_ID
//        8,int64_t,OL_DELIVERY_D
//        8,int64_t,OL_QUANTITY
//        8,double,OL_AMOUNT
//        24,int64_t,OL_DIST_INFO
//#if !TPCC_SMALL
        for (uint32_t ol = 1; ol <= o_ol_cnt; ol++) {
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
            t_orderline->get_new_row(row, 0, row_id);
            row->set_primary_key(orderlineKey(ol, oid, did, wid));
#endif
            data_ = new char[ORDERLINE_SIZE];
            memcpy(data_, &oid, 8);
            memcpy(data_+8, &did, 8);
            memcpy(data_+8+8, &wid, 8);
            memcpy(data_+8+8+8, &ol, 8);
            uint64_t i_id = URand(1, 100000, wid - 1);
            memcpy(data_+8+8+8+8, &i_id, 8);
            memcpy(data_+8+8+8+8+8, &wid, 8);
            uint64_t dt1 = 5;
            uint64_t dt2 = 0;
            uint64_t dt3=(double) URand(1, 999999, wid - 1) / 100;
            if (oid < 2101) {
                memcpy(data_+8+8+8+8+8+8, &o_entry, 8);
                memcpy(data_+8+8+8+8+8+8+8, &dt1, 8);
                memcpy(data_+8+8+8+8+8+8+8+8, &dt2, 8);
            } else {
                memcpy(data_+8+8+8+8+8+8, &dt2, 8);
                memcpy(data_+8+8+8+8+8+8+8, &dt1, 8);
                memcpy(data_+8+8+8+8+8+8+8+8,  &dt3, 8);
            }
            char ol_dist_info[24];
            MakeAlphaString(24, 24, ol_dist_info, wid - 1);
            memcpy(data_+8+8+8+8+8+8+8+8+8, ol_dist_info, 24);

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
            memcpy(row->data, data_, ORDERLINE_SIZE);
#endif
            index_insert(i_orderline, orderlineKey(ol, oid, did, wid), row, data_, wh_to_part(wid));

//            row->set_value(OL_O_ID, oid);
//            row->set_value(OL_D_ID, did);
//            row->set_value(OL_W_ID, wid);
//            row->set_value(OL_NUMBER, ol);
//            row->set_value(OL_I_ID, URand(1, 100000, wid - 1));
//            row->set_value(OL_SUPPLY_W_ID, wid);
//            if (oid < 2101) {
//                row->set_value(OL_DELIVERY_D, o_entry);
//                row->set_value(OL_AMOUNT, 0);
//            } else {
//                row->set_value(OL_DELIVERY_D, 0);
//                row->set_value(OL_AMOUNT, (double) URand(1, 999999, wid - 1) / 100);
//            }
//            row->set_value(OL_QUANTITY, 5);
//            char ol_dist_info[24];
//            MakeAlphaString(24, 24, ol_dist_info, wid - 1);
//            row->set_value(OL_DIST_INFO, ol_dist_info);
//            index_insert(i_orderline, orderlineKey(ol, oid, did, wid), row,
//                         wh_to_part(wid));
        }
//#endif
        // NEW ORDER
//        8,int64_t,NO_O_ID
//        8,int64_t,NO_D_ID
//        8,int64_t,NO_W_ID
        if (oid > 2100) {
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
            t_neworder->get_new_row(row, 0, row_id);
            row->set_primary_key(neworderKey(oid, did, wid));
#endif
            data_ = new char[NORDER_SIZE];
            uint64_t oid_ = oid;
            memcpy(data_, &oid_, 8);
            memcpy(data_+8, &did, 8);
            memcpy(data_+8+8, &wid, 8);
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
            memcpy(row->data, data_, NORDER_SIZE);
#endif
            index_insert(i_neworder, neworderKey(oid, did, wid), row, data_, wh_to_part(wid));

//            row->set_value(NO_O_ID, (int64_t) oid);
//            row->set_value(NO_D_ID, did);
//            row->set_value(NO_W_ID, wid);
//            index_insert(i_neworder, neworderKey(oid, did, wid), row,
//                         wh_to_part(wid));
        }
    }

    delete data_;

}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void 
tpcc_wl::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
    uint32_t i;
    // Init with consecutive values
    for(i = 0; i < g_cust_per_dist; i++)
        perm_c_id[i] = i+1;

    // shuffle
    for(i=0; i < g_cust_per_dist-1; i++) {
        uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
        uint64_t tmp = perm_c_id[i];
        perm_c_id[i] = perm_c_id[j];
        perm_c_id[j] = tmp;
    }
}


/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/

void * tpcc_wl::threadInitWarehouse(void * This) {
    tpcc_wl * wl = (tpcc_wl *) This;
    int tid = ATOM_FETCH_ADD(wl->next_tid, 1);
    uint32_t wid = tid + 1;
    assert((uint64_t)tid < g_num_wh);

    if (tid == 0)
        wl->init_tab_item();
    wl->init_tab_wh( wid );
    wl->init_tab_dist( wid );
    wl->init_tab_stock( wid );
    for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
        wl->init_tab_cust(did, wid);
        wl->init_tab_order(did, wid);
        for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++)
            wl->init_tab_hist(cid, did, wid);
    }

    return NULL;
}
