#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "btree_store.h"
#include "tpcc_const.h"
#include "tpcc_helper.h"
#include "catalog.h"

void tpcc_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (tpcc_wl *) h_wl;
}

RC tpcc_txn_man::run_txn(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;
	switch (m_query->type) {
//		case TPCC_PAYMENT :
//			return run_payment(m_query); break;
		case TPCC_NEW_ORDER :
			return run_new_order(m_query); break;
//		case TPCC_ORDER_STATUS :
//			return run_order_status(m_query); break;
//		case TPCC_DELIVERY :
//			return run_delivery(m_query); break;
		case TPCC_STOCK_LEVEL :
			return run_stock_level(m_query); break;
		default:
			assert(false);
	}
}

RC tpcc_txn_man::run_payment(tpcc_query * query) {
	RC rc = RCOK;
	uint64_t key;
	itemid_t * item;

	uint64_t w_id = query->w_id;
    uint64_t c_w_id = query->c_w_id;
	/*====================================================+
    	EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/

	// TODO for variable length variable (string). Should store the size of 
	// the variable.
	key = query->w_id;
	INDEX * index = _wl->i_warehouse; 
	item = (itemid_t *)index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	row_t * r_wh_local;
	if (g_wh_update)
		r_wh_local = get_row(r_wh, WR);
	else 
		r_wh_local = get_row(r_wh, RD);

	if (r_wh_local == NULL) {
		return finish(Abort);
	}
	double w_ytd;
	
	r_wh_local->get_value(W_YTD, w_ytd);
	if (g_wh_update) {
		r_wh_local->set_value(W_YTD, w_ytd + query->h_amount);
	}
	char w_name[11];
	char * tmp_str = r_wh_local->get_value(W_NAME);
	memcpy(w_name, tmp_str, 10);
	w_name[10] = '\0';
	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	key = distKey(query->d_id, query->d_w_id);
	item = (itemid_t *)index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	row_t * r_dist_local = get_row(r_dist, WR);
	if (r_dist_local == NULL) {
		return finish(Abort);
	}

	double d_ytd;
	r_dist_local->get_value(D_YTD, d_ytd);
	r_dist_local->set_value(D_YTD, d_ytd + query->h_amount);
	char d_name[11];
	tmp_str = r_dist_local->get_value(D_NAME);
	memcpy(d_name, tmp_str, 10);
	d_name[10] = '\0';

	/*====================================================================+
		EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
		INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		FROM district
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+====================================================================*/

	row_t * r_cust;
	if (query->by_last_name) { 
		/*==========================================================+
			EXEC SQL SELECT count(c_id) INTO :namecnt
			FROM customer
			WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
		+==========================================================*/
		/*==========================================================================+
			EXEC SQL DECLARE c_byname CURSOR FOR
			SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
			c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
			ORDER BY c_first;
			EXEC SQL OPEN c_byname;
		+===========================================================================*/

//		uint64_t key = custNPKey(reinterpret_cast<uint64_t>(query->c_last), query->c_d_id, query->c_w_id);
        uint64_t key = custNPKey(query->d_id, query->w_id, query->c_last);
		// XXX: the list is not sorted. But let's assume it's sorted...
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		item = (itemid_t *)index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);
		
		/*============================================================================+
			for (n=0; n<namecnt/2; n++) {
				EXEC SQL FETCH c_byname
				INTO :c_first, :c_middle, :c_id,
					 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
					 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
			}
			EXEC SQL CLOSE c_byname;
		+=============================================================================*/
		// XXX: we don't retrieve all the info, just the tuple we are interested in
	}
	else { // search customers by cust_id
		/*=====================================================================+
			EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
			c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
			c_discount, c_balance, c_since
			INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
			:c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
			:c_discount, :c_balance, :c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+======================================================================*/
		key = custKey(query->c_id, query->c_d_id, query->c_w_id);
		INDEX * index = _wl->i_customer_id;
		item = (itemid_t *)index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		r_cust = (row_t *) item->location;
	}

  	/*======================================================================+
	   	EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
   		WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
   	+======================================================================*/
	row_t * r_cust_local = get_row(r_cust, WR);
	if (r_cust_local == NULL) {
		return finish(Abort);
	}
	double c_balance;
	double c_ytd_payment;
	double c_payment_cnt;

	r_cust_local->get_value(C_BALANCE, c_balance);
	r_cust_local->set_value(C_BALANCE, c_balance - query->h_amount);
	r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
	r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + query->h_amount);
	r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
	r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

	char * c_credit = r_cust_local->get_value(C_CREDIT);

	if ( strstr(c_credit, "BC") ) {
	
		/*=====================================================+
		    EXEC SQL SELECT c_data
			INTO :c_data
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+=====================================================*/
//	  	char c_new_data[501];
//	  	sprintf(c_new_data,"| %4d %2d %4d %2d %4d $%7.2f",
//	      	c_id, c_d_id, c_w_id, d_id, w_id, query->h_amount);
//		char * c_data = r_cust->get_value("C_DATA");
//	  	strncat(c_new_data, c_data, 500 - strlen(c_new_data));
//		r_cust->set_value("C_DATA", c_new_data);
			
	}
	
	char h_data[25];
	strncpy(h_data, w_name, 10);
	int length = strlen(h_data);
	if (length > 10) length = 10;
	strcpy(&h_data[length], "    ");
	strncpy(&h_data[length + 4], d_name, 10);
	h_data[length+14] = '\0';
	/*=============================================================================+
	  EXEC SQL INSERT INTO
	  history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
	  VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
	  +=============================================================================*/
//	row_t * r_hist;
//	uint64_t row_id;
//	_wl->t_history->get_new_row(r_hist, 0, row_id);
//	r_hist->set_value(H_C_ID, c_id);
//	r_hist->set_value(H_C_D_ID, c_d_id);
//	r_hist->set_value(H_C_W_ID, c_w_id);
//	r_hist->set_value(H_D_ID, d_id);
//	r_hist->set_value(H_W_ID, w_id);
//	int64_t date = 2013;		
//	r_hist->set_value(H_DATE, date);
//	r_hist->set_value(H_AMOUNT, h_amount);
#if !TPCC_SMALL
//	r_hist->set_value(H_DATA, h_data);
#endif
//	insert_row(r_hist, _wl->t_history);

	assert( rc == RCOK );
	return finish(rc);
}

RC tpcc_txn_man::run_new_order(tpcc_query * query) {
	RC rc = RCOK;
	uint64_t key;
	itemid_t * item;
	INDEX * index;
    row_t *index_entry_;
	
	bool remote = query->remote;
	uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
	uint64_t ol_cnt = query->ol_cnt;
	/*=======================================================================+
	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
		FROM customer, warehouse
		WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
	+========================================================================*/
	key = w_id;
	index = _wl->i_warehouse; 
	index_entry_ = reinterpret_cast<row_t *>(index_read(index, key, wh_to_part(w_id)));
    if(index_entry_ == NULL){
        return finish(Abort);
    }

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
	uint64_t payload_ = *reinterpret_cast<uint64_t *>(index_entry_->data);
    itemid_t *itemid_t_ptr = reinterpret_cast<itemid_t *>(payload_);
    void *master_row = itemid_t_ptr->location;
#endif
    row_t *r_wh_local = get_row(master_row, RD);

	if (r_wh_local == NULL) {
		return finish(Abort);
	}

	double w_tax;
    auto field_pos = _wl->t_warehouse->schema->get_field_index(W_TAX);
    w_tax = *reinterpret_cast<uint64_t *>(&(r_wh_local->data[field_pos]));

	key = custKey(c_id, d_id, w_id);
	index = _wl->i_customer_id;
    index_entry_ = reinterpret_cast<row_t *>(index_read(index, key, wh_to_part(w_id)));
	if(index_entry_ == NULL){
        printf("customer read fail.");
        return finish(Abort);
	}

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
    payload_ = *reinterpret_cast<uint64_t *>(index_entry_->data);
    itemid_t_ptr = reinterpret_cast<itemid_t *>(payload_);
    master_row = itemid_t_ptr->location;
#endif
    row_t * r_cust_local = get_row(master_row, RD);

	if (r_cust_local == NULL) {
		return finish(Abort);
	}
	uint64_t c_discount;
    auto field_pos_cus = _wl->t_customer->schema->get_field_index(C_DISCOUNT);
    c_discount = *reinterpret_cast<uint64_t *>(&(r_cust_local->data[field_pos_cus]));

	/*==================================================+
	EXEC SQL SELECT d_next_o_id, d_tax
		INTO :d_next_o_id, :d_tax
		FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
	EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
		WH ERE d _id = :d_id AN D d _w _id = :w _id ;
	+===================================================*/
	key = distKey(d_id, w_id);
    index_entry_ = reinterpret_cast<row_t *>(index_read(_wl->i_district, key, wh_to_part(w_id)));
    if(index_entry_ == NULL){
        return finish(Abort);
    }

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
    payload_ = *reinterpret_cast<uint64_t *>(index_entry_->data);
    itemid_t_ptr = reinterpret_cast<itemid_t *>(payload_);
    master_row = itemid_t_ptr->location;
#endif

    row_t * r_dist_local = get_row(master_row, WR);
	if (r_dist_local == NULL) {
		return finish(Abort);
	}

	int64_t o_id;
    auto field_pos_dis = _wl->t_district->schema->get_field_index(D_NEXT_O_ID);
    o_id = *reinterpret_cast<int64_t *>(&(r_dist_local->data[field_pos_dis]));
	o_id ++;
	memcpy(&(r_dist_local->data[field_pos_dis]), &o_id, 8);

	char *data;
	/*========================================================================================+
	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
	+========================================================================================*/
	row_t * r_order;
	uint64_t row_id;
#if TPCC_ALOW_INSERT
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
	_wl->t_order->get_new_row(r_order, 0, row_id);
    r_order->set_primary_key(orderKey(o_id, d_id, w_id));
#endif
    data = new char[ORDER_SIZE];
    memcpy(data, &o_id, 8);
    memcpy(data+8, &c_id, 8);
    memcpy(data+8+8, &d_id, 8);
    memcpy(data+8+8+8, &w_id, 8);
    uint64_t o_entry = 2024;
    memcpy(data+8+8+8+8, &o_entry, 8);
    uint64_t dt = 0;
    memcpy(data+8+8+8+8+8, &dt, 8);
    memcpy(data+8+8+8+8+8+8, &ol_cnt, 8);
    int64_t all_local = (remote? 0 : 1);
    memcpy(data+8+8+8+8+8+8+8, &all_local, 8);
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
    memcpy(r_order->data, data, ORDER_SIZE);
#endif
    index_insert(_wl->i_order, orderKey(o_id, d_id, w_id), r_order, data, wh_to_part(w_id));
#endif

	/*=======================================================+
    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
	row_t * r_no;
#if TPCC_ALOW_INSERT
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
	_wl->t_neworder->get_new_row(r_no, 0, row_id);
	r_no->set_primary_key(neworderKey(o_id, d_id, w_id));
#endif

    data = new char[NORDER_SIZE];
    uint64_t o_id_ = o_id;
    memcpy(data, &o_id_, 8);
    memcpy(data+8, &d_id, 8);
    memcpy(data+8+8, &w_id, 8);
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
    memcpy(r_no->data, data, NORDER_SIZE);
#endif
    index_insert(_wl->i_neworder, neworderKey(o_id, d_id, w_id), r_no, data, wh_to_part(w_id));
#endif

	for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {

		uint64_t ol_i_id = query->items[ol_number].ol_i_id;
		uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
		uint64_t ol_quantity = query->items[ol_number].ol_quantity;
		/*===========================================+
		EXEC SQL SELECT i_price, i_name , i_data
			INTO :i_price, :i_name, :i_data
			FROM item
			WHERE i_id = :ol_i_id;
		+===========================================*/
		key = ol_i_id;
        index_entry_ = reinterpret_cast<row_t *>(index_read(_wl->i_item, key, 0));
        if(index_entry_ == NULL){
            continue;
        }

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        payload_ = *reinterpret_cast<uint64_t *>(index_entry_->data);
        if (payload_ == 0) continue;
        itemid_t_ptr = reinterpret_cast<itemid_t *>(payload_);
        master_row = itemid_t_ptr->location;
#endif

        row_t * r_item_local = get_row(master_row, RD);
		if (r_item_local == NULL) {
            continue;
		}
		int64_t i_price;

        auto field_pos_price = _wl->t_item->schema->get_field_index(I_PRICE);
        i_price = *reinterpret_cast<int64_t *>(&(r_item_local->data[field_pos_price]));

		/*===================================================================+
		EXEC SQL SELECT s_quantity, s_data,
				s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
				s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
			INTO :s_quantity, :s_data,
				:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
				:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
			FROM stock
			WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
		EXEC SQL UPDATE stock SET s_quantity = :s_quantity
			WHERE s_i_id = :ol_i_id
			AND s_w_id = :ol_supply_w_id;
		+===============================================*/

		uint64_t stock_key = stockKey(ol_i_id, ol_supply_w_id);
		INDEX * stock_index = _wl->i_stock;
        index_entry_ = reinterpret_cast<row_t *>(index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), WR));
        if(index_entry_ == NULL){
            continue;
        }

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        payload_ = *reinterpret_cast<uint64_t *>(index_entry_->data);
        itemid_t_ptr = reinterpret_cast<itemid_t *>(payload_);
        master_row = itemid_t_ptr->location;
#endif

        row_t * r_stock_local = get_row(master_row, WR);
		if (r_stock_local == NULL) {
            continue;
		}
		
		// XXX s_dist_xx are not retrieved.
		UInt64 s_quantity;
		int64_t s_remote_cnt;
        auto field_pos_quantity = _wl->t_stock->schema->get_field_index(S_QUANTITY);
        s_quantity = *reinterpret_cast<uint64_t *>(&(r_stock_local->data[field_pos_quantity]));

		int64_t s_ytd;
		int64_t s_order_cnt;

        auto field_pos_ytd = _wl->t_stock->schema->get_field_index(S_YTD);
        s_ytd = *reinterpret_cast<int64_t *>(&(r_stock_local->data[field_pos_ytd]));
        uint64_t n_ynt = (s_ytd + ol_quantity);
        memcpy(&(r_stock_local->data[field_pos_ytd]), &n_ynt, 8);

        auto field_pos_cnt = _wl->t_stock->schema->get_field_index(S_ORDER_CNT);
        s_order_cnt = *reinterpret_cast<int64_t *>(&(r_stock_local->data[field_pos_cnt]));
        uint64_t o_cnt = (s_order_cnt + 1);
        memcpy(&(r_stock_local->data[field_pos_cnt]), &o_cnt, 8);

		if (remote) {
            auto field_pos_rcnt = _wl->t_stock->schema->get_field_index(S_REMOTE_CNT);
            s_remote_cnt = *reinterpret_cast<int64_t *>(&(r_stock_local->data[field_pos_rcnt]));
			s_remote_cnt ++;
            memcpy(&(r_stock_local->data[field_pos_rcnt]), &(s_remote_cnt), 8);
		}
		uint64_t quantity;
		if (s_quantity > ol_quantity + 10) {
			quantity = s_quantity - ol_quantity;
		} else {
			quantity = s_quantity - ol_quantity + 91;
		}
        memcpy(&(r_stock_local->data[field_pos_quantity]), &(quantity), 8);

		/*====================================================+
		EXEC SQL INSERT
			INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
				ol_i_id, ol_supply_w_id,
				ol_quantity, ol_amount, ol_dist_info)
			VALUES(:o_id, :d_id, :w_id, :ol_number,
				:ol_i_id, :ol_supply_w_id,
				:ol_quantity, :ol_amount, :ol_dist_info);
		+====================================================*/
		// XXX district info is not inserted.
		row_t * r_ol;
		uint64_t row_id;
#if TPCC_ALOW_INSERT
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
		_wl->t_orderline->get_new_row(r_ol, 0, row_id);
		r_ol->set_primary_key(orderlineKey(ol_number, o_id, d_id, w_id));
#endif

        data = new char[ORDERLINE_SIZE];
        memcpy(data, &o_id, 8);
        memcpy(data+8, &d_id, 8);
        memcpy(data+8+8, &w_id, 8);
        memcpy(data+8+8+8, &ol_number, 8);
        memcpy(data+8+8+8+8, &ol_i_id, 8);
        memcpy(data+8+8+8+8+8, &ol_supply_w_id, 8);
        uint64_t dt1 = 0;
        memcpy(data+8+8+8+8+8+8, &dt1, 8);
        memcpy(data+8+8+8+8+8+8+8, &ol_quantity, 8);
        int w_tax=1, d_tax=1;
		int64_t ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
        memcpy(data+8+8+8+8+8+8+8+8,  &ol_amount, 8);
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        memcpy(r_ol->data, data, ORDERLINE_SIZE);
#endif
        index_insert(_wl->i_orderline, orderlineKey(ol_number, o_id, d_id, w_id), r_ol, data, wh_to_part(w_id));
#endif

	}
	assert( rc == RCOK );

#if TPCC_ALOW_INSERT
    delete data;
#endif

	return finish(rc);
}
//////////////////////////////////////////////////////
// Stock Level
//////////////////////////////////////////////////////
row_t* tpcc_txn_man::stock_level_getOId(uint64_t d_w_id, uint64_t d_id) {
    // SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?
    auto index = _wl->i_district;
    auto key = distKey(d_id, d_w_id);
    auto part_id = wh_to_part(d_w_id);

    row_t *index_entry_ = reinterpret_cast<row_t *>(index_read(index, key, part_id));
    if(index_entry_ == NULL){
        return NULL;
    }

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
    uint64_t payload_ = *reinterpret_cast<uint64_t *>(index_entry_->data);
    itemid_t *itemid_t_ptr = reinterpret_cast<itemid_t *>(payload_);
    void *master_row = itemid_t_ptr->location;
#endif
    row_t * r_dis_local = get_row(master_row, RD);

    return r_dis_local;
}

bool tpcc_txn_man::stock_level_getStockCount(uint64_t ol_w_id, uint64_t ol_d_id,
                                             int64_t ol_o_id, uint64_t s_w_id,
                                             uint64_t threshold,
                                             uint64_t* out_distinct_count) {
    // SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
    // WHERE OL_W_ID = ?
    //   AND OL_D_ID = ?
    //   AND OL_O_ID < ?
    //   AND OL_O_ID >= ?
    //   AND S_W_ID = ?
    //   AND S_I_ID = OL_I_ID
    //   AND S_QUANTITY < ?

    // 20 orders * 15 items = 300; use 301 to check any errors.
    uint64_t ol_i_id_list[301];
    size_t list_size = 0;

    auto index = _wl->i_orderline;
    auto key = orderlineKey(1, ol_o_id - 1, ol_d_id, ol_w_id);
    auto max_key = orderlineKey(15, ol_o_id - 20, ol_d_id, ol_w_id);
    auto part_id = wh_to_part(ol_w_id);

    row_t* rows[301];
    uint64_t count = 301;
    row_t *index_entry_;
    uint64_t payload_;
    itemid_t *itemid_t_ptr;
    void *master_row;
    for (int i = 0; i < 301; ++i) {
        index_entry_ = reinterpret_cast<row_t *>(index_read(index, key, part_id));
        if(index_entry_ == nullptr) {
            rows[i] = NULL;
            continue;
        }
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        payload_ = *reinterpret_cast<uint64_t *>(index_entry_->data);
        itemid_t_ptr = reinterpret_cast<itemid_t *>(payload_);
        master_row = itemid_t_ptr->location;
#endif
        row_t * r_ol_local = get_row(master_row, RD);
        if (r_ol_local == nullptr) return false;
        rows[i] = r_ol_local;
    }

    for (uint64_t i = 0; i < count; i++) {
        auto orderline = rows[i];
        if (orderline == NULL) continue;

        uint64_t ol_i_id, ol_supply_w_id;
//        orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
        auto field_pos_supply = _wl->t_orderline->schema->get_field_index(OL_SUPPLY_W_ID);
        ol_supply_w_id = *reinterpret_cast<int64_t *>(&(orderline->data[field_pos_supply]));
        if (ol_supply_w_id != s_w_id) continue;

//        orderline->get_value(OL_I_ID, ol_i_id);
        auto field_pos_i_id = _wl->t_orderline->schema->get_field_index(OL_I_ID);
        ol_i_id = *reinterpret_cast<int64_t *>(&(orderline->data[field_pos_i_id]));

        assert(list_size < sizeof(ol_i_id_list) / sizeof(ol_i_id_list[0]));
        ol_i_id_list[list_size] = ol_i_id;
        list_size++;
    }
    assert(list_size <= count);

    uint64_t distinct_ol_i_id_list[count];
    uint64_t distinct_ol_i_id_count = 0;
    uint64_t result = 0;

    for (uint64_t i = 0; i < list_size; i++) {
        uint64_t ol_i_id = ol_i_id_list[i];

        bool duplicate = false;
        for (uint64_t j = 0; j < distinct_ol_i_id_count; j++)
            if (distinct_ol_i_id_list[j] == ol_i_id) {
                duplicate = true;
                break;
            }
        if (duplicate) continue;

        distinct_ol_i_id_list[distinct_ol_i_id_count++] = ol_i_id;

        auto key = stockKey(ol_i_id, s_w_id);
        auto index = _wl->i_stock;
        auto part_id = wh_to_part(s_w_id);
        index_entry_ = reinterpret_cast<row_t *>(index_read(index, key, part_id));
        if(index_entry_ == NULL) return false;
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
        payload_ = *reinterpret_cast<uint64_t *>(index_entry_->data);
        itemid_t_ptr = reinterpret_cast<itemid_t *>(payload_);
        master_row = itemid_t_ptr->location;
#endif
        row_t * r_stock_local = get_row(master_row, RD);
//        auto row = search(index, key, part_id, RD);
        if (r_stock_local == NULL) return false;

        uint64_t s_quantity;
//        row->get_value(S_QUANTITY, s_quantity);
        auto field_pos_quantity = _wl->t_stock->schema->get_field_index(S_QUANTITY);
        s_quantity = *reinterpret_cast<int64_t *>(&(r_stock_local->data[field_pos_quantity]));
        if (s_quantity < threshold) result++;
    }

    *out_distinct_count = result;
    return true;
}
RC tpcc_txn_man::run_stock_level(tpcc_query * query) {

    uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t threshold = query->threshold;
    threshold=16;

    auto district = stock_level_getOId(w_id, d_id);
    if (district == NULL) {
        return finish(Abort);
    }
    int64_t o_id;
    district->get_value(D_NEXT_O_ID, o_id);

    uint64_t distinct_count;
    if (!stock_level_getStockCount(w_id, d_id, o_id, w_id,
                                   threshold, &distinct_count)) {
        return finish(Abort);
    }
    (void)distinct_count;

    return finish(RCOK);
}
RC tpcc_txn_man::run_order_status(tpcc_query * query) {
/*	row_t * r_cust;
	if (query->by_last_name) {
		// EXEC SQL SELECT count(c_id) INTO :namecnt FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
		// EXEC SQL DECLARE c_name CURSOR FOR SELECT c_balance, c_first, c_middle, c_id
		// FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id ORDER BY c_first;
		// EXEC SQL OPEN c_name;
		// if (namecnt%2) namecnt++; / / Locate midpoint customer for (n=0; n<namecnt/ 2; n++)
		// {
		//	   	EXEC SQL FETCH c_name
		//	   	INTO :c_balance, :c_first, :c_middle, :c_id;
		// }
		// EXEC SQL CLOSE c_name;

		uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted... 
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		uint64_t thd_id = get_thd_id();
		itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);
	} else {
		// EXEC SQL SELECT c_balance, c_first, c_middle, c_last
		// INTO :c_balance, :c_first, :c_middle, :c_last
		// FROM customer
		// WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
		uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
		INDEX * index = _wl->i_customer_id;
		itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		r_cust = (row_t *) item->location;
	}
#if TPCC_ACCESS_ALL

	row_t * r_cust_local = get_row(r_cust, RD);
	if (r_cust_local == NULL) {
		return finish(Abort);
	}
	double c_balance;
	r_cust_local->get_value(C_BALANCE, c_balance);
	char * c_first = r_cust_local->get_value(C_FIRST);
	char * c_middle = r_cust_local->get_value(C_MIDDLE);
	char * c_last = r_cust_local->get_value(C_LAST);
#endif
	// EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
	// INTO :o_id, :o_carrier_id, :entdate FROM orders
	// ORDER BY o_id DESC;
	uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
	INDEX * index = _wl->i_order;
	itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
	row_t * r_order = (row_t *) item->location;
	row_t * r_order_local = get_row(r_order, RD);
	if (r_order_local == NULL) {
		assert(false); 
		return finish(Abort);
	}

	uint64_t o_id, o_entry_d, o_carrier_id;
	r_order_local->get_value(O_ID, o_id);
#if TPCC_ACCESS_ALL
	r_order_local->get_value(O_ENTRY_D, o_entry_d);
	r_order_local->get_value(O_CARRIER_ID, o_carrier_id);
#endif
#if DEBUG_ASSERT
	itemid_t * it = item;
	while (it != NULL && it->next != NULL) {
		uint64_t o_id_1, o_id_2;
		((row_t *)it->location)->get_value(O_ID, o_id_1);
		((row_t *)it->next->location)->get_value(O_ID, o_id_2);
		assert(o_id_1 > o_id_2);
	}
#endif

	// EXEC SQL DECLARE c_line CURSOR FOR SELECT ol_i_id, ol_supply_w_id, ol_quantity,
	// ol_amount, ol_delivery_d
	// FROM order_line
	// WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
	// EXEC SQL OPEN c_line;
	// EXEC SQL WHENEVER NOT FOUND CONTINUE;
	// i=0;
	// while (sql_notfound(FALSE)) {
	// 		i++;
	//		EXEC SQL FETCH c_line
	//		INTO :ol_i_id[i], :ol_supply_w_id[i], :ol_quantity[i], :ol_amount[i], :ol_delivery_d[i];
	// }
	key = orderlineKey(query->w_id, query->d_id, o_id);
	index = _wl->i_orderline;
	item = index_read(index, key, wh_to_part(query->w_id));
	assert(item != NULL);
#if TPCC_ACCESS_ALL
	// TODO the rows are simply read without any locking mechanism
	while (item != NULL) {
		row_t * r_orderline = (row_t *) item->location;
		int64_t ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d;
		r_orderline->get_value(OL_I_ID, ol_i_id);
		r_orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
		r_orderline->get_value(OL_QUANTITY, ol_quantity);
		r_orderline->get_value(OL_AMOUNT, ol_amount);
		r_orderline->get_value(OL_DELIVERY_D, ol_delivery_d);
		item = item->next;
	}
#endif

final:
	assert( rc == RCOK );
	return finish(rc)*/
	return RCOK;
}


//TODO concurrency for index related operations is not completely supported yet.
// In correct states may happen with the current code.

RC 
tpcc_txn_man::run_delivery(tpcc_query * query) {
/*
	// XXX HACK if another delivery txn is running on this warehouse, simply commit.
	if ( !ATOM_CAS(_wl->delivering[query->w_id], false, true) )
		return finish(RCOK);

	for (int d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
		uint64_t key = distKey(d_id, query->w_id);
		INDEX * index = _wl->i_orderline_wd;
		itemid_t * item = index_read(index, key, wh_to_part(query->w_id));
		assert(item != NULL);
		while (item->next != NULL) {
#if DEBUG_ASSERT
			uint64_t o_id_1, o_id_2;
			((row_t *)item->location)->get_value(OL_O_ID, o_id_1);
			((row_t *)item->next->location)->get_value(OL_O_ID, o_id_2);
			assert(o_id_1 > o_id_2);
#endif
			item = item->next;
		}
		uint64_t no_o_id;
		row_t * r_orderline = (row_t *)item->location;
		r_orderling->get_value(OL_O_ID, no_o_id);
		// TODO the orderline row should be removed from the table and indexes.
		
		index = _wl->i_order;
		key = orderPrimaryKey(query->w_id, d_id, no_o_id);
		itemid_t * item = index_read(index, key, wh_to_part(query->w_id));
		row_t * r_order = (row_t *)item->location;
		row_t * r_order_local = get_row(r_order, WR);

		uint64_t o_c_id;
		r_order_local->get_value(O_C_ID, o_c_id);
		r_order_local->set_value(O_CARRIER_ID, query->o_carrier_id);

		item = index_read(_wl->i_order_line, orderlineKey(query->w_id, d_id, no_o_id));
		double sum_ol_amount;
		double ol_amount;
		while (item != NULL) {
			// TODO the row is not locked
			row_t * r_orderline = (row_t *)item->location;
			r_orderline->set_value(OL_DELIVERY_D, query->ol_delivery_d);
			r_orderline->get_value(OL_AMOUNT, ol_amount);
			sum_ol_amount += ol_amount;
		}
		
		key = custKey(o_c_id, d_id, query->w_id);
		itemid_t * item = index_read(_wl->i_customer_id, key, wh_to_part(query->w_id));
		row_t * r_cust = (row_t *)item->location;
		double c_balance;
		uint64_t c_delivery_cnt;
	}
*/
	return RCOK;
}

