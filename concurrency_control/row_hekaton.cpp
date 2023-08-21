#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_hekaton.h"
#include "mem_alloc.h"
#include <mm_malloc.h>
#include "btree_store.h"

#if CC_ALG == HEKATON

void Row_hekaton::init(row_t * row) {
#if JUMP_PREFETCHING
    _his_len = 1;
#else
    _his_len = 4;
#endif
    _write_history = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len, 64);
    for (uint32_t i = 0; i < _his_len; i++){
        _write_history[i].row = NULL;
//#if JUMP_PREFETCHING
//        memset(_write_history[i].distance_, 'j',4096);
//#endif
    }
    _write_history[0].row = row;
    _write_history[0].begin_txn = false;
    _write_history[0].end_txn = false;
    _write_history[0].begin = 0;
    _write_history[0].end = INF;

    _his_latest = 0;
    _his_oldest = 0;
    _exists_prewrite = false;

    blatch = false;

#if JUMP_PREFETCHING
    his_oldest = _write_history;
    his_latest = _write_history;
    his_older = NULL;
    his_oldest->next = his_older;
#endif
}
WriteHisEntry * Row_hekaton::initw(row_t * row) {

    WriteHisEntry *_write_history = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) , 64);
    _write_history->row=row;
    _write_history->begin_txn=false;
    _write_history->end_txn=false;
    _write_history->begin=0;
    _write_history->end=INF;

#if JUMP_PREFETCHING
    _write_history->next=NULL;
    _write_history->jump=NULL;
#endif
    return _write_history;
}
void Row_hekaton::doubleHistory()
{
    WriteHisEntry * temp = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len * 2, 64);
    uint32_t idx = _his_oldest;
    for (uint32_t i = 0; i < _his_len; i++) {
        temp[i] = _write_history[idx];
        idx = (idx + 1) % _his_len;
        temp[i + _his_len].row = NULL;
        temp[i + _his_len].begin_txn = false;
        temp[i + _his_len].end_txn = false;
    }

    _his_oldest = 0;
    _his_latest = _his_len - 1;
    _mm_free(_write_history);
    _write_history = temp;

    _his_len *= 2;
//    printf("double history, lengh:%u", _his_len);
}

RC Row_hekaton:: access(txn_man * txn, TsType type, row_t * row) {
    RC rc = RCOK;
#if ENGINE_TYPE == PTR0
    if(type == O_REQ || type == S_REQ){
		//if read-only, just return OK
		rc = RCOK;
		txn->cur_row = row;

		return rc;
	}

	ts_t ts = txn->get_ts();
    while (!ATOM_CAS(blatch, false, true))
      PAUSE
	if (type == R_REQ) {
		if (ts < _write_history[_his_oldest].begin) {
			rc = Abort;
		} else if (ts > _write_history[_his_latest].begin) {
			// may create a commit dependency. For now, I always return non-speculative entries.
			rc = RCOK;
			//if exist pre writer, and has not finished
			if (exists_prewriter()){
				txn->cur_row = _write_history[_his_latest].row;
			}else{
				txn->cur_row = row;
			}
		} else {
			rc = RCOK;
			// ts is between _oldest_wts and _latest_wts, should find the correct version
			uint32_t i = _his_latest;
			bool find = false;
#if JUMP_PREFETCHING == true
            __builtin_prefetch(reinterpret_cast<char *>(&_write_history[i]), 0, 3);
#endif
			while (true) {
				i = (i == 0)? _his_len - 1 : i - 1;
#if JUMP_PREFETCHING == true
                __builtin_prefetch(reinterpret_cast<char *>(&_write_history[i-2]), 0, 3);
#endif
				if (_write_history[i].begin < ts) {
					assert(_write_history[i].end > ts);
					txn->cur_row = _write_history[i].row;
					find = true;
					break;
				}
				if (i == _his_oldest)
					break;
			}
			assert(find);
		}
	} else if (type == P_REQ) {
		if (_exists_prewrite || ts < _write_history[_his_latest].begin) {
			rc = Abort;
		} else {
			rc = RCOK;
            _exists_prewrite = true;

			uint32_t id = reserveRow(txn);
			_write_history[id].begin_txn = true;
			_write_history[id].begin = txn->get_txn_id();
			uint32_t pre_id = (id == 0)? _his_len - 1 : id - 1;
			_write_history[pre_id].end_txn = true;
			_write_history[pre_id].end = txn->get_txn_id();
			_write_history[id].row= (row_t *) _mm_malloc(sizeof(row_t), 64);
			_write_history[id].row->init(PAYLOAD_SIZE);

			row_t * res_row = _write_history[id].row;
			assert(res_row);
			memcpy(res_row->data, row->data, PAYLOAD_SIZE);

			txn->cur_row = row;
		}
	}  else {
		assert(false);
	}
	blatch = false;
#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE ==PTR2
    if(type == O_REQ || type == S_REQ){
        rc = RCOK;
        txn->cur_row = _write_history[_his_latest].row;
        return rc;
    }

    ts_t ts = txn->get_ts();
    while (!ATOM_CAS(blatch, false, true))
    PAUSE
//    assert(_write_history[_his_latest].end == INF || _write_history[_his_latest].end_txn);

    if (type == R_REQ) {
 //simulating test for long version chain
#if JUMP_PREFETCHING == true
//        int jmp=0;
//        uint32_t i = _his_oldest;
//        i = (i == (_his_len - 1))?  0 : i + 1;
//#if JUMP_1== true || JUMP_2== true || JUMP_3== true
//        __builtin_prefetch(reinterpret_cast<void *>((char *)_write_history+sizeof(_write_history)*i), 0, 1);
//#endif
        int j=0;
        char *ptr_cur;
        WriteHisEntry *w_h_cur=this->his_oldest;
        bool find = false;
        while (true) {
            j++;
            if (j>_his_len) break;
#if JUMP_1 == true
            auto ptr= w_h_cur->jump;
            if (ptr != NULL) {
                __builtin_prefetch(reinterpret_cast<void *>(ptr), 0, 1);
            }
#endif
//            ptr_cur= (char *)_write_history+sizeof(_write_history)*i;
//            w_h_cur = reinterpret_cast<WriteHisEntry *>(ptr_cur);
            if (w_h_cur->begin < ts && w_h_cur->end >= ts) {
                txn->cur_row = w_h_cur->row;
                find = true;
                break;
            }

            w_h_cur = w_h_cur->next;
//            jmp++;
        }


#else
        if (ISOLATION_LEVEL == REPEATABLE_READ) {
            rc = RCOK;
            txn->cur_row = _write_history[_his_latest].row;
        } else if ( ts < _write_history[_his_oldest].begin) {
            rc = Abort;
        } else if ( ts > _write_history[_his_latest].begin) {
            // TODO. should check the next history entry. If that entry is locked by a preparing txn,
            // may create a commit dependency. For now, I always return non-speculative entries.
            rc = RCOK;
            txn->cur_row = _write_history[_his_latest].row;
        } else {
            rc = RCOK;
            // ts is between _oldest_wts and _latest_wts, should find the correct version
            uint32_t i = _his_latest;
            bool find = false;

            while (true) {
                i = (i == 0)? _his_len - 1 : i - 1;
                if (_write_history[i].begin < ts) {
                    assert(_write_history[i].end >= ts);
                    txn->cur_row = _write_history[i].row;
                    find = true;
                    break;
                }
                if (i == _his_oldest) {
                    break;
                }

            }
            assert(find);

            printf("key:%lu, find i:%d, write history length:%u. \n",txn->cur_row->get_primary_key(),i, _his_len);
        }
#endif

    } else if (type == P_REQ) {
        if (_exists_prewrite || ts < _write_history[_his_latest].begin) {
            rc = Abort;
        } else {
            rc = RCOK;
            _exists_prewrite = true;
#if JUMP_PREFETCHING == true
            row_t * res_row;
            res_row = (row_t *) _mm_malloc(sizeof(row_t), 64);
            res_row->init(100);
            memcpy(res_row->data, his_latest->row->data, 100);
            WriteHisEntry *new_wh = initw(res_row);
            res_row->set_primary_key(his_latest->row->get_primary_key());
            new_wh->begin_txn = true;
            new_wh->begin = txn->get_txn_id();
            his_latest->end_txn = true;
            his_latest->end = txn->get_txn_id();

            if (his_older!=NULL) {
                his_older->jump = new_wh;
            }
            his_older = his_latest;
            his_latest = new_wh;

#else
            uint32_t id = reserveRow(txn);
            uint32_t pre_id = (id == 0)? _his_len - 1 : id - 1;
            _write_history[id].begin_txn = true;
            _write_history[id].begin = txn->get_txn_id();
            _write_history[pre_id].end_txn = true;
            _write_history[pre_id].end = txn->get_txn_id();
            row_t * res_row = _write_history[id].row;
            assert(res_row);
            res_row->copy(_write_history[_his_latest].row);
            res_row->set_primary_key(_write_history[_his_latest].row->get_primary_key());
#endif

            txn->cur_row = res_row;

//            printf("key:%lu, write history length:%u. \n",res_row->get_primary_key(), _his_len);
        }
    }  else {
        assert(false);
    }

    blatch = false;
#endif

    return rc;
}

uint32_t Row_hekaton::reserveRow(txn_man * txn)
{
    // Garbage Collection
    uint32_t idx;

#if GARBAGE == true
    ts_t min_ts = glob_manager->get_min_ts(txn->get_thd_id());
    if ((_his_latest + 1) % _his_len == _his_oldest // history is full
        && min_ts > _write_history[_his_oldest].end)
    {
        while (_write_history[_his_oldest].end < min_ts)
        {
            assert(_his_oldest != _his_latest);
            _his_oldest = (_his_oldest + 1) % _his_len;
        }
    }
#endif

    if ((_his_latest + 1) % _his_len != _his_oldest)
        // _write_history is not full, return the next entry.
        idx = (_his_latest + 1) % _his_len;
    else {
        // write_history is already full
        // If _his_len is small, double it.
        if (_his_len < g_thread_cnt) {
            doubleHistory();
            idx = (_his_latest + 1) % _his_len;
        } else {
            // _his_len is too large, should replace the oldest history
//            printf("_his_latest:%u, his_oldest:%u , _his_len:%u,\n",_his_latest,_his_oldest,_his_len);
            idx = _his_oldest;
            _his_oldest = (_his_oldest + 1) % _his_len;
        }

    }

    // some entries are not taken. But the row of that entry is NULL.
#if ENGINE_TYPE != PTR0
    if (!_write_history[idx].row) {
        _write_history[idx].row = (row_t *) _mm_malloc(sizeof(row_t), 64);
        _write_history[idx].row->init(MAX_TUPLE_SIZE);
    }
#endif
    return idx;
}

RC
Row_hekaton::prepare_read(txn_man * txn, row_t * row, ts_t commit_ts)
{
    RC rc;
    while (!ATOM_CAS(blatch, false, true))
    PAUSE
    // TODO may pass in a pointer to the history entry to reduce the following scan overhead.

#if ENGINE_TYPE == PTR0
    if (_write_history[_his_latest].row == row) {
		if (txn->get_ts() < _write_history[_his_latest].begin) {
			rc = Abort;
		}else if (!_write_history[_his_latest].end_txn && _write_history[_his_latest].end < commit_ts) {
			rc = Abort;
		}else {
			rc = RCOK;
		}
	}else {
		rc = RCOK;
	}
#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
    uint32_t idx = _his_latest;
    while (true) {
        if (_write_history[idx].row == row) {
            if (txn->get_ts() < _write_history[idx].begin) {
                rc = Abort;
            } else if (!_write_history[idx].end_txn && _write_history[idx].end > commit_ts)
                rc = RCOK;
            else if (!_write_history[idx].end_txn && _write_history[idx].end < commit_ts) {
                rc = Abort;
            } else {
                // TODO. if the end is a txn id, should check that status of that txn.
                // but for simplicity, we just commit
                rc = RCOK;
            }
            break;
        }

        if (idx == _his_oldest) {
            rc = Abort;
            break;
        }

        idx = (idx == 0)? _his_len - 1 : idx - 1;
    }
#endif

    blatch = false;
    return rc;
}

void Row_hekaton::post_process(txn_man * txn, ts_t commit_ts, RC rc)
{
    while (!ATOM_CAS(blatch, false, true))
    PAUSE
#if  JUMP_PREFETCHING == true
    WriteHisEntry * new_entry = his_latest;
    WriteHisEntry * older_entry = his_older;
    assert(new_entry->begin_txn && new_entry->begin == txn->get_txn_id());
    older_entry->end_txn = false; //the version is history entry
    _exists_prewrite = false;
    if (rc == RCOK) {
        assert(commit_ts > older_entry->begin);
        older_entry->end = commit_ts;
        new_entry->begin = commit_ts;
        new_entry->end = INF;

        older_entry->next = new_entry;
        _his_len++;
        total_chain_length++;
//        printf("len:%d, ",_his_len);
    } else {
        older_entry->end = INF;
        his_latest = his_older;
    }
#else
    WriteHisEntry * entry = &_write_history[ (_his_latest + 1) % _his_len ];
    assert(entry->begin_txn && entry->begin == txn->get_txn_id());

    _write_history[_his_latest].end_txn = false; //the version is history entry
    _exists_prewrite = false;
    if (rc == RCOK) {
        assert(commit_ts > _write_history[_his_latest].begin);
        _write_history[ _his_latest ].end = commit_ts;
        entry->begin = commit_ts;
        entry->end = INF;
//        printf("latest version timestamp bg:%lu,ed:%lu, \n",
//               _write_history[_his_latest].begin,_write_history[_his_latest].end);
        _his_latest = (_his_latest + 1) % _his_len;
        assert(_his_latest != _his_oldest);
    } else {
        _write_history[ _his_latest ].end = INF;
    }
#endif

#if ENGINE_TYPE == PTR0
    LeafNode *leaf_nd = reinterpret_cast<LeafNode *>(this->leaf_node);
    auto leaf_nd_frozen = leaf_nd->IsFrozen();
    auto curr_row = txn->cur_row;
    auto update_dt = curr_row->manager->update_data;
    auto update_len = curr_row->manager->update_length;
    if (leaf_nd_frozen){
        while (leaf_nd->counter_insert != VALID_COUNTER)
            PAUSE

        auto has_splitting = curr_row->IsSplitting();
        row_t *next_row;
        while (has_splitting){
            next_row = reinterpret_cast<row_t *>(curr_row->location);
            has_splitting = next_row->IsSplitting();
            curr_row = next_row;
        }
        auto curr_data_location = curr_row->data;
        char *update_location = curr_data_location;
        memcpy(update_location, update_dt, update_len);
    }else{
        leaf_nd->counter_insert.fetch_add(1, memory_order_seq_cst);
        auto update_location = txn->cur_row->data;
        memcpy(update_location, update_dt, update_len);
        leaf_nd->counter_insert.fetch_sub(1, memory_order_seq_cst);
    }
    leaf_nd->counter_update.fetch_sub(1,memory_order_seq_cst);
    //has been split and has no update txn referenced
    if (leaf_nd->counter_update.load(memory_order_seq_cst) == 0
                        && leaf_nd->counter_insert.load()==VALID_COUNTER){
        delete[] reinterpret_cast<char *>(leaf_nd);
    }
#endif

    blatch = false;
}

#endif
