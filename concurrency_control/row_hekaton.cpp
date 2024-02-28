#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_hekaton.h"
#include "mem_alloc.h"
#include <mm_malloc.h>
#include "btree_store.h"

#if CC_ALG == HEKATON

void Row_hekaton::init(row_t * row) {
//if jump_prefetching = true. then simulating long version list
//each version has a header(write_history) and a row, multiple version linked through the next pointer
#if JUMP_PREFETCHING
    _his_len = 1;
#else
    _his_len = 4;
#endif
    _write_history = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len, 64);

    _write_history[0].row = row;
    _write_history[0].begin_txn = false;
    _write_history[0].end_txn = false;
    _write_history[0].begin = 0;
    _write_history[0].end = INF;
    _write_history[0].next = NULL;

    _jump_queue = new std::vector<WriteHisEntry *>();
    _jump_queue->push_back(_write_history);

    _his_latest = 0;
    _his_oldest = 0;
    _exists_prewrite = false;

    blatch = false;

#if JUMP_PREFETCHING
//     his_oldest = &_write_history ;
     his_latest = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len, 64);
     his_latest->next = _write_history;
//    his_older = _write_history;
//    his_oldest->next = his_older;
//    his_latest->next = his_oldest;
//     his_new = nullptr;
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
//    _write_history->jump=NULL;
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

RC Row_hekaton::access(txn_man * txn, TsType type, row_t * row) {
    RC rc = RCOK;
    if(type == O_REQ || type == S_REQ){
        rc = RCOK;
        txn->cur_row = _write_history[_his_latest].row;
        return rc;
    }

    ts_t ts = txn->get_ts();
    while (!ATOM_CAS(blatch, false, true))
        PAUSE
    if (type == R_REQ) {
        //simulating test for long version chain
//        WriteHisEntry *w_h_old = _write_history;
        WriteHisEntry *w_h_cur = his_latest->next;
    #if JUMP_PREFETCHING == true
        bool find = false;
        #if JUMP_queue == true
             __builtin_prefetch(reinterpret_cast<void *>(w_h_cur), 0, PREFETCH_LEVEL);
        #endif
        auto itr_bg = _jump_queue->cend();

        while (true) {
            #if JUMP_1 == true
                auto ptr= w_h_cur->jump;
                if (ptr != NULL) {
                    __builtin_prefetch(reinterpret_cast<void *>(ptr), 0, PREFETCH_LEVEL);
                }
            #endif
            #if JUMP_queue == true
                --itr_bg;
                auto pptr = *itr_bg;
                if (pptr != NULL) {
                   __builtin_prefetch(reinterpret_cast<void *>(pptr), 0, PREFETCH_LEVEL);
                }
            #endif

            if (w_h_cur->begin < ts && w_h_cur->end >= ts) {
                txn->cur_row = w_h_cur->row;
                find = true;
                break;
            }

            w_h_cur = w_h_cur->pre;
            if (w_h_cur == nullptr){
                break;
            }
        }

    #endif
   } else if (type == P_REQ) {
        WriteHisEntry *w_h_cur = his_latest->next;
        if (_exists_prewrite || ts < w_h_cur->begin) {
            rc = Abort;
        } else {
            rc = RCOK;
            _exists_prewrite = true;
        #if JUMP_PREFETCHING == true
                row_t * res_row;
                res_row = (row_t *) _mm_malloc(sizeof(row_t), 64);
                res_row->init(100);
                memcpy(res_row->data, w_h_cur->row->data, 100);
                res_row->set_primary_key(w_h_cur->row->get_primary_key());

                WriteHisEntry *new_wh = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) , 64);
                new_wh[0].row = res_row;
                new_wh[0].begin_txn = true;
                new_wh[0].end_txn = false;
                new_wh[0].begin = txn->get_txn_id();
                new_wh[0].end = INF;
                new_wh[0].pre = w_h_cur;

                w_h_cur->end_txn = true;
                w_h_cur->end = txn->get_thd_id();
                w_h_cur->next = new_wh;
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

    blatch = false;
    return rc;
}

void Row_hekaton::post_process(txn_man * txn, ts_t commit_ts, RC rc)
{
    while (!ATOM_CAS(blatch, false, true))
        PAUSE
    #if  JUMP_PREFETCHING == true
        auto latest_entry = his_latest->next;
        auto new_entry = latest_entry->next;
        assert(new_entry->begin_txn && new_entry->begin == txn->get_txn_id());
        latest_entry->end_txn = false; //the version is history entry
        _exists_prewrite = false;
        if (rc == RCOK) {
            assert(commit_ts > latest_entry->begin);
            latest_entry->end = commit_ts;
            new_entry->begin = commit_ts;
            new_entry->end = INF;

            his_latest->next = new_entry;

    #if  JUMP_PREFETCHING_CHAIN == true
            total_chain_length++;
    #endif
    #if JUMP_queue == true
            WriteHisEntry * prefth = new_entry;
            _jump_queue->push_back( prefth );
    #endif
            _his_len++;
    //        printf("len:%d, ",_his_len);
        } else {
            latest_entry->end_txn = false;
            latest_entry->end = INF;
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

    blatch = false;
}

#endif
