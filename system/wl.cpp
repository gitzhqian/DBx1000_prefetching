#include "global.h"
#include "helper.h"
#include "wl.h"
#include "row.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "btree_store.h"
#include "catalog.h"
#include "mem_alloc.h"

RC workload::init() {
	sim_done = false;
	return RCOK;
}

RC workload::init_schema(string schema_file) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);	
	string line;
	ifstream fin(schema_file);
    Catalog * schema;
    while (getline(fin, line)) {
		if (line.compare(0, 6, "TABLE=") == 0) {
			string tname;
			tname = &line[6];
			schema = (Catalog *) _mm_malloc(sizeof(Catalog), CL_SIZE);
			getline(fin, line);
			int col_count = 0;
			// Read all fields for this table.
			vector<string> lines;
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}
			schema->init( tname.c_str(), lines.size() );
			for (UInt32 i = 0; i < lines.size(); i++) {
				string line = lines[i];
			    size_t pos = 0;
				string token;
				int elem_num = 0;
				int size = 0;
				string type;
				string name;
				while (line.length() != 0) {
					pos = line.find(",");
					if (pos == string::npos)
						pos = line.length();
	    			token = line.substr(0, pos);
			    	line.erase(0, pos + 1);
					switch (elem_num) {
					case 0: size = atoi(token.c_str()); break;
					case 1: type = token; break;
					case 2: name = token; break;
					default: assert(false);
					}
					elem_num ++;
				}
				assert(elem_num == 3);
                schema->add_col((char *)name.c_str(), size, (char *)type.c_str());
				col_count ++;
			}
			table_t * cur_tab = (table_t *) _mm_malloc(sizeof(table_t), CL_SIZE);
			cur_tab->init(schema);
			tables[tname] = cur_tab;
        } else if (!line.compare(0, 6, "INDEX=")) {
			string iname;
			iname = &line[6];
			getline(fin, line);

            vector<string> lines;
            while (line.length() > 1) {
                lines.push_back(line);
                getline(fin, line);
            }

            vector<string> items;
            for (UInt32 i = 0; i < lines.size(); i++) {
                string line = lines[i];
                string token;
                size_t pos;
                while (line.length() != 0) {
                    pos = line.find(",");
                    if (pos == string::npos)
                        pos = line.length();
                    token = line.substr(0, pos);
                    items.push_back(token);
                    line.erase(0, pos + 1);
                }
            }

			string tname(items[0]);
            string secondary(items[3]);
            string sname(items[2]);

			int part_cnt = (CENTRAL_INDEX)? 1 : g_part_cnt;
			if (tname == "ITEM")
				part_cnt = 1;

#if ENGINE_TYPE == PTR0
//            INDEX * index = index_btree_store::GetInstance();
            INDEX * index = (INDEX *) _mm_malloc(sizeof(INDEX), 64);
            new(index) INDEX();
        #if WORKLOAD == YCSB
            index->init_btree_store(KEY_SIZE, tables[tname]);
        #endif
#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
    #if INDEX_STRUCT == IDX_HASH
            INDEX * index = (INDEX *) _mm_malloc(sizeof(INDEX), 64);
            new(index) INDEX();
        #if WORKLOAD == YCSB
                index->init(part_cnt, tables[tname], g_synth_table_size * 2);
        #elif WORKLOAD == TPCC
                assert(tables[tname] != NULL);
                index->init(part_cnt, tables[tname], stoi( items[1] ) * part_cnt);
        #endif
    #elif INDEX_STRUCT == IDX_BTREE
                INDEX * index = (INDEX *) _mm_malloc(sizeof(INDEX), 64);
                new(index) INDEX();
//                index->init(part_cnt, tables[tname]);
                index->init_btree_store(KEY_SIZE, tables[tname]);
    #endif
#endif
			indexes[iname] = index;

#if SECONDARY_INDEX
			if (secondary == "1"){
                INDEX *index_s = (INDEX *) _mm_malloc(sizeof(INDEX), 64);
                new(index_s) INDEX();
#if ENGINE_TYPE == PTR0
                index_s->init_btree_store(S1_KEY_SIZE, tables[tname]);
#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
                index_s->init_btree_store(S1_KEY_SIZE, tables[tname]);
#endif
                indexes[sname] = index_s;
			}
#endif
		}
    }
	fin.close();
	return RCOK;
}



void workload::index_insert(string index_name, uint64_t key, row_t * row) {
	assert(false);
	INDEX * index = (INDEX *) indexes[index_name];
	index_insert(index, key, row, row->data);
}

void workload::index_insert(INDEX * index, uint64_t key, row_t * row, char *data_,
                                                         int64_t part_id) {
    RC rc;
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), pid );
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

    row_t * new_row = NULL;
    void *row_item;

#if ENGINE_TYPE == PTR1
    uint64_t new_row_addr = reinterpret_cast<uint64_t>(row);
    char *data = reinterpret_cast<char *>(&new_row_addr);
    retry:
    rc = index->index_insert(key, row_item, data);
#elif ENGINE_TYPE == PTR2
    uint64_t new_row_addr = reinterpret_cast<uint64_t>(m_item);
    char *data = reinterpret_cast<char *>(&new_row_addr);
    retry:
    rc = index->index_insert(key, row_item, data);
#elif ENGINE_TYPE == PTR0
    void *row_item;
    retry:
    rc = index->index_insert(key, row_item, data_);
#endif

    if (rc!=RCOK) {
        goto retry;
    }
//	rc = index->index_insert(key, m_item, pid);
    assert( rc == RCOK );

    new_row = reinterpret_cast<row_t *>(row_item);
    new_row->valid = true;

}


