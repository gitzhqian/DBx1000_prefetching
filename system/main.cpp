#include <random>
#include <regex>
#include <thread>
#include <benchmarks/ycsb_query.h>
#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "test.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "btree_store.h"

#if ENABLE_PCM == true
#include "pcm/cpucounters.h"
#include "pcm/pcm-memory.cpp"
//#include "pcm/pcm-latency.cpp"
#include "pcm/utils.h"
#include <sys/utsname.h>
#endif

void * f(void *);

[[noreturn]] void * monitor(void * id);
//<search key, {<block,right neighbor>,<block,right neighbor>,<block,right neighbor>,,,}>

uint64_t last_monitor;

thread_t ** m_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

bool Greater(std::pair<uint64_t, uint64_t> fr1, std::pair<uint64_t, uint64_t> fr2){ return fr1.second > fr2.second;}

int main(int argc, char* argv[])
{
	parser(argc, argv);
	
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats.init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
	glob_manager->init();
	if (g_cc_alg == DL_DETECT) 
		dl_detector.init();
	printf("mem_allocator initialized!\n");
	workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl; break;
		case TPCC :
			m_wl = new tpcc_wl; break;
		case TEST :
			m_wl = new TestWorkload; 
			((TestWorkload *)m_wl)->tick();
			break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("workload(database) initialized!\n");

	if (WORKLOAD == YCSB){
        uint64_t table_sz = m_wl->tables["MAIN_TABLE"]->get_table_size();
        printf("current main table size:%lu \n", table_sz);
        printf("current main index levels:%u \n", m_wl->indexes["MAIN_INDEX"]->GetTreeHeight(
                (table_sz - 1), KEY_SIZE));
        printf("current main index number of inner node:%u, number of leaf node:%u \n",
               m_wl->indexes["MAIN_INDEX"]->GetNumberOfNodes().first,
               m_wl->indexes["MAIN_INDEX"]->GetNumberOfNodes().second);
       // printf("current secondary index size:%lu \n", m_wl->indexes["SECONDARY_INDEX"]->secondary_index_.size());
	}

	uint64_t thd_cnt = g_thread_cnt;
	pthread_t p_thds[thd_cnt - 1];
	m_thds = new thread_t * [thd_cnt];
	for (uint32_t i = 0; i < thd_cnt; i++)
		m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), 64);
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), 64);
	if (WORKLOAD != TEST)
		query_queue->init(m_wl);
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

    for (auto itr = distance_4_paths_set.begin(); itr != distance_4_paths_set.end(); ++itr) {
        distance_4_paths.emplace_back(*itr);
    }
    for (auto itr = distance_5_paths_set.begin(); itr != distance_5_paths_set.end(); ++itr) {
        distance_5_paths.emplace_back(*itr);
    }
    printf("distance_6_paths_ size:%lu. \n", distance_6_paths_.size());
    for (int i = 0; i < distance_6_paths_.size(); ++i) {
        auto node = distance_6_paths_[i];
        if (node == nullptr) continue;
        auto ret = distance_6_paths_map.find(reinterpret_cast<uint64_t>(node));
        if (ret == distance_6_paths_map.end()){
            distance_6_paths_map.emplace(reinterpret_cast<uint64_t>(node),1);
            distance_6_paths.emplace_back(node);
        }else{
            ret->second += 1;
        }
    }
    printf("distance_6_paths_map size:%lu. \n", distance_6_paths_map.size());
//    std::sort(distance_6_paths_map.begin(), distance_6_paths_map.end(), Greater);
    std::set<int> cnter;
    std::map<int, std::vector<void *>> total_cnter;
    for (auto itr = distance_6_paths_map.begin(); itr != distance_6_paths_map.end(); ++itr) {
        auto node_counter = *itr;
        auto counter = node_counter.second;
        auto node = node_counter.first;
        auto ret = cnter.emplace(counter);
        if (ret.second){
            std::vector<void *> vtc;
            vtc.emplace_back(reinterpret_cast<void *>(node));
            total_cnter.emplace(counter,vtc);
        }else{
            auto ret = total_cnter.find(counter);
            if (ret != total_cnter.end()){
                ret->second.emplace_back(reinterpret_cast<void *>(node));
            }
        }
    }
    uint32_t i=0;
    std::map<int, std::vector<void *>>::reverse_iterator itr_rev;
    for (itr_rev = total_cnter.rbegin(); itr_rev != total_cnter.rend()&&i<5000; itr_rev++) {
        auto nods = (*itr_rev).second;
        auto counts = (*itr_rev).first;
        int nodsz = nods.size();
        for (int j = 0; j < nods.size(); ++j) {
            distance_6_paths_k.emplace_back(nods[j]);
        }
        i = i+nodsz;
//        printf("total_cnter, access count:%d, node size %zu, \n",counts,nods.size());
    }
    printf("distance_6_paths_k size:%lu. \n", distance_6_paths_k.size());

    printf("query_queue initialized!\n");
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif

	for (uint32_t i = 0; i < thd_cnt; i++){
        m_thds[i]->init(i, m_wl);
	}
    printf("threads initialized!\n");

//#if (PATH_PREFETCHING == true || AHEAD_PREFETCH == true)
	//generate keys and access paths of the queries
//	auto ycsb_index =  m_wl->indexes["MAIN_INDEX"];
//    ycsb_index->GetKeysPaths(all_search_keys, KEY_SIZE,  keys_paths);
//    printf("search keys' access paths initialized!\n");
//#endif

	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

#if ENABLE_PCM == true
//    print_environment();
    PCM *pcm_;
    std::unique_ptr<SystemCounterState> before_sstate;
    std::unique_ptr<SystemCounterState> after_sstate;
    pcm_ = PCM::getInstance();
    auto status = pcm_->program();
    if (status != PCM::Success) {
        std::cout << "Error opening PCM: " << status << std::endl;
        if (status == PCM::PMUBusy)
            pcm_->resetPMU();
        else
            exit(0);
    }
    before_sstate = tbb::internal::make_unique<SystemCounterState>();
    *before_sstate = getSystemCounterState();
#endif

    printf("running...\n");
	// spawn and run txns again.
#if PATH_PREFETCHING == true
	uint32_t pthid = thd_cnt;
    pthread_create(&p_thds[pthid], NULL, monitor, (void *)(uint64_t)pthid);
#endif
	int64_t starttime = get_server_clock();
	for (uint32_t i = 0; i < thd_cnt - 1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f, (void *)vid);
	}
	f((void *)(thd_cnt - 1));
	for (uint32_t i = 0; i < thd_cnt - 1; i++)
		pthread_join(p_thds[i], NULL);
	int64_t endtime = get_server_clock();

#if PATH_PREFETCHING == true
    history_search_keys.empty();
#endif

	if (WORKLOAD != TEST) {
		printf("PASS! SimTime = %ld\n", endtime - starttime);
		if (STATS_ENABLE)
			stats.print();
	} else {
		((TestWorkload *)m_wl)->summarize();
	}

#if JUMP_PREFETCHING
    uint64_t key_sz = distinct_search_keys.size();
	uint64_t total_chain_sz = total_chain_length;
	uint64_t average = total_chain_sz/key_sz;
	printf("key_sz:%lu,total chain lenght:%lu,the average chain length:%lu. \n",
           key_sz, total_chain_sz, average);
#endif

#if ENABLE_PCM == true
    after_sstate = tbb::internal::make_unique<SystemCounterState>();
    *after_sstate = getSystemCounterState();

    std::cout << "PCM Metrics:"
              << "\n"
              << "\tL2 Hits: "             << getL2CacheHits(*before_sstate, *after_sstate) << "\n"
              << "\tL3 Hits: "             << getL3CacheHits(*before_sstate, *after_sstate) << "\n"
              << "\tL1 misses: "           << ( getL2CacheHits(*before_sstate, *after_sstate)+
                                                getL3CacheHits(*before_sstate, *after_sstate)+
                                                getL3CacheMisses(*before_sstate, *after_sstate) )<< "\n"
              << "\tL2 misses: "           << getL2CacheMisses(*before_sstate, *after_sstate) << "\n"
              << "\tL3 misses: "           << getL3CacheMisses(*before_sstate, *after_sstate) << "\n"
              << "\tIPC: "                 << getIPC(*before_sstate, *after_sstate) << "\n"
              << "\tCycles: "              << getCycles(*before_sstate, *after_sstate) << "\n"
              << "\tInstructions: "        << getInstructionsRetired(*before_sstate, *after_sstate) << "\n"
              << "\tDRAM Reads (bytes): "  << getBytesReadFromMC(*before_sstate, *after_sstate) << "\n"
              << "\tDRAM Writes (bytes): " << getBytesWrittenToMC(*before_sstate, *after_sstate) << "\n"
              << std::endl;
    // calculate the throughput and abort rate for the first round.
    pcm_->cleanup();
#endif

	//finally, compute current table size
    if (WORKLOAD == YCSB){
//        auto the_table = m_wl->tables["MAIN_TABLE"];
//        uint64_t tab_size = the_table->get_table_size();
//        printf("current main table size:%lu \n", tab_size);
//        printf("current main index levels(include leaf):%u \n",
//               m_wl->indexes["MAIN_INDEX"]->GetTreeHeight((tab_size - 1), KEY_SIZE));
//        printf("current main index number of inner node:%u, number of leaf node:%u \n",
//               m_wl->indexes["MAIN_INDEX"]->GetNumberOfNodes().first,
//               m_wl->indexes["MAIN_INDEX"]->GetNumberOfNodes().second);
//        auto the_index_s = m_wl->indexes["SECONDARY_INDEX"];
        //auto s_sz = the_index_s->secondary_index_.size();
        //printf("current secondary index size:%lu \n", s_sz);
    }


	return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid]->run();
	return NULL;
}

//struct frequences{
//    void *node;
//    uint64_t counter;
//};
//bool Greater(frequences fr1, frequences fr2){ return fr1.counter > fr2.counter;}
[[noreturn]] void * monitor(void * id){
    PinToCore(2);
    // Continue till signal is not false
//    std::vector<frequences> hist_6;
    uint64_t monitor_flg=0;
    while (true) {
//        uint64_t starttime = get_sys_clock();
//        printf("distance 3 size: %lu. \n", distance_3_paths.size());
        __builtin_prefetch((const void *) (root_node), 0, 1);
        for (auto dis3 = distance_3_paths.cbegin();  dis3 != distance_3_paths.cend();  ++dis3 ) {
            void *ptr = *dis3;
            if (ptr == nullptr) continue;
            BaseNode *node_r = reinterpret_cast<BaseNode *>(ptr);
            for (uint32_t i = 0; i < DRAM_BLOCK_SIZE / CACHE_LINE_SIZE; ++i) {
                __builtin_prefetch((const void *)((char *)node_r + i * CACHE_LINE_SIZE), 0, 1);
            }
        }

//        printf("distance 4 size: %lu. \n", distance_4_paths.size());
        //100 million operations, 10  million dataset, height =7, access size=
        //100 million operations, 20  million dataset, height =7, access size=
        //100 million operations, 1   million dataset, height =6, access size=550(0.2MB)
        //height = 5: his_path[1,2,3]
        int dsz4=distance_4_paths.size() ;
        for (int i=0; i<dsz4; i++) {
            void * ptr = distance_4_paths[i];
            if (ptr == nullptr) continue;
            BaseNode *node_r = reinterpret_cast<BaseNode *>(ptr);
            for (uint32_t i = 0; i < SPLIT_THRESHOLD / CACHE_LINE_SIZE; ++i) {
                __builtin_prefetch((const void *)((char *)node_r + i * CACHE_LINE_SIZE), 0, 1);
            }
        }
//        printf("distance 5 size: %lu. \n", distance_5_paths.size());
        //100 million operations, 10  million dataset, height =7, access size=7333(3.5MB),
        //100 million operations, 20  million dataset, height =7, access size=14107(7MB)
        //100 million operations, 1   million dataset, height =6, access size=7535(3.7MB)
        //height = 6: his_path[1,2,3,4]
        int dsz5=distance_5_paths.size();
        for (int i=0; i<dsz5; i++) {
            void * ptr = distance_5_paths[i];
            if (ptr == nullptr) continue;
            BaseNode *node_r = reinterpret_cast<BaseNode *>(ptr);
            for (uint32_t i = 0; i < SPLIT_THRESHOLD / CACHE_LINE_SIZE; ++i) {
                __builtin_prefetch((const void *)((char *)node_r + i * CACHE_LINE_SIZE), 0, 1);
            }
        }
//        printf("distance 6 size: %lu. \n", distance_6_paths.size());
        //100 million operations, 10 million dataset, height =7,  access size=116440(58MB),
        //100 million operations, 20 million dataset, height =7,  access size=204196(102MB)
        //height = 7: his_path[1,2,3,4,5]
//        int dsz6=distance_6_paths.size();
//        monitor_flg = (monitor_flg == 14) ? 1: (monitor_flg+1);
//        int start = (dsz6/14)*(monitor_flg-1);
//        int end   = (dsz6/14)*monitor_flg;
//        for (int i=start; i<end; i++) {
//            void * ptr = distance_6_paths[i];
//            if (ptr == nullptr) continue;
//            BaseNode *node_r = reinterpret_cast<BaseNode *>(ptr);
//            for (uint32_t i = 0; i < SPLIT_THRESHOLD / CACHE_LINE_SIZE; ++i) {
//                __builtin_prefetch((const void *)((char *)node_r + i * CACHE_LINE_SIZE), 0, 1);
//            }
//        }

//        printf("distance 6 k size: %lu. \n", distance_6_paths_k.size());
        int dsz6_k=distance_6_paths_k.size();
        for (int i=0; i<dsz6_k; i++) {
            void * ptr = distance_6_paths_k[i];
            if (ptr == nullptr) continue;
            BaseNode *node_r = reinterpret_cast<BaseNode *>(ptr);
            for (uint32_t i = 0; i < SPLIT_THRESHOLD / CACHE_LINE_SIZE; ++i) {
                __builtin_prefetch((const void *)((char *)node_r + i * CACHE_LINE_SIZE), 0, 1);
            }
        }

        std::this_thread::sleep_for(std::chrono::microseconds(80000));

        //height = 7: his_path[1,2,3,4,5]
//        uint64_t hisreq_sz = history_requests.size();
//        for (int i=0; i<hisreq_sz; ++i) {
//            auto req_ = history_requests.front();
//            auto req = reinterpret_cast<ycsb_request *>(req_);
//            if (req == nullptr) continue;
//            if (req->paths.size() > 4){
//                void *dis6_node = req->paths[4].first;
//                bool find = false;
//                for (int i = 0; i <hist_6.size() ; ++i) {
//                    if (hist_6[i].node == dis6_node){
//                        hist_6[i].counter += 1;
//                        find = true;
//                    }
//                }
//                if(!find){
//                    frequences fre;
//                    fre.node = dis6_node;
//                    fre.counter =1;
//                    hist_6.emplace_back(fre);
//                }
//            }
//            history_requests.pop();
//        }
//
//        int dsz6=hist_6.size();
////        printf("distance 6 size: %d. \n", dsz6);
//        if (dsz6 > 6000) {
//            std::sort(hist_6.begin(), hist_6.end(), Greater);
//            dsz6 = 6000;
//        }
//        for (int i=0; i<dsz6; i++) {
//            void * ptr = hist_6[i].node;
//            if (ptr == nullptr) continue;
//            BaseNode *node_r = reinterpret_cast<BaseNode *>(ptr);
//            for (uint32_t i = 0; i < SPLIT_THRESHOLD / CACHE_LINE_SIZE; ++i) {
//                __builtin_prefetch((const void *)((char *)node_r + i * CACHE_LINE_SIZE), 0, 1);
//            }
//        }

        //4. Sleep a bit, 1 second
//        std::this_thread::sleep_for(std::chrono::microseconds(2000));
//        printf("monitoring, total time: %lu", get_sys_clock() - starttime) ;
    }

}

/*
__builtin_prefetch (const void *addr[, rw[, locality]])
addr (required)
Represents the address of the memory.

rw (optional)
A compile-time constant which can take the values:
0 (default): prepare the prefetch for a read
1 : prepare the prefetch for a write to the memory

 locality (optional)
A compile-time constant integer which can take the following temporal locality (L) values:
0: None, the data can be removed from the cache after the access.
1: Low, L3 cache, leave the data in the L3 cache level after the access.
2: Moderate, L2 cache, leave the data in L2 and L3 cache levels after the access.
3 (default): High, L1 cache, leave the data in the L1, L2, and L3 cache levels after the access.
*/
