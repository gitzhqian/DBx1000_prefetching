#ifndef _CONFIG_H_
#define _CONFIG_H_


/***********************************************/
// Simulation + Hardware
/***********************************************/
#define THREAD_CNT					3
#define PART_CNT					1
// each transaction only accesses 1 virtual partition.
//But the lock/ts manager and index are not aware of such partitioning.
//VIRTUAL_PART_CNT describes the request distribution and is only used to generate queries.
//For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT			1
#define PAGE_SIZE					4096 
#define CL_SIZE						64
// CPU_FREQ is used to get accurate timing info 
#define CPU_FREQ 					3.5 	// in GHz/s

// # of transactions to run for warmup
#define WARMUP						0
// YCSB or TPCC
#define WORKLOAD 					YCSB
// print the transaction latency distribution
#define PRT_LAT_DISTR				false
#define STATS_ENABLE				true
#define TIME_ENABLE					true

#define CACHE_LINE_SIZE             64

#define MEM_ALLIGN					8 

// [THREAD_ALLOC]
#define THREAD_ALLOC				false
#define THREAD_ARENA_SIZE			(1UL << 22) 
#define MEM_PAD 					true

// [PART_ALLOC] 
#define PART_ALLOC 					false
#define MEM_SIZE					(1UL << 30) 
#define NO_FREE						false

/***********************************************/
// Concurrency Control
/***********************************************/
// WAIT_DIE, NO_WAIT, DL_DETECT, TIMESTAMP, MVCC, HEKATON, HSTORE, OCC, VLL, TICTOC, SILO
// TODO TIMESTAMP does not work at this moment
#define CC_ALG 						HEKATON
#define ISOLATION_LEVEL 			SNAPSHOT

// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER					false
// transaction roll back changes after abort
#define ROLL_BACK					true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN					false
#define BUCKET_CNT					31
#define ABORT_PENALTY 				100000
#define ABORT_BUFFER_SIZE			10
#define ABORT_BUFFER_ENABLE			false


// [ INDEX ]
#define ENABLE_LATCH				true
#define CENTRAL_INDEX				true
#define CENTRAL_MANAGER 			false
#define INDEX_STRUCT				IDX_BTREE
//#define INDEX_STRUCT				IDX_HASH
#define BTREE_ORDER 				64

// [DL_DETECT] 
#define DL_LOOP_DETECT				1000 	// 100 us
#define DL_LOOP_TRIAL				100	// 1 us
#define NO_DL						KEY_ORDER
#define TIMEOUT						1000000 // 1ms
// [TIMESTAMP]
#define TS_TWR						false
#define TS_ALLOC					TS_CAS
#define TS_BATCH_ALLOC				false
#define TS_BATCH_NUM				1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
//#define HIS_RECYCLE_LEN				10
//#define MAX_PRE_REQ					1024
//#define MAX_READ_REQ				1024
#define MIN_TS_INTVL				5000000 //5 ms. In nanoseconds
// [OCC]
#define MAX_WRITE_SET				10
#define PER_ROW_VALID				true
// [TICTOC]
#define WRITE_COPY_FORM				"data" // ptr or data
#define TICTOC_MV					true
#define WR_VALIDATION_SEPARATE		true
#define WRITE_PERMISSION_LOCK		true
#define ATOMIC_TIMESTAMP			"false"
// [TICTOC, SILO]
#define VALIDATION_LOCK				"waiting" // no-wait or waiting
#define PRE_ABORT					"true"
#define ATOMIC_WORD					true
// [HSTORE]
// when set to true, hstore will not access the global timestamp.
// This is fine for single partition transactions. 
#define HSTORE_LOCAL_TS				false
// [VLL] 
#define TXN_QUEUE_SIZE_LIMIT		THREAD_CNT

/***********************************************/
// Logging
/***********************************************/
#define LOG_COMMAND					false
#define LOG_REDO					false
#define LOG_BATCH_TIME				10000 // in ms

/***********************************************/
// Benchmark
/***********************************************/
// max number of rows touched per transaction
#define MAX_ROW_PER_TXN				1024
#define QUERY_INTVL 				1UL
#define MAX_TXN_PER_PART 			(1000*1)
#define FIRST_PART_LOCAL 			true
#define MAX_TUPLE_SIZE				100 // in bytes

// ==== [YCSB] ====
#define INIT_PARALLELISM			1
//#define SYNTH_TABLE_SIZE 			(1024 * 1024 * 10)
#define SYNTH_TABLE_SIZE 			(1024 * 1024)
#define ZIPF_THETA 					0.0
#define READ_PERC 					0.5
#define WRITE_PERC 					0.5
#define SCAN_PERC 					0.0
#define INSERT_PERC                 0.0
#define SCAN_LEN					30
#define PART_PER_TXN 				1
#define PERC_MULTI_PART				1
#define REQ_PER_QUERY				10
#define FIELD_PER_TUPLE				10
// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL					false
// Some of the transactions read the data but never use them. 
// If TPCC_ACCESS_ALL == fales, then these parts of the transactions
// are not modeled.
#define TPCC_ACCESS_ALL 			false 
#define WH_UPDATE					true
#define NUM_WH 						5
#define TPCC_ALOW_INSERT            false
//
enum TPCCTxnType {TPCC_ALL, 
				TPCC_PAYMENT, 
				TPCC_NEW_ORDER, 
				TPCC_ORDER_STATUS, 
				TPCC_DELIVERY, 
				TPCC_STOCK_LEVEL};
extern TPCCTxnType 					g_tpcc_txn_type;

#define TXN_TYPE					TPCC_ALL
#define PERC_PAYMENT 				0.5
#define FIRSTNAME_MINLEN 			8
#define FIRSTNAME_LEN 				16
#define LASTNAME_LEN 				16
#define DIST_PER_WARE				10

/***********************************************/
// TODO centralized CC management. 
/***********************************************/
#define MAX_LOCK_CNT				(20 * THREAD_CNT) 
#define TSTAB_SIZE                  50 * THREAD_CNT
#define TSTAB_FREE                  TSTAB_SIZE 
#define TSREQ_FREE                  4 * TSTAB_FREE
#define MVHIS_FREE                  4 * TSTAB_FREE
#define SPIN                        false

/***********************************************/
// Test cases
/***********************************************/
#define TEST_ALL					true
enum TestCases {
	READ_WRITE,
	CONFLICT
};
extern TestCases					g_test_case;
/***********************************************/
// DEBUG info
/***********************************************/
#define WL_VERB						true
#define IDX_VERB					false
#define VERB_ALLOC					true

#define DEBUG_LOCK					false
#define DEBUG_TIMESTAMP				false
#define DEBUG_SYNTH					false
#define DEBUG_ASSERT				false
#define DEBUG_CC					false //true

/***********************************************/
// Constant
/***********************************************/
// INDEX_STRUCT
#define IDX_HASH 					1
#define IDX_BTREE					2

// WORKLOAD
#define YCSB						1
#define TPCC						2
#define TEST						3
// Concurrency Control Algorithm
#define NO_WAIT						1
#define WAIT_DIE					2
#define DL_DETECT					3
#define TIMESTAMP					4
#define MVCC						5
#define HSTORE						6
#define OCC							7
#define TICTOC						8
#define SILO						9
#define VLL							10
#define HEKATON 					11
//Isolation Levels 
#define SERIALIZABLE				1
#define SNAPSHOT					2
#define REPEATABLE_READ				3
// TIMESTAMP allocation method.
#define TS_MUTEX					1
#define TS_CAS						2
#define TS_HW						3
#define TS_CLOCK					4

//database engine
#define PTR0                        0
#define PTR1                        1
#define PTR2                        2

#define PREFETCH_LEVEL              3
#define LEAF_PREFETCH               false
#define AHEAD_PREFETCH              false
#define PREFETCHING					false
#define PATH_PREFETCHING            false
#define JUMP_PREFETCHING			true
#define JUMP_PREFETCHING_CHAIN      false
#define JUMP_1			            false
#define JUMP_queue			        true
#define INSERT_INDIRECT				false
#define SECONDARY_INDEX             false
#define KEY_SIZE                     8
#define S1_KEY_SIZE                  10
#define S1_PAYLOAD_SIZE              8
#define S1_SPLIT_THRESHOLD           1024
#define S1_DRAM_BLOCK_SIZE           1536
#define VALID_COUNTER                32766
//with the supported processors
#define ENABLE_PCM                   false
#define GARBAGE                      false

//#define ENGINE_TYPE                 PTR0
//#define ENGINE_TYPE                 PTR1
#define ENGINE_TYPE                   PTR2

//#define DRAM_BLOCK_SIZE             16*1024
//#define SPLIT_THRESHOLD             1024
//#define PAYLOAD_SIZE                1000

#define DRAM_BLOCK_SIZE             1536
#define SPLIT_THRESHOLD             512
#define PAYLOAD_SIZE                8

#define BUFFER_SEGMENT_SIZE          1024*1024
#define MERGE_THRESHOLD              32*1024

#define DEFAULT_BLOCKS                  16*1024*1024
#define DURAL_POINTER_ARRAY_MAX_SIZE    16*1024


#endif
