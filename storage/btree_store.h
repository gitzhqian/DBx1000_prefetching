//
// Created by zhangqian on 2023/4/11.
//
#ifndef _BTREE_STORE_H_
#define _BTREE_STORE_H_



#include <iostream>
#include <list>
#include <algorithm>
#include "valen_buffer.h"
#include "index_base.h"
#include "row.h"
#include "table.h"
#include "row_hekaton.h"
#include "global.h"

/**
#record size :
#|50 |100 |150 |300 |450 |650 |800 |1050 |
#node size :(entries|number_of_inners|number_of_leafs|tree_levels)
[    ]|0.5*1024          |1*1024             |1.5*1024           |2*1024             |4*1024             |8*1024             |16*1024             |32*1024           |64*1024           |128*1024          |256*1024         |512*1024         |640*1024         |1024*1024        |
[8   ]|4,486706,5177344,6|10,135720,2057667,6|15,105726,1310653,6|20,79412 ,1021267,5|42,34850 ,458778 ,5|84,16164 ,211511 ,5|170,5548  ,80074  ,5|340,3148 ,44352 ,5|682,1508 ,21196 ,4|1364,742 ,10208 ,4|2730,264 ,4421 ,4|5460,124 ,2053 ,4|6826,114 ,2048 ,4|10922,58 ,1024 ,3|
[50  ]|4,486706,5177344,6|10,135720,2057667,6|15,105726,1310653,6|20,79412 ,1021267,5|41,34478 ,458869 ,5|83,16022 ,211499 ,5|166,7024  ,93994  ,5|334,3146 ,43922 ,5|668,1502 ,21793 ,4|1337,792 ,9957  ,4|2674,380 ,5429 ,4|5349,128 ,2108 ,4|6687,112 ,2048 ,4|10699,56 ,1024 ,3|
[100 ]|3,486626,5242879,6|6 ,270840,3454917,6|10,135720,2059782,6|13,115304,1455794,6|27,59686 ,720896 ,5|55,26054 ,336686 ,5|110,9248  ,143557 ,5|221,4420 ,65537 ,5|442,2098 ,34883 ,4|885 ,1020,16713 ,4|1771,444 ,8192 ,4|3542,226 ,4096 ,4|4427,226 ,4096 ,4|7084 ,114,2048 ,4|
[200 ]|0,0     ,0      ,0|3 ,486626,5242879,6|6 ,270840,3451647,6|8 ,212874,2555909,6|16,105532,1245196,6|32,53074 ,589862 ,5|65 ,16212 ,264779 ,5|131,8678 ,131071,5|264,4018 ,63667 ,5|528 ,1922,30600 ,4|1056,926 ,16042,4|2113,468 ,8192 ,4|2642,390 ,5229 ,4|4227 ,224,4096 ,4|
[350 ]|0,0     ,0      ,0|0 ,0     ,0      ,0|3 ,486626,5242879,6|5 ,271014,3473408,6|10,135720,2057667,6|20,79412 ,1021267,5|41 ,34478 ,458869 ,5|82 ,15688,210984,5|164,6302 ,93063 ,5|329 ,3042,44472 ,5|658 ,1486,22338,4|1317,778 ,9864 ,4|1646,478 ,8210 ,4|2634 ,394,5167 ,4|
[500 ]|0,0     ,0      ,0|0 ,0     ,0      ,0|0 ,     0,      0,0|3 ,486626,5242879,6|7 ,213246,2621434,6|14,115520,1451373,6|29 ,52438 ,659796 ,5|59 ,26016,327680,5|119,8966 ,136027,5|239 ,4362,65535 ,5|478 ,1932,33169,4|956 ,948 ,16318,4|1195,922 ,13743,4|1913 ,472,8192 ,4|
[650 ]|0,0     ,0      ,0|0 ,0     ,0      ,0|0 ,     0,      0,0|0 ,     0,      0,0|5 ,271014,3473408,6|11,135502,1725450,6|23 ,55224 ,851967 ,5|46 ,31914,418511,5|93 ,16218,196607,5|187 ,5178,78422 ,5|375 ,2582,38412,5|751 ,1104,17763,4|938 ,908 ,16300,4|1502 ,498,8366 ,4|
[800 ]|0,0     ,0      ,0|0 ,0     ,0      ,0|0 ,     0,      0,0|0 ,     0,      0,0|4 ,486706,5187344,6|9 ,135628,2074842,6|19 ,79640 ,1022596,6|38 ,37616,524287,5|77 ,18142,240118,5|154 ,7904,108881,5|309 ,3586,53851,5|618 ,1674,25523,4|772 ,1010,16760,4|1236 ,918,14214,4|
[1000]|0,0     ,0      ,0|0 ,0     ,0      ,0|0 ,     0,      0,0|0 ,     0,      0,0|3 ,486626,5242879,6|7 ,213246,2621422,6|15 ,105726,1310653,6|31 ,53236,655226,5|62 ,25438,318556,5|125 ,8450,131071,5|250 ,4306,64558,5|500 ,1852,31851,4|625 ,1646,25312,4|1000 ,916,16168,4|
[1050]|0,0     ,0      ,0|0 ,0     ,0      ,0|0 ,     0,      0,0|0 ,     0,      0,0|3 ,486626,5242879,6|7 ,213246,2621434,6|14 ,115520,1451373,6|29 ,52438,659796,5|59 ,26016,327680,5|119 ,8966,136027,5|238 ,4458,65535,5|477 ,1926,33345,4|596 ,1760,27465,4|954  ,950,16307,4|

---------------------------------------------------------------
inner node:
 size = 1024bytes = 40 entries
---------------------------------------------------------------
leaf node:
===============================================================
node size = 15 entries
record size:
| 20   | 50   | 100  | 200  | 400  | 600  | 800   | 1000  |
| 1088 | 1536 | 2368 | 3776 | 6784 | 9876 | 12800 | 15872 |
| 1536 | 1536 | 1536 | 1536 | 1536 | 1536 | 1536  | 1536  |
===============================================================
record size=100
node size:
5   | 10   | 15   | 20   | 25   | 30   | 35   |
896 | 1600 | 2368 | 3072 | 3800 | 4480 | 5248 |
576 | 1024 | 1536 | 1984 | 2496 | 2944 | 3456 |
===============================================================
**/
/** leaf node
* For the primary index, key size=8, layout:
 * |header----------------|
 * |meta1|meta2|meta3|....|
 * |key1 |key2 |key3 |....|
 * |data1|data2|data3|....|
 * meta1 contains key1
* For the primary index or secondary index, key size>8, layout:
 * |header-----------------------------|
 * |meta1    |meta2    |meta3    |.....|
 * |key1data1|key2data2|key3data3|.....|
 * leaf node size=1536bytes=22entries
*/
/**
 * meta = row_t
 * ptr1 and ptr2, a row_t size = 88bytes
 * ptr0, a row_t size = 48bytes
 */

constexpr static int MAX_FREEZE_RETRY = 4;//2 3 4
constexpr static int MAX_INSERT_RETRY = 2;//4 6 8

struct ParameterSet {
    uint32_t split_threshold;
    uint32_t merge_threshold;
    uint32_t leaf_node_size;
    uint32_t payload_size;
    uint32_t key_size;

    ParameterSet() : split_threshold(3072), merge_threshold(1024),
                     leaf_node_size(4096), payload_size(8), key_size(8) {}

    ParameterSet(uint32_t split_threshold_, uint32_t merge_threshold_,
                 uint32_t leaf_node_size_,  uint32_t payload_size_, uint32_t key_size_)
            : split_threshold(split_threshold_),
              merge_threshold(merge_threshold_),
              leaf_node_size(leaf_node_size_), payload_size(payload_size_),
              key_size(key_size_){}

    ~ParameterSet()  = default;
};

struct ReturnCode {
    enum RC {
        RetInvalid,           //0
        RetOk,                //1
        RetKeyExists,         //2
        RetNotFound,          //3
        RetNodeFrozen,        //4
        RetCASFail,           //5
        RetNotEnoughSpace,    //6
        RetNotNeededUpdate,   //7
        RetRetryFailure,      //8 //9
        RetDirty
    };

    uint8_t rc;

    constexpr explicit ReturnCode(uint8_t r) : rc(r) {}

    constexpr ReturnCode() : rc(RetInvalid) {}

    ~ReturnCode() = default;

    constexpr bool inline IsInvalid() const { return rc == RetInvalid; }

    constexpr bool inline IsOk() const { return rc == RetOk; }

    constexpr bool inline IsKeyExists() const { return rc == RetKeyExists; }

    constexpr bool inline IsNotFound() const { return rc == RetNotFound; }

    constexpr bool inline IsNotNeeded() const { return rc == RetNotNeededUpdate; }

    constexpr bool inline IsNodeFrozen() const { return rc == RetNodeFrozen; }

    constexpr bool inline IsCASFailure() const { return rc == RetCASFail; }

    constexpr bool inline IsNotEnoughSpace() const { return rc == RetNotEnoughSpace; }
    constexpr bool inline IsRetryFailure() const { return rc == RetRetryFailure; }
    constexpr bool inline IsRetDirty() const { return rc == RetDirty; }

    static inline ReturnCode NodeFrozen() { return ReturnCode(RetNodeFrozen); }

    static inline ReturnCode KeyExists() { return ReturnCode(RetKeyExists); }

    static inline ReturnCode CASFailure() { return ReturnCode(RetCASFail); }

    static inline ReturnCode Ok() { return ReturnCode(RetOk); }

    static inline ReturnCode NotNeededUpdate() { return ReturnCode(RetNotNeededUpdate); }

    static inline ReturnCode NotFound() { return ReturnCode(RetNotFound); }

    static inline ReturnCode NotEnoughSpace() { return ReturnCode(RetNotEnoughSpace); }
    static inline ReturnCode RetryFailure() { return ReturnCode(RetRetryFailure); }
    static inline ReturnCode WriteDirty() { return ReturnCode(RetDirty); }
};

struct NodeHeader {
    // Header:
    // |-----64 bits----|---32 bits---|---32 bits---|
    // |   status word  |     size    | sorted count|
    //
    // Sorted count is actually the index into the first metadata entry for unsorted records.
    // Size is node size(node header, record meta entries, block space).
    // Status word(64-bit) is subdivided into five fields.
    //    (Internal nodes only use the first two (control and frozen) while leaf nodes use all the five.)
    // Following the node header is a growing array of record meta entries.
    struct StatusWord {
        uint64_t word = 0;

        StatusWord() : word(0) {}

        explicit StatusWord(volatile uint64_t word) : word(word) {}

        static const uint64_t kControlMask = uint64_t{0x7} << 61;           // Bits 64-62
        static const uint64_t kFrozenMask = uint64_t{0x1} << 60;            // Bit 61
        static const uint64_t kRecordCountMask = uint64_t{0xFFFF} << 44;    // Bits 60-45
        static const uint64_t kBlockSizeMask = uint64_t{0x3FFFFF} << 22;    // Bits 44-23
        static const uint64_t kDeleteSizeMask = uint64_t{0x3FFFFF} << 0;    // Bits 22-1

        inline StatusWord Freeze() {
            return StatusWord{word | kFrozenMask};
        }

        inline StatusWord UnFreeze() {
            return StatusWord{word & ~kFrozenMask};
        }

        inline bool IsFrozen() {
            bool is_frozen = (word & kFrozenMask) > 0;
            return is_frozen;
        }

        inline uint16_t GetRecordCount() { return (uint16_t) ((word & kRecordCountMask) >> 44); }

        inline void SetRecordCount(uint16_t count) {
            word = (word & (~kRecordCountMask)) | (uint64_t{count} << 44);
        }

        inline uint32_t GetBlockSize() { return (uint32_t) ((word & kBlockSizeMask) >> 22); }

        inline void SetBlockSize(uint32_t sz) {
            word = (word & (~kBlockSizeMask)) | (uint64_t{sz} << 22);
        }

        inline uint32_t GetDeletedSize() { return (uint32_t) (word & kDeleteSizeMask); }

        inline void SetDeleteSize(uint32_t sz) {
            word = (word & (~kDeleteSizeMask)) | uint64_t{sz};
        }

        inline void PrepareForInsert(uint32_t sz) {
            M_ASSERT(sz > 0, "PrepareForInsert fail, sz <=0. ");
            // Increment [record count] by one and [block size] by payload size(total size)
            word += ((uint64_t{1} << 44) + (uint64_t{sz} << 22));
        }
        inline void FailForInsert(uint32_t sz) {
            // decrease [record count] by one and [block size] by payload size
            word -= ((uint64_t{1} << 44) + (uint64_t{sz} << 22));
        }
    };

    uint32_t size = 0;
    uint32_t sorted_count = 0;
    StatusWord status;

    NodeHeader() : size(0), sorted_count(0) {}

    inline StatusWord GetStatus() {
        return status;
    }
};

class Stack;
class BaseNode {
public:
    bool is_leaf = false;
    NodeHeader header;

    static const inline int KeyCompare(const char *key1, uint32_t size1,
                                       const char *key2, uint32_t size2) {
        if (!key1) {
            return -1;
        } else if (!key2) {
            return 1;
        }
        int cmp;

        size_t min_size = std::min<size_t>(size1, size2);
        if (min_size < 16) {
            cmp = my_memcmp(key1, key2, min_size);
        } else {
            cmp = memcmp(key1, key2, min_size);
        }
        if (cmp == 0) {
            return size1 - size2;
        }
        return cmp;
    }
    static const inline int my_memcmp(const char *key1, const char *key2, uint32_t size) {
        for (uint32_t i = 0; i < size; i++) {
            if (key1[i] != key2[i]) {
                return key1[i] - key2[i];
            }
        }
        return 0;
    }

    // Set the frozen bit to prevent future modifications to the node
    bool Freeze();
    bool UnFreeze();

    explicit BaseNode(bool leaf, uint32_t size) : is_leaf(leaf) {
        header.size = size;
    }

    inline bool IsLeaf() { return is_leaf; }

    inline NodeHeader *GetHeader() { return &header; }

    inline bool IsFrozen() {
        return GetHeader()->GetStatus().IsFrozen();
    }

};

// Internal node: immutable once created, no free space, keys are always sorted
// operations that might mutate the InternalNode:
//    a. create a new node, this will set the freeze bit in status
//    b. update a pointer, this will check the status field and swap in a new pointer
// in both cases, the record metadata should not be touched,
// thus we can safely dereference them without a wrapper.
class InternalNode : public BaseNode {
public:
    uint32_t segment_index=0;
    row_m row_meta[0];

    static void  New(InternalNode **new_node, uint32_t alloc_size,
                     InnerNodeBuffer *inner_node_buffer)   {
//        alloc_size = (alloc_size + sizeof(uint64_t) - 1)/ sizeof(uint64_t) * sizeof(uint64_t);
//        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
//        char *inner_node_ = entry_.second;

//        *new_node = reinterpret_cast<InternalNode *>(inner_node_);
//        memset((*new_node), 0, alloc_size);
        *new_node = reinterpret_cast<InternalNode *>(_mm_malloc(alloc_size, 64));

        (*new_node)->header.size = alloc_size;
//        (*new_node)->segment_index = entry_.first;
    }

// Create an internal node with a new key and associated child pointers inserted
// based on an existing internal node
    static void  New(InternalNode *src_node,   char *key, uint32_t key_size,
                     uint64_t left_child_addr, uint64_t right_child_addr,
                     InternalNode **new_node, InnerNodeBuffer *inner_node_buffer)   {
        size_t alloc_size = src_node->GetHeader()->size +
                              row_m::PadKeyLength(key_size) +
                              sizeof(right_child_addr) + sizeof(row_m);

//        alloc_size = (alloc_size + sizeof(uint64_t) - 1)/ sizeof(uint64_t) * sizeof(uint64_t);
//        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
//        char *inner_node_ = entry_.second;
//        *new_node = reinterpret_cast<InternalNode *>(inner_node_);
//        memset((*new_node), 0, alloc_size);
        *new_node = reinterpret_cast<InternalNode *>(_mm_malloc(alloc_size, 64));

        new(*new_node) InternalNode(alloc_size, src_node, 0,
                                    src_node->header.sorted_count,
                                    key, key_size, left_child_addr, right_child_addr);
//        (*new_node)->segment_index = entry_.first;
    }

// Create an internal node with a single separator key and two pointers
    static void  New(  char * key, uint32_t key_size, uint64_t left_child_addr,
                     uint64_t right_child_addr, InternalNode **new_node,
                     InnerNodeBuffer *inner_node_buffer) {
        size_t alloc_size = sizeof(InternalNode);
        alloc_size = alloc_size + row_m::PadKeyLength(key_size);
        alloc_size = alloc_size + sizeof(left_child_addr) + sizeof(right_child_addr);
        alloc_size = alloc_size + sizeof(row_m) * 2;

//        alloc_size = (alloc_size + sizeof(uint64_t) - 1)/ sizeof(uint64_t) * sizeof(uint64_t);
//        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
//        char *inner_node_ = entry_.second;
//        *new_node = reinterpret_cast<InternalNode *>(inner_node_);

        *new_node =  reinterpret_cast<InternalNode *>(_mm_malloc(alloc_size, 64));
//        memset((*new_node), 0, alloc_size);

        new(*new_node) InternalNode(alloc_size, key, key_size,
                                    left_child_addr, right_child_addr);
//        (*new_node)->segment_index = entry_.first;
    }
    static void  New(InternalNode *src_node, uint32_t begin_meta_idx,
                           uint32_t nr_records,   char * key, uint32_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr,
                           InternalNode **new_node, uint64_t left_most_child_addr,
                           InnerNodeBuffer *inner_node_buffer) {
        // Figure out how large the new node will be
        size_t alloc_size = sizeof(InternalNode);
        if (begin_meta_idx > 0) {
            // Will not copy from the first element (dummy key), so add it here
            alloc_size += src_node->row_meta[0].GetPaddedKeyLength() + sizeof(uint64_t);
            alloc_size += sizeof(row_m);
        }

        assert(nr_records > 0);
        for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
            row_m meta = src_node->row_meta[i];
            alloc_size += meta.GetPaddedKeyLength() + sizeof(uint64_t);
            alloc_size += sizeof(row_m);
        }

        // Add the new key, if provided
        if (key) {
            M_ASSERT(key_size > 0, "key_size > 0.");
            alloc_size += (row_m::PadKeyLength(key_size) + sizeof(uint64_t) + sizeof(row_m));
        }

//        alloc_size = (alloc_size + sizeof(uint64_t) - 1)/ sizeof(uint64_t) * sizeof(uint64_t);
//        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
//        char *inner_node_ = entry_.second;
//        *new_node = reinterpret_cast<InternalNode *>(inner_node_);
//        memset(*new_node, 0, alloc_size);
        *new_node = reinterpret_cast<InternalNode *>(_mm_malloc(alloc_size, 64));

        new (*new_node) InternalNode(alloc_size, src_node, begin_meta_idx, nr_records,
                                     key, key_size, left_child_addr, right_child_addr,
                                     left_most_child_addr);
//        (*new_node)->segment_index = entry_.first;

    }

    ~InternalNode() = default;

    InternalNode(uint32_t node_size,   char * key, uint32_t key_size,
                 uint64_t left_child_addr, uint64_t right_child_addr);

    InternalNode(uint32_t node_size, InternalNode *src_node,
                 uint32_t begin_meta_idx, uint32_t nr_records,
                   char * key, uint32_t key_size,
                 uint64_t left_child_addr, uint64_t right_child_addr,
                 uint64_t left_most_child_addr = 0,
                 uint32_t value_size = sizeof(uint64_t));

    bool PrepareForSplit(Stack &stack, uint32_t split_threshold,
                           char * key, uint32_t key_size,
                         uint64_t left_child_addr, uint64_t right_child_addr,
                         InternalNode **new_node, bool backoff,
                         InnerNodeBuffer *inner_node_buffer);

    uint32_t GetChildIndex(char * key, uint32_t key_size, bool get_le = true);

    inline uint64_t *GetPayloadPtr(row_m meta) {
        char *ptr = reinterpret_cast<char *>(this) + meta.GetOffset() + meta.GetPaddedKeyLength();
        return reinterpret_cast<uint64_t *>(ptr);
    }

    ReturnCode Update(row_m meta, InternalNode *old_child, InternalNode *new_child ) ;

    inline BaseNode *GetChildByMetaIndex(uint32_t index) {
        uint64_t child_addr;
        row_m red_child = this->row_meta[index];

        GetRawRow(red_child, nullptr, nullptr, &child_addr);
//        printf("GetChildByMetaIndex: %u, %lu \n", index, child_addr);
        if (child_addr == 0) return nullptr;

        BaseNode *rt_node = reinterpret_cast<BaseNode *> (child_addr);
        return rt_node;
    }

    inline bool GetRawRow(row_m meta, char **data, char **key, uint64_t *payload) {
        char *tmp_data = reinterpret_cast<char *>(this) + meta.GetOffset();
        if (data != nullptr) {
            *data = tmp_data;
        }
        auto padded_key_len = meta.GetPaddedKeyLength();
        if (key != nullptr) {
            // zero key length dummy record
            *key = padded_key_len == 0 ? nullptr : tmp_data;
        }
        //if innernode this payload may be nullptr
        if (payload != nullptr) {
            uint64_t tmp_payload;
            tmp_payload = *reinterpret_cast<uint64_t *> (tmp_data + padded_key_len);

            *payload = tmp_payload;
        }

        return true;
    }

    inline uint32_t GetSegmentIndex(){
        return segment_index;
    }

    inline char *GetKey(row_m meta) {
        if (!meta.IsVisible()) {
            return nullptr;
        }
        return &(reinterpret_cast<char *>(this))[meta.GetOffset()];
    }

    // return the row_meta[i]
    inline row_m GetRowMeta(uint32_t i) {
        // ensure the metadata is installed
//        auto meta = reinterpret_cast<uint64_t>(row_meta + i);
        auto meta = row_meta[i];

        return meta;
    }

};

class LeafNode : public BaseNode {
public:
    //|0,1,2,3,4,,,,,,15 bits|16,17,18,,,,,31 bits|
    //|insert,split operation|txn update operation|
    volatile std::atomic<int16_t> counter_insert = ATOMIC_VAR_INIT(0);
    volatile std::atomic<int16_t> counter_update = ATOMIC_VAR_INIT(0);
    row_t row_meta[0];

    static void New(LeafNode **mem, uint32_t node_size, DramBlockPool *leaf_node_pool);
    static void NewS(LeafNode **mem, uint32_t node_size, DramBlockS1Pool *leaf_node_pool_s1);

    static inline uint32_t GetUsedSpace(NodeHeader::StatusWord status) {
        uint32_t used_space = sizeof(LeafNode);
        used_space = used_space + status.GetBlockSize();
        used_space = used_space + (status.GetRecordCount() * sizeof(row_t));
//        LOG_DEBUG("LeafNode::GetUsedSpace: %u ",used_space);
        return used_space;
    }

    explicit LeafNode(uint32_t node_size) : BaseNode(true, node_size){}

    ~LeafNode() = default;

    void Copy_Key(char *dest, char *src, uint32_t key_sz);
    void Copy_Payload(char *dest, char *src, uint32_t payload_sz);

    ReturnCode Insert(char * key, uint32_t key_size,
                      char *payload, uint32_t payload_size,
                      row_t **meta,
                      uint32_t split_threshold ,
                      DualPointer **dural_pointer);

    ReturnCode Read(char * key, uint32_t key_size, row_t **meta );

    bool PrepareForSplit(Stack &stack,
                         uint32_t split_threshold,
                         uint32_t key_size, uint32_t payload_size,
                         LeafNode **left, LeafNode **right,
                         InternalNode **new_parent, bool backoff,
                         DramBlockPool *leaf_node_pool,
                         InnerNodeBuffer *inner_node_buffer);
    bool PrepareForSplitS(Stack &stack,
                        uint32_t split_threshold,
                        uint32_t key_size, uint32_t payload_size,
                        LeafNode **left, LeafNode **right,
                        InternalNode **new_parent, bool backoff,
                        DramBlockS1Pool *leaf_node_pool_s1,
                        InnerNodeBuffer *inner_node_pool);

    void CopyFrom(LeafNode *node,
                  typename std::vector<row_t>::iterator begin_it,
                  typename std::vector<row_t>::iterator end_it,
                  uint32_t payload_size );
    void CopyFromS(LeafNode *node,
                    std::vector<row_t>::iterator begin_it,
                    std::vector<row_t>::iterator end_it,
                    uint32_t payload_size);

    ReturnCode RangeScanBySize(char * key1,
                               uint32_t size1,
                               uint32_t to_scan,
                               std::list<row_t *> *result);
    ReturnCode RangeScanBySizeS(char * key, uint32_t key_size,
                                 uint32_t to_scan,
                                 std::list<row_t *> *result);

    ReturnCode SearchRowMeta(char * key, uint32_t key_size,
                          row_t **out_metadata, uint32_t start_pos = 0,
                          uint32_t end_pos = (uint32_t) -1,
                          bool check_concurrency = true );
    ReturnCode SearchRowMetaS(char * key, uint32_t key_size,
                              row_t **out_metadata_ptr,
                              uint32_t start_pos = 0,
                              uint32_t end_pos = (uint32_t) -1,
                              bool check_concurrency= false );
    std::pair<uint64_t, int> GetLastRow() {
        auto count = header.GetStatus().GetRecordCount();
        uint64_t last = 0;
        for (uint32_t i = count; i >0; --i) {
            row_t *curr_meta = &row_meta[i];
            //if the record is not deleted
            if (curr_meta->IsVisible() && curr_meta->valid) {
                auto record_pointer = *reinterpret_cast<uint64_t *>(curr_meta->data);
                auto m_item = reinterpret_cast<itemid_t *>(record_pointer);
                auto master_row = m_item->location;
                auto last_row = reinterpret_cast<row_t *>(master_row);
                last = last_row->get_primary_key();
                break;
            }
        }
        std::pair<uint64_t, int> ret = std::make_pair(last, count);
        return ret;
    }


private:
    enum Uniqueness {
        IsUnique, Duplicate, ReCheck, NodeFrozen
    };

    Uniqueness CheckUnique(char * key, uint32_t key_size);
    Uniqueness CheckUniqueS(char * key, uint32_t key_size);

    Uniqueness RecheckUnique(char * key, uint32_t key_size, uint32_t end_pos);

    uint32_t SortRecordsByKey(vector<row_t> &meta_vec,  uint32_t payloadSize) ;
};

class index_btree_store;
struct Stack {
    struct Frame {
        Frame() : node(nullptr), meta_index() {}

        ~Frame() {}

        InternalNode *node;
        uint32_t meta_index;
    };

    static const uint32_t kMaxFrames = 32;
    Frame frames[kMaxFrames];
    uint32_t num_frames;
    BaseNode *root;
    index_btree_store *tree;

    Stack() : num_frames(0) {}

    ~Stack() { num_frames = 0; }

    inline void Push(InternalNode *node, uint32_t meta_index) {
        M_ASSERT(num_frames < kMaxFrames,"stack push num_frames < kMaxFrames.");
        auto &frame = frames[num_frames++];
        frame.node = node;
        frame.meta_index = meta_index;
    }

    inline Frame *Pop() { return num_frames == 0 ? nullptr : &frames[--num_frames]; }

    inline void Clear() {
        root = nullptr;
        num_frames = 0;
    }

    inline bool IsEmpty() { return num_frames == 0; }

    inline Frame *Top() { return num_frames == 0 ? nullptr : &frames[num_frames - 1]; }

    inline BaseNode *GetRoot() { return root; }

    inline void SetRoot(BaseNode *node) { root = node; }
};

class Iterator;
//============================BTree Store===================
class index_btree_store : public index_base {
public:
    RC	init(uint64_t part_cnt){ return RCOK; }
    RC	init(uint32_t key_size, table_t * table_){ return RCOK; }


    RC 	index_next(uint64_t thd_id, void * &item, bool samekey = false) { return RCOK;}
    RC  index_insert(idx_key_t key, void * item, int part_id=-1 ){ return RCOK;}
    bool index_exist(idx_key_t key) {return true;}

    RC  index_insert(idx_key_t key, void * &item, char *payload);
    int index_scan(idx_key_t key, int range, void** output);
    RC	index_read(idx_key_t key, void * &item);
    RC	index_read(idx_key_t key, void * &item, int part_id = -1);
    RC	index_read(idx_key_t key, void * &item, int part_id = -1, int thd_id=0);
    RC	index_read(idx_key_t key, void * &item, access_t type,
                     std::array<std::pair<void *, void *>,10> &ptr_vector,
                     int part_id = -1, int thd_id=0);
    RC  index_insert_s(char *key, uint32_t key_size, void * &item, char *payload);
    RC	index_read_s(char *key, uint32_t key_size, void * &item);

    void init_btree_store(uint32_t key_length, table_t * table_){

        //for inner node
        ValenBufferPool *pool_ = new ValenBufferPool(500000000,500000000);
        auto inner_node_pool_ = new InnerNodeBuffer(pool_);

        if (key_length == 8){
            ParameterSet param(SPLIT_THRESHOLD, MERGE_THRESHOLD, DRAM_BLOCK_SIZE,
                               PAYLOAD_SIZE, KEY_SIZE);
            auto leaf_node_pool_ = new DramBlockPool(DEFAULT_BLOCKS, DEFAULT_BLOCKS);
            init_btree_store(param, KEY_SIZE, table_, leaf_node_pool_, nullptr, inner_node_pool_);
        }else if (key_length == 10){
            ParameterSet param(S1_SPLIT_THRESHOLD, MERGE_THRESHOLD, S1_DRAM_BLOCK_SIZE,
                               S1_PAYLOAD_SIZE, S1_KEY_SIZE);
            auto leaf_node_pool_s_ = new DramBlockS1Pool(DEFAULT_BLOCKS, DEFAULT_BLOCKS);
            init_btree_store(param, S1_KEY_SIZE, table_, nullptr, leaf_node_pool_s_, inner_node_pool_);
        }

    }
    void init_btree_store(ParameterSet param, uint32_t key_size, table_t *table_,
                          DramBlockPool *leaf_node, DramBlockS1Pool *leaf_node_pl_s1,
                          InnerNodeBuffer *inner_node);
    LeafNode *TraverseToLeaf(Stack *stack, char * key,
                             uint32_t key_size, bool le_child = true);
    inline std::unique_ptr<Iterator> RangeScanBySize(char * key1, uint32_t key_size, uint32_t scan_size,
                     std::array<std::pair<uint64_t,std::array<std::pair<void *, void *>, 10>>,3> &scan_paths) {
        return tbb::internal::make_unique<Iterator>(this, key1, key_size, scan_size,scan_paths);
    }

    void ReleaseLeafNode(uint32_t key_size, char *node){
        if (key_size>8){
            this->leaf_node_pool_s1->Release(reinterpret_cast<DramBlockS1 *>(node), 0);
        }else{
            this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(node), 0);
        }
    }

    uint32_t GetTreeHeight(uint64_t key, uint32_t key_size){
        thread_local Stack stack;
        stack.tree = this;
        stack.Clear();

        TraverseToLeaf(&stack, reinterpret_cast<char *>(&key),   key_size);

        //parents
        uint32_t level = stack.num_frames;
        level= level+1;
        return level;
    }

    void GetKeysPaths(std::set<uint64_t> search_keys, uint32_t key_size,
                      tbb::concurrent_hash_map<uint64_t, std::vector<std::pair<void *,void *>>> &keys_paths){
        uint32_t search_key_count = search_keys.size();
        assert(search_key_count > 0);
        printf("get all search keys: %u \n",search_key_count);
        BaseNode *node = GetRootNodeSafe();
        root_node = reinterpret_cast<void *>(node);

        for (auto itm = search_keys.cbegin(); itm != search_keys.cend(); ++itm) {
            //[<current block, right neighbor>]
            uint64_t search_key = (*itm);
            char *key = reinterpret_cast<char *>(&search_key);

            InternalNode *parent = nullptr;
            BaseNode *node_r = nullptr;
            uint32_t meta_index = 0;
            assert(node);
            int h = 1;
            std::vector<std::pair<void *,void *>> neibors;
            while (!node->IsLeaf()) {
                parent = reinterpret_cast<InternalNode *>(node);
                //binary search in inner node
                meta_index = parent->GetChildIndex(key, key_size, false);
                node = parent->GetChildByMetaIndex(meta_index);

                assert(node);
                if (!node->IsLeaf()){
                    if (meta_index > 1){
                        BaseNode *node_ptr = node;
                        node_r = parent->GetChildByMetaIndex(meta_index-1);
                        BaseNode *node_r_ptr = node_r;
                        if (node_r != nullptr){
                            neibors.emplace_back(std::make_pair(reinterpret_cast<void *>(node_ptr),
                                                                reinterpret_cast<void *>(node_r_ptr)));
                        }
                    }
                }

                h = h+1;
            }
            keys_paths.emplace(search_key, neibors);
        }

        printf("access paths size:%lu. \n", keys_paths.size());
    }

    bool GetKeyPaths(uint64_t search_key, uint32_t key_size,
                     std::array<std::pair<void *, void *>,10> &key_paths){

        BaseNode *node = GetRootNodeSafe();
        //[<current block, right neighbor>]
        char *key = reinterpret_cast<char *>(&search_key);
        InternalNode *parent = nullptr;
        BaseNode *node_r = nullptr;
        uint32_t meta_index = 0;
        assert(node);
        int h = 0;
//        std::vector<std::pair<void *,void *>> neibors;
        while (!node->IsLeaf()) {
            parent = reinterpret_cast<InternalNode *>(node);
            if (parent == nullptr || parent->header.size == 0){
                return false;
            }
            //binary search in inner node
            meta_index = parent->GetChildIndex(key, key_size, false);
            node = parent->GetChildByMetaIndex(meta_index);
            if (node == nullptr || node->header.size == 0){
                return false;
            }

            assert(node);
            if (!node->IsLeaf()){
                BaseNode *node_ptr = node;

#if PATH_PREFETCHING == true
                if (h<2){
                    void *nodeptr = reinterpret_cast<void *>(node_ptr);
                    distance_3_paths_set.insert(nodeptr);
                }
                if (h==2){
                    void *nodeptr = reinterpret_cast<void *>(node_ptr);
                    distance_4_paths_set.insert(nodeptr);
                }
                if (h==3){
                    void *nodeptr = reinterpret_cast<void *>(node_ptr);
                    distance_5_paths_set.insert(nodeptr);
                }
                if (h==4){
                    void *nodeptr = reinterpret_cast<void *>(node_ptr);
//                    distance_6_set.emplace(nodeptr);
                    distance_6_paths_set.insert(nodeptr);
                }
#endif

                BaseNode *node_r_ptr = nullptr;
                if (meta_index > 1){
                    node_r = parent->GetChildByMetaIndex(meta_index-1);
                    if (node_r != nullptr){
                        node_r_ptr = node_r;
                    }
                }
                key_paths[h] = (std::make_pair(reinterpret_cast<void *>(node_ptr),
                                                      reinterpret_cast<void *>(node_r_ptr)));

            }

            h = h+1;
        }

        return true;
//        printf("access paths size:%lu. \n", keys_paths.size());
    }
    std::pair<uint64_t,int> GetKeyScanPaths(uint64_t search_key, uint32_t key_size,
                         std::array<std::pair<void *, void *>,10> &key_paths ){
        BaseNode *node = GetRootNodeSafe();
        char *key = reinterpret_cast<char *>(&search_key);
        InternalNode *parent = nullptr;
        BaseNode *node_r = nullptr;
        uint32_t meta_index = 0;
        assert(node);
        int h = 0;
        while (!node->IsLeaf()) {
            parent = reinterpret_cast<InternalNode *>(node);
            if (parent == nullptr || parent->header.size == 0){
                return {0,0};
            }
            //binary search in inner node
            meta_index = parent->GetChildIndex(key, key_size, false);
            node = parent->GetChildByMetaIndex(meta_index);
            if (node == nullptr || node->header.size == 0){
                return {0,0};
            }
            assert(node);
            if (!node->IsLeaf()){
//                BaseNode *node_ptr = node;
//                if (h<2){
//                    void *nodeptr = reinterpret_cast<void *>(node_ptr);
//                    if (std::find(distance_3_paths.begin(),distance_3_paths.end(),nodeptr) == distance_3_paths.end()){
//                        distance_3_paths.push_back(nodeptr);
//                    }
//                }
//                if (h==2){
//                    void *nodeptr = reinterpret_cast<void *>(node_ptr);
//                    distance_4_paths_set.emplace(nodeptr);
//                }
//                if (h==3){
//                    void *nodeptr = reinterpret_cast<void *>(node_ptr);
//                    distance_5_paths_set.emplace(nodeptr);
//                }
//                if (h==4){
//                    void *nodeptr = reinterpret_cast<void *>(node_ptr);
//                    distance_6_paths_.emplace_back(nodeptr);
//                }
//
//                BaseNode *node_r_ptr = nullptr;
//                if (meta_index > 1){
//                    node_r = parent->GetChildByMetaIndex(meta_index-1);
//                    if (node_r != nullptr){
//                        node_r_ptr = node_r;
//                    }
//                }
//                key_paths[h] = (std::make_pair(reinterpret_cast<void *>(node_ptr),
//                                               reinterpret_cast<void *>(node_r_ptr)));
            }
            h = h+1;
        }

        LeafNode *node_l = reinterpret_cast<LeafNode *>(node);
        auto ret = node_l->GetLastRow();

        return ret;
//        printf("access paths size:%lu. \n", keys_paths.size());
    }

    void ScanLeafNode(BaseNode *real_root) {
        auto leaf_node = reinterpret_cast<LeafNode *>(real_root) ;
        auto record_count = leaf_node->GetHeader()->GetStatus().GetRecordCount();
        for (uint32_t i = 0; i < record_count; ++i) {
            row_t meta = leaf_node->row_meta[i];
            if (meta.IsVisible() && !meta.IsInserting()){
                __attribute__((unused)) uint64_t key = *reinterpret_cast<uint64_t *>(meta.primary_key);
//                printf("leaf node key:%lu \n", key);
//                auto ret = table_size.insert(key);
//            table_size_all.emplace_back(key);
            }
        }
    }
    void ScanInnerNode(BaseNode *real_root) {
        auto inner_node = reinterpret_cast<InternalNode *>(real_root);
        auto sorted_count = inner_node->GetHeader()->sorted_count;
        for (uint32_t i = 0; i < sorted_count; ++i) {
            row_m meta = inner_node->row_meta[i];
            char *ptr = reinterpret_cast<char *>(inner_node) + meta.GetOffset() + meta.GetPaddedKeyLength();
            uint64_t node_addr =  *reinterpret_cast<uint64_t *>(ptr);
            BaseNode *node = reinterpret_cast<BaseNode *>(node_addr);
            if (node == NULL) {
                break;
            } else if (node->IsLeaf()){
                number_of_leaf_node++;
                ScanLeafNode(node);
            }else {
                number_of_inner_node++;
                ScanInnerNode(node);
            }
        }
    }
    void index_store_scan(){
        auto real_root = this->GetRootNodeSafe();

        if (real_root->IsLeaf()){
            number_of_leaf_node++;
            ScanLeafNode(real_root);
        }else{
            number_of_inner_node++;
            ScanInnerNode(real_root);
        }
    }
    std::pair<uint32_t,uint32_t> GetNumberOfNodes(){
        index_store_scan();
        return std::make_pair(number_of_inner_node,number_of_leaf_node);
    }

    std::set<std::string> secondary_index_;

    inline BaseNode *GetRootNodeSafe() {
        auto root_node = root;
        return reinterpret_cast<BaseNode *>(root_node);
    }

private:
    ReturnCode Insert(char *key, uint32_t key_size, char *payload, row_t **inrt_meta);
    int Scan(char * start_key, uint32_t key_size , uint32_t range, void **output);

    bool ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr);
    uint32_t AddDefaultRowDualPointerArray();
    void RowDualPointer(DualPointer **dual_ptr);

    BaseNode *root;
    DramBlockPool *leaf_node_pool;
    DramBlockS1Pool *leaf_node_pool_s1;
    InnerNodeBuffer *inner_node_pool;
    ParameterSet parameters;
    uint32_t number_of_leaf_node=0;
    uint32_t number_of_inner_node=0;
};

class Iterator {
public:
    explicit Iterator(index_btree_store *tree, char * begin_key, uint32_t begin_size, uint32_t scan_size,
                      std::array<std::pair<uint64_t,std::array<std::pair<void *, void *>, 10>>,3> &scan_paths) :
            key(begin_key), key_size(begin_size), tree(tree), remaining_size(scan_size) {

        std::array<std::pair<void *, void *>,10> ptr_vector = scan_paths[0].second;
        BaseNode *root = this->tree->GetRootNodeSafe();
#if AHEAD_PREFETCH == true
        __builtin_prefetch((const void *) (root), 0, PREFETCH_LEVEL);
        uint32_t ptr_sz = ptr_vector.size();
        for (int i = 0; i < ptr_sz; ++i) {
            void *node_ = ptr_vector[i].first;
            if(node_ == nullptr) continue;
            for (uint32_t i = 0; i < SPLIT_THRESHOLD / CACHE_LINE_SIZE; ++i) {
                __builtin_prefetch((const void *)((char *)node_ + i * CACHE_LINE_SIZE), 0, PREFETCH_LEVEL);
            }
        }
#endif
        node = this->tree->TraverseToLeaf(nullptr, begin_key, begin_size);
        if (node == nullptr) return ;

        if (key_size>8){
            node->RangeScanBySizeS(begin_key, begin_size,  scan_size, &item_vec);
        }else{
            node->RangeScanBySize(begin_key, begin_size,  scan_size, &item_vec);
        }
    }

    ~Iterator() = default;

    /**
     * one by one leaf traverse for keys
     * @return
     */
    inline row_t *GetNext(std::array<std::pair<uint64_t,std::array<std::pair<void *, void *>, 10>>,3> &scan_paths) {
        if (item_vec.empty() || remaining_size == 0) {
            return nullptr;
        }

        remaining_size -= 1;
        // we have more than one record
        if (item_vec.size() > 1) {
            auto front = std::move(item_vec.front());
            item_vec.pop_front();
            return front;
        }

        // there's only one record in the vector
        auto last_record = std::move(item_vec.front());
        item_vec.pop_front();
        char *last_record_key;
        last_record_key =  reinterpret_cast<char *>(last_record->primary_key) ;

        if (SCAN_LEN == 10){
            return nullptr;
        } else if(SCAN_LEN == 20 && count==1){
            return nullptr;
        } else if(SCAN_LEN == 30 && count==2){
            return nullptr;
        }
        count = count+1;
        std::array<std::pair<void *, void *>,10> ptr_vector = scan_paths[count].second;
#if AHEAD_PREFETCH == true
        uint32_t ptr_sz = ptr_vector.size();
        for (int i = 0; i < ptr_sz; ++i) {
            void *node_ = ptr_vector[i].first;
            if(node_ == nullptr) continue;
            for (uint32_t i = 0; i < SPLIT_THRESHOLD / CACHE_LINE_SIZE; ++i) {
                __builtin_prefetch((const void *)((char *)node_ + i * CACHE_LINE_SIZE), 0, PREFETCH_LEVEL);
            }
        }
#endif

        node = this->tree->TraverseToLeaf(nullptr,
                                          last_record_key,
                                          key_size,
                                          false);
        if (node == nullptr) {
            return nullptr;
        }
        item_vec.clear();
        uint32_t last_len = key_size;

        node->RangeScanBySize(last_record_key, last_len, remaining_size, &item_vec);

        // check if we hit the same record
        if (!item_vec.empty()) {
            auto new_front = item_vec.front();
            char *n_key =  reinterpret_cast<char *>(new_front->primary_key) ;
            char *l_key =  reinterpret_cast<char *>(last_record->primary_key) ;
            if (BaseNode::KeyCompare(n_key, key_size, l_key, key_size) == 0) {
                item_vec.clear();
                return last_record;
            }
        }
        return last_record;
    }

    inline row_t *GetNextS() {
        if (item_vec.empty() || remaining_size == 0) {
            return nullptr;
        }

        remaining_size -= 1;
        // we have more than one record
        if (item_vec.size() > 1) {
            auto front = std::move(item_vec.front());
            item_vec.pop_front();
            return front;
        }

        // there's only one record in the vector
        auto last_record = std::move(item_vec.front());
        item_vec.pop_front();
        char *last_record_key;
        last_record_key =  last_record->data;

        std::array<std::pair<void *, void *>,10> ptr_vector;
        node = this->tree->TraverseToLeaf(nullptr,
                                          last_record_key,
                                          key_size,
                                          false);
        if (node == nullptr) {
            return nullptr;
        }
        item_vec.clear();
        uint32_t last_len = key_size;

#if PREFETCHING
        auto count = node->GetHeader()->status.GetRecordCount();
        uint32_t prefetch_length = sizeof(row_t) * count;
        uint32_t prefetch_count = (prefetch_length / CACHE_LINE_SIZE);
        for (uint32_t i = 0; i < prefetch_count; ++i) {
            __builtin_prefetch((const void *) ((char *) node + i * CACHE_LINE_SIZE), 0, PREFETCH_LEVEL);
        }
#endif

        node->RangeScanBySize(last_record_key, last_len, remaining_size, &item_vec);

        // check if we hit the same record
        if (!item_vec.empty()) {
            auto new_front = item_vec.front();
            char *n_key =  new_front->data ;
            char *l_key =  last_record->data ;
            if (BaseNode::KeyCompare(n_key, key_size, l_key, key_size) == 0) {
                item_vec.clear();
                return last_record;
            }
        }
        return last_record;
    }

private:
    char * key;
    uint32_t key_size;
    uint32_t remaining_size;
    index_btree_store *tree;
    LeafNode *node;
    std::list<row_t *> item_vec;
    int count = 0;
};

#endif