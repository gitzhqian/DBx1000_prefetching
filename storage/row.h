#pragma once 

#include <cassert>
#include <atomic>
#include <unordered_map>
#include <memory>
#include "global.h"

#define DECL_SET_VALUE(type) \
	void set_value(int col_id, type value);

#define SET_VALUE(type) \
	void row_t::set_value(int col_id, type value) { \
		set_value(col_id, &value); \
	}

#define DECL_GET_VALUE(type)\
	void get_value(int col_id, type & value);

#define GET_VALUE(type)\
	void row_t::get_value(int col_id, type & value) {\
		int pos = get_schema()->get_field_index(col_id);\
		value = *(type *)&data[pos];\
	}

class table_t;
class Catalog;
class txn_man;
class Row_lock;
class Row_mvcc;
class Row_hekaton;
class Row_ts;
class Row_occ;
class Row_tictoc;
class Row_silo;
class Row_vll;

class row_m{
public:
    //64 bits
    uint64_t meta;

    ~row_m() {};

    row_m() : meta(0) {}
    explicit row_m(uint64_t meta) : meta(meta)  {}

    static const uint64_t kControlMask = uint64_t{0x1} << 63;          // Bits 64
    static const uint64_t kVisibleMask = uint64_t{0x1} << 62;          // Bit 63
    static const uint64_t kKeyLengthMask = uint64_t{0x2FFF} << 48;     // Bits 62-49
    static const uint64_t kTotalLengthMask = uint64_t{0xFFFF} << 32;   // Bits 48-33
    static const uint64_t kOffsetMask = uint64_t{0xFFFFFFFF};          // Bits 32-1


    bool IsNull() const {
        return (meta == std::numeric_limits<uint64_t>::max() && meta == 0);
    }

    bool operator==(const row_m &rhs) const {
        return meta == rhs.meta ;
    }

    bool operator!=(const row_m &rhs) const {
        return !operator==(rhs);
    }

    inline bool IsVacant() { return meta == 0; }

    inline uint16_t GetKeyLength() const { return (uint16_t) ((meta & kKeyLengthMask) >> 48); }

    // Get the padded key length from accurate key length
    inline uint16_t GetPaddedKeyLength() const{
        auto key_length = GetKeyLength();
        return PadKeyLength(key_length);
    }

    static inline constexpr uint16_t  PadKeyLength(uint16_t key_length) {
        return (key_length + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t);
    }

    inline uint32_t GetOffset() { return (uint32_t) (meta & kOffsetMask); }

    inline bool IsVisible() {
        bool i_v = (meta & kVisibleMask) > 0;
        return i_v;
    }

    inline void FinalizeForInsert(uint64_t offset, uint64_t key_len) {
        // Set the visible bit,the actual offset,key length,
        meta =  (key_len << 48) | (uint64_t{1} << 62)| (uint64_t{0} << 32) | (offset);
        meta = meta & (~kControlMask);

        auto g_k_l = GetKeyLength();
        assert(g_k_l == key_len);
    }

};

class row_t{
public:

    row_t():data(nullptr),location(nullptr),manager(nullptr), key_size(0), version_t(0){};
    row_t(uint8_t version_t): version_t(0){ };

	RC init(table_t * host_table, uint64_t part_id, uint64_t row_id = 0, uint32_t tuple_size = 0);
	void init(int size);
	RC switch_schema(table_t * host_table);
	// not every row has a manager
	void init_manager(row_t * row);

	table_t * get_table();
	Catalog * get_schema();
	const char * get_table_name();
	uint64_t get_field_cnt();
	uint64_t get_tuple_size();

	void copy(row_t * src);

#if ENGINE_TYPE == PTR0
    void set_primary_key(uint64_t key) {
        memcpy(primary_key, reinterpret_cast<char *>(&key), sizeof(uint64_t));

    };
    uint64_t get_primary_key() {
        uint64_t key_ = *reinterpret_cast<uint64_t *>(primary_key);
        return key_;
    };
#elif (ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2)
	void 		set_primary_key(uint64_t key) { _primary_key = key; };
    void 		set_part_id(uint64_t part_id) { _part_id = part_id; };
    void 		set_row_id(uint64_t row_id) { _row_id = row_id; };
	uint64_t 	get_primary_key() {return _primary_key; };
	uint64_t 	get_part_id() { return _part_id; };
    uint64_t    get_row_id() { return _row_id; };
#endif

	void set_value(int id, void * ptr);
	void set_value(int id, void * ptr, int size);
	void set_value(const char * col_name, void * ptr);
	char * get_value(int id);
	char * get_value(char * col_name);
	
	DECL_SET_VALUE(uint64_t);
	DECL_SET_VALUE(int64_t);
	DECL_SET_VALUE(double);
	DECL_SET_VALUE(UInt32);
	DECL_SET_VALUE(SInt32);

	DECL_GET_VALUE(uint64_t);
	DECL_GET_VALUE(int64_t);
	DECL_GET_VALUE(double);
	DECL_GET_VALUE(UInt32);
	DECL_GET_VALUE(SInt32);


	void set_data(char * data, uint64_t size);
	char * get_data();

	void free_row();

	// for concurrency control. can be lock, timestamp etc.
	RC get_row(access_t type, txn_man * txn, row_t *& row);
	void return_row(access_t type, txn_man * txn, row_t * row);
	
  #if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
    Row_lock * manager;
  #elif CC_ALG == TIMESTAMP
   	Row_ts * manager;
  #elif CC_ALG == MVCC
  	Row_mvcc * manager;
  #elif CC_ALG == HEKATON
  	Row_hekaton * manager;
  #elif CC_ALG == OCC
  	Row_occ * manager;
  #elif CC_ALG == TICTOC
  	Row_tictoc * manager;
  #elif CC_ALG == SILO
  	Row_silo * manager;
  #elif CC_ALG == VLL
  	Row_vll * manager;
  #endif

    char * data;
    bool valid;

    void * location; //pointing to the location that holds the realtime location
    uint32_t key_size;
    char primary_key[KEY_SIZE];
    // low bits -->
    // [ splitting | inserting |  visible  |  deleted  ]
    // [  0..1     |   1..2    |    2..3   |   3..4    ]
    volatile uint8_t version_t;
    static const uint8_t LAT_VISIBLE_MASK = uint8_t{0x1} << 2;     //bits 4-3
    static const uint8_t LAT_INSERTING_MASK = uint8_t{0x1} << 1;   //bits 3-2
    static const uint8_t LAT_SPLIT_MASK = uint8_t{0x1} << 0;     //bits 2-1

    inline void mark_visible(bool is_visible)
    {
        if (is_visible){
            version_t = version_t | LAT_VISIBLE_MASK;
        }else{
            version_t = version_t & (~LAT_VISIBLE_MASK);
        }
    }
    inline bool IsVisible() {
        auto is_vsb = version_t & LAT_VISIBLE_MASK;
        return is_vsb > 0;
    }
    inline void mark_inserting(bool is_insert)
    {
        if (is_insert){
            version_t = version_t | LAT_INSERTING_MASK;
        }else{
            version_t = version_t & (~LAT_INSERTING_MASK);
        }
    }
    inline bool IsInserting() { return (version_t & LAT_INSERTING_MASK) > 0; }
    inline void mark_splitting(bool is_splitting)
    {
        if (is_splitting){
            version_t = version_t | LAT_SPLIT_MASK;
        }else{
            version_t = version_t & (~LAT_SPLIT_MASK);
        }
    }
    inline bool IsSplitting() { return (version_t & LAT_SPLIT_MASK) > 0; }

#if (ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2)
    table_t * table;
    row_t * next;
#endif

#if ENGINE_TYPE == PTR0
#elif (ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2)
private:
	// primary key should be calculated from the data stored in the row.
	uint64_t 		_primary_key;
	uint64_t		_part_id;
	uint64_t 		_row_id;
#endif

};

class DualPointer {
public:
    // pointing to the row_t's realtime location
    uint64_t row_t_location;
#if INSERT_INDIRECT == true
    // pointing to the latest version
    uint64_t latest_version;
    DualPointer() : row_t_location(0), latest_version(0) {}

    DualPointer(uint64_t r_t_ptr, uint64_t l_v_ptr) : row_t_location(r_t_ptr), latest_version(l_v_ptr) {}

    bool IsNull() const {
        return (row_t_location ==0 && latest_version == 0);
    }

    bool operator==(const DualPointer &rhs) const {
        return ( row_t_location == rhs.row_t_location && latest_version == rhs.latest_version);
    }
#else
    DualPointer() : row_t_location(0) {}
    explicit DualPointer(uint64_t r_t_ptr) : row_t_location(r_t_ptr){}

    bool IsNull() const {
        return (row_t_location ==0);
    }

    bool operator==(const DualPointer &rhs) const {
        return ( row_t_location == rhs.row_t_location);
    }
#endif



} __attribute__((__aligned__(8))) __attribute__((__packed__));

extern DualPointer INVALID_DUALPOINTER;
const size_t INVALID_DUAL_POINTER_ARRAY_OFFSET = std::numeric_limits<size_t>::max();

class DualPointerArray {
public:
    DualPointerArray(uint32_t id) : id_(id) {
        dual_pointers_.reset(new dual_pointer_array_());
    }

    ~DualPointerArray() {}

    size_t AllocateDualPointerArray() {
        if (counter_ >= DURAL_POINTER_ARRAY_MAX_SIZE) {
            return INVALID_DUAL_POINTER_ARRAY_OFFSET;
        }

        size_t indirection_id =
                counter_.fetch_add(1, std::memory_order_relaxed);

        if (indirection_id >= DURAL_POINTER_ARRAY_MAX_SIZE) {
            return INVALID_DUAL_POINTER_ARRAY_OFFSET;
        }
        return indirection_id;
    }
    DualPointer *GetIndirectionByOffset(const size_t &offset) {
        return &(dual_pointers_->at(offset));
    }
    inline uint32_t GetId() { return id_; }

private:
    typedef std::array<DualPointer, DURAL_POINTER_ARRAY_MAX_SIZE> dual_pointer_array_;
    std::unique_ptr<dual_pointer_array_> dual_pointers_;
    std::atomic<size_t> counter_ = ATOMIC_VAR_INIT(0);
    uint32_t id_;
};

class DuralPointerManager {
public:
    DuralPointerManager() {}
    // Singleton
    static DuralPointerManager &GetInstance(){
        static DuralPointerManager dual_pointer_manager;
        return dual_pointer_manager;
    }
    uint32_t GetNextDulaPointerArrayId() { return ++dual_pointer_array_id_; }
    uint32_t GetCurrentDualPointerArrayId() { return dual_pointer_array_id_; }
    void AddDualPointerArray(const uint32_t id,
                             std::shared_ptr<DualPointerArray> location){
        auto ret = dual_pointer_array_locator_[id] = location;
    }
    void DropDualPointerArray(const uint32_t id){
        dual_pointer_array_locator_[id] = dual_pointer_empty_array_;
    }
    void ClearDualPointerArray(){
        dual_pointer_array_locator_.clear();
    }

private:
    std::atomic<uint32_t> dual_pointer_array_id_ = ATOMIC_VAR_INIT(0);
    std::unordered_map<uint32_t, std::shared_ptr<DualPointerArray>> dual_pointer_array_locator_;
    static std::shared_ptr<DualPointerArray> dual_pointer_empty_array_;
};
