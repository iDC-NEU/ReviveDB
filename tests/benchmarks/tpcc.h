#ifndef NVMDB_TPCC_H
#define NVMDB_TPCC_H

#include "nvm_table.h"
#include "index/nvm_index.h"
#include "index/nvm_index_tuple.h"
#include "common/test_declare.h"
#include <glog/logging.h>

namespace NVMDB {
namespace TPCC {

#define DIST_PER_WARE 10
#define MAXITEMS 100000
#define CUST_PER_DIST 3000
#define ORD_PER_DIST 3000

/* definitions for new order transaction */
#define MAX_NUM_ITEMS 15
#define MAX_ITEM_LEN 24

#define TIMESTAMP_LEN 80

/* valid item ids are numbered consecutively [1..MAXITEMS] */
const int notfound = MAXITEMS + 1;

#define __IN
#define __OUT
#define __INOUT

/* table id */
typedef enum TableType : uint32_t {
    TABLE_WAREHOUSE = 0,
    TABLE_DISTRICT = 1,
    TABLE_STOCK = 2,
    TABLE_ITEM = 3,
    TABLE_CUSTOMER = 4,
    TABLE_ORDER = 5,
    TABLE_NEWORDER = 6,
    TABLE_ORDERLINE = 7,
    TABLE_HISTORY = 8,
    TABLE_INVALID = 9
} TableType;

#define TABLE_FIRST TABLE_WAREHOUSE
#define TABLE_OFFSET(type) (type - TABLE_FIRST)
#define TABLE_NUM (TABLE_OFFSET(TABLE_INVALID))

/* Assume we need only one secondary index. Begin from TABLE_INVALID + 1 */
#define SECOND_INDEX(type) (TABLE_OFFSET(type) + TABLE_NUM + 1)

ColumnDesc WarehouseColDesc[] = {
    InitColDesc(COL_TYPE_INT), /* w_id */

    InitColDesc(COL_TYPE_LONG),        /* w_ytd */
    InitColDesc(COL_TYPE_FLOAT),       /* w_tax */
    InitVarColDesc(COL_TYPE_VARCHAR, 11), /* w_name */
    InitVarColDesc(COL_TYPE_VARCHAR, 21), /* w_street_1 */
    InitVarColDesc(COL_TYPE_VARCHAR, 21), /* w_street_2 */
    InitVarColDesc(COL_TYPE_VARCHAR, 21), /* w_city */
    InitVarColDesc(COL_TYPE_VARCHAR, 3),  /* w_state */
    InitVarColDesc(COL_TYPE_VARCHAR, 10), /* w_zip */
};

#define col_w_id 0
#define col_w_ytd 1
#define col_w_tax 2
#define col_w_name 3
#define col_w_street_1 4
#define col_w_street_2 5
#define col_w_city 6
#define col_w_state 7
#define col_w_zip 8

ColumnDesc DistrictColDesc[] = {
    InitColDesc(COL_TYPE_INT), /* d_id */
    InitColDesc(COL_TYPE_INT), /* d_w_id */

    InitColDesc(COL_TYPE_LONG),        /* d_ytd */
    InitColDesc(COL_TYPE_FLOAT),       /* d_tax */
    InitColDesc(COL_TYPE_INT),         /* d_next_o_id */
    InitVarColDesc(COL_TYPE_VARCHAR, 11), /* d_name */
    InitVarColDesc(COL_TYPE_VARCHAR, 21), /* d_street_1 */
    InitVarColDesc(COL_TYPE_VARCHAR, 21), /* d_street_2 */
    InitVarColDesc(COL_TYPE_VARCHAR, 21), /* d_city */
    InitVarColDesc(COL_TYPE_VARCHAR, 3),  /* d_state */
    InitVarColDesc(COL_TYPE_VARCHAR, 10), /* d_zip */
};

#define col_d_id 0
#define col_d_w_id 1
#define col_d_ytd 2
#define col_d_tax 3
#define col_d_next_o_id 4
#define col_d_name 5
#define col_d_street_1 6
#define col_d_street_2 7
#define col_d_city 8
#define col_d_state 9
#define col_d_zip 10

ColumnDesc StockColDesc[] = {
    InitColDesc(COL_TYPE_INT), /* s_w_id */
    InitColDesc(COL_TYPE_INT), /* s_i_id */

    InitColDesc(COL_TYPE_INT),         /* s_quantity */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* s_dist_01 */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* s_dist_02 */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* s_dist_03 */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* s_dist_04 */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* s_dist_05 */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* s_dist_06 */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* s_dist_07 */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* s_dist_08 */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* s_dist_09 */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* s_dist_10 */
    InitVarColDesc(COL_TYPE_VARCHAR, 51), /* s_data */
};

#define col_s_w_id 0
#define col_s_i_id 1
#define col_s_quantity 2
#define col_s_dist_01 3
#define col_s_dist_02 4
#define col_s_dist_03 5
#define col_s_dist_04 6
#define col_s_dist_05 7
#define col_s_dist_06 8
#define col_s_dist_07 9
#define col_s_dist_08 10
#define col_s_dist_09 11
#define col_s_dist_10 12
#define col_s_data 13

ColumnDesc ItemColDesc[] = {
    InitColDesc(COL_TYPE_INT), /* i_id */

    InitColDesc(COL_TYPE_INT),         /* i_im_id */
    InitColDesc(COL_TYPE_FLOAT),       /* i_price */
    InitVarColDesc(COL_TYPE_VARCHAR, 25), /* i_name */
    InitVarColDesc(COL_TYPE_VARCHAR, 51), /* i_data */
};

#define col_i_id 0
#define col_i_im_id 1
#define col_i_price 2
#define col_i_name 3
#define col_i_data 4

ColumnDesc CustomerColDesc[] = {
    InitColDesc(COL_TYPE_INT), /* c_id */
    InitColDesc(COL_TYPE_INT), /* c_d_id */
    InitColDesc(COL_TYPE_INT), /* c_w_id */

    InitColDesc(COL_TYPE_FLOAT),        /* c_discount */
    InitColDesc(COL_TYPE_FLOAT),        /* c_balance */
    InitVarColDesc(COL_TYPE_VARCHAR, 4),  /* c_last */
    InitVarColDesc(COL_TYPE_VARCHAR, 3),   /* c_credit */
    InitVarColDesc(COL_TYPE_VARCHAR, 501), /* c_data */
    InitVarColDesc(COL_TYPE_VARCHAR, 17),  /* c_first */
    InitVarColDesc(COL_TYPE_VARCHAR, 3),   /* c_middle */
    InitVarColDesc(COL_TYPE_VARCHAR, 21),  /* c_street_1 */
    InitVarColDesc(COL_TYPE_VARCHAR, 21),  /* c_street_2 */
    InitVarColDesc(COL_TYPE_VARCHAR, 21),  /* c_city */
    InitVarColDesc(COL_TYPE_VARCHAR, 3),   /* c_state */
    InitVarColDesc(COL_TYPE_VARCHAR, 10),  /* c_zip */
    InitVarColDesc(COL_TYPE_VARCHAR, 17),  /* c_phone */
    InitVarColDesc(COL_TYPE_VARCHAR, 12),  /* c_since */
    InitColDesc(COL_TYPE_INT),          /* c_credit_lim */
};

#define col_c_id 0
#define col_c_d_id 1
#define col_c_w_id 2
#define col_c_discount 3
#define col_c_balance 4
#define col_c_last 5
#define col_c_credit 6
#define col_c_data 7
#define col_c_first 8
#define col_c_middle 9
#define col_c_street_1 10
#define col_c_street_2 11
#define col_c_city 12
#define col_c_state 13
#define col_c_zip 14
#define col_c_phone 15
#define col_c_since 16
#define col_c_credit_lim 17

ColumnDesc OrderColDesc[] = {
    InitColDesc(COL_TYPE_INT), /* o_id */
    InitColDesc(COL_TYPE_INT), /* o_d_id */
    InitColDesc(COL_TYPE_INT), /* o_w_id */

    InitColDesc(COL_TYPE_INT),           /* o_c_id */
    InitColDesc(COL_TYPE_UNSIGNED_LONG), /* o_entry_d */
    InitColDesc(COL_TYPE_INT),           /* o_carrier_id */
    InitColDesc(COL_TYPE_INT),           /* o_ol_cnt */
    InitColDesc(COL_TYPE_INT),           /* o_all_local */
};

#define col_o_id 0
#define col_o_d_id 1
#define col_o_w_id 2
#define col_o_c_id 3
#define col_o_entry_d 4
#define col_o_carrier_id 5
#define col_o_ol_cnt 6
#define col_o_all_local 7

ColumnDesc NewOrderColDesc[] = {
    InitColDesc(COL_TYPE_INT), /* no_o_id */
    InitColDesc(COL_TYPE_INT), /* no_d_id */
    InitColDesc(COL_TYPE_INT), /* no_w_id */
};

#define col_no_o_id 0
#define col_no_d_id 1
#define col_no_w_id 2

ColumnDesc OrderLineColDesc[] = {
    InitColDesc(COL_TYPE_INT), /* ol_w_id */
    InitColDesc(COL_TYPE_INT), /* ol_d_id */
    InitColDesc(COL_TYPE_INT), /* ol_o_id */
    InitColDesc(COL_TYPE_INT), /* ol_number */

    InitColDesc(COL_TYPE_INT),           /* ol_i_id */
    InitColDesc(COL_TYPE_INT),           /* ol_supply_w_id */
    InitColDesc(COL_TYPE_UNSIGNED_LONG), /* ol_delivery_d */
    InitColDesc(COL_TYPE_INT),           /* ol_quantity */
    InitColDesc(COL_TYPE_FLOAT),         /* ol_amount */
    InitVarColDesc(COL_TYPE_VARCHAR, 25),   /* ol_dist_info */
};

#define col_ol_w_id 0
#define col_ol_d_id 1
#define col_ol_o_id 2
#define col_ol_number 3
#define col_ol_i_id 4
#define col_ol_supply_w_id 5
#define col_ol_delivery_d 6
#define col_ol_quantity 7
#define col_ol_amount 8
#define col_ol_dist_info 9

ColumnDesc HistoryColDesc[] = {
    InitColDesc(COL_TYPE_INT),           /* h_c_id */
    InitColDesc(COL_TYPE_INT),           /* h_c_d_id */
    InitColDesc(COL_TYPE_INT),           /* h_c_w_id */
    InitColDesc(COL_TYPE_INT),           /* h_d_id */
    InitColDesc(COL_TYPE_INT),           /* h_w_id */
    InitColDesc(COL_TYPE_UNSIGNED_LONG), /* h_date */
    InitColDesc(COL_TYPE_LONG),          /* h_amount */
    InitVarColDesc(COL_TYPE_VARCHAR, 25),   /* h_data */
};

#define col_h_c_id 0
#define col_h_c_d_id 1
#define col_h_c_w_id 2
#define col_h_d_id 3
#define col_h_w_id 4
#define col_h_date 5
#define col_h_amount 6
#define col_h_data 7

/* convert col name to col id. */
#define COL_ID(name) col##_##name

#define CAL_COL_CNT(col_desc) sizeof(col_desc) / sizeof(ColumnDesc)

TableDesc TableDescArr[TABLE_NUM] = {
    {&WarehouseColDesc[0], CAL_COL_CNT(WarehouseColDesc)}, {&DistrictColDesc[0], CAL_COL_CNT(DistrictColDesc)},
    {&StockColDesc[0], CAL_COL_CNT(StockColDesc)},         {&ItemColDesc[0], CAL_COL_CNT(ItemColDesc)},
    {&CustomerColDesc[0], CAL_COL_CNT(CustomerColDesc)},   {&OrderColDesc[0], CAL_COL_CNT(OrderColDesc)},
    {&NewOrderColDesc[0], CAL_COL_CNT(NewOrderColDesc)},   {&OrderLineColDesc[0], CAL_COL_CNT(OrderLineColDesc)},
    {&HistoryColDesc[0], CAL_COL_CNT(HistoryColDesc)}};

#define TABLE_COL_DESC(type) TableDescArr[TABLE_OFFSET(type)].col_desc
#define TABLE_COL_CNT(type) TableDescArr[TABLE_OFFSET(type)].col_cnt
#define TABLE_ROW_LEN(type) TableDescArr[TABLE_OFFSET(type)].row_len

void InitTableDesc() {
    for (uint32_t i = 0; i < TABLE_NUM; i++) {
        uint32_t type = TABLE_FIRST + i;
        InitColumnDesc(TABLE_COL_DESC(type), TABLE_COL_CNT(type), TABLE_ROW_LEN(type));
    }
    DCHECK(TABLE_COL_CNT(TABLE_WAREHOUSE) == COL_ID(w_zip) - COL_ID(w_id) + 1);
    DCHECK(TABLE_COL_CNT(TABLE_DISTRICT) == COL_ID(d_zip) - COL_ID(d_id) + 1);
    DCHECK(TABLE_COL_CNT(TABLE_STOCK) == COL_ID(s_data) - COL_ID(s_w_id) + 1);
    DCHECK(TABLE_COL_CNT(TABLE_ITEM) == COL_ID(i_data) - COL_ID(i_id) + 1);
    DCHECK(TABLE_COL_CNT(TABLE_CUSTOMER) == COL_ID(c_credit_lim) - COL_ID(c_id) + 1);
    DCHECK(TABLE_COL_CNT(TABLE_ORDER) == COL_ID(o_all_local) - COL_ID(o_id) + 1);
    DCHECK(TABLE_COL_CNT(TABLE_NEWORDER) == COL_ID(no_w_id) - COL_ID(no_o_id) + 1);
    DCHECK(TABLE_COL_CNT(TABLE_ORDERLINE) == COL_ID(ol_dist_info) - COL_ID(ol_w_id) + 1);
    DCHECK(TABLE_COL_CNT(TABLE_HISTORY) == COL_ID(h_data) - COL_ID(h_c_id) + 1);
}

/* REQUIRE: col_name correct. */
#define FETCH_COL(tuple, col_name, col_value) tuple.GetCol(COL_ID(col_name), (char *)&col_value)

#define GET_COL(tuple, col_name) tuple.GetCol(COL_ID(col_name))
#define GET_COL_INT(tuple, col_name) *(int *)GET_COL(tuple, col_name)
#define GET_COL_FLOAT(tuple, col_name) *(float *)GET_COL(tuple, col_name)
#define GET_COL_LONG(tuple, col_name) *(int64 *)GET_COL(tuple, col_name)
#define GET_COL_UNSIGNED_LONG(tuple, col_name) *(uint64 *)GET_COL(tuple, col_name)

#define EQUAL_COL(tuple, col_name, col_value) tuple.ColEqual(COL_ID(col_name), (char *)&col_value)
#define SET_COL(tuple, col_name, col_value) tuple.SetCol(COL_ID(col_name), (char *)&col_value)

#define UPDATE_COL(tuple, col_name, col_value) tuple.UpdateCol(COL_ID(col_name), (char *)&col_value)

#define STACK_TUPLE(table_type, tuple_name)                   \
    RAMTuple tuple_name                                       \
    {                                                         \
        TABLE_COL_DESC(table_type), TABLE_ROW_LEN(table_type) \
    }
#define STACK_WAREHOUSE(tuple_name) STACK_TUPLE(TABLE_WAREHOUSE, tuple_name)
#define STACK_DISTRICT(tuple_name) STACK_TUPLE(TABLE_DISTRICT, tuple_name)
#define STACK_STOCK(tuple_name) STACK_TUPLE(TABLE_STOCK, tuple_name)
#define STACK_ITEM(tuple_name) STACK_TUPLE(TABLE_ITEM, tuple_name)
#define STACK_CUSTOMER(tuple_name) STACK_TUPLE(TABLE_CUSTOMER, tuple_name)
#define STACK_ORDER(tuple_name) STACK_TUPLE(TABLE_ORDER, tuple_name)
#define STACK_NEWORDER(tuple_name) STACK_TUPLE(TABLE_NEWORDER, tuple_name)
#define STACK_ORDERLINE(tuple_name) STACK_TUPLE(TABLE_ORDERLINE, tuple_name)
#define STACK_HISTORY(tuple_name) STACK_TUPLE(TABLE_HISTORY, tuple_name)

#define HEAP_TUPLE(table_type) new RAMTuple(TABLE_COL_DESC(table_type), TABLE_ROW_LEN(table_type))
#define HEAP_WAREHOUSE() HEAP_TUPLE(TABLE_WAREHOUSE)
#define HEAP_DISTRICT() HEAP_TUPLE(TABLE_DISTRICT)
#define HEAP_STOCK() HEAP_TUPLE(TABLE_STOCK)
#define HEAP_ITEM() HEAP_TUPLE(TABLE_ITEM)
#define HEAP_CUSTOMER() HEAP_TUPLE(TABLE_CUSTOMER)
#define HEAP_ORDER() HEAP_TUPLE(TABLE_ORDER)
#define HEAP_NEWORDER() HEAP_TUPLE(TABLE_NEWORDER)
#define HEAP_ORDERLINE() HEAP_TUPLE(TABLE_ORDERLINE)
#define HEAP_HISTORY() HEAP_TUPLE(TABLE_HISTORY)

IndexColumnDesc WarehousePKDesc[] = {{COL_ID(w_id)}};

#define pk_col_w_id 0

IndexColumnDesc DistrictPKDesc[] = {{COL_ID(d_w_id)}, {COL_ID(d_id)}};

#define pk_col_d_w_id 0
#define pk_col_d_id 1

IndexColumnDesc StockPKDesc[] = {{COL_ID(s_w_id)}, {COL_ID(s_i_id)}};

#define pk_col_s_w_id 0
#define pk_col_s_i_id 1

IndexColumnDesc ItemPKDesc[] = {{COL_ID(i_id)}};

#define pk_col_i_id 0

IndexColumnDesc CustomerPKDesc[] = {{COL_ID(c_w_id)}, {COL_ID(c_d_id)}, {COL_ID(c_id)}};

#define pk_col_c_w_id 0
#define pk_col_c_d_id 1
#define pk_col_c_id 2

IndexColumnDesc CustomerSKDesc[] = {{COL_ID(c_w_id)}, {COL_ID(c_d_id)}, {COL_ID(c_last)}, {COL_ID(c_id)}};

#define sk_col_c_w_id 0
#define sk_col_c_d_id 1
#define sk_col_c_last 2
#define sk_col_c_id 3

IndexColumnDesc OrderPKDesc[] = {{COL_ID(o_w_id)}, {COL_ID(o_d_id)}, {COL_ID(o_id)}};

#define pk_col_o_w_id 0
#define pk_col_o_d_id 1
#define pk_col_o_id 2

IndexColumnDesc OrderSKDesc[] = {{COL_ID(o_w_id)}, {COL_ID(o_d_id)}, {COL_ID(o_c_id)}, {COL_ID(o_id)}};

#define sk_col_o_w_id 0
#define sk_col_o_d_id 1
#define sk_col_o_c_id 2
#define sk_col_o_id 3

IndexColumnDesc NewOrderPKDesc[] = {{COL_ID(no_w_id)}, {COL_ID(no_d_id)}, {COL_ID(no_o_id)}};

#define pk_col_no_w_id 0
#define pk_col_no_d_id 1
#define pk_col_no_o_id 2

IndexColumnDesc OrderLinePKDesc[] = {{COL_ID(ol_w_id)}, {COL_ID(ol_d_id)}, {COL_ID(ol_o_id)}, {COL_ID(ol_number)}};

#define pk_col_ol_w_id 0
#define pk_col_ol_d_id 1
#define pk_col_ol_o_id 2
#define pk_col_ol_number 3

/* convert index col name to index col id. */
#define INDEX_COL_ID(index, name) index##_##col##_##name

#define CAL_INDEX_COL_CNT(index_col_desc) sizeof(index_col_desc) / sizeof(IndexColumnDesc)

// conflict with index.cpp
struct IndexDesc {
    IndexColumnDesc *index_col_desc = nullptr;
    uint32 index_col_cnt{};
    uint64 index_len{};
};

IndexDesc IndexDescArr[] = {{&WarehousePKDesc[0], CAL_INDEX_COL_CNT(WarehousePKDesc)},
                            {&DistrictPKDesc[0], CAL_INDEX_COL_CNT(DistrictPKDesc)},
                            {&StockPKDesc[0], CAL_INDEX_COL_CNT(StockPKDesc)},
                            {&ItemPKDesc[0], CAL_INDEX_COL_CNT(ItemPKDesc)},
                            {&CustomerPKDesc[0], CAL_INDEX_COL_CNT(CustomerPKDesc)},
                            {&OrderPKDesc[0], CAL_INDEX_COL_CNT(OrderPKDesc)},
                            {&NewOrderPKDesc[0], CAL_INDEX_COL_CNT(NewOrderPKDesc)},
                            {&OrderLinePKDesc[0], CAL_INDEX_COL_CNT(OrderLinePKDesc)},
                            {&CustomerSKDesc[0], CAL_INDEX_COL_CNT(CustomerSKDesc)},
                            {&OrderSKDesc[0], CAL_INDEX_COL_CNT(OrderSKDesc)}};

#define INDEX_COL_DESC(type) IndexDescArr[TABLE_OFFSET(type)].index_col_desc
#define INDEX_COL_CNT(type) IndexDescArr[TABLE_OFFSET(type)].index_col_cnt
#define INDEX_LEN(type) IndexDescArr[TABLE_OFFSET(type)].index_len

void InitIndexDesc() {
    /* primary key index */
    for (uint32_t i = 0; i < TABLE_NUM - 1; i++) {
        uint32_t type = TABLE_FIRST + i;
        InitIndexDesc(INDEX_COL_DESC(type), TABLE_COL_DESC(type), INDEX_COL_CNT(type), INDEX_LEN(type));
    }
    /* secondary index */
    InitIndexDesc(IndexDescArr[8].index_col_desc, TABLE_COL_DESC(TABLE_CUSTOMER), IndexDescArr[8].index_col_cnt,
                  IndexDescArr[8].index_len);
    InitIndexDesc(IndexDescArr[9].index_col_desc, TABLE_COL_DESC(TABLE_ORDER), IndexDescArr[9].index_col_cnt,
                  IndexDescArr[9].index_len);
    DCHECK(INDEX_COL_CNT(TABLE_WAREHOUSE) == INDEX_COL_ID(pk, w_id) - INDEX_COL_ID(pk, w_id) + 1);
    DCHECK(INDEX_COL_CNT(TABLE_DISTRICT) == INDEX_COL_ID(pk, d_id) - INDEX_COL_ID(pk, d_w_id) + 1);
    DCHECK(INDEX_COL_CNT(TABLE_STOCK) == INDEX_COL_ID(pk, s_i_id) - INDEX_COL_ID(pk, s_w_id) + 1);
    DCHECK(INDEX_COL_CNT(TABLE_ITEM) == INDEX_COL_ID(pk, i_id) - INDEX_COL_ID(pk, i_id) + 1);
    DCHECK(INDEX_COL_CNT(TABLE_CUSTOMER) == INDEX_COL_ID(pk, c_id) - INDEX_COL_ID(pk, c_w_id) + 1);
    DCHECK(INDEX_COL_CNT(TABLE_ORDER) == INDEX_COL_ID(pk, o_id) - INDEX_COL_ID(pk, o_w_id) + 1);
    DCHECK(INDEX_COL_CNT(TABLE_NEWORDER) == INDEX_COL_ID(pk, no_o_id) - INDEX_COL_ID(pk, no_w_id) + 1);
    DCHECK(INDEX_COL_CNT(TABLE_ORDERLINE) == INDEX_COL_ID(pk, ol_number) - INDEX_COL_ID(pk, ol_w_id) + 1);
    DCHECK(IndexDescArr[8].index_col_cnt == INDEX_COL_ID(sk, c_id) - INDEX_COL_ID(sk, c_w_id) + 1);
    DCHECK(IndexDescArr[9].index_col_cnt == INDEX_COL_ID(sk, o_id) - INDEX_COL_ID(sk, o_w_id) + 1);
}

/* REQUIRE: col_name correct. */
#define SET_INDEX_COL(tuple, index, col_name, col_value) tuple.SetCol(INDEX_COL_ID(index, col_name), (char *)&col_value)

#define SET_INDEX_VAR(tuple, index, col_name, col_value) tuple.SetCol(INDEX_COL_ID(index, col_name), col_value, true)

#define INDEX_TUPLE(table_type, tuple_name)                                                                      \
    DRAMIndexTuple tuple_name                                                                                    \
    {                                                                                                            \
        TABLE_COL_DESC(table_type), INDEX_COL_DESC(table_type), INDEX_COL_CNT(table_type), INDEX_LEN(table_type) \
    }
#define WAREHOUSE_INDEX(tuple_name) INDEX_TUPLE(TABLE_WAREHOUSE, tuple_name)
#define DISTRICT_INDEX(tuple_name) INDEX_TUPLE(TABLE_DISTRICT, tuple_name)
#define STOCK_INDEX(tuple_name) INDEX_TUPLE(TABLE_STOCK, tuple_name)
#define ITEM_INDEX(tuple_name) INDEX_TUPLE(TABLE_ITEM, tuple_name)
#define CUSTOMER_INDEX(tuple_name) INDEX_TUPLE(TABLE_CUSTOMER, tuple_name)
#define ORDER_INDEX(tuple_name) INDEX_TUPLE(TABLE_ORDER, tuple_name)
#define NEWORDER_INDEX(tuple_name) INDEX_TUPLE(TABLE_NEWORDER, tuple_name)
#define ORDERLINE_INDEX(tuple_name) INDEX_TUPLE(TABLE_ORDERLINE, tuple_name)

#define CUSTOMER_SEC_INDEX(tuple_name)                                                                 \
    DRAMIndexTuple tuple_name                                                                          \
    {                                                                                                  \
        TABLE_COL_DESC(TABLE_CUSTOMER), IndexDescArr[8].index_col_desc, IndexDescArr[8].index_col_cnt, \
            IndexDescArr[8].index_len                                                                  \
    }
#define ORDER_SEC_INDEX(tuple_name)                                                                 \
    DRAMIndexTuple tuple_name                                                                       \
    {                                                                                               \
        TABLE_COL_DESC(TABLE_ORDER), IndexDescArr[9].index_col_desc, IndexDescArr[9].index_col_cnt, \
            IndexDescArr[9].index_len                                                               \
    }

/* SanityCheck Before Insert Tuple */
static inline void SanityCheckWarehouse(const RAMTuple &w) {
    // Assert (GET_COL_INT(w, w_id) >= wh_start && GET_COL_INT(w, w_id) <= wh_end);
    DCHECK(strlen(GET_COL(w, w_state)) == 2);
    DCHECK(strlen(GET_COL(w, w_zip)) == 9);
}

static inline void SanityCheckDistrict(const RAMTuple &d) {
    // Assert (GET_COL_INT(d, d_w_id) >= wh_start && GET_COL_INT(d, d_w_id) <= wh_end);
    DCHECK(GET_COL_INT(d, d_id) >= 1 && GET_COL_INT(d, d_id) <= DIST_PER_WARE);
    DCHECK(GET_COL_INT(d, d_next_o_id) >= 3001);
    DCHECK(strlen(GET_COL(d, d_state)) == 2);
    DCHECK(strlen(GET_COL(d, d_zip)) == 9);
}

static inline void SanityCheckItem(const RAMTuple &i) {
    DCHECK(GET_COL_INT(i, i_id) >= 1 && GET_COL_INT(i, i_id) <= MAXITEMS);
    DCHECK(GET_COL_FLOAT(i, i_price) >= 1.0 && GET_COL_FLOAT(i, i_price) <= 100.0);
}

static inline void SanityCheckCustomer(const RAMTuple &c) {
    // Assert (GET_COL_INT(c, c_w_id) >= wh_start && GET_COL_INT(c, c_w_id) <= wh_end);
    DCHECK(GET_COL_INT(c, c_d_id) >= 1 && GET_COL_INT(c, c_d_id) <= DIST_PER_WARE);
    DCHECK(GET_COL_INT(c, c_id) >= 1 && GET_COL_INT(c, c_id) <= CUST_PER_DIST);
    DCHECK(!strcmp(GET_COL(c, c_credit), "BC") || !strcmp(GET_COL(c, c_credit), "GC"));
    DCHECK(!strcmp(GET_COL(c, c_middle), "OE"));
}

static inline void SanityCheckStock(const RAMTuple &s) {
    // Assert (GET_COL_INT(s, s_w_id) >= wh_start && GET_COL_INT(s, s_w_id) <= wh_end);
    DCHECK(GET_COL_INT(s, s_i_id) >= 1 && GET_COL_INT(s, s_i_id) <= MAXITEMS);
}

static inline void SanityCheckNewOrder(const RAMTuple &no) {
    // Assert (GET_COL_INT(no, no_w_id) >= wh_start && GET_COL_INT(no, no_w_id) <= wh_end);
    DCHECK(GET_COL_INT(no, no_d_id) >= 1 && GET_COL_INT(no, no_d_id) <= DIST_PER_WARE);
}

static inline void SanityCheckOrder(const RAMTuple &o) {
    // Assert (GET_COL_INT(o, o_w_id) >= wh_start && GET_COL_INT(o, o_w_id) <= wh_end);
    DCHECK(GET_COL_INT(o, o_d_id) >= 1 && GET_COL_INT(o, o_d_id) <= DIST_PER_WARE);
    DCHECK(GET_COL_INT(o, o_c_id) >= 1 && GET_COL_INT(o, o_c_id) <= CUST_PER_DIST);
    DCHECK(GET_COL_UNSIGNED_LONG(o, o_carrier_id) >= 0 && GET_COL_INT(o, o_carrier_id) <= DIST_PER_WARE);
    DCHECK(GET_COL_INT(o, o_ol_cnt) >= 5 && GET_COL_INT(o, o_ol_cnt) <= MAX_NUM_ITEMS);
}

static inline void SanityCheckOrderLine(const RAMTuple &ol) {
    // DCHECK(GET_COL_INT(ol, ol_w_id) >= wh_start && GET_COL_INT(ol, ol_w_id) <= wh_end);
    DCHECK(GET_COL_INT(ol, ol_d_id) >= 1 && GET_COL_INT(ol, ol_d_id) <= DIST_PER_WARE);
    DCHECK(GET_COL_INT(ol, ol_number) >= 1 && GET_COL_INT(ol, ol_number) <= MAX_NUM_ITEMS);
    DCHECK(GET_COL_INT(ol, ol_i_id) >= 1 && GET_COL_INT(ol, ol_i_id) <= MAXITEMS);
}

namespace tpcc {
const int MIN_ROW_ID = 0;
const int MAX_ROW_ID = INT32_MAX;
}  // namespace tpcc

void neworder_to_oid_range(__OUT DRAMIndexTuple &keyb, __OUT DRAMIndexTuple &keye, int w_id, int d_id) {
    SET_INDEX_COL(keyb, pk, no_w_id, w_id);
    SET_INDEX_COL(keyb, pk, no_d_id, d_id);
    SET_INDEX_COL(keyb, pk, no_o_id, tpcc::MIN_ROW_ID);
    SET_INDEX_COL(keye, pk, no_w_id, w_id);
    SET_INDEX_COL(keye, pk, no_d_id, d_id);
    SET_INDEX_COL(keye, pk, no_o_id, tpcc::MAX_ROW_ID);
}

void order_to_oid_range(__OUT DRAMIndexTuple &keyb, __OUT DRAMIndexTuple &keye, int w_id, int d_id) {
    SET_INDEX_COL(keyb, pk, o_w_id, w_id);
    SET_INDEX_COL(keyb, pk, o_d_id, d_id);
    SET_INDEX_COL(keyb, pk, o_id, tpcc::MIN_ROW_ID);
    SET_INDEX_COL(keye, pk, o_w_id, w_id);
    SET_INDEX_COL(keye, pk, o_d_id, d_id);
    SET_INDEX_COL(keye, pk, o_id, tpcc::MAX_ROW_ID);
}

void orderline_to_number_range(__OUT DRAMIndexTuple &keyb, __OUT DRAMIndexTuple &keye, int w_id, int d_id, int o_id_low,
                               int o_id_high) {
    DCHECK(o_id_low <= o_id_high);
    SET_INDEX_COL(keyb, pk, ol_w_id, w_id);
    SET_INDEX_COL(keyb, pk, ol_d_id, d_id);
    SET_INDEX_COL(keyb, pk, ol_o_id, o_id_low);
    SET_INDEX_COL(keyb, pk, ol_number, tpcc::MIN_ROW_ID);
    SET_INDEX_COL(keye, pk, ol_w_id, w_id);
    SET_INDEX_COL(keye, pk, ol_d_id, d_id);
    SET_INDEX_COL(keye, pk, ol_o_id, o_id_high);
    SET_INDEX_COL(keye, pk, ol_number, tpcc::MAX_ROW_ID);
}

void orderline_to_number_range(__OUT DRAMIndexTuple &keyb, __OUT DRAMIndexTuple &keye, int w_id, int d_id, int o_id) {
    orderline_to_number_range(keyb, keye, w_id, d_id, o_id, o_id);
}

void customer_to_sk_range(__OUT DRAMIndexTuple &keyb, __OUT DRAMIndexTuple &keye, int c_w_id, int c_d_id, char *c_last) {
    SET_INDEX_COL(keyb, sk, c_w_id, c_w_id);
    SET_INDEX_COL(keyb, sk, c_d_id, c_d_id);
    SET_INDEX_VAR(keyb, sk, c_last, c_last);
    SET_INDEX_COL(keyb, sk, c_id, tpcc::MIN_ROW_ID);
    SET_INDEX_COL(keye, sk, c_w_id, c_w_id);
    SET_INDEX_COL(keye, sk, c_d_id, c_d_id);
    SET_INDEX_VAR(keye, sk, c_last, c_last);
    SET_INDEX_COL(keye, sk, c_id, tpcc::MAX_ROW_ID);
}

void order_to_sk_range(__OUT DRAMIndexTuple &keyb, __OUT DRAMIndexTuple &keye, int c_w_id, int c_d_id, int c_id) {
    SET_INDEX_COL(keyb, sk, o_w_id, c_w_id);
    SET_INDEX_COL(keyb, sk, o_d_id, c_d_id);
    SET_INDEX_COL(keyb, sk, o_c_id, c_id);
    SET_INDEX_COL(keyb, sk, o_id, tpcc::MIN_ROW_ID);
    SET_INDEX_COL(keye, sk, o_w_id, c_w_id);
    SET_INDEX_COL(keye, sk, o_d_id, c_d_id);
    SET_INDEX_COL(keye, sk, o_c_id, c_id);
    SET_INDEX_COL(keye, sk, o_id, tpcc::MAX_ROW_ID);
}

/* Load tables in order */
const TableType tableNeedLoad[] = {TABLE_WAREHOUSE, TABLE_DISTRICT, TABLE_ITEM,
                                   TABLE_CUSTOMER,  TABLE_STOCK,    TABLE_ORDER};
#define TABLE_LOAD_NUM (sizeof(tableNeedLoad) / sizeof(TableType))

thread_local unsigned int g_seed;

// init random seed for fast_rand() - shall be called per thread
void fast_rand_srand(int seed) {
    g_seed = seed;
}

inline int fast_rand(void) {
    g_seed = (214013 * g_seed + 2531011);
    return (g_seed >> 16) & 0x7FFF;
}

// return number uniformly distributed b/w min and max, inclusive
int RandomNumber(int min, int max) {
    // TBD: multiple choice: glibc/rand(), ermia/RAND() and fast_rand()
    return min + (fast_rand() % ((max - min) + 1));
}

/*
 * make a ``random a-string'': a string of random alphanumeric
 * characters of a random length of minimum x, maximum y, and
 * mean (y+x)/2
 *
 * this implementation is simplified to accelerate load and it does not
 * impact tpcc run performance.
 */
void MakeAlphaString(int x, int y, char str[]) {
    static const char *alphanum = "0123456789"
                                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                  "abcdefghijklmnopqrstuvwxyz";
    int arrmax = 61; /* index of last array element */
    int i, len;
    len = RandomNumber(x, y);
    for (i = 0; i < len; i++)
        str[i] = alphanum[RandomNumber(0, arrmax)];
    str[len] = 0;
}

/*
 * like MakeAlphaString, only numeric characters only
 */
void MakeNumberString(int x, int y, char str[]) {
    static const char *numeric = "0123456789";
    int arrmax = 9;
    int i, len;

    len = RandomNumber(x, y);
    for (i = 0; i < len; i++)
        str[i] = numeric[RandomNumber(0, arrmax)];
    str[len] = 0;
}

int NURand(unsigned A, unsigned x, unsigned y) {
    static int first = 1;
    unsigned C, C_255 = 0, C_1023 = 0, C_8191 = 0;

    if (first) {
        C_255 = RandomNumber(0, 255);
        C_1023 = RandomNumber(0, 1023);
        C_8191 = RandomNumber(0, 8191);
        first = 0;
    }

    switch (A) {
        case 255:
            C = C_255;
            break;
        case 1023:
            C = C_1023;
            break;
        case 8191:
            C = C_8191;
            break;
        default:
            fprintf(stderr, "NURand: unexpected value (%d) of A used\n", A);
            abort();
    }

    return (int)(((RandomNumber(0, A) | RandomNumber(x, y)) + C) % (y - x + 1)) + x;
}

thread_local int perm_count;
thread_local int nums[CUST_PER_DIST];

#define swap_int(a, b) \
    {                  \
        int tmp;       \
        tmp = a;       \
        a = b;         \
        b = tmp;       \
    }

void InitPermutation(void) {
    int *cur;
    int i, j;

    perm_count = 0;

    /* initialize with consecutive values [1..ORD_PER_DIST] */
    for (i = 0, cur = nums; i < ORD_PER_DIST; i++, cur++) {
        *cur = i + 1;
    }

    /* now, shuffle */
    for (i = 0; i < ORD_PER_DIST - 1; i++) {
        j = (int)RandomNumber(i + 1, ORD_PER_DIST - 1);
        swap_int(nums[i], nums[j]);
    }
}

int &GetPermutation(void) {
    if (perm_count >= ORD_PER_DIST) {
        fprintf(stderr, "GetPermutation: past end of list!\n");
        abort();
    }
    return nums[perm_count++];
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      MakeAddress() | DESCRIPTION |      Build an Address |
 * ARGUMENTS
 * +==================================================================
 */
void MakeAddress(char *str1, char *str2, char *city, char *state, char *zip) {
    MakeAlphaString(10, 20, str1); /* Street 1 */
    MakeAlphaString(10, 20, str2); /* Street 2 */
    MakeAlphaString(10, 20, city); /* City */
    MakeAlphaString(2, 2, state);  /* State */
    MakeNumberString(9, 9, zip);   /* Zip */
}

/*==================================================================+
  | ROUTINE NAME
  |      Lastname
  | DESCRIPTION
  |      TPC-C Lastname Function.
  | ARGUMENTS
  |      num  - non-uniform random number
  |      name - last name string
  +==================================================================*/
void Lastname(int num, char *name) {
    static const char *n[] = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"};
    name[0] = n[num / 100][0];
    name[1] = n[(num / 10) % 10][0];
    name[2] = n[num % 10][0];
}

// Transaction names
const char *tname[] = {"neword", "payment", "ordstat", "deliv", "stockl"};

struct RunStat {
    uint64_t nCommitted_ = 0;
    uint64_t nAborted_ = 0;
};

#define CACHELINE_BYTES 64
#define CACHELINE_ALIGNED __attribute__((aligned(CACHELINE_BYTES)))

struct CACHELINE_ALIGNED TpccRunStat {
    RunStat runstat_[5];
    uint64_t nTotalCommitted_ = 0;
    uint64_t nTotalAborted_ = 0;
};

/*
 * turn system time into database format
 * the format argument should be a strftime() format string that produces
 *   a datetime string acceptable to the database
 */
void gettimestamp(char str[], size_t len) {
    time_t ltime;
    struct tm newtime;

    ltime = time(&ltime);
    localtime_r(&ltime, &newtime);
    asctime_r(&newtime, str);
}

#define pick_dist_info(sr, ol_dist_info, ol_supply_w_id)       \
    switch (ol_supply_w_id) {                                  \
        case 1:                                                \
            strncpy(ol_dist_info, GET_COL(sr, s_dist_01), 25); \
            break;                                             \
        case 2:                                                \
            strncpy(ol_dist_info, GET_COL(sr, s_dist_02), 25); \
            break;                                             \
        case 3:                                                \
            strncpy(ol_dist_info, GET_COL(sr, s_dist_03), 25); \
            break;                                             \
        case 4:                                                \
            strncpy(ol_dist_info, GET_COL(sr, s_dist_04), 25); \
            break;                                             \
        case 5:                                                \
            strncpy(ol_dist_info, GET_COL(sr, s_dist_05), 25); \
            break;                                             \
        case 6:                                                \
            strncpy(ol_dist_info, GET_COL(sr, s_dist_06), 25); \
            break;                                             \
        case 7:                                                \
            strncpy(ol_dist_info, GET_COL(sr, s_dist_07), 25); \
            break;                                             \
        case 8:                                                \
            strncpy(ol_dist_info, GET_COL(sr, s_dist_08), 25); \
            break;                                             \
        case 9:                                                \
            strncpy(ol_dist_info, GET_COL(sr, s_dist_09), 25); \
            break;                                             \
        case 10:                                               \
            strncpy(ol_dist_info, GET_COL(sr, s_dist_10), 25); \
            break;                                             \
    }

void GetSplitRange(int DOP, uint32_t size, int seq, __OUT uint32_t *start, __OUT uint32_t *end,
                   bool is_warehouse = true) {
    int range;

    DCHECK(seq >= 0 && seq <= DOP - 1);
    range = size / DOP;
    /* Warehouse start from 1, but RowId not. */
    *start = range * seq + is_warehouse;
    *end = *start + range - 1;
    if (seq == DOP - 1)
        *end = size;
    if (*end < *start) {
        LOG(ERROR) << "GetSplitRange Failed!" << std::endl;
    }
}

}
}
#endif  // NVMDB_TPCC_H
