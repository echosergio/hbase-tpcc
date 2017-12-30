# HBase: TPC-C Benchmark

The TPC-C benchmark simulates the activity of any company that must manage, sell, and distribute products or services. TPC-C defines nine tables with the following cardinalities.  

<p align="center">
  <img  src="https://upload.wikimedia.org/wikipedia/commons/6/63/Sch%C3%A9ma_Datab%C3%A1ze_metody_TPC-C.png" alt="Database schema" width="500">
  <br>
</p>

<p align="center"><em><a href="http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf">tpc-c_v5.11.0.pdf</a></em></p>

## Schema 

**Warehouse**: W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD  
Key: W_ID

**District**: D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID  
Key: D_W_ID, D_ID

**Item**: I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA  
Key: I_ID

**New_order**: NO_O_ID, NO_D_ID, NO_W_ID  
Key: NO_W_ID, NO_D_ID, NO_O_ID

**Orders**: O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D,O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL  
Key: O_W_ID, O_D_ID, O_ID

**History**: H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID ,H_W_ID, H_DATE, H_AMOUNT, H_DATA  
Key: table without a defined key

**Customer**: C_ID, C_W_ID, C_D_ID, C_FIRST,C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE , C_CREDIT, C_CREDITLIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA  
Key: C_W_ID, C_D_ID, C_ID

**Stock**: S_I_ID, S_W_ID, S_QUANTITY, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA  
Key: S_W_ID, S_I_ID

**Order_line**: OL_O_ID, O_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO  
Key: OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER

## Queries 

The goal of this project is to develop a Java program using HBase to create and load the tables, and implementing the following queries:

**Query1**: List the customers with an order from a given warehouse and district during time interval specified by a START_DATE (included) and END_DATE (excluded).

**Query2**: Insert/update (up to 6 times) the discount for a given customer, warehouse and district.

**Query3:** Show the latest 4 discounts for a given customer, warehouse and district.

**Query4**: List all the customers from a given list of districts in a specified warehouse

## Build

`mvn clean install`

## Execution

Get the executable file from HBaseTPCC/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/

`./HBaseTPCC localhost:2181 createTables`  
`./HBaseTPCC localhost:2181 loadTables /Data/CSVS`  
`./HBaseTPCC localhost:2181 query1 1 5 '2013-11-29 00:00:00.000' '2013-12-05 00:00:00.000'`  
`./HBaseTPCC localhost:2181 query2 3 6 8 10,15,20,50,5,10`  
`./HBaseTPCC localhost:2181 query3 3 6 8`  
`./HBaseTPCC localhost:2181 query4 2 3,4,5`