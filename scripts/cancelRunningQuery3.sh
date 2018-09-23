#!/bin/bash
set -x

curl -b /tmp/.drill_cookies_$2 -k -X POST -H "Content-Type: application/json" -d '{"queryType":"SQL", "query": "select t.wr_returning_customer_sk,t.wr_returned_date_sk,cast(sum(t.wr_return_amt)/sum(t.wr_return_quantity) as bigint) as avg_return_amt from `dfs.tpcds_sf100_parquet`.web_returns t where (t.wr_item_sk in (select ws_item_sk from `dfs.tpcds_sf100_parquet`.web_sales where ws_sales_price < 50 or ws_ext_sales_price < 1000) or t.wr_item_sk in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) and t.wr_reason_sk in (10) group by t.wr_returning_customer_sk,t.wr_returned_date_sk having sum(t.wr_return_amt)/sum(t.wr_return_quantity) >= 50.0 order by t.wr_returning_customer_sk,t.wr_returned_date_sk"}' $1/query.json

set +x
