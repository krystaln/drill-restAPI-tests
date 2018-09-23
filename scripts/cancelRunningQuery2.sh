#!/bin/bash
set -x

curl -c /tmp/.drill_cookies_$2 -X POST -k -d j_username=$2 -d j_password=$3 $1/j_security_check
curl -b /tmp/.drill_cookies_$2 -k -X POST -H "Content-Type: application/json" -d '{"queryType":"SQL", "query": "select t.ss_customer_sk,t.ss_store_sk,t.ss_sold_date_sk,cast (sum(t.ss_sales_price)/sum(t.ss_quantity) as decimal (25,20)) as avg_price from `dfs.tpcds_sf100_parquet`.store_sales t where (t.ss_item_sk in (select i_item_sk from `dfs.tpcds_sf100_parquet`.item where i_manufact_id = 10 or i_category_id = 5) or t.ss_item_sk in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25)) and t.ss_store_sk in (1, 2, 3) group by t.ss_customer_sk,t.ss_store_sk,t.ss_sold_date_sk having sum(t.ss_sales_price)/sum(t.ss_quantity) >= 50.0 order by t.ss_customer_sk,t.ss_store_sk,t.ss_sold_date_sk"}' $1/query.json

set +x
