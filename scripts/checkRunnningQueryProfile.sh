#!/bin/bash
set -x

curl -X POST -H "Content-Type: application/json" -d '{"queryType":"SQL", "query": "select count (distinct ss_customer_sk),count(distinct ss_item_sk),count(distinct ss_store_sk),max(ss_ticket_number),sum(ss_item_sk),max(ss_net_profit) from `dfs.tpcds_sf100_parquet`.store_sales"}' $1/query.json

set +x
