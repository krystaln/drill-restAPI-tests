#!/bin/bash
set -x

curl -X POST -H "Content-Type: application/json" -d '{"queryType":"SQL", "query": "select max(ss_sold_date_sk),min(ss_sold_time_sk),count(distinct ss_store_sk) from `dfs.tpcds_sf100_parquet`.store_sales"}' $1/query.json

set +x
