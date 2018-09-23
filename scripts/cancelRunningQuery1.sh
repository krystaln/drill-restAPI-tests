#!/bin/bash
set -x

curl -c /tmp/.drill_cookies_$2 -X POST -k -d j_username=$2 -d j_password=$3 $1/j_security_check
curl -b /tmp/.drill_cookies_$2 -k -X POST -H "Content-Type: application/json" -d '{"queryType":"SQL", "query": "SELECT COUNT(DISTINCT c_customer_sk) as c_customer_sk FROM `dfs.tpcds_sf100_parquet`.customer WHERE c_customer_sk IN (SELECT SS_CUSTOMER_SK FROM `dfs.tpcds_sf100_parquet`.store_sales)"}' $1/query.json

set +x
