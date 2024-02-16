#!/usr/bin/env bash 
set -eu

PWD=$(pwd)
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && cd .. && pwd )"

cd "${PROJECT_DIR}/dbtlearn"
dbt deps
dbt debug

cd "${PROJECT_DIR}dbt_dagster_project"                                                                                                                                                            
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev -h 0.0.0.0 -p 3000

cd "${PWD}"
