
with  __dbt__cte__src_hosts as (
with raw_hosts as (
    select ID, NAME, IS_SUPERHOST, CREATED_AT, UPDATED_AT from AIRBNB.raw.raw_hosts
)
select
    id as host_id,
    name as host_name,
    IS_SUPERHOST,
    CREATED_AT,
    UPDATED_AT
from raw_hosts
), src_hosts as (
    select host_id,
           host_name,
           IS_SUPERHOST,
           CREATED_AT,
           UPDATED_AT
    from __dbt__cte__src_hosts
)
select
    host_id,
    nvl(host_name, 'N/A') as host_name,
    IS_SUPERHOST,
    CREATED_AT,
    UPDATED_AT
from src_hosts