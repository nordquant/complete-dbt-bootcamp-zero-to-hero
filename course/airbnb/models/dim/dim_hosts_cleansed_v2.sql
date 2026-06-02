{{ config(
    materialized='table'
) }}
with src_hosts as (
    select host_id,
           host_name,
           IS_SUPERHOST,
           CREATED_AT,
           UPDATED_AT
    from {{ ref('src_hosts') }}
)
select
    host_id,
    nvl(host_name, 'N/A') as host_name,
    IS_SUPERHOST,
    CREATED_AT,
    UPDATED_AT
from src_hosts
