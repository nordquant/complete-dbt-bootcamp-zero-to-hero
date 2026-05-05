{#
  You might have `view` as the materialization as we only 
  replace `materialized` with `table` when we implement constraints. 
#}
{{
  config(
    materialized = 'table' 
    )
}} 
WITH src_hosts AS (
    SELECT
        *
    FROM
        {{ ref('src_hosts') }}
)
SELECT
    host_id,
    NVL(
        host_name,
        'Anonymous'
    ) AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    src_hosts
