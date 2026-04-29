{{
  config(
    materialized = 'view'
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
    CASE is_superhost
        WHEN TRUE THEN 'Superhost'
        ELSE FALSE THEN 'Regular'
    END AS host_type,
    created_at,
    updated_at
FROM
    src_hosts
