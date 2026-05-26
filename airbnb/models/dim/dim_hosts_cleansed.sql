WITH scr_hosts AS (
    SELECT * FROM {{  ref('src_hosts') }}
)
SELECT
    id,
    NVL(
        host_name, 
        'Anonymous'
    ) AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    scr_hosts
