WITH RAW_HOSTS AS (
    SELECT * FROM AIRBNB.RAW.RAW_HOSTS
)
SELECT
    id,
    name AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM    
    RAW_HOSTS