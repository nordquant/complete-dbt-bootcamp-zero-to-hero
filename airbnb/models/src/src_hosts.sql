WITH RAW_HOSTS AS (
    SELECT 
        * 
    FROM 
        {{ source('airbnb', 'hosts') }}
)
SELECT
    id AS host_id,
    name AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM    
    RAW_HOSTS