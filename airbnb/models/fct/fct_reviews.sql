{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}
WITH src_reviews AS (
    SELECT 
        * 
    FROM 
        {{ ref('src_reviews') }}
)
SELECT 
    * 
FROM 
    src_reviews
WHERE 
    review_text IS NOT NULL
{% if is_incremental() %}
    AND review_date > (SELECT max(review_date) FROM {{ this }})
{% endif %}