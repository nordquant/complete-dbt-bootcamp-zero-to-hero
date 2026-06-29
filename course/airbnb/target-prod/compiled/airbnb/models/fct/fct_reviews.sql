

with  __dbt__cte__src_reviews as (
with raw_reviews as (
    select LISTING_ID, DATE, REVIEWER_NAME, COMMENTS, SENTIMENT from AIRBNB.raw.raw_reviews
    )
select
    listing_id,
    date as review_date,
    REVIEWER_NAME,
    COMMENTS as review_text,
    SENTIMENT as review_sentiment
from raw_reviews
), src_reviews as (
    select LISTING_ID, REVIEW_DATE, REVIEWER_NAME, REVIEW_TEXT, REVIEW_SENTIMENT from __dbt__cte__src_reviews
)
select
    md5(cast(coalesce(cast(listing_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(review_date as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(reviewer_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(review_text as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as review_id,
    LISTING_ID, REVIEW_DATE, REVIEWER_NAME, REVIEW_TEXT, REVIEW_SENTIMENT
from src_reviews
where REVIEW_TEXT is not null

  
    and REVIEW_DATE >= (select max(REVIEW_DATE) from AIRBNB.PROD.fct_reviews)
    
  
