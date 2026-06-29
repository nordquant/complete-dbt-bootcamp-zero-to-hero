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