{{ config(
  materialized = 'table',
  tags = ['fact']
) }}

with fct_reviews as (
  select LISTING_ID, REVIEW_DATE, REVIEWER_NAME, REVIEW_TEXT, REVIEW_SENTIMENT from {{ ref('fct_reviews') }}
),

full_moon_dates as (
  select FULL_MOON_DATE from {{ ref('seed_full_moon_dates') }}
)

select r.*,
  case
    when fm.full_moon_date is null then 'not full moon'
    else 'full moon'
  end as is_full_moon
from
  fct_reviews r
  left join full_moon_dates fm
  on (to_date(r.review_date) = dateadd(day, 1, fm.full_moon_date))
