{#
  microbatch: dbt loops over fixed time windows and loads one batch at a time,
  instead of rebuilding the whole table or maintaining a manual is_incremental() filter.
  - event_time (OUTPUT side): column used to SLICE this model output into batches.
  - begin: earliest point of the time range; first/full-refresh run builds batches
    from here up to "now".
  - batch_size='year': one batch per calendar year (e.g. 2009..current year).

  When full_refresh is set to false, put the start and end time explicitly:
  dbt run -s +mart_fullmoon_reviews --event-time-start "2020-01-01" --event-time-end "2030-01-01"
#}
{{ config(
  materialized = 'incremental',
  incremental_strategy='microbatch',
  event_time='review_date',
  begin='2009-06-20',
  batch_size='year',
  full_refresh=false,
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
