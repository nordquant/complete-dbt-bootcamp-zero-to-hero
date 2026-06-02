{# event_time (INPUT side): lets downstream microbatch models filter this table
   per batch window (dbt injects "where review_date >= <start> and < <end>").
   Without it, every batch scans the full table -> duplicate-record risk.
#}
{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        event_time='review_date'
    )
}}
with src_reviews as (
    select LISTING_ID, REVIEW_DATE, REVIEWER_NAME, REVIEW_TEXT, REVIEW_SENTIMENT from {{ ref('src_reviews') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} as review_id,
    LISTING_ID, REVIEW_DATE, REVIEWER_NAME, REVIEW_TEXT, REVIEW_SENTIMENT
from src_reviews
where REVIEW_TEXT is not null
{% if is_incremental() %}
  {% if var("start_date", False) and var("end_date", False) %}
    {{ log('Loading ' ~ this ~ ' incrementally (start_date: ' ~ var("start_date") ~ ', end_date: ' ~ var("end_date") ~ ')', info=True) }}
    and review_date >= '{{ var("start_date") }}'
    and review_date < '{{ var("end_date") }}'
  {% else %}
    and REVIEW_DATE >= (select max(REVIEW_DATE) from {{ this }})
    {{ log('Loading ' ~ this ~ ' incrementally (all missing dates)', info=True) }}
  {% endif %}
{% endif %}
