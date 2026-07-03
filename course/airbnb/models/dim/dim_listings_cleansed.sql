{{ config(
    materialized='view',
    event_time='created_at'
) }}
with src_listings as (
    select listing_id,
           listing_name,
           listing_url,
           room_type,
           minimum_nights,
           host_id,
           price_str,
           created_at,
           updated_at
    from {{ ref('src_listings') }}
)
select
    listing_id,
    listing_name,
    room_type,
    case
        when minimum_nights = 0 then 1
        else minimum_nights
    end as minimum_nights,
    host_id,
    replace(
        price_str,
        '$'
    ) :: NUMBER (
        10,
        2
    ) as price,
    price_str,
    created_at,
    updated_at
from src_listings
