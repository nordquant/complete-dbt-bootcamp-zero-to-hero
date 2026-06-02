with l as (
    select LISTING_ID, LISTING_NAME, ROOM_TYPE, MINIMUM_NIGHTS, HOST_ID, PRICE, CREATED_AT, UPDATED_AT
    from {{ ref('dim_listings_cleansed') }}
),
h as (
    select HOST_ID, HOST_NAME, IS_SUPERHOST, CREATED_AT, UPDATED_AT
    from {{ ref('dim_hosts_cleansed', v=1) }}
)
select l.LISTING_ID,
       l.LISTING_NAME,
       l.ROOM_TYPE,
       l.MINIMUM_NIGHTS,
       l.PRICE,
       l.HOST_ID,
       h.HOST_NAME,
       h.IS_SUPERHOST as host_is_superhost,
       l.CREATED_AT,
       greatest(l.UPDATED_AT, h.UPDATED_AT) as updated_at
from l left join h on (h.HOST_ID = l.HOST_ID)
