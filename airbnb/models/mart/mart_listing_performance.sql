with listing_base as (
    SELECT 
        listing_id,
        listing_name,
        room_type,
        price,
        minimum_nights,
        host_id,
        host_name,
        host_is_superhost
    FROM {{ ref('dim_listings_w_hosts') }}
),
host_metrics as (
    SELECT 
        host_id,
        total_listings,
        avg_listing_price,
        portfolio_size,
        host_status,
        host_tenure
    FROM {{ ref('int_host_performance') }}

),
final as (
    SELECT 
        l.listing_id,
        l.listing_name,
        l.room_type,
        l.price,
        l.minimum_nights,
        l.host_id,
        l.host_name,
        l.host_is_superhost,
        h.total_listings,
        h.avg_listing_price as host_portfolio_avg_price,
        h.portfolio_size,
        h.host_status,
        h.host_tenure,
        CASE
            WHEN h.total_listings = 1 THEN 'only_listing'
            WHEN l.price >= h.avg_listing_price THEN 'above_portfolio_avg'
            ELSE 'below_portfolio_avg'
        END as price_position_vs_portfolio,
        CASE 
            WHEN l.price < 50 THEN 'budget'
            WHEN l.price >= 50 AND l.price < 150 THEN 'mid_range'
            WHEN l.price >= 150 AND l.price < 300 THEN 'premium'
            ELSE 'luxury'
        END as price_segment
    FROM listing_base l
    LEFT JOIN host_metrics h ON l.host_id = h.host_id
)
SELECT * FROM final