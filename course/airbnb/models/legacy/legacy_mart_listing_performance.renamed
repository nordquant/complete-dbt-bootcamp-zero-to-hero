
WITH raw_listings AS (
    SELECT
        id AS listing_id,
        name AS listing_name,
        room_type,
        CASE
            WHEN price IS NULL THEN 0
            ELSE CAST(
                REPLACE(
                    REPLACE(price, '$', ''),
                    ',', ''
                ) AS DECIMAL(10,2)
            )
        END AS price_clean,
        minimum_nights,
        host_id,
        created_at,
        updated_at
    FROM {{ source('airbnb', 'listings') }}
    WHERE id IS NOT NULL
),

raw_hosts AS (
    SELECT
        id AS host_id,
        name AS host_name,
        created_at AS host_since,
        is_superhost AS host_is_superhost
    FROM {{ source('airbnb', 'hosts') }}
    WHERE id IS NOT NULL
),

host_aggregates AS (
    SELECT
        host_id,
        COUNT(DISTINCT listing_id) AS host_total_listings,
        AVG(price_clean) AS host_avg_price,
        MIN(price_clean) AS host_min_price,
        MAX(price_clean) AS host_max_price
    FROM raw_listings
    GROUP BY host_id
),

listing_with_context AS (
    SELECT
        l.listing_id,
        l.listing_name,
        l.room_type,
        l.price_clean,
        l.minimum_nights,

        l.host_id,
        h.host_name,
        h.host_since,
        CASE
            WHEN h.host_is_superhost = 't' THEN 'superhost'
            ELSE 'standard'
        END AS host_status,

        ha.host_total_listings,
        ha.host_avg_price AS host_portfolio_avg_price,
        ha.host_min_price AS host_portfolio_min_price,
        ha.host_max_price AS host_portfolio_max_price,

        CASE
            WHEN COALESCE(ha.host_total_listings, 0) = 0 THEN 'inactive'
            WHEN COALESCE(ha.host_total_listings, 0) = 1 THEN 'single_listing'
            WHEN COALESCE(ha.host_total_listings, 0) BETWEEN 2 AND 5 THEN 'small_portfolio'
            WHEN COALESCE(ha.host_total_listings, 0) BETWEEN 6 AND 15 THEN 'medium_portfolio'
            ELSE 'large_portfolio'
        END AS host_portfolio_size,

        l.created_at,
        l.updated_at

    FROM raw_listings l
    LEFT JOIN raw_hosts h ON l.host_id = h.host_id
    LEFT JOIN host_aggregates ha ON l.host_id = ha.host_id
),

final AS (
    SELECT
        listing_id,
        listing_name,
        room_type,
        price_clean,
        minimum_nights,
        host_id,
        host_name,
        host_status,
        host_total_listings,
        host_portfolio_size,
        host_portfolio_avg_price,
        host_portfolio_min_price,
        host_portfolio_max_price,

        CASE
            WHEN host_total_listings = 1 THEN 'only_listing'
            WHEN price_clean >= host_portfolio_avg_price THEN 'above_portfolio_avg'
            ELSE 'below_portfolio_avg'
        END AS price_position_vs_portfolio,

        CASE
            WHEN price_clean = 0 THEN 'free'
            WHEN price_clean < 50 THEN 'budget'
            WHEN price_clean BETWEEN 50 AND 100 THEN 'moderate'
            WHEN price_clean BETWEEN 100 AND 200 THEN 'premium'
            ELSE 'luxury'
        END AS price_tier,

        CASE
            WHEN minimum_nights = 1 THEN 'short_stay'
            WHEN minimum_nights BETWEEN 2 AND 7 THEN 'week_stay'
            WHEN minimum_nights BETWEEN 8 AND 30 THEN 'extended_stay'
            ELSE 'long_term'
        END AS stay_length_category,

        CASE
            WHEN host_status = 'superhost' AND price_clean >= 150 THEN 'premium_superhost'
            WHEN host_status = 'superhost' AND price_clean < 150 THEN 'value_superhost'
            WHEN room_type = 'Entire home/apt' AND price_clean >= 200 THEN 'luxury_entire_home'
            WHEN price_clean < 50 THEN 'budget_friendly'
            ELSE 'standard'
        END AS market_segment,

        CASE WHEN host_status = 'superhost' THEN TRUE ELSE FALSE END AS has_superhost_badge,
        CASE WHEN room_type = 'Entire home/apt' THEN TRUE ELSE FALSE END AS is_entire_home,
        CASE WHEN minimum_nights <= 3 THEN TRUE ELSE FALSE END AS allows_short_stays,

        CASE
            WHEN host_total_listings > 10 AND host_status = 'superhost'
                THEN 'professional_host'
            WHEN host_total_listings > 10
                THEN 'multi_property_host'
            WHEN host_total_listings = 1
                THEN 'single_property_host'
            ELSE 'small_portfolio_host'
        END AS host_business_type,

        created_at,
        updated_at

    FROM listing_with_context
)

SELECT * FROM final
