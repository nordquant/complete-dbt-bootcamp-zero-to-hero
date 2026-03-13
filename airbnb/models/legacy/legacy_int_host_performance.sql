
WITH raw_host_data AS (
    SELECT
        id AS host_id,
        NAME AS host_name,
        created_at AS host_since,
        is_superhost AS host_is_superhost
    FROM {{ source('airbnb', 'hosts') }}
    WHERE id IS NOT NULL
),

raw_listing_data AS (
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

-- Aggregate listing metrics by host
host_listing_aggregates AS (
    SELECT
        host_id,
        COUNT(DISTINCT listing_id) AS total_listings,
        
        -- Price metrics
        AVG(price_clean) AS avg_listing_price,
        MIN(price_clean) AS min_listing_price,
        MAX(price_clean) AS max_listing_price,
        
        -- Room type distribution
        COUNT(DISTINCT CASE WHEN room_type = 'Entire home/apt' THEN listing_id END) AS entire_home_count,
        COUNT(DISTINCT CASE WHEN room_type = 'Private room' THEN listing_id END) AS private_room_count,
        COUNT(DISTINCT CASE WHEN room_type = 'Shared room' THEN listing_id END) AS shared_room_count,
        COUNT(DISTINCT CASE WHEN room_type = 'Hotel room' THEN listing_id END) AS hotel_room_count,
        
        -- Minimum nights metrics
        AVG(minimum_nights) AS avg_minimum_nights,
        MIN(minimum_nights) AS min_minimum_nights,
        MAX(minimum_nights) AS max_minimum_nights
        
    FROM raw_listing_data
    GROUP BY host_id
),

-- Join host data with aggregates
host_performance AS (
    SELECT
        h.host_id,
        h.host_name,
        h.host_since,
        h.host_is_superhost,
        
        -- Listing counts
        COALESCE(l.total_listings, 0) AS total_listings,
        COALESCE(l.entire_home_count, 0) AS entire_home_count,
        COALESCE(l.private_room_count, 0) AS private_room_count,
        COALESCE(l.shared_room_count, 0) AS shared_room_count,
        COALESCE(l.hotel_room_count, 0) AS hotel_room_count,
        
        -- Price metrics
        l.avg_listing_price,
        l.min_listing_price,
        l.max_listing_price,
        
        -- Minimum nights metrics
        l.avg_minimum_nights,
        l.min_minimum_nights,
        l.max_minimum_nights,
        
        -- Host classification based on listing count
        CASE
            WHEN COALESCE(l.total_listings, 0) = 0 THEN 'inactive'
            WHEN COALESCE(l.total_listings, 0) = 1 THEN 'single_listing'
            WHEN COALESCE(l.total_listings, 0) BETWEEN 2 AND 5 THEN 'small_portfolio'
            WHEN COALESCE(l.total_listings, 0) BETWEEN 6 AND 15 THEN 'medium_portfolio'
            ELSE 'large_portfolio'
        END AS portfolio_size,
        
        -- Host status
        CASE
            WHEN h.host_is_superhost THEN 'superhost'
            ELSE 'standard'
        END AS host_status,
        
        -- Host tenure (how long they've been hosting)
        CASE
            WHEN h.host_since IS NULL THEN 'unknown'
            WHEN DATEDIFF('day', h.host_since, CURRENT_DATE()) < 365 THEN 'new_host'
            WHEN DATEDIFF('day', h.host_since, CURRENT_DATE()) BETWEEN 365 AND 1095 THEN 'experienced_host'
            ELSE 'veteran_host'
        END AS host_tenure,
        
        -- Days since host joined
        DATEDIFF('day', h.host_since, CURRENT_DATE()) AS days_as_host,
        
        -- Dominant room type
        CASE
            WHEN COALESCE(l.entire_home_count, 0) >= COALESCE(l.private_room_count, 0) 
                AND COALESCE(l.entire_home_count, 0) >= COALESCE(l.shared_room_count, 0)
                AND COALESCE(l.entire_home_count, 0) >= COALESCE(l.hotel_room_count, 0)
                THEN 'entire_home'
            WHEN COALESCE(l.private_room_count, 0) >= COALESCE(l.shared_room_count, 0)
                AND COALESCE(l.private_room_count, 0) >= COALESCE(l.hotel_room_count, 0)
                THEN 'private_room'
            WHEN COALESCE(l.shared_room_count, 0) >= COALESCE(l.hotel_room_count, 0)
                THEN 'shared_room'
            ELSE 'hotel_room'
        END AS dominant_room_type
        
    FROM raw_host_data h
    LEFT JOIN host_listing_aggregates l
        ON h.host_id = l.host_id
)

SELECT * FROM host_performance

-- ⚠️ TODO: Refactor this model to:
-- 1. Use {{ ref('dim_hosts_cleansed') }} instead of source('airbnb', 'hosts')
-- 2. Use {{ ref('dim_listings_cleansed') }} instead of source('airbnb', 'listings')
-- 3. Remove duplicated price parsing logic
-- 4. Move to models/intermediate/ folder
-- 5. Rename to int_host_performance.sql
