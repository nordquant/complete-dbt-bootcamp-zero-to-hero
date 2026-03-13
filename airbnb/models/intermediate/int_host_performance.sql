{{
  config(
    materialized = 'table'
  )
}}
WITH dim_hosts AS (
  SELECT
    host_id,
    host_name,
    is_superhost,
    created_at
  FROM {{ ref('dim_hosts_cleansed') }}
),

dim_listings AS (
  SELECT
    listing_id,
    room_type,
    price,
    minimum_nights,
    host_id
  FROM {{ ref('dim_listings_cleansed') }}
),

{#
    Host Performance Metrics CTE
    
    Aggregates listing and host information to calculate performance metrics for each host.
    
    Metrics calculated:
    - total_listings: Count of distinct listings per host
    - avg_listing_price: Average price across all host listings
    - min_listing_price: Minimum price among host listings
    - max_listing_price: Maximum price among host listings
    - avg_minimum_nights: Average minimum night requirement for host listings
    - entire_home_count: Count of "Entire home/apt" listings
    - private_room_count: Count of "Private room" listings
    - shared_room_count: Count of "Shared room" listings
    - hotel_room_count: Count of "Hotel room" listings
    
    Source tables:
    - dim_hosts: Host dimension table (left join anchor)
    - dim_listings: Listings dimension table
    
    Grain: One row per host_id
#}
host_listing_metrics AS (
  SELECT
    h.host_id,
    h.host_name,
    h.is_superhost,
    h.created_at,
    COUNT(DISTINCT dl.listing_id) AS total_listings,
    ROUND(AVG(dl.price), 2) AS avg_listing_price,
    MIN(dl.price) AS min_listing_price,
    MAX(dl.price) AS max_listing_price,
    ROUND(AVG(dl.minimum_nights), 2) AS avg_minimum_nights,
    COALESCE(SUM(CASE WHEN dl.room_type = 'Entire home/apt' THEN 1 ELSE 0 END), 0) AS entire_home_count,
    COALESCE(SUM(CASE WHEN dl.room_type = 'Private room' THEN 1 ELSE 0 END), 0) AS private_room_count,
    COALESCE(SUM(CASE WHEN dl.room_type = 'Shared room' THEN 1 ELSE 0 END), 0) AS shared_room_count,
    COALESCE(SUM(CASE WHEN dl.room_type = 'Hotel room' THEN 1 ELSE 0 END), 0) AS hotel_room_count
  FROM dim_hosts h
  LEFT JOIN dim_listings dl ON h.host_id = dl.host_id
  GROUP BY h.host_id, h.host_name, h.is_superhost, h.created_at
),

dominant_room_type_calc AS (
  SELECT
    host_id,
    CASE
      WHEN entire_home_count >= private_room_count 
        AND entire_home_count >= shared_room_count 
        AND entire_home_count >= hotel_room_count 
        AND entire_home_count > 0 THEN 'Entire home/apt'
      WHEN private_room_count >= shared_room_count 
        AND private_room_count >= hotel_room_count 
        AND private_room_count > 0 THEN 'Private room'
      WHEN shared_room_count >= hotel_room_count 
        AND shared_room_count > 0 THEN 'Shared room'
      WHEN hotel_room_count > 0 THEN 'Hotel room'
      ELSE 'No listings'
    END AS dominant_room_type
  FROM host_listing_metrics
)

SELECT
  h.host_id,
  h.host_name,
  h.total_listings,
  h.avg_listing_price,
  h.min_listing_price,
  h.max_listing_price,
  h.entire_home_count,
  h.private_room_count,
  h.shared_room_count,
  h.hotel_room_count,
  h.avg_minimum_nights,
  CASE
    WHEN h.total_listings = 0 THEN 'inactive'
    WHEN h.total_listings = 1 THEN 'single_listing'
    WHEN h.total_listings BETWEEN 2 AND 5 THEN 'small_portfolio'
    WHEN h.total_listings BETWEEN 6 AND 15 THEN 'medium_portfolio'
    WHEN h.total_listings >= 16 THEN 'large_portfolio'
  END AS portfolio_size,
  CASE
    WHEN h.is_superhost = 't' THEN 'superhost'
    ELSE 'standard'
  END AS host_status,
  CASE
    WHEN DATEDIFF(YEAR, h.created_at, CURRENT_DATE()) < 1 THEN 'new_host'
    WHEN DATEDIFF(YEAR, h.created_at, CURRENT_DATE()) BETWEEN 1 AND 3 THEN 'experienced_host'
    WHEN DATEDIFF(YEAR, h.created_at, CURRENT_DATE()) > 3 THEN 'veteran_host'
  END AS host_tenure,
  d.dominant_room_type
FROM host_listing_metrics h
LEFT JOIN dominant_room_type_calc d ON h.host_id = d.host_id
