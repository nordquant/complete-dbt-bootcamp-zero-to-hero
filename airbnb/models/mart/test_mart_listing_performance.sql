-- tests/mart/test_mart_listing_performance.sql

-- Test: No duplicate listing IDs
SELECT listing_id, COUNT(*) as count
FROM {{ ref('mart_listing_performance') }}
GROUP BY listing_id
HAVING COUNT(*) > 1

-- Test: Price is not null
SELECT *
FROM {{ ref('mart_listing_performance') }}
WHERE price IS NULL

-- Test: Valid price segments
SELECT DISTINCT price_segment
FROM {{ ref('mart_listing_performance') }}
WHERE price_segment NOT IN ('budget', 'mid_range', 'premium', 'luxury')

-- Test: Valid price position values
SELECT DISTINCT price_position_vs_portfolio
FROM {{ ref('mart_listing_performance') }}
WHERE price_position_vs_portfolio NOT IN ('only_listing', 'above_portfolio_avg', 'below_portfolio_avg')

-- Test: Price segment logic correctness
SELECT *
FROM {{ ref('mart_listing_performance') }}
WHERE (price < 50 AND price_segment != 'budget')
    OR (price >= 50 AND price < 150 AND price_segment != 'mid_range')
    OR (price >= 150 AND price < 300 AND price_segment != 'premium')
    OR (price >= 300 AND price_segment != 'luxury')

-- Test: Host ID is not null
SELECT *
FROM {{ ref('mart_listing_performance') }}
WHERE host_id IS NULL