```
CREATE SCHEMA airbnb;



-- =====================================================
-- 1. RAW_LISTINGS
-- =====================================================

CREATE OR REPLACE TABLE workspace.airbnb.raw_listings (
    id INT,
    listing_url STRING,
    name STRING,
    room_type STRING,
    minimum_nights STRING,
    host_id INT,
    price STRING,
    created_at STRING,
    updated_at TIMESTAMP
);

COPY INTO workspace.airbnb.raw_listings
FROM 's3://dbtlearn/listings.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'quote' = '"'
)
COPY_OPTIONS (
    'mergeSchema' = 'false'
);


-- SCHEMA SCHECK
CREATE OR REPLACE TABLE test AS 
SELECT
  *
FROM
  read_files(
    's3://dbtlearn/listings.csv',
    header => "True",
    inferschema => "True",
  )

-- =====================================================
-- 2. RAW_REVIEWS
-- =====================================================

CREATE OR REPLACE TABLE workspace.airbnb.raw_reviews (
    listing_id INT,
    date TIMESTAMP,
    reviewer_name STRING,
    comments STRING,
    sentiment STRING
);

COPY INTO workspace.airbnb.raw_reviews
FROM 's3://dbtlearn/reviews.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'quote' = '"'
)
COPY_OPTIONS (
    'mergeSchema' = 'false'
);

-- =====================================================
-- 3. RAW_HOSTS
-- =====================================================

CREATE OR REPLACE TABLE workspace.airbnb.raw_hosts (
    id INT,
    name STRING,
    is_superhost STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

COPY INTO workspace.airbnb.raw_hosts
FROM 's3://dbtlearn/hosts.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'quote' = '"'
)
COPY_OPTIONS (
    'mergeSchema' = 'false'
);

```
