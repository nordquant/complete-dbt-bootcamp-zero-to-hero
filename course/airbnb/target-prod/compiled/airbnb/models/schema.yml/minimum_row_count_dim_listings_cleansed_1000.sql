

select count(*) as cnt from AIRBNB.PROD.dim_listings_cleansed having count(*) < 1000
