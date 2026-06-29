
select * from AIRBNB.PROD.fct_reviews as reviews join AIRBNB.PROD.dim_listings_cleansed as listings using(listing_id)
  where listings.created_at > reviews.review_date