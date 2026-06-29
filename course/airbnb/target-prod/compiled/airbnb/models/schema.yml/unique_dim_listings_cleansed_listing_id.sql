
    
    

select
    listing_id as unique_field,
    count(*) as n_records

from AIRBNB.PROD.dim_listings_cleansed
where listing_id is not null
group by listing_id
having count(*) > 1


