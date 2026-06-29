
    
    

with child as (
    select host_id as from_field
    from AIRBNB.PROD.dim_listings_cleansed
    where host_id is not null
),

parent as (
    select host_id as to_field
    from AIRBNB.PROD.dim_hosts_cleansed_v2
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


