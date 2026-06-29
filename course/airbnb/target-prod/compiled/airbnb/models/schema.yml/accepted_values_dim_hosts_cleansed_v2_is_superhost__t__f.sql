
    
    

with all_values as (

    select
        is_superhost as value_field,
        count(*) as n_records

    from AIRBNB.PROD.dim_hosts_cleansed_v2
    group by is_superhost

)

select *
from all_values
where value_field not in (
    't','f'
)


