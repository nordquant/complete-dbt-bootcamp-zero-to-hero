




    with grouped_expression as (
    select
        
        
    
  
( 1=1 and max(price) <= 8000
)
 as expression


    from AIRBNB.PROD.dim_listings_w_hosts
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors





