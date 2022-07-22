{{ config(materialized='table') }}

/*
    Uncomment the line below to remove records with null `id` values
    with open_traffic_new as (

    select * from open_traffic

)

select *
from open_traffic_new
*/

select * from open_traffic where type = 'Car'
