{{ config(materialized='table') }}

select * from open_traffic where type = 'Bus'
