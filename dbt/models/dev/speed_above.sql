{{ config(materialized='table') }}

select * from open_traffic where avg_speed > 50.0
