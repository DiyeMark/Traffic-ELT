{{ config(materialized='view') }}

select * from {{ ref('open_traffic_all') }} where avg_speed > 50.0
