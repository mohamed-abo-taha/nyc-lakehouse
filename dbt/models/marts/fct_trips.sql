-- Gold fact: one row per trip, conformed to the dimensions.
-- Incremental on pickup time so re-runs only process new trips; deduplicated on a
-- surrogate key because TLC trips have no natural primary key.
{{ config(materialized='incremental', unique_key='trip_id') }}

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
    {% if is_incremental() %}
    where pickup_at > (select coalesce(max(pickup_at), timestamp '1900-01-01') from {{ this }})
    {% endif %}
),

keyed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'pickup_at', 'dropoff_at', 'pickup_zone_id', 'dropoff_zone_id',
            'vendor_id', 'fare_amount', 'tip_amount', 'total_amount', 'trip_distance'
        ]) }} as trip_id,
        cast(strftime(pickup_at, '%Y%m%d') as integer) as date_key,
        vendor_id,
        pickup_zone_id,
        dropoff_zone_id,
        payment_type_id,
        rate_code_id,
        pickup_at,
        dropoff_at,
        passenger_count,
        trip_distance,
        trip_minutes,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount
    from trips
)

select *
from keyed
qualify row_number() over (partition by trip_id order by pickup_at) = 1
