-- Silver: clean, cast, rename, and constrain the raw yellow-taxi trips.
-- Unknown foreign keys are coalesced to their dimension's "unknown" member so the
-- star schema stays referentially complete. Out-of-window and impossible rows
-- (negative fares, dropoff before pickup, absurd durations) are dropped.

with src as (
    select * from {{ source('bronze', 'yellow_trips') }}
)

select
    coalesce(cast(VendorID as integer), 0)                as vendor_id,
    cast(tpep_pickup_datetime as timestamp)               as pickup_at,
    cast(tpep_dropoff_datetime as timestamp)              as dropoff_at,
    coalesce(cast(PULocationID as integer), 264)          as pickup_zone_id,
    coalesce(cast(DOLocationID as integer), 264)          as dropoff_zone_id,
    coalesce(cast(payment_type as integer), 5)            as payment_type_id,
    coalesce(cast(RatecodeID as integer), 99)             as rate_code_id,
    cast(passenger_count as integer)                      as passenger_count,
    cast(trip_distance as double)                         as trip_distance,
    date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_minutes,
    cast(fare_amount as double)                           as fare_amount,
    cast(tip_amount as double)                            as tip_amount,
    cast(tolls_amount as double)                          as tolls_amount,
    cast(total_amount as double)                          as total_amount
from src
where tpep_pickup_datetime is not null
  and tpep_dropoff_datetime is not null
  and tpep_dropoff_datetime >= tpep_pickup_datetime
  and date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) < 24 * 60
  and trip_distance >= 0
  and fare_amount >= 0
  and total_amount >= 0
  and tpep_pickup_datetime >= timestamp '{{ var("start_date") }}'
  and tpep_pickup_datetime <  timestamp '{{ var("end_date") }}'
