-- Zone dimension from the official TLC taxi-zone lookup (seed).
select
    cast(LocationID as integer) as zone_id,
    Borough                     as borough,
    Zone                        as zone,
    service_zone
from {{ ref('taxi_zone_lookup') }}
