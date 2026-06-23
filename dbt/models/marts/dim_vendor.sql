select
    cast(vendor_id as integer) as vendor_id,
    vendor_name
from {{ ref('vendor') }}
