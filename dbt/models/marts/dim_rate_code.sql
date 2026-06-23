select
    cast(rate_code_id as integer) as rate_code_id,
    rate_code_name
from {{ ref('rate_code') }}
