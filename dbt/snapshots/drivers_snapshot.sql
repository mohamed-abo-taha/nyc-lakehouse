{# Slowly Changing Dimension Type 2: keep full history of the drivers dimension.
   Each run, dbt closes the previous version (sets dbt_valid_to) and inserts a new
   one when a row changes, keyed on driver_id and detected via updated_at. #}
{% snapshot drivers_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='driver_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select driver_id, name, status, updated_at
from {{ source('raw_oltp', 'drivers') }}
{% endsnapshot %}
