-- Date dimension generated over the ingest window.
with spine as (
    select cast(d as date) as date_day
    from range(
        timestamp '{{ var("start_date") }}',
        timestamp '{{ var("end_date") }}',
        interval 1 day
    ) t(d)
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    extract(year  from date_day)  as year,
    extract(month from date_day)  as month,
    extract(day   from date_day)  as day_of_month,
    extract(dow   from date_day)  as day_of_week,
    dayname(date_day)             as day_name,
    (extract(dow from date_day) in (0, 6)) as is_weekend
from spine
