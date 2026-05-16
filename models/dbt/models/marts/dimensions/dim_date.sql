-- Date spine covering all Cricsheet historical data (1970 → 2035)
with spine as (
    select cast(unnest(
        generate_series(date '1970-01-01', date '2035-12-31', interval '1 day')
    ) as date) as date_val
)

select
    cast(strftime(date_val, '%Y%m%d') as integer)                 as date_id,
    date_val                                                       as date,
    cast(strftime(date_val, '%Y') as integer)                      as year,
    cast(strftime(date_val, '%m') as integer)                      as month,
    cast(strftime(date_val, '%d') as integer)                      as day,
    cast(strftime(date_val, '%j') as integer)                      as day_of_year,
    cast(ceil(cast(strftime(date_val, '%m') as integer) / 3.0) as integer) as quarter,
    cast(strftime(date_val, '%W') as integer)                      as week_of_year,
    -- DuckDB: 0=Sunday, 1=Monday, …, 6=Saturday
    cast(strftime(date_val, '%w') as integer)                      as day_of_week,
    strftime(date_val, '%A')                                       as day_name,
    strftime(date_val, '%B')                                       as month_name,
    strftime(date_val, '%Y-%m')                                    as year_month,
    cast(strftime(date_val, '%w') as integer) in (0, 6)            as is_weekend
from spine
