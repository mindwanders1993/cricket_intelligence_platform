with venues as (
    select * from {{ ref('stg_silver_venues') }}
),

deduped as (
    select venue_name, city
    from venues
    qualify row_number() over (partition by venue_name order by _snapshot_date desc) = 1
)

select
    venue_name,
    city
from deduped
where venue_name is not null
