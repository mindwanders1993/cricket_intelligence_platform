with competitions as (
    select * from {{ ref('stg_silver_competitions') }}
),

deduped as (
    select competition_name
    from competitions
    qualify row_number() over (partition by competition_name order by _snapshot_date desc) = 1
)

select competition_name
from deduped
where competition_name is not null
