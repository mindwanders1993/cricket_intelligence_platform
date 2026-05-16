with teams as (
    select * from {{ ref('stg_silver_teams') }}
),

deduped as (
    select team_name, team_type
    from teams
    qualify row_number() over (partition by team_name order by _snapshot_date desc) = 1
)

select
    team_name,
    team_type
from deduped
where team_name is not null
