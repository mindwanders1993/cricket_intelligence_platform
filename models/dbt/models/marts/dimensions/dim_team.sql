{{ config(
    materialized='incremental',
    unique_key='team_name',
    on_schema_change='sync_all_columns'
) }}

-- depends_on: {{ ref('stg_silver_matches') }}

with teams as (
    select * from {{ ref('stg_silver_teams') }}
    {% if is_incremental() %}
    where team_name in (
        select distinct team_a from {{ ref('stg_silver_matches') }}
        where match_id in (select match_id from control.match_file_audit where gold_loaded_at is null)
        union
        select distinct team_b from {{ ref('stg_silver_matches') }}
        where match_id in (select match_id from control.match_file_audit where gold_loaded_at is null)
    )
    {% endif %}
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
