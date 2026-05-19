{{ config(
    materialized='incremental',
    unique_key='match_id',
    on_schema_change='sync_all_columns'
) }}

select
    match_id,
    season,
    match_type,
    gender,
    match_date,
    team_a,
    team_b,
    venue,
    city,
    balls_per_over,
    limit_overs,
    event_name,
    event_number,
    toss_winner,
    toss_decision,
    winner,
    outcome_result,
    outcome_method,
    win_by_runs,
    win_by_wickets,
    win_by_innings,
    _snapshot_date
from {{ ref('stg_silver_matches') }}
{% if is_incremental() %}
where match_id in (
    select match_id from control.match_file_audit where gold_loaded_at is null
)
{% endif %}
