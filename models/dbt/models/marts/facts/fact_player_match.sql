{{ config(
    materialized='incremental',
    unique_key=['match_id', 'player_name'],
    on_schema_change='sync_all_columns'
) }}

-- Grain: one row per player per match (batting + bowling summary).
with players as (
    select * from {{ ref('stg_silver_match_players') }}
),

match_context as (
    select match_id, match_type, season, gender, venue
    from {{ ref('dim_match') }}
),

batting as (
    select
        match_id,
        batter_person_id                                            as person_id,
        sum(runs_batter)                                            as runs_scored,
        sum(case when is_legal_ball then 1 else 0 end)             as balls_faced,
        sum(case when runs_batter = 4 then 1 else 0 end)           as fours,
        sum(case when runs_batter = 6 then 1 else 0 end)           as sixes,
        max(case when is_wicket then true else false end)           as was_dismissed
    from {{ ref('fact_delivery') }}
    where batter_person_id is not null
    group by match_id, batter_person_id
),

bowling as (
    select
        match_id,
        bowler_person_id                                            as person_id,
        sum(case when is_legal_ball     then 1 else 0 end)         as balls_bowled,
        sum(runs_total)                                             as runs_conceded,
        sum(case when is_bowler_wicket  then 1 else 0 end)         as wickets_taken
    from {{ ref('fact_delivery') }}
    where bowler_person_id is not null
    group by match_id, bowler_person_id
)

select
    p.match_id,
    p.person_id,
    p.player_name,
    p.team,
    mc.match_type,
    mc.season,
    mc.gender,
    mc.venue,

    -- Batting
    coalesce(b.runs_scored, 0)                                      as runs_scored,
    coalesce(b.balls_faced, 0)                                      as balls_faced,
    coalesce(b.fours, 0)                                            as fours,
    coalesce(b.sixes, 0)                                            as sixes,
    coalesce(b.was_dismissed, false)                                as was_dismissed,
    round(coalesce(b.runs_scored, 0) * 100.0
          / nullif(b.balls_faced, 0), 2)                            as batting_strike_rate,

    -- Bowling
    coalesce(bw.balls_bowled, 0)                                    as balls_bowled,
    round(coalesce(bw.balls_bowled, 0) / 6.0, 1)                   as overs_bowled,
    coalesce(bw.runs_conceded, 0)                                   as runs_conceded,
    coalesce(bw.wickets_taken, 0)                                   as wickets_taken,
    round(coalesce(bw.runs_conceded, 0) * 6.0
          / nullif(bw.balls_bowled, 0), 2)                          as economy_rate,

    p._snapshot_date
from players p
join match_context mc using (match_id)
left join batting   b  using (match_id, person_id)
left join bowling   bw using (match_id, person_id)

{% if is_incremental() %}
WHERE match_id IN (
  SELECT match_id FROM control.match_file_audit
  WHERE gold_loaded_at IS NULL
)
{% endif %}
