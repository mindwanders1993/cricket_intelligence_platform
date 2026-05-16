-- Grain: one row per innings per match.
with innings as (
    select * from {{ ref('stg_silver_innings') }}
),

match_context as (
    select match_id, match_type, season, gender, venue
    from {{ ref('dim_match') }}
),

-- Aggregate deliveries to get innings totals
delivery_totals as (
    select
        match_id,
        innings_number,
        sum(runs_total)                                             as total_runs,
        sum(case when is_legal_ball then 1 else 0 end)             as legal_balls,
        sum(case when is_wicket then 1 else 0 end)                 as wickets_fallen,
        sum(case when runs_batter in (4, 6) then 1 else 0 end)     as boundaries,
        sum(case when runs_batter = 6 then 1 else 0 end)           as sixes
    from {{ ref('fact_delivery') }}
    group by match_id, innings_number
)

select
    i.match_id,
    i.innings_number,
    i.team,
    i.super_over,
    i.declared,
    i.forfeited,
    i.target_runs,
    i.target_overs,
    mc.match_type,
    mc.season,
    mc.gender,
    mc.venue,
    dt.total_runs,
    dt.legal_balls,
    round(dt.legal_balls / 6.0, 1)                                as overs_faced,
    dt.wickets_fallen,
    dt.boundaries,
    dt.sixes,
    round(dt.total_runs * 6.0 / nullif(dt.legal_balls, 0), 2)    as run_rate,
    i._snapshot_date
from innings i
join match_context mc using (match_id)
left join delivery_totals dt using (match_id, innings_number)
