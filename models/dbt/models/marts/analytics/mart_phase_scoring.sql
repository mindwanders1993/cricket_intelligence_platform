-- Grain: one row per match_type × season × phase.
-- Phase boundaries differ by format:
--   T20 / IT20:   powerplay 0-5, middle 6-14, death 15+
--   ODI / ODM:    powerplay 0-9, middle 10-39, death 40+
-- Tests have no phase concept — filtered out.
with phased as (
    select
        match_type,
        season,
        case
            when match_type in ('T20', 'IT20', 'T20B') and over_number < 6  then 'powerplay'
            when match_type in ('T20', 'IT20', 'T20B') and over_number < 15 then 'middle'
            when match_type in ('T20', 'IT20', 'T20B')                       then 'death'
            when match_type in ('ODI', 'ODM') and over_number < 10           then 'powerplay'
            when match_type in ('ODI', 'ODM') and over_number < 40           then 'middle'
            when match_type in ('ODI', 'ODM')                                then 'death'
        end                                                         as phase,
        runs_total,
        runs_batter,
        extra_wides,
        is_wicket,
        is_legal_ball,
        is_dot_ball
    from {{ ref('fact_delivery') }}
)

select
    match_type,
    season,
    phase,
    count(*)                                                        as total_deliveries,
    sum(case when is_legal_ball then 1 else 0 end)                 as legal_balls,
    sum(runs_total)                                                 as total_runs,
    round(
        sum(runs_total) * 6.0 / nullif(sum(case when is_legal_ball then 1 else 0 end), 0),
        2
    )                                                               as run_rate,
    round(
        sum(case when runs_batter in (4, 6) then 1 else 0 end) * 100.0
        / nullif(sum(case when is_legal_ball then 1 else 0 end), 0),
        2
    )                                                               as boundary_pct,
    round(
        sum(case when is_dot_ball then 1 else 0 end) * 100.0
        / nullif(sum(case when is_legal_ball then 1 else 0 end), 0),
        2
    )                                                               as dot_ball_pct,
    sum(case when is_wicket then 1 else 0 end)                     as wickets,
    round(
        sum(case when is_wicket then 1 else 0 end) * 100.0
        / nullif(sum(case when is_legal_ball then 1 else 0 end), 0),
        2
    )                                                               as wicket_pct
from phased
where phase is not null
group by match_type, season, phase
