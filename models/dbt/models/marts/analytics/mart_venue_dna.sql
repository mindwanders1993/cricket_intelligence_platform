-- Grain: one row per venue × match_type.
with innings_aggs as (
    select
        m.venue,
        m.match_type,
        count(distinct i.match_id)                                            as matches,
        round(avg(case when i.innings_number = 1 then i.total_runs end), 1) as avg_first_innings_score,
        round(avg(case when i.innings_number = 2 then i.total_runs end), 1) as avg_second_innings_score,
        round(sum(i.total_runs) * 6.0 / nullif(sum(i.legal_balls), 0), 2)   as overall_run_rate,
        round(sum(i.boundaries) * 100.0 / nullif(sum(i.legal_balls), 0), 2) as boundary_pct,
        round(sum(i.sixes) * 100.0 / nullif(sum(i.legal_balls), 0), 2)      as six_pct
    from {{ ref('fact_innings') }} i
    join {{ ref('dim_match') }} m using (match_id)
    where m.venue is not null
    group by m.venue, m.match_type
),

chasing as (
    select
        m.venue,
        m.match_type,
        count(*)                                                              as chasing_matches,
        sum(case when i.team = m.winner then 1 else 0 end)                   as chasing_wins
    from {{ ref('dim_match') }} m
    join {{ ref('stg_silver_innings') }} i
         on m.match_id = i.match_id and i.innings_number = 2
    where m.winner is not null
      and m.outcome_result = 'win'
      and m.venue is not null
    group by m.venue, m.match_type
)

select
    a.venue,
    a.match_type,
    a.matches,
    a.avg_first_innings_score,
    a.avg_second_innings_score,
    a.overall_run_rate,
    a.boundary_pct,
    a.six_pct,
    c.chasing_matches,
    c.chasing_wins,
    round(c.chasing_wins * 100.0 / nullif(c.chasing_matches, 0), 2)          as chasing_win_pct
from innings_aggs a
left join chasing c using (venue, match_type)
