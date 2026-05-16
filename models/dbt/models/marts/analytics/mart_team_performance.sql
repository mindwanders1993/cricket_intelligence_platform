-- Grain: one row per team × match_type × season.
with team_matches as (
    select match_id, match_type, season, team_a as team, winner, outcome_result, toss_winner
    from {{ ref('stg_silver_matches') }}
    union all
    select match_id, match_type, season, team_b as team, winner, outcome_result, toss_winner
    from {{ ref('stg_silver_matches') }}
)

select
    team,
    match_type,
    season,
    count(*)                                                                 as matches_played,
    sum(case when winner = team                                  then 1 else 0 end) as wins,
    sum(case when winner is not null and winner != team
             and outcome_result not in ('tie', 'no result')     then 1 else 0 end) as losses,
    sum(case when outcome_result = 'tie'                         then 1 else 0 end) as ties,
    sum(case when outcome_result = 'no result'                   then 1 else 0 end) as no_results,
    sum(case when toss_winner = team                             then 1 else 0 end) as toss_wins,
    round(
        sum(case when winner = team then 1 else 0 end) * 100.0
        / nullif(count(*), 0),
        2
    )                                                                        as win_pct
from team_matches
where team is not null
group by team, match_type, season
