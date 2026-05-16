-- Grain: one row per bowler × match_type × season.
-- Only counts bowler-credit dismissals (caught, bowled, lbw, stumped,
-- caught & bowled, hit wicket) — run-outs and retired-hurt are excluded.
with innings_spell as (
    select
        bowler_person_id                                            as person_id,
        bowler                                                      as player_name,
        match_id,
        innings_number,
        match_type,
        season,
        sum(case when is_legal_ball     then 1 else 0 end)         as legal_balls,
        sum(runs_total)                                             as runs_conceded,
        sum(case when is_bowler_wicket  then 1 else 0 end)         as wickets_taken
    from {{ ref('fact_delivery') }}
    where bowler_person_id is not null
    group by
        bowler_person_id, bowler, match_id, innings_number, match_type, season
)

select
    person_id,
    player_name,
    match_type,
    season,
    count(*)                                                        as innings_bowled,
    sum(legal_balls)                                                as total_balls,
    round(sum(legal_balls) / 6.0, 1)                               as overs_bowled,
    sum(runs_conceded)                                              as total_runs,
    sum(wickets_taken)                                              as total_wickets,
    max(wickets_taken)                                              as best_innings_wickets,
    round(sum(runs_conceded) * 6.0 / nullif(sum(legal_balls), 0), 2) as economy_rate,
    round(
        sum(runs_conceded) * 1.0 / nullif(sum(wickets_taken), 0),
        2
    )                                                               as bowling_average,
    round(
        sum(legal_balls) * 1.0 / nullif(sum(wickets_taken), 0),
        2
    )                                                               as bowling_strike_rate
from innings_spell
group by person_id, player_name, match_type, season
