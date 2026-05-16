-- Grain: one row per player × match_type × season.
with innings_scores as (
    select
        batter_person_id                                            as person_id,
        batter                                                      as player_name,
        match_id,
        innings_number,
        match_type,
        season,
        sum(runs_batter)                                            as innings_runs,
        sum(case when is_legal_ball then 1 else 0 end)             as balls_faced,
        sum(case when runs_batter = 4  then 1 else 0 end)          as fours,
        sum(case when runs_batter = 6  then 1 else 0 end)          as sixes,
        max(case when is_wicket then true else false end)          as was_dismissed
    from {{ ref('fact_delivery') }}
    where batter_person_id is not null
    group by
        batter_person_id, batter, match_id, innings_number, match_type, season
)

select
    person_id,
    player_name,
    match_type,
    season,
    count(*)                                                        as innings,
    sum(innings_runs)                                               as total_runs,
    sum(balls_faced)                                                as total_balls,
    sum(fours)                                                      as total_fours,
    sum(sixes)                                                      as total_sixes,
    max(innings_runs)                                               as highest_score,
    sum(case when innings_runs >= 50  then 1 else 0 end)           as fifties,
    sum(case when innings_runs >= 100 then 1 else 0 end)           as hundreds,
    sum(case when was_dismissed then 1 else 0 end)                 as dismissals,
    round(
        sum(innings_runs) * 1.0
        / nullif(sum(case when was_dismissed then 1 else 0 end), 0),
        2
    )                                                               as batting_average,
    round(sum(innings_runs) * 100.0 / nullif(sum(balls_faced), 0), 2) as strike_rate
from innings_scores
group by person_id, player_name, match_type, season
