-- Grain: one row per batter × bowler × match_type (minimum 6 balls faced).
-- Powers the "batter vs bowler" matchup analysis view.
select
    batter_person_id,
    batter                                                          as batter_name,
    bowler_person_id,
    bowler                                                          as bowler_name,
    match_type,
    count(*)                                                        as balls_faced,
    sum(case when is_legal_ball then 1 else 0 end)                 as legal_balls,
    sum(runs_batter)                                                as runs_scored,
    sum(case when is_dot_ball   then 1 else 0 end)                 as dot_balls,
    sum(case when runs_batter = 4 then 1 else 0 end)               as fours,
    sum(case when runs_batter = 6 then 1 else 0 end)               as sixes,
    sum(case when is_wicket      then 1 else 0 end)                as dismissals,
    round(
        sum(runs_batter) * 100.0
        / nullif(sum(case when is_legal_ball then 1 else 0 end), 0),
        2
    )                                                               as strike_rate,
    round(
        sum(case when is_dot_ball then 1 else 0 end) * 100.0
        / nullif(sum(case when is_legal_ball then 1 else 0 end), 0),
        2
    )                                                               as dot_ball_pct
from {{ ref('fact_delivery') }}
where batter_person_id is not null
  and bowler_person_id is not null
group by batter_person_id, batter, bowler_person_id, bowler, match_type
having count(*) >= 6
