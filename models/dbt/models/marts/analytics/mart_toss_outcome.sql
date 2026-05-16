-- Grain: one row per match_type × season × toss_decision.
select
    match_type,
    season,
    toss_decision,
    count(*)                                                        as matches,
    sum(case when toss_winner = winner then 1 else 0 end)          as toss_winner_won,
    round(
        sum(case when toss_winner = winner then 1 else 0 end) * 100.0
        / nullif(count(*), 0),
        2
    )                                                               as toss_win_match_win_pct,
    sum(case when toss_decision = 'bat'   then 1 else 0 end)       as chose_bat,
    sum(case when toss_decision = 'field' then 1 else 0 end)       as chose_field,
    sum(
        case when toss_decision = 'bat' and toss_winner = winner
             then 1 else 0 end
    )                                                               as bat_first_and_won,
    sum(
        case when toss_decision = 'field' and toss_winner = winner
             then 1 else 0 end
    )                                                               as field_first_and_won
from {{ ref('stg_silver_matches') }}
where toss_winner is not null
  and toss_decision is not null
group by match_type, season, toss_decision
