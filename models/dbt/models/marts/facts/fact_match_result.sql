-- Grain: one row per match.
-- Extends dim_match with two derived columns: toss_winner_won, loser.
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
    event_name,
    toss_winner,
    toss_decision,
    winner,
    outcome_result,
    outcome_method,
    win_by_runs,
    win_by_wickets,
    win_by_innings,
    player_of_match,
    -- NULL propagates naturally if either side is unknown.
    toss_winner = winner                                            as toss_winner_won,
    case
        when winner = team_a then team_b
        when winner = team_b then team_a
    end                                                             as loser,
    _snapshot_date
from {{ ref('dim_match') }}
