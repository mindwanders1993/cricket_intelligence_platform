-- Grain assertion: (match_id, player_name) must be unique in fact_player_of_match.
-- Returns rows iff a duplicate exists.
select
    match_id,
    player_name,
    count(*) as n
from {{ ref('fact_player_of_match') }}
group by 1, 2
having count(*) > 1
