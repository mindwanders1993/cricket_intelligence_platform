-- Grain: one row per (match_id, player_of_match name).
-- Bridge table that explodes the player_of_match array. Tied matches with
-- multiple MOTM (EC-006) get one row per recipient. QUALIFY guards against
-- duplicate names within the same source array (data-quality artifact in source JSON).
with exploded as (
    select
        match_id,
        unnest(player_of_match) as player_name,
        _snapshot_date
    from {{ ref('stg_silver_matches') }}
    where player_of_match is not null
)
select
    match_id,
    player_name,
    _snapshot_date
from exploded
qualify row_number() over (partition by match_id, player_name order by _snapshot_date) = 1
