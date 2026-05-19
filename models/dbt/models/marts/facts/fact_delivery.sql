{{ config(
    materialized='incremental',
    unique_key=['match_id', 'innings_number', 'over_number', 'delivery_number'],
    on_schema_change='sync_all_columns'
) }}

-- Grain: one row per ball bowled.
-- Resolves batter/bowler names to person_ids via match_players, and joins
-- the wickets table on delivery key to expose dismissal_kind + player_out.
with deliveries as (
    select * from {{ ref('stg_silver_deliveries') }}
),

match_context as (
    select match_id, match_type, season, gender
    from {{ ref('dim_match') }}
),

-- Deduplicate match_players by (match_id, player_name) — same name can
-- appear twice in a match if both teams have a player with the same name.
-- For fact_delivery we just need *a* person_id mapping, so we pick the
-- first deterministically.
players as (
    select match_id, player_name, person_id
    from {{ ref('stg_silver_match_players') }}
    qualify row_number() over (
        partition by match_id, player_name
        order by person_id nulls last
    ) = 1
),

-- Deduplicate wickets to one row per delivery. Multi-wicket deliveries
-- (e.g., bowler-credited dismissal + run-out of non-striker on the same
-- ball) would otherwise inflate the fact_delivery grain via LEFT JOIN.
-- Prefer the bowler-credited wicket so the dismissal_kind/player_out
-- shown on the delivery row matches the standard scoring convention.
wickets as (
    select
        match_id,
        innings_number,
        over_number,
        delivery_number,
        kind            as dismissal_kind,
        player_out
    from {{ ref('stg_silver_wickets') }}
    qualify row_number() over (
        partition by match_id, innings_number, over_number, delivery_number
        order by case
            when kind in ('caught', 'bowled', 'lbw', 'stumped',
                          'caught and bowled', 'hit wicket') then 0
            else 1
        end, kind
    ) = 1
)

select
    d.match_id,
    d.innings_number,
    d.over_number,
    d.delivery_number,

    -- Batter
    d.batter,
    bp.person_id                                                    as batter_person_id,

    -- Bowler
    d.bowler,
    bwp.person_id                                                   as bowler_person_id,

    d.non_striker,

    -- Run components
    d.runs_batter,
    d.runs_extras,
    d.runs_total,
    d.runs_non_boundary,

    -- Extra types (null = not applicable)
    d.extra_wides,
    d.extra_noballs,
    d.extra_byes,
    d.extra_legbyes,
    d.extra_penalty,

    -- Wicket flags + details
    d.is_wicket,
    w.dismissal_kind,
    w.player_out,
    -- Bowler is credited only for these dismissal kinds. Run-outs, retired
    -- hurt/out, obstructing the field, timed out, etc. go against the batter
    -- but are NOT counted in the bowler's wicket tally.
    case
        when w.dismissal_kind in (
            'caught', 'bowled', 'lbw', 'stumped',
            'caught and bowled', 'hit wicket'
        ) then true
        else false
    end                                                             as is_bowler_wicket,

    -- Ball classification flags
    case when d.extra_wides is null and d.extra_noballs is null
         then true else false end                                   as is_legal_ball,
    case when d.runs_batter = 0 and d.extra_wides is null
         then true else false end                                   as is_dot_ball,

    -- Match context (denormalised for query convenience)
    mc.match_type,
    mc.season,
    mc.gender,

    d._snapshot_date
from deliveries d
join match_context mc using (match_id)
left join players  bp  on d.match_id = bp.match_id  and d.batter = bp.player_name
left join players  bwp on d.match_id = bwp.match_id and d.bowler = bwp.player_name
left join wickets  w   on d.match_id = w.match_id
                       and d.innings_number  = w.innings_number
                       and d.over_number     = w.over_number
                       and d.delivery_number = w.delivery_number

{% if is_incremental() %}
WHERE match_id IN (
  SELECT match_id FROM control.match_file_audit
  WHERE gold_loaded_at IS NULL
)
{% endif %}
