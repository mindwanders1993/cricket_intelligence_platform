with names as (
    select
        player_name    as cricsheet_name,
        min(person_id) as person_id
    from {{ ref('stg_silver_match_players') }}
    where player_name is not null
    group by player_name
),

aliases as (
    select cricsheet_name, display_name
    from {{ ref('player_aliases') }}
),

enriched as (
    select
        n.cricsheet_name,
        coalesce(a.display_name, p.full_name,   n.cricsheet_name) as display_name,
        coalesce(p.unique_name,                  n.cricsheet_name) as unique_name,
        n.person_id
    from names n
    left join aliases                 a using (cricsheet_name)
    left join {{ ref('dim_player') }} p using (person_id)
)

select * from enriched
order by display_name
