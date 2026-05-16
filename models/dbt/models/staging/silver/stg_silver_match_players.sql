select * from {{ source('silver', 'match_players') }}
