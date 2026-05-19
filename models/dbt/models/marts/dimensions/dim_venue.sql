{{ config(
    materialized='incremental',
    unique_key='venue_name',
    on_schema_change='sync_all_columns'
) }}

-- depends_on: {{ ref('stg_silver_matches') }}

with venues as (
    select * from {{ ref('stg_silver_venues') }}
    {% if is_incremental() %}
    where venue_name in (
        select distinct venue from {{ ref('stg_silver_matches') }}
        where match_id in (select match_id from control.match_file_audit where gold_loaded_at is null)
    )
    {% endif %}
),

deduped as (
    select venue_name, city
    from venues
    qualify row_number() over (partition by venue_name order by _snapshot_date desc) = 1
)

select
    venue_name,
    city
from deduped
where venue_name is not null
