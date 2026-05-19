{{ config(
    materialized='incremental',
    unique_key='competition_name',
    on_schema_change='sync_all_columns'
) }}

-- depends_on: {{ ref('stg_silver_matches') }}

with competitions as (
    select * from {{ ref('stg_silver_competitions') }}
    {% if is_incremental() %}
    where competition_name in (
        select distinct event_name from {{ ref('stg_silver_matches') }}
        where match_id in (select match_id from control.match_file_audit where gold_loaded_at is null)
    )
    {% endif %}
),

deduped as (
    select competition_name
    from competitions
    qualify row_number() over (partition by competition_name order by _snapshot_date desc) = 1
)

select competition_name
from deduped
where competition_name is not null
