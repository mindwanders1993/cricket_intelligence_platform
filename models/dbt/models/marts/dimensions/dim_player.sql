with persons as (
    select * from {{ ref('stg_silver_persons') }}
),

key_ids as (
    select
        identifier as person_id,
        max(case when source_system = 'cricinfo'  then source_identifier end) as cricinfo_id,
        max(case when source_system = 'espn'      then source_identifier end) as espn_id,
        max(case when source_system = 'cricbuzz'  then source_identifier end) as cricbuzz_id,
        max(case when source_system = 'wikidata'  then source_identifier end) as wikidata_id,
        max(case when source_system = 'twitter'   then source_identifier end) as twitter_handle,
        max(case when source_system = 'instagram' then source_identifier end) as instagram_handle
    from {{ ref('stg_silver_person_identifiers') }}
    group by identifier
),

-- Latest snapshot per person (people.csv re-ingested weekly)
deduped as (
    select *
    from persons
    qualify row_number() over (partition by person_id order by _snapshot_date desc) = 1
)

select
    d.person_id,
    d.name                                                        as full_name,
    d.unique_name,
    k.cricinfo_id,
    k.espn_id,
    k.cricbuzz_id,
    k.wikidata_id,
    k.twitter_handle,
    k.instagram_handle,
    d._snapshot_date,
    d._ingested_at,
    d._pipeline_run_id
from deduped d
left join key_ids k using (person_id)
