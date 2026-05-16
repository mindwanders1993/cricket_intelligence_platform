select * from {{ source('silver', 'person_identifiers') }}
