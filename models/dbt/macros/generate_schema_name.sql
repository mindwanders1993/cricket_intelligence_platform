-- Override dbt's default schema-name generation so `+schema: gold` in
-- dbt_project.yml produces `gold` (not `main_gold`).
-- Without this, dbt prepends target.schema (`main` for dbt-duckdb default),
-- producing `main_gold`, `main_staging` — which downstream consumers
-- (Superset, FastAPI, AI assistant) would not find under the expected names.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
