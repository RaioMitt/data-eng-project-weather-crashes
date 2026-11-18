{# Only for the silver layer: use the schema name "silver" as-is (not public_silver).
   Every other model/seed keeps dbt's default behaviour. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name | trim == 'silver' -%}
        silver
    {%- else -%}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    {%- endif -%}
{%- endmacro %}
