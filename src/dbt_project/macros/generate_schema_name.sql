{# Silver and gold: use the schema name exactly as given (not public_silver / public_gold).
   Every other model/seed keeps dbt's default behaviour. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if (custom_schema_name | trim) in ['silver', 'gold'] -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    {%- endif -%}
{%- endmacro %}
