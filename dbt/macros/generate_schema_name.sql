{# Use the custom schema name verbatim (silver / gold / seeds) instead of the
   default target_schema + '_' + custom prefixing, so the warehouse layers read
   cleanly. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
