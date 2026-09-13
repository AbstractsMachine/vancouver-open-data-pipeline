{#
  Dataset naming. Models declare schema='ca_staging' / 'ca_analytics' /
  'ca_marts' / 'ca_seeds'; they land in <target.schema>_staging,
  <target.schema>_analytics, <target.schema>_marts, <target.schema>_seeds
  (with DBT_SCHEMA=vancouver: vancouver_staging, vancouver_analytics, …).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim | replace('ca_', '', 1) }}
    {%- endif -%}
{%- endmacro %}
