{% macro generate_schema_name(custom_schema_name, node) -%}

  {% set custom_schema_name_cleansed = custom_schema_name | trim | upper %}
  {% set target_schema_cleansed = target.schema | trim | upper %}

  {%- if custom_schema_name is none -%}

    {# No custom schema: always use target schema as-is (uppercased above) #}
    {{ target_schema_cleansed }}

  {%- elif target.name == 'prod' -%}
    {# Prod: use clean custom schema name only #}
    {{ custom_schema_name_cleansed }}

  {%- elif target.name == 'staging' -%}

    {# Staging: prefix with STAGING_ to keep it distinct #}
    STAGING_{{ custom_schema_name_cleansed }}

  {%- else -%}

    {# Dev / feature branches: prefix with personal/branch schema. will start with `dbt_` (see profiles) #}
    {{ target_schema_cleansed }}_{{ custom_schema_name_cleansed }}

  {%- endif -%}

{%- endmacro %}