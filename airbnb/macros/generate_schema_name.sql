{% macro generate_schema_name(custom_schema_name, node) -%}

  {% set custom_schema_name_cleansed = custom_schema_name | trim | upper %}
  {% set target_schema_cleansed = target.schema | trim | upper %}

  {%- if custom_schema_name is none -%}
    {# No custom schema: always use target schema as-is (uppercased above) #}
    {{ target_schema_cleansed }}
  {%- else -%}
    {%- if target.name == 'prod' -%}
        {# Prod: use clean custom schema name only #}
        {{ custom_schema_name_cleansed }}
    {%- else -%}
      {# Staging / Dev / feature branches: prefix with personal/branch schema. #}
      {{ target_schema_cleansed }}_{{ custom_schema_name_cleansed }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}