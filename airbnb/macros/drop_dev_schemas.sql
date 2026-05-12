{% macro drop_dev_schemas() %}

  {# Get env name from env var, trim and uppercase #}
  {% set env_name = env_var('DBT_ENV_NAME') | trim | upper %}

  {# Safety guard: never run in prod or staging #}
  {% if env_name in ('PROD', 'STAGING', '') %}
    {{ exceptions.raise_compiler_error("Cannot run in PROD or STAGING or with empty DBT_ENV_NAME!") }}
  {% endif %}

  {# Build the schema prefix #}
  {% set prefix = 'DBT_' ~ env_name %}
  {{ log(" * drop_dev_schemas: Looking for schemas with prefix:  " ~ prefix, info=True) }}

  {# Find all schemas matching the prefix #}
  {% set results = run_query(
    "SELECT schema_name
     FROM information_schema.schemata
     WHERE schema_name ILIKE '" ~ prefix ~ "%'"
  ) %}

  {# Drop each matching schema #}
  {% if execute %}
    {% set schemas = results.columns[0].values() %}

    {% if schemas | length == 0 %}
      {{ log(" * drop_dev_schemas: No schemas found matching prefix: " ~ prefix, info=True) }}
    {% else %}
      {{ log(" * drop_dev_schemas: Found " ~ schemas | length ~ " schema(s) to drop:", info=True) }}
      {% for schema in schemas %}
        {{ log(" * drop_dev_schemas: Dropping: " ~ schema, info=True) }}
        {% do run_query("DROP SCHEMA IF EXISTS " ~ schema ~ " CASCADE") %}
      {% endfor %}
    {% endif %}

  {% endif %}

{% endmacro %}
