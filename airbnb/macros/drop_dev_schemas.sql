{% macro drop_dev_schemas() %}

  {# Use the resolved target schema as the prefix (e.g. DBT_<env_name> on the dev target) #}
  {% set prefix = target.schema | upper %}

  {# Safety guard: only drop dev schemas (prefixed DBT_), never PROD/STAGING #}
  {% if not prefix.startswith('DBT_') %}
    {{ exceptions.raise_compiler_error("Refusing to drop schemas: target.schema (" ~ prefix ~ ") is not a dev schema (must start with DBT_)") }}
  {% endif %}

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
