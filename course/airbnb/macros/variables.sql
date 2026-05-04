{# 
  Macro: learn_variables
  Purpose: Demonstrates the two common ways to define and use variables in dbt:
    1. Jinja-scoped variables, defined locally with {% set %}.
    2. dbt project variables, accessed via the var() function and configured
       in dbt_project.yml (or overridden on the CLI with --vars).
#}
{% macro learn_variables() %}
  {# Jinja variable: only available within this macro's scope. #}
  {% set your_name_jinja = "Lei" %}

  {# Log the Jinja variable to the dbt run output (info=True prints at INFO level). #}
  {{ log("Hello " ~ your_name_jinja, info=True) }}

  {# dbt variable: resolved from project vars or the --vars CLI flag at runtime. #}
  {{ log("Hello dbt user " ~ var("user_name", "NO USERNAME IS SET!!") ~ "!", info=True) }}
{% endmacro %}
