{%- macro month_name(date, short=True, language="default") -%}
    {{ adapter.dispatch("month_name", "dbt_date")(date, short, language) }}
{%- endmacro %}

{%- macro default__month_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- set f = "MON" if short else "MONTH" -%} to_char({{ date }}, '{{ f }}')
    {%- else -%} {{ dbt_date.month_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro %}

{%- macro bigquery__month_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- set f = "%b" if short else "%B" -%}
        format_date('{{ f }}', cast({{ date }} as date))
    {%- else -%} {{ dbt_date.month_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro %}

{%- macro snowflake__month_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- set f = "MON" if short else "MMMM" -%} to_char({{ date }}, '{{ f }}')
    {%- else -%} {{ dbt_date.month_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro %}

{%- macro postgres__month_name(date, short, language) -%}
    {%- if language == "default" -%}
        {# FM = Fill mode, which suppresses padding blanks #}
        {%- set f = "FMMon" if short else "FMMonth" -%} to_char({{ date }}, '{{ f }}')
    {%- else -%} {{ dbt_date.month_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro %}

{%- macro duckdb__month_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- if short -%} substr(monthname({{ date }}), 1, 3)
        {%- else -%} monthname({{ date }})
        {%- endif -%}
    {%- else -%} {{ dbt_date.month_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro %}

{%- macro spark__month_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- set f = "MMM" if short else "MMMM" -%} date_format({{ date }}, '{{ f }}')
    {%- else -%} {{ dbt_date.month_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro %}

{%- macro trino__month_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- set f = "b" if short else "M" -%} date_format({{ date }}, '%{{ f }}')
    {%- else -%} {{ dbt_date.month_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro %}
