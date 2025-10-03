{%- macro day_name(date, short=True, language="default") -%}
    {{ adapter.dispatch("day_name", "dbt_date")(date, short, language) }}
{%- endmacro -%}

{%- macro default__day_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- set f = "Dy" if short else "Day" -%} to_char({{ date }}, '{{ f }}')
    {%- else -%} {{ dbt_date.day_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro -%}

{%- macro snowflake__day_name(date, short, language) -%}
    {%- if language == "default" -%}
        {{ dbt_date.day_name_localized(date, short, "en") }}
    {%- else -%} {{ dbt_date.day_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro -%}

{%- macro bigquery__day_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- set f = "%a" if short else "%A" -%}
        format_date('{{ f }}', cast({{ date }} as date))
    {%- else -%} {{ dbt_date.day_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro -%}

{%- macro postgres__day_name(date, short, language) -%}
    {%- if language == "default" -%}
        {# FM = Fill mode, which suppresses padding blanks #}
        {%- set f = "FMDy" if short else "FMDay" -%} to_char({{ date }}, '{{ f }}')
    {%- else -%} {{ dbt_date.day_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro -%}

{%- macro duckdb__day_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- if short -%} substr(dayname({{ date }}), 1, 3)
        {%- else -%} dayname({{ date }})
        {%- endif -%}
    {%- else -%} {{ dbt_date.day_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro -%}

{%- macro spark__day_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- set f = "E" if short else "EEEE" -%} date_format({{ date }}, '{{ f }}')
    {%- else -%} {{ dbt_date.day_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro -%}

{%- macro trino__day_name(date, short, language) -%}
    {%- if language == "default" -%}
        {%- set f = "a" if short else "W" -%} date_format({{ date }}, '%{{ f }}')
    {%- else -%} {{ dbt_date.day_name_localized(date, short, language) }}
    {%- endif -%}
{%- endmacro -%}
