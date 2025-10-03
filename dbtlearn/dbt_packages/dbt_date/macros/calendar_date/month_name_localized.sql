{%- macro month_name_localized(date, short=True, language="default") -%}
    {{ adapter.dispatch("month_name_localized", "dbt_date")(date, short, language) }}
{%- endmacro -%}

{%- macro default__month_name_localized(date, short, language) -%}
    case
        {% for month_num in range(1, 12) %}
            when {{ dbt_date.date_part("month", date) }} = {{ month_num }}
            then
                '{{ dbt_date.get_localized_datepart_names(language, "months")["short" if short else "long"]["" ~ month_num] }}'
        {% endfor %}
    end
{%- endmacro -%}
