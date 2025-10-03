{%- macro day_name_localized(date, short=True, language="default") -%}
    {{ adapter.dispatch("day_name_localized", "dbt_date")(date, short, language) }}
{%- endmacro -%}

{%- macro default__day_name_localized(date, short, language) -%}
    case
        {% for day_num in range(1, 8) %}  -- Similar to python, last argument is not included
            when {{ dbt_date.day_of_week(date) }} = {{ day_num }}
            then
                '{{ dbt_date.get_localized_datepart_names(language, "weekdays")["short" if short else "long"]["" ~ day_num] }}'
        {% endfor %}
    end
{%- endmacro -%}
