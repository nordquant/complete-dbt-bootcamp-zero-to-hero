{#
    If 1 January is on a Monday, Tuesday, Wednesday or Thursday, it is in W01. If it is on a Friday, it is part of W53
    of the previous year. If it is on a Saturday, it is part of the last week of the previous year which is numbered
    W52 in a common year and W53 in a leap year. If it is on a Sunday, it is part of W52 of the previous year.
    https://en.wikipedia.org/wiki/ISO_week_date

    Format: YYYY-Www
#}
{%- macro iso_year_week(date) -%}
    {{ adapter.dispatch("iso_year_week", "dbt_date")(date) }}
{%- endmacro -%}

{%- macro default__iso_year_week(date) -%}
    case
        when {{ dbt_date.iso_week_of_year(date) }} = 1
        then
            concat(
                {{ dbt_date.date_part("year", dbt_date.iso_week_end(date)) }},
                '-W',
                lpad(concat('',{{ dbt_date.iso_week_of_year(date) }}), 2, '0')
            )
        else
            concat(
                {{ dbt_date.date_part("year", dbt_date.iso_week_start(date)) }},
                '-W',
                lpad(concat('', {{ dbt_date.iso_week_of_year(date) }}), 2, '0')
            )
    end

{%- endmacro -%}

{%- macro trino__iso_year_week(date) -%}
    case
        when {{ dbt_date.iso_week_of_year(date) }} = 1
        then
            concat(
                cast(
                    {{ dbt_date.date_part("year", dbt_date.iso_week_end(date)) }}
                    as varchar
                ),
                '-W',
                lpad(cast({{ dbt_date.iso_week_of_year(date) }} as varchar), 2, '0')
            )
        else
            concat(
                cast(
                    {{ dbt_date.date_part("year", dbt_date.iso_week_start(date)) }}
                    as varchar
                ),
                '-W',
                lpad(cast({{ dbt_date.iso_week_of_year(date) }} as varchar), 2, '0')
            )
    end
{%- endmacro -%}
