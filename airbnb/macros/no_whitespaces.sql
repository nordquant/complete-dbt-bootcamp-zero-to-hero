{% macro no_whitespaces(model) -%}

{%- set conditions = [] -%}

{%- for col in adapter.get_columns_in_relation(model) -%}
    {%- if col.is_string() -%}
        {%- do conditions.append(
            col.name ~ " IS NOT NULL AND TRIM(" ~ col.name ~ ") <> ''"
        ) -%}
    {%- endif -%}
{%- endfor -%}

select *
from {{ model }}
where {{ conditions | join(' AND ') }}

{%- endmacro %}     