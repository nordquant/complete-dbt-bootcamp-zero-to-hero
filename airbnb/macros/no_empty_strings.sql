{# Here is my solution after removing most of the white spaces #}

{% macro no_empty_strings(model) %}
    {%- for col in adapter.get_columns_in_relation(model) -%}
        {%- if col.is_string() %}
            {{ col.name }} IS NOT NULL AND {{ col.name }} <> '' AND
        {%- endif %}
    {%- endfor %}
    TRUE
{% endmacro %}
