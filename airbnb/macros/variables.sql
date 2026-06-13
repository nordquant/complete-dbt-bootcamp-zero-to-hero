{% macro learn_variables() %}
    
    {% set your_name_jinja = "Patrick" %} 
    {{ log("Hello " ~ your_name_jinja, info=True ) }}

    {{ log("hello dbt user " ~ var("user_name", "No username is set!!") ~ "!", info=True) }}
{% endmacro %}