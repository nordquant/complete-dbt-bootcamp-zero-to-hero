{% macro logging_and_variables() %}
    {{ log("Call your mom!") }}
    {{ log("Call your mom!", info=True) }}
    -- {{ log("This shouldn't be printed", info=True) }}
    {# log("This really shouldn't be printed", info=True) #}

    {% set your_name = "Zoltan" %}
    {{ log("Hello " ~ your_name ~ ", call your Mom!", info=true) }}


    {# log("Now, hello " ~ var("user_name") ~ "!", info=true) #}
    {{ log("Now, hello " ~ var("user_name", "...mmmhmmm...") ~ "!", info=true) }}

    {% if var("user_name", False) %}
        {{ log("User name defined:" ~ var("user_name", False), info=True) }}
    {% else %}
        {{ log("User name not defined", info=True) }}
    {% endif %}
{% endmacro %}