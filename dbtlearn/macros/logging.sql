{% macro learn_logging() %}
    {{ log("Call your mom!") }}
    {{ log("Call your dad!", info=True) }} --> Logs to the screen, too
--  {{ log("Call your dad!", info=True) }} --> This will be put to the screen
    {# log("Call your dad!", info=True) #} --> This won't be executed
{% endmacro %}
