{# There are two layers of execution. 1. macro execution, 2. sql execution #}
{# The log() method is executed at macro execution layer #}
{# The SQL style comment "--" will not work in macro execution layer by jinja #}
{% macro learn_logging() %}
    {# log("Call your dad!", info=True) #}
{% endmacro %}
