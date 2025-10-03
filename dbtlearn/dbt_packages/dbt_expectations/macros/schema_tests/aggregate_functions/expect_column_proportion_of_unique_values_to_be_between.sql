{% test expect_column_proportion_of_unique_values_to_be_between(model, column_name,
                                                            min_value=None,
                                                            max_value=None,
                                                            group_by=None,
                                                            row_condition=None,
                                                            strictly=False
                                                            ) %}
{% set expression %}
case 
  when count({{ column_name }}) = 0 then 1 -- Return 1 if division by zero
  else cast(count(distinct {{ column_name }}) as {{ dbt.type_float() }})/count({{ column_name }})
end
{% endset %}
{{ dbt_expectations.expression_between(model,
                                        expression=expression,
                                        min_value=min_value,
                                        max_value=max_value,
                                        group_by_columns=group_by,
                                        row_condition=row_condition,
                                        strictly=strictly
                                        ) }}

{% endtest %}
