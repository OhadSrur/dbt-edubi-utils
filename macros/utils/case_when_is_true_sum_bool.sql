{% macro case_when_is_true_sum_bool(boolean_measure, when_is_false=FALSE) %}
        {%- if not (when_is_false) %}sum({{boolean_measure}}::int){% else %}sum((not({{boolean_measure}}))::int){% endif %}
{%- endmacro %}