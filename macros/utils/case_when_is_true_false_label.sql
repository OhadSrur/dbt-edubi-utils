{% macro case_when_is_true_false_label(predicate, true_label, false_label, when_is_not_true=FALSE) %}
        CASE WHEN {%- if not (when_is_not_true) %} {{ predicate }} {% else %} not ({{ predicate }}) {% endif %}
        THEN '{{ true_label }}'
        ELSE '{{ false_label }}'
        END
{%- endmacro %}