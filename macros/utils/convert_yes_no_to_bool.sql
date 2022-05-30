{% macro convert_yes_no_to_bool(predicate) %}
        CASE WHEN UPPER({{ predicate }}) = 'YES' THEN TRUE
        WHEN UPPER({{ predicate }}) = 'NO' THEN FALSE
        ELSE null
        END
{%- endmacro %}