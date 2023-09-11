{% macro convert_yes_no_to_bool(predicate) %}
        CASE WHEN UPPER({{ predicate }})in ('YES','Y') THEN TRUE
        WHEN UPPER({{ predicate }}) in ('NO','N') THEN FALSE
        ELSE null
        END
{%- endmacro %}