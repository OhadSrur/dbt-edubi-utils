{# return the current year number #}
{% macro current_year() %}
        extract(year from current_date)
{% endmacro %}