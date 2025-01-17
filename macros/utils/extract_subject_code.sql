{# Extract subject_code by removing the year level from the assessment code #}
{% macro extract_subject_code(string_col,pattern_number=1) %}
        {% if pattern_number == 1 %}
        {# example: 10CH -> CH #}
        SUBSTRING({{string_col}} FROM '\d*(\D.*)')
        {% elif pattern_number == 2 %}
        {# example: 07CH -> CH #}
        right({{string_col}},2)
        {% endif %}
{%- endmacro %}