{# Extract course_code by removing the class number from the end of the class code #}
{% macro extract_course_code(string_col,pattern_number=1) %}
        {% if pattern_number == 1 %}
        {# example: 10CH.1 -> 10CH #}
                SPLIT_PART({{string_col}},'.',1)
        {% elif pattern_number == 2 %}
        {# example: 10CH1 -> 10CH #}
                SUBSTRING({{string_col}} FROM '^(\d+[A-Z]+)')
        {% endif %}
{%- endmacro %}