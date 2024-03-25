{# Use this macro to remove HTML tags from column #}
{% macro extract_data_from_html_tags(column_name,remove_first_occurance=FALSE) %}
    {% if remove_first_occurance %}
        regexp_replace({{ column_name }},'<[^>]+>','','')
    {% else %}
        regexp_replace({{ column_name }},'<[^>]+>','','g')
    {% endif %}
{% endmacro %}