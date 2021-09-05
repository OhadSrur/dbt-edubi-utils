{% macro get_max_valid_date(table_name,date_column,where_clause=None) %}
    (
    select
        max({{date_column}})::date  as max_date

    from {{ ref(table_name) }}
    where   {{date_column}}>'1900-01-01'
            and {{date_column}}<'9999-01-01'
    {% if where_clause != None -%}
            and {{where_clause}}
    {% endif -%}
    )
{% endmacro %}