{% test is_constant(model, column_name, group_by_columns = []) %}

select
{% if group_by_columns|length() > 0 %}
    {% set select_gb_cols = group_by_columns|join(' , ') %}
    {% set groupby_gb_cols = 'group by ' + group_by_columns|join(',') %}
    {{ select_gb_cols }},
{% endif %}

count(distinct {{ column_name }}) as distinct_count

from {{ model }} {{ groupby_gb_cols }}

/* if count distinct column is greater than 1, it means the value is not constant
so we should return it to raise an error. */
having count(distinct {{ column_name }}) > 1

{% endtest %}