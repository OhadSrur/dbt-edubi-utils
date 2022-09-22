{% macro coalesce_most_recent_column(col_name,number_of_cols) %}
        coalesce(
{%- for i in range(2,number_of_cols+1)|reverse -%}
        {{- col_name }}_{{ i -}}{%- if not loop.last -%},{%- endif -%}
{%- endfor -%}
        ,0)
{%- endmacro -%}