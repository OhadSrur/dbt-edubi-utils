{# This macro sums columns with the same column name #}
{% macro sum_columns(col_name,number_of_cols,coalesce_cols = true) %}
{%- for i in range(number_of_cols) -%}
{%- if coalesce_cols -%}
        coalesce({{- col_name }}_{{ loop.index -}},0){%- if not loop.last -%}+{%- endif -%}
{%- else -%}
        {{- col_name }}_{{ loop.index -}}{%- if not loop.last -%}+{%- endif -%}
{% endif %}
{%- endfor -%}
{% endmacro %}