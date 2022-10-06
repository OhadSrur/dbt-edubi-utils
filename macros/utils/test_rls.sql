{# This marcro is used for testing RLS, what it does it replaces the email address for the user #}
{% macro test_rls(col_name) %}
  {% if target.name == 'prod' %}
        {{col_name}}
  {%- else -%}
        coalesce((select new_upn
        FROM {{ ref('rls_dev') }}
        WHERE {{col_name}}=replace_upn),{{col_name}})
  {%- endif -%}
{% endmacro %}