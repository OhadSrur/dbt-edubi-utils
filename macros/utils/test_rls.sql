{# This marcro is used for testing RLS, what it does it replaces the email address for the user #}
{% macro test_rls(col_name) %}
  {% if target.name == 'prod' %}
        {{col_name}}
  {%- else -%}
        coalesce((select user_principal_name
        FROM {{ ref('rls_dev') }}
        WHERE {{col_name}}=replace_staff_email),{{col_name}})
  {%- endif -%}
{% endmacro %}