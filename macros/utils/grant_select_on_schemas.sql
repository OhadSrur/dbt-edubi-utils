{% macro grant_select_on_schemas(schemas, groups, fetch_result=True) %}
  {% set groups_csv = 'group ' ~  groups | join(', group ') %}
  {% for schema in schemas %}
    grant usage on schema "{{ schema }}" to {{ groups_csv }};
    grant select on all tables in schema "{{ schema }}" to {{ groups_csv }};
    alter default privileges in schema "{{ schema }}"
        grant select on tables to {{ groups_csv }};
  {% endfor %}
{% endmacro %}