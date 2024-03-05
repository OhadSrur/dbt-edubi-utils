{# return the current date #}
{% macro current_date() %}
        current_date AT Time Zone '{{ var('client_timezone') }}'
{% endmacro %}