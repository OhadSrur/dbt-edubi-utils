{# return the current date #}
{% macro current_date() %}
        (now() AT Time Zone '{{ var('client_timezone') }}')::date
{% endmacro %}