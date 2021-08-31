{% test assert_no_keywords(model, column_name) %}
{{ 
    config(
    enabled = target.name in ['dev'])
}}

with wordslist as(
    select keyword from "OSRUR_test"."local_keywords"
)

    select *

    from {{ model }}
    cross join
    wordslist
    where {{ column_name }} ilike '%' || keyword || '%'

{{ log("Running assert_no_keywords: " ~ model.name ~ ", column_name: " ~ column_name ) }}

{% endtest %}