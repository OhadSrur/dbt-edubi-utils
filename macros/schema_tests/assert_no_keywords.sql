{% test assert_no_keywords(model, column_name) %}

with wordslist as(
    select keyword from {{ ref('local_keywords') }}
)

    select *

    from {{ model }}
    cross join
    wordslist
    where {{ column_name }} ilike '%' || keyword || '%'

{{ log("Running assert_no_keywords: " ~ model.name ~ ", column_name: " ~ column_name ) }}

{% endtest %}