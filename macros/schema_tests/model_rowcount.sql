{% test model_rowcount(model, count, where_clause=None) %}

WITH source AS (

    SELECT *
    FROM {{ model }}

), counts AS (

    SELECT count(*) AS row_count
    FROM source
    {% if where_clause != None %}
    WHERE {{ where_clause }}
    {% endif %}

)

SELECT row_count
FROM counts
WHERE row_count != {{ count }}

{% endtest %}
