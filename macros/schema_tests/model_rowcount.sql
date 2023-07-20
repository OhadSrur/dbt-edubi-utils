{% test model_rowcount(model, count, where_clause=None, test_direction='!=') %}
{# Use this model to test if the model equals to what is expected #}
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
WHERE row_count {{test_direction}} {{ count }}

{% endtest %}
