{% test unique_column_group(model, col_group, col_check_dup, condition=None) %}

WITH source AS (

    SELECT {{ col_group }},
        count({{ col_check_dup }})
    FROM {{ model }}
    {% if condition != None %}
    WHERE {{ condition }}
    {% endif %}
    group by 1
    having count({{ col_check_dup }})>1

)

SELECT *
FROM source

{{ log("Running unique_column_group: " ~ model.name ~ ", column_group: " ~ col_group ~ ", col_check_dup:" ~ col_check_dup ~ ", condition:" ~ condition) }}

{% endtest %}