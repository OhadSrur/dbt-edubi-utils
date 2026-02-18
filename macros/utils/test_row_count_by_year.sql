{% test row_count_by_year(model, year_counts, threshold=1000) %}

{#
    Checks that the actual row count for each academic_year in the model is within
    `threshold` rows of the expected count.

    Args:
        year_counts: list of dicts with keys:
            - year     (int)  : the academic year
            - expected (int)  : the expected row count for that year
        threshold   (int)  : allowable difference (±) before the test fails
#}

with actual as (

    select
        academic_year,
        count(*) as actual_count
    from {{ model }}
    group by academic_year

),

expected as (

    {% for entry in year_counts %}
    select
        {{ entry.year }}     as academic_year,
        {{ entry.expected }} as expected_count
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

)

select
    actual.academic_year,
    actual.actual_count,
    expected.expected_count,
    abs(actual.actual_count - expected.expected_count) as diff,
    {{ threshold }} as threshold
from actual
inner join expected using (academic_year)
where abs(actual.actual_count - expected.expected_count) > {{ threshold }}

{% endtest %}
