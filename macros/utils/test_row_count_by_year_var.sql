{% test row_count_by_year_var(model, var_name, threshold_var=none, threshold=1000) %}

{#
    A project level variant of row_count_by_year.
    Expected counts are read from a dbt var (set per-client in dbt_project.yml).

    Args:
        var_name      (str) : name of the var holding a list of {year, expected} dicts
        threshold_var (str) : optional var name to read threshold from (overrides threshold)
        threshold     (int) : fallback threshold if threshold_var is not set (default 1000)

    Client configures in their dbt_project.yml:
        vars:
          fct_student_class_absence__row_counts_threshold: 5000
          fct_student_class_absence__row_counts:
              - year: 2023
              expected: 86898
              - year: 2024
              expected: 117583
#}

{% set var_data = var(var_name, {}) %}
{% set year_counts = var_data.get('year_counts', []) %}
{% set effective_threshold = var(threshold_var, threshold) if threshold_var else threshold %}

{% if year_counts | length == 0 %}

    -- No expected counts configured for this client; test always passes.
    select 1 where false

{% else %}

with actual as (
    select
        academic_year,
        count(*) as actual_count
    from {{ model }}
    group by academic_year
),

expected as (
    {% for entry in year_counts %}
    select {{ entry.year }} as academic_year, {{ entry.expected }} as expected_count
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    actual.academic_year,
    actual.actual_count,
    expected.expected_count,
    abs(actual.actual_count - expected.expected_count) as diff,
    {{ effective_threshold }}                           as threshold
from actual
inner join expected using (academic_year)
where abs(actual.actual_count - expected.expected_count) > {{ effective_threshold }}

{% endif %}

{% endtest %}
