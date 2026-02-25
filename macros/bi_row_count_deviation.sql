-- Fails if a model's row count for any academic_year changes by more than EITHER
-- the absolute OR percentage threshold. Two threshold levels apply:
--   previous years – absolute: {{ var('bi_row_count_audit__absolute_threshold', 1000) }} rows
--                              pct: {{ var('bi_row_count_audit__pct_threshold', 0.1) }} (10%)
--   current year   – absolute: {{ var('bi_row_count_audit__absolute_threshold_current_year', 5000) }} rows
--                              pct: {{ var('bi_row_count_audit__pct_threshold_current_year', 0.25) }} (25%)
--
-- Per-model overrides (any subset of the four threshold keys):
--   bi_row_count_audit__model_overrides:
--     fct_student_class_absence:
--       absolute_threshold: 2000
--       pct_threshold: 0.15
--       absolute_threshold_current_year: 10000
--       pct_threshold_current_year: 0.4
--
-- A first-run with no prior snapshot always passes.

{% macro bi_row_count_deviation() %}

with ranked as (
    select
        model_name,
        academic_year,
        snapshot_date,
        row_count,
        lag(row_count)      over (partition by model_name, academic_year order by snapshot_date) as prev_row_count,
        lag(snapshot_date)  over (partition by model_name, academic_year order by snapshot_date) as prev_snapshot_date,
        row_number()        over (partition by model_name, academic_year order by snapshot_date desc) as rn
    from {{ ref('bi__row_count_daily_audit') }}
    where academic_year is not null
),

latest as (
    select
        *,
        academic_year::int = extract(year from current_date)::int as is_current_year
    from ranked
    where rn = 1 and prev_row_count is not null
),

deviations as (
    select
        model_name,
        academic_year,
        is_current_year,
        snapshot_date                                                as current_snapshot_date,
        row_count                                                    as current_count,
        prev_snapshot_date,
        prev_row_count,
        abs(row_count - prev_row_count)                             as absolute_diff,
        round(
            abs(row_count - prev_row_count)
            / nullif(prev_row_count::numeric, 0) * 100, 2
        )                                                            as pct_diff,
        case
            {% for model, overrides in var('bi_row_count_audit__model_overrides', {}).items() %}
            when model_name = '{{ model }}' and is_current_year = true
                then {{ overrides.get('absolute_threshold_current_year', var('bi_row_count_audit__absolute_threshold_current_year', 5000)) }}
            when model_name = '{{ model }}' and is_current_year = false
                then {{ overrides.get('absolute_threshold', var('bi_row_count_audit__absolute_threshold', 1000)) }}
            {% endfor %}
            when is_current_year = true
                then {{ var('bi_row_count_audit__absolute_threshold_current_year', 5000) }}
            else {{ var('bi_row_count_audit__absolute_threshold', 1000) }}
        end                                                          as absolute_threshold,
        case
            {% for model, overrides in var('bi_row_count_audit__model_overrides', {}).items() %}
            when model_name = '{{ model }}' and is_current_year = true
                then {{ overrides.get('pct_threshold_current_year', var('bi_row_count_audit__pct_threshold_current_year', 0.25)) }} * 100
            when model_name = '{{ model }}' and is_current_year = false
                then {{ overrides.get('pct_threshold', var('bi_row_count_audit__pct_threshold', 0.1)) }} * 100
            {% endfor %}
            when is_current_year = true
                then {{ var('bi_row_count_audit__pct_threshold_current_year', 0.25) }} * 100
            else {{ var('bi_row_count_audit__pct_threshold', 0.1) }} * 100
        end                                                          as pct_threshold
    from latest
)

select *
from deviations
where absolute_diff > absolute_threshold
or pct_diff > pct_threshold

{% endmacro %}
