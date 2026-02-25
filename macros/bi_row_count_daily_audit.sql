-- Generates the daily row-count snapshot for all models listed under
-- var('bi_row_count_audit__models', []).
--
-- Add models to that var in dbt_project.yml:
--
--   vars:
--     bi_row_count_audit__models:
--       - fct_allwell_npi_scores
--       - fct_naplan
--       - dim_academic_semester
--
-- Returns an empty result when no models are configured so that the
-- incremental model materialises without error on a fresh project.

{% macro bi_row_count_daily_audit() %}

{% set models = var('bi_row_count_audit__models', []) %}

{% if models | length == 0 %}

    -- No models configured for bi_row_count_audit__models; returning empty result.
    select
        null::varchar       as model_name,
        null::int           as academic_year,
        null::bigint        as row_count,
        null::date          as snapshot_date,
        null::timestamptz   as emitted_date_at
    where false

{% else %}

with
{% for model_name in models %}
{{ model_name }}            as (select '{{ model_name }}' as model_name, academic_year::int, count(*) as row_count from {{ ref(model_name) }} group by academic_year),
{% endfor %}

all_models as (
{% for model_name in models %}
    select * from {{ model_name }}{% if not loop.last %} union all{% endif %}

{% endfor %}
)

select
    model_name,
    academic_year,
    row_count,
    {{ edubi_utils.convert_date_part_to_name(
        'yyyy-mm-dd',
        "current_timestamp at time zone 'AEST'")
    }}::date                                                            as snapshot_date,
    {{ current_timestamp() }} at time zone '{{var('client_timezone')}}' as emitted_date_at

from all_models

{% endif %}

{% endmacro %}
