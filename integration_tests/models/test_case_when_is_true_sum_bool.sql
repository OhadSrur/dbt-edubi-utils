{{ config(materialized='view') }}

with data_test as (
    select * from {{ source('test_source','data_case_when_is_true_sum_bool') }}
),

final as (
    select
        group_label,
        {{ case_when_is_true_sum_bool('col_to_check::bool')}}                           as result_expected,
        {{ case_when_is_true_sum_bool('col_to_check::bool',when_is_false='1:bool')}}    as result_expected_inverted

    from data_test
    group by 1
)

select * from final