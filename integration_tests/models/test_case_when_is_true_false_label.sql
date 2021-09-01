{{ config(materialized='view') }}

with data_test as (
    select * from {{ source('test_source','data_case_when_is_true_false_label') }}
),

final as (
    select
        {{ case_when_is_true_false_label('predicate::bool','T','F')}}                           as actual,
        result_expected                                                                         as expected

    from data_test

union all

    select
        {{ case_when_is_true_false_label('predicate::bool','T','F',when_is_not_true='1:bool')}} as actual,
        result_expected_inverted                                                                as expected

    from data_test
)

select * from final