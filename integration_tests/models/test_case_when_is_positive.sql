{{ config(materialized='view') }}

with data_test as (
    select * from {{ source('test_source','data_case_when_is_positive') }}
),

final as (
    select
        {{ case_when_is_positive('col_to_check')}}                              as actual,
        result_expected                                                         as expected

    from data_test

    union all

    select
        {{ case_when_is_positive('col_to_check',when_is_negative='1:BOOL')}}    as actual,
        result_expected_inverted                                                as expected

    from data_test
)

select * from final