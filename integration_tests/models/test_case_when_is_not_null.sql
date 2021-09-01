{{ config(materialized='view') }}

with data_test as (
    select * from {{ source('test_source','data_case_when_is_not_null') }}
),

final as (
    select
        {{ case_when_is_not_null('col_to_check')}}                              as actual,
        result_expected                                                         as expected

    from data_test

    union all

    select
        {{ case_when_is_not_null('col_to_check',when_is_null='1:BOOL')}}        as actual,
        result_expected_inverted                                                as expected

    from data_test
)

select * from final