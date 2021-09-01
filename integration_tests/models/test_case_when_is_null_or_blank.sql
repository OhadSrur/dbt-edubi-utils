{{ config(materialized='view') }}

with data_test as (
    select * from {{ source('test_source','data_case_when_is_null_or_blank') }}
),

final as (
    select
        {{ case_when_is_null_or_blank('col_to_check','a')}}                     as actual,
        result_expected                                                         as expected

    from data_test
)

select * from final