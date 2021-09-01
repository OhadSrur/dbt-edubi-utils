{{ config(materialized='view') }}

with data_test as (
    select * from {{ source('test_source','data_convert_string_to_int') }}
),

final as (
    select
        {{ convert_string_to_int('col_to_check')}}      as actual,
        result_expected                                 as expected

    from data_test
)

select * from final