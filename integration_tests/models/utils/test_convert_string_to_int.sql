{{
    edubi_utils.cte([('data_test','data_convert_string_to_int')],cte_ref_type='source', source_name='test_source')
}},

final as (
    select
        {{ edubi_utils.convert_string_to_int('col_to_check')}}  as actual,
        result_expected                                         as expected

    from data_test
)

select * from final