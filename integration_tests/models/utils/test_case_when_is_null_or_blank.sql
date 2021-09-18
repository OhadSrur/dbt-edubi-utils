{{
    edubi_utils.cte([('data_test','data_case_when_is_null_or_blank')],cte_ref_type='source', source_name='test_source')
}},

final as (
    select
        {{ edubi_utils.case_when_is_null_or_blank('col_to_check','a')}}     as actual,
        result_expected                                                     as expected

    from data_test
)

select * from final