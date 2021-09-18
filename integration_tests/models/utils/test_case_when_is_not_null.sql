{{
    edubi_utils.cte([('data_test','data_case_when_is_not_null')],cte_ref_type='source', source_name='test_source')
}},

final as (
    select
        {{ edubi_utils.case_when_is_not_null('col_to_check')}}                          as actual,
        result_expected                                                                 as expected

    from data_test

    union all

    select
        {{ edubi_utils.case_when_is_not_null('col_to_check',when_is_null='1:BOOL')}}    as actual,
        result_expected_inverted                                                        as expected

    from data_test
)

select * from final