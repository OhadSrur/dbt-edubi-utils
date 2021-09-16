{{
    cte([('data_test','data_case_when_is_positive')],cte_ref_type='source', source_name='test_source')
}},

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