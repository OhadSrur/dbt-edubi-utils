{{
    edubi_utils.cte([('data_test','data_case_when_is_true_sum_bool')],cte_ref_type='source', source_name='test_source')
}},

final as (
    select
        group_label,
        {{ edubi_utils.case_when_is_true_sum_bool('col_to_check::bool')}}                           as result_expected,
        {{ edubi_utils.case_when_is_true_sum_bool('col_to_check::bool',when_is_false='1:bool')}}    as result_expected_inverted

    from data_test
    group by 1
)

select * from final