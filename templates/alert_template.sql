{{
    config(
        enabled=var('alert_',true)
    )
}}

{{
    edubi_utils.cte([('source','')])
}},

prep as (

    select
        -- Key

        -- Ref Key
        
        -- Attributes
        
        -- Measures
        
        -- Flags

    from source
),

final as (

    select *,
        
        -- Meta
        {{ edubi_utils.record_hash('prep') }}                               as record_hash,
        {{ current_timestamp() }} at time zone '{{var('client_timezone')}}' as emitted_date_at,
        {{ edubi_utils.convert_date_part_to_name(
            'yyyy-mm-dd',
            "current_timestamp at time zone 'AEST'") 
        }}                                                                  as emitted_date

    from prep
)

select * from final