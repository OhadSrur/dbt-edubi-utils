with 

{% if var('sys_src__ai_') == 'seqta' %}
seqta_final as (

    select

    from {{ ref('') }}
    where 
)
{% endif %}

{% if var('sys_src__ai_') == 'syn' %}
syn_final as (

    select

    from {{ ref('') }}
    where 
)
{% endif %}

{% if var('sys_src__ai_') == 'sentral' %}
sentral_final as (

    select

    from {{ ref('') }}
    where 
)
{% endif %}

select * from {{ var('sys_src__ai_') }}_final