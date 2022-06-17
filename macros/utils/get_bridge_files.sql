   
{% macro get_bridge_files() %}

    {% set bridge_files = [] %}

    {% if var('package__syn_academic_enabled') %} 
    {% set _ = bridge_files.append(ref('_pb_synergetic_academic')) %}
    {% endif %}

    {% if var('package__syn_finance_enabled') %} 
    {% set _ = bridge_files.append(ref('_pb_synergetic_finance')) %}
    {% endif %}

    {% if var('package__seqta_enabled') %} 
    {% set _ = bridge_files.append(ref('_pb_seqta_academic')) %}
    {% endif %}

    {{ return(bridge_files) }}

{% endmacro %}