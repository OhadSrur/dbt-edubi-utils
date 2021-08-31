{%- macro case_when_debit_credit(column_name,type_is_credit=FALSE, switch_credit_debit_direction_for_posting_source='', column_source = 'posting_source') -%}
        CASE WHEN {% if switch_credit_debit_direction_for_posting_source != '' -%}({% endif %}({{ column_name }}::DECIMAL 
        {%- if type_is_credit %} < {% else %} > {% endif -%}0.0
        {% if switch_credit_debit_direction_for_posting_source != '' -%} and {{ column_source }} not in ('{{ switch_credit_debit_direction_for_posting_source }}')) 
                OR ({{ column_name }}::DECIMAL {%- if type_is_credit %} > {% else %} < {% endif -%}
                0.0 and {{ column_source }} in ('{{ switch_credit_debit_direction_for_posting_source }}')))
                {%- else -%} ) {%- endif %}
        THEN abs({{ column_name }}::DECIMAL)
        ELSE 0::DECIMAL
        END
{%- endmacro %}