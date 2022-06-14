{% macro across_sum(var_list, script_string = '{{var}}', final_comma = false) %}

  {% for v in var_list %}
  {{ script_string | replace('{{var}}', v) }}
  {%- if not loop.last %}+{% endif %}
  {%- if loop.last and final_comma|default(false) %},{% endif %}
  {% endfor %}

{% endmacro %}