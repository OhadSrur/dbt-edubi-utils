/*
 * Macro: convert_string_to_int
 *
 * Description:
 *   Safely converts a string column to a numeric type (integer or numeric) with optional
 *   character replacements. Only converts values that match a valid number pattern.
 *
 * Parameters:
 *   - string_value (required): The column name or expression to convert
 *   - default_none_int_value (optional): Value to return when conversion fails. Default: 'null'
 *   - cast_type (optional): Target numeric type ('int' or 'numeric'). Default: 'int'
 *   - replacements (optional): Dictionary of characters to replace before conversion. Default: {}
 *       Example: {'e': '', 'x': ''} replaces 'e' and 'x' with empty strings
 *   - debug (optional): Enable debug logging. Default: false
 *
 * Returns:
 *   SQL case statement that converts valid numeric strings to the specified type,
 *   or returns the default value for invalid inputs.
 *
 * Examples:
 *
 *   -- Basic conversion to integer
 *   {{ edubi_utils.convert_string_to_int('score_column') }}
 *
 *   -- Convert to numeric (decimal) with default value of 0
 *   {{ edubi_utils.convert_string_to_int(
 *       string_value='result',
 *       cast_type='numeric',
 *       default_none_int_value='0')
 *   }}
 *
 *   -- Replace special characters before conversion
 *   {{ edubi_utils.convert_string_to_int(
 *       string_value='grade',
 *       cast_type='numeric',
 *       replacements={'e': '', 'E': '', 'x': ''})
 *   }}
 *
 *   -- Using project variable for replacements
 *   {{ edubi_utils.convert_string_to_int(
 *       string_value='result',
 *       cast_type='numeric',
 *       replacements=var('syn_special_marks', {}))
 *   }}
 *
 * Notes:
 *   - Uses regex pattern '^[0-9]+(\.[0-9]+)?$' to validate numeric strings
 *   - Rejects strings with double dots '..' to prevent invalid decimals
 *   - Replacements are applied before validation
 *   - Common use case: removing letters like 'e' from numeric grades (e.g., '85e' becomes '85')
 */
{% macro convert_string_to_int(string_value, default_none_int_value='null', cast_type='int', replacements={}, debug=false) %}
    {%- if debug -%}
        {{ log("=== convert_string_to_int DEBUG ===", info=True) }}
        {{ log("string_value: " ~ string_value, info=True) }}
        {{ log("replacements value: " ~ replacements, info=True) }}
        {{ log("replacements length: " ~ (replacements | length), info=True) }}
    {%- endif -%}

    {%- set ns = namespace(value=string_value) -%}

    {#-- Apply replacements if provided --#}
    {%- if replacements and replacements | length > 0 -%}
        {%- if debug -%}
            {{ log(">>> Applying replacements!", info=True) }}
        {%- endif -%}
        {%- for old, new in replacements.items() -%}
            {%- if debug -%}
                {{ log(">>> Replacing '" ~ old ~ "' with '" ~ new ~ "'", info=True) }}
            {%- endif -%}
            {%- set ns.value = "replace(" ~ ns.value ~ ", '" ~ old ~ "', '" ~ new ~ "')" -%}
            {%- if debug -%}
                {{ log(">>> Value after replacement: " ~ ns.value, info=True) }}
            {%- endif -%}
        {%- endfor -%}
        {%- if debug -%}
            {{ log(">>> Final value: " ~ ns.value, info=True) }}
        {%- endif -%}
    {%- elif debug -%}
        {{ log(">>> No replacements applied", info=True) }}
    {%- endif -%}

    case
        when {{ ns.value }} ~ '^[0-9]+(\.[0-9]+)?$' and {{ ns.value }} !~ '\.\.'
        then {{ ns.value }}::{{ cast_type }}
        else {{ default_none_int_value | default('null') }}
    end
{%- endmacro %}