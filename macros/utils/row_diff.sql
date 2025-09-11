{#
  row_diff.sql - dbt macro for comparing two tables row by row
  
  This file contains four macros:
  1. row_diff_sql: Returns raw SQL string for table comparison
  2. row_diff: Model-friendly version for use in dbt models  
  3. row_diff_operation: CLI operation that executes and logs results
  4. row_diff_sql_show: CLI operation that displays the generated SQL

  ===== DOCUMENTATION =====

  ## row_diff_sql
  
  Core macro that generates SQL to compare two identically-structured tables row by row.
  
  ### Arguments
  - table_old (string): Fully qualified name of baseline table (e.g., 'prd_stg.my_table')
  - table_new (string): Fully qualified name of comparison table (e.g., 'dev_stg.my_table')
  - unique_key (string): Name of the primary/unique key column for joining
  - compare_columns (list, string, or wildcard): Columns to include in comparison. Supports:
    - Array format: ["col1", "col2", "col3"]
    - String with brackets: "['col1','col2','col3']"
    - Comma-separated string: "col1, col2, col3"
    - Wildcard: "*" to select all columns automatically
  - null_token (string, optional): Value to replace NULLs before hashing. Default: '∅'
  
  ### Output
  Returns SQL that produces a result set with:
  - unique_key: The join key value
  - change_type: One of 'added_in_new', 'removed_in_new', or 'changed'
  - hash_old: Hash of old table row (NULL for added rows)
  - hash_new: Hash of new table row (NULL for removed rows)
  - All specified columns from both tables (prefixed with old_ and new_)
  
  ### Key Features
  - ✅ Wildcard support: Use "*" to automatically include ALL table columns
  - ✅ Flexible input formats: Supports arrays, strings, and bracket notation
  - ✅ Clean SQL output: Optimized whitespace and formatting
  - ✅ Full column visibility: Shows actual column values, not just hashes
  
  ### Usage Examples
  
  #### Basic Usage with Specific Columns
  {{ row_diff_sql(
      table_old='prd_stg.customers',
      table_new='dev_stg.customers', 
      unique_key='customer_id',
      compare_columns=['name', 'email', 'status']
  ) }}
  
  #### Wildcard Usage (All Columns)
  {{ row_diff_sql(
      table_old='prd_stg.students',
      table_new='dev_stg.students',
      unique_key='student_id',
      compare_columns='*'
  ) }}

  ## row_diff
  
  Model-friendly version of row_diff_sql for use in dbt models.
  
  ### Arguments
  Same as row_diff_sql.
  
  ### Usage in Models
  Create a model file (e.g., models/analysis/customer_diff.sql):
  
  {{
    config(
      materialized='table'
    )
  }}
  
  {{
    row_diff(
      table_old='prd_stg.customers',
      table_new='dev_stg.customers',
      unique_key='customer_id',
      compare_columns=['name', 'email', 'status'],
      null_token='<NULL>'
    )
  }}
  
  Then run: dbt run --select customer_diff

  ## row_diff_operation
  
  CLI operation that executes the table comparison and logs detailed summary statistics.
  
  ### Arguments
  Same as row_diff_sql.
  
  ### Features
  - Executes comparison and shows summary statistics
  - Displays up to 20 sample differing rows
  - Shows breakdown by change type (added/removed/changed)
  - Handles both wildcard and specific column comparisons
  
  ### Usage Examples
  
  #### Array Format (Recommended)
  dbt run-operation row_diff_operation --args '{
    "table_old": "dev_stg.assessment_results",
    "table_new": "dev_stg.assessment_results_v2",
    "unique_key": "_key_assessment_result",
    "compare_columns": "['results_period_id','academic_year','assessment_result_code','assessment_result_desc','assessment_result_type','is_assessment_mark']"
  }''
  
  #### Comma-Separated String Format
  dbt run-operation row_diff_operation --args '{
    "table_old": "prd_stg.students",
    "table_new": "dev_stg.students",
    "unique_key": "student_id", 
    "compare_columns": "name, year_level, status",
    "null_token": "<NULL>"
  }''
  
  #### Wildcard Format (All Columns)
  dbt run-operation row_diff_operation --args '{
    "table_old": "dev_stg.lu_assessment_periods",
    "table_new": "dev_stg.lu_assessment_periods_v1",
    "unique_key": "_key_period",
    "compare_columns": "*"
  }''
  
  ### Sample Output
  === ROW DIFF SUMMARY ===
  Table Old: dev_stg.stg_tass__lu_assessment_result_period
  Table New: dev_stg.stg_tass__lu_assessment_result_period_v1
  Unique Key: _key_tass__lu_assessment_result_period
  Compare Columns: ['results_period_id', 'academic_year']
  
  RESULTS:
    Total differences: 3
    Added in new: 1
    Removed in new: 0
    Changed: 2
  
  SAMPLE ROWS (up to 20):
    12345 | changed | abc123hash -> def456hash
    67890 | changed | ghi789hash -> jkl012hash  
    54321 | added_in_new | null -> mno345hash

  ## row_diff_sql_show
  
  CLI operation that displays the generated SQL without executing it.
  Useful for debugging, understanding the generated query, or copying SQL for manual execution.
  
  ### Arguments
  Same as row_diff_sql.
  
  ### Usage Examples
  
  #### Show SQL for Specific Columns
  dbt run-operation row_diff_sql_show --args '{
    "table_old": "dev_stg.stg_tass__lu_assessment_result_period",
    "table_new": "dev_stg.stg_tass__lu_assessment_result_period_v1",
    "unique_key": "_key_tass__lu_assessment_result_period",
    "compare_columns": ["results_period_id","academic_year"]
  }''
  
  #### Show SQL for All Columns (Wildcard)
  dbt run-operation row_diff_sql_show --args '{
    "table_old": "prd_stg.customers", 
    "table_new": "dev_stg.customers",
    "unique_key": "customer_id",
    "compare_columns": "*"
  }''
  
  ### Output
  Displays clean, formatted SQL that can be copied and executed directly in your database client.

  ## Input Format Compatibility
  
  The macros support multiple input formats for maximum flexibility:
  
  | Format | Example | Use Case |
  |--------|---------|----------|
  | Array | ["col1", "col2"] | dbt YAML, recommended |
  | String with brackets | "['col1','col2']" | Command line with quotes |  
  | Comma-separated | "col1, col2" | Simple string input |
  | Wildcard | "*" | Include all columns |
  
  All formats are automatically parsed and handled correctly by the macro.

  ===== END DOCUMENTATION =====
#}

{#  Macro 1: row_diff_sql
  Core logic that generates the comparison SQL
#}
{% macro row_diff_sql(table_old, table_new, unique_key, compare_columns, null_token='∅') %}
  
  {%- if compare_columns == '*' -%}
    {%- set columns_query -%}
      select column_name 
      from information_schema.columns
      where table_name = '{{ table_old.split('.')[-1] }}'
        and table_schema = '{{ table_old.split('.')[-2] if '.' in table_old else target.schema }}'
        and column_name != '{{ unique_key }}'
      order by ordinal_position
    {%- endset -%}
    {%- if execute -%}
      {%- set column_results = run_query(columns_query) -%}
      {%- set compare_cols = column_results.columns[0].values() -%}
    {%- else -%}
      {%- set compare_cols = [] -%}
    {%- endif -%}
  {%- elif compare_columns is string -%}
    {# Handle string format like "['col1','col2','col3']" #}
    {%- if compare_columns.startswith('[') and compare_columns.endswith(']') -%}
      {%- set clean_string = compare_columns[1:-1] -%}
      {%- set compare_cols = clean_string.split(',') | map('trim') | map('replace', "'", '') | map('replace', '"', '') | list -%}
    {%- else -%}
      {%- set compare_cols = compare_columns.split(',') | map('trim') | list -%}
    {%- endif -%}
  {%- else -%}
    {%- set compare_cols = compare_columns -%}
  {%- endif -%}

  {%- set hash_columns = [] -%}
  {%- for col in compare_cols -%}
    {%- do hash_columns.append("coalesce(cast(" ~ col ~ " as text), '" ~ null_token ~ "')") -%}
  {%- endfor -%}
  
  {%- set all_columns_old = [] -%}
  {%- set all_columns_new = [] -%}
  {%- do all_columns_old.append("old_table." ~ unique_key ~ " as old_" ~ unique_key) -%}
  {%- do all_columns_new.append("new_table." ~ unique_key ~ " as new_" ~ unique_key) -%}
  {%- for col in compare_cols -%}
    {%- do all_columns_old.append("old_table." ~ col ~ " as old_" ~ col) -%}
    {%- do all_columns_new.append("new_table." ~ col ~ " as new_" ~ col) -%}
  {%- endfor -%}

with old_table_hashed as (
  select 
    {{ unique_key }},
    md5(concat_ws('||', {{ hash_columns | join(', ') }})) as row_hash
    {%- for col in compare_cols %}
    ,{{ col }}
    {%- endfor %}
  from {{ table_old }}
),

new_table_hashed as (
  select 
    {{ unique_key }},
    md5(concat_ws('||', {{ hash_columns | join(', ') }})) as row_hash
    {%- for col in compare_cols %}
    ,{{ col }}
    {%- endfor %}
  from {{ table_new }}
),

comparison as (
  select 
    coalesce(old_table.{{ unique_key }}, new_table.{{ unique_key }}) as {{ unique_key }},
    
    case 
      when old_table.{{ unique_key }} is null then 'added_in_new'
      when new_table.{{ unique_key }} is null then 'removed_in_new'
      when old_table.row_hash != new_table.row_hash then 'changed'
      else 'identical'
    end as change_type,
    
    old_table.row_hash as hash_old,
    new_table.row_hash as hash_new
    {%- for col in compare_cols %}
    ,old_table.{{ col }} as old_{{ col }}
    ,new_table.{{ col }} as new_{{ col }}
    {%- endfor %}
    
  from old_table_hashed as old_table
  full outer join new_table_hashed as new_table
    on old_table.{{ unique_key }} = new_table.{{ unique_key }}
)

select * 
from comparison
where change_type != 'identical'
order by 
  case change_type 
    when 'added_in_new' then 1
    when 'removed_in_new' then 2  
    when 'changed' then 3
  end,
  {{ unique_key }}

{% endmacro %}

{#  Macro 2: row_diff
  Model-friendly version that returns the SQL result
#}
{% macro row_diff(table_old, table_new, unique_key, compare_columns, null_token='∅') %}
  
  {{ return(row_diff_sql(table_old, table_new, unique_key, compare_columns, null_token)) }}
  
{% endmacro %}

{#  Macro 3: row_diff_operation
  CLI operation that executes and logs results
#}
{% macro row_diff_operation(table_old, table_new, unique_key, compare_columns, null_token='∅') %}
  
  {% set diff_sql = row_diff_sql(table_old, table_new, unique_key, compare_columns, null_token) %}
  
  {# Execute the comparison query #}
  {% if execute %}
    {% set results = run_query(diff_sql) %}
    
    {# Log summary statistics #}
    {% set total_rows = results.rows | length %}
    {% set added_count = results.rows | selectattr('1', 'equalto', 'added_in_new') | list | length %}
    {% set removed_count = results.rows | selectattr('1', 'equalto', 'removed_in_new') | list | length %}
    {% set changed_count = results.rows | selectattr('1', 'equalto', 'changed') | list | length %}
    
    {{ log("=== ROW DIFF SUMMARY ===", info=true) }}
    {{ log("Table Old: " ~ table_old, info=true) }}
    {{ log("Table New: " ~ table_new, info=true) }}
    {{ log("Unique Key: " ~ unique_key, info=true) }}
    {{ log("Compare Columns: " ~ compare_columns, info=true) }}
    {{ log("", info=true) }}
    {{ log("RESULTS:", info=true) }}
    {{ log("  Total differences: " ~ total_rows, info=true) }}
    {{ log("  Added in new: " ~ added_count, info=true) }}
    {{ log("  Removed in new: " ~ removed_count, info=true) }}
    {{ log("  Changed: " ~ changed_count, info=true) }}
    
    {% if total_rows > 0 %}
      {{ log("", info=true) }}
      {{ log("SAMPLE ROWS (up to 20):", info=true) }}
      {{ log("", info=true) }}
      
      {# Display sample rows with cleaner format #}
      {% if compare_columns == '*' %}
        {# Show more detailed column information when using wildcard #}
        {% for row in results.rows[:5] %}
          {% set unique_key_val = row[0] %}
          {% set change_type_val = row[1] %}
          {{ log("  " ~ unique_key_val ~ " | " ~ change_type_val ~ " | Row " ~ loop.index ~ " has " ~ (results.column_names | length) ~ " columns", info=true) }}
          {% if loop.first %}
            {{ log("  Columns: " ~ (results.column_names | join(', ')), info=true) }}
          {% endif %}
        {% endfor %}
      {% else %}
        {# Original format for specific column comparisons #}
        {% for row in results.rows[:10] %}
          {% set unique_key_val = row[0] %}
          {% set change_type_val = row[1] %}
          {% set hash_old_val = row[2] %}
          {% set hash_new_val = row[3] %}
          {{ log("  " ~ unique_key_val ~ " | " ~ change_type_val ~ " | " ~ hash_old_val ~ " -> " ~ hash_new_val, info=true) }}
        {% endfor %}
      {% endif %}
      
      {% if total_rows > 20 %}
        {{ log("", info=true) }}
        {{ log("... " ~ (total_rows - 20) ~ " more rows", info=true) }}
      {% endif %}
    {% else %}
      {{ log("", info=true) }}
      {{ log("No differences found! Tables are identical.", info=true) }}
    {% endif %}
    
    {{ log("", info=true) }}
    {{ log("=== END ROW DIFF ===", info=true) }}
    
  {% else %}
    {{ log("row_diff_operation can only be run in execute mode", info=true) }}
  {% endif %}
  
{% endmacro %}

{#  Macro 4: row_diff_sql_show
  Helper operation to display the generated SQL from row_diff_sql
#}
{% macro row_diff_sql_show(table_old, table_new, unique_key, compare_columns, null_token='∅') %}
  
  {% set generated_sql = row_diff_sql(table_old, table_new, unique_key, compare_columns, null_token) %}
  
  {{ log("=== GENERATED SQL ===", info=true) }}
  {{ log(generated_sql, info=true) }}
  {{ log("=== END SQL ===", info=true) }}
  
{% endmacro %}


