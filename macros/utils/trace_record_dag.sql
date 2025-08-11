-- macros/utils/trace_record_dag.sql
{#-
  trace_record_dag
  -------------
  Trace counts through a model’s lineage using a raw SQL WHERE clause that YOU supply.
  Walk either upstream (parents) or downstream (children). For each visited node
  (model/source/seed), the macro:
    1) Resolves the physical relation (database.schema.identifier),
    2) Verifies the relation exists (adapter.get_relation) — if missing, SKIP,
    3) (Optional) Pre-validates column names referenced in your WHERE — if missing, SKIP,
    4) Executes COUNT(*) with your WHERE,
    5) Logs a compact per-node result.
    6) (Optional) If output_columns are provided, prints sample data from each relation.

  This macro **does not return** a value; it logs only.

  Parameters
    target_model_name (str)  : Model name in dbt (not package-qualified).
    where_clause      (str)  : Raw SQL to place after WHERE (e.g., "student_id = 'A12345'").
                               Not sanitized. Intended for dev use only.
    include_schemas   (list) : Optional allowlist of schemas to check.
    exclude_schemas   (list) : Optional blocklist of schemas to skip.
    direction         (str)  : 'upstream' (default) or 'downstream'.
    verbose           (bool) : When true, emit SKIPPED logs. Default false.
    precheck_columns  (bool) : When true, best-effort check that identifiers in WHERE
                               exist as columns on each relation; if not, SKIP. Default true.

    -- New parameters for sample data output:
    output_columns    (list) : When provided, also print data: SELECT {columns} ... LIMIT {max_rows}.
    max_rows          (int)  : Max rows to print per relation when output_columns is set. Default 10.
    print_format      (str)  : 'table' (default), 'csv', or 'json'.

  Notes
    - Relation resolution:
        sources      → node.identifier
        models/seeds → node.alias if set, else node.name
    - Traversal is transitive (BFS) via dbt’s dependency graph.
    - Column precheck is best-effort (simple tokenizer, no regex). Complex SQL may not be fully parsed.

  Example
    edev dbt run-operation edubi_utils.trace_record_dag --args '{
      "target_model_name": "clean_syn__student_assessment_results",
      "where_clause": "student_assessment_results_seq_key = 10871210",
      "direction": "upstream",
      "output_columns": ["student_assessment_results_seq_key", "class_code", "academic_year", "term_number", "mark_out_of"],
      "max_rows": 10,
      "print_format": "table",
      "verbose": false
    }''
-#}

{% macro trace_record_dag__print_rows(rel_str, headers, table, fmt) %}
  {% if table is none %}
    {{ log(rel_str ~ " -> no result returned.", info=True) }}
    {% do return(None) %}
  {% endif %}

  {% if fmt == 'csv' %}
    {{ log(headers | join(','), info=True) }}
    {% for row in table.rows %}
      {{ log(row | map('string') | join(','), info=True) }}
    {% endfor %}

  {% elif fmt == 'json' %}
    {% set out = [] %}
    {% for r in table.rows %}
      {% set d = {} %}
      {% for i in range(0, headers | length) %}
        {% do d.update({ (headers[i]): (r[i]) }) %}
      {% endfor %}
      {% do out.append(d) %}
    {% endfor %}
    {{ log(tojson(out), info=True) }}

  {% else %}
    {# ---- TABLE FORMAT: header above rows ---- #}
    {{ log(rel_str ~ " (table):", info=True) }}
    {{ log(headers | join(' | '), info=True) }}
    {{ log('-' * (headers | join(' | ') | length), info=True) }}  {# optional separator #}
    {% for row in table.rows %}
      {{ log(row | map('string') | join(' | '), info=True) }}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro trace_record_dag(
  target_model_name,
  where_clause,
  include_schemas=[],
  exclude_schemas=[],
  direction='upstream',
  verbose=false,
  precheck_columns=true,
  output_columns=[],
  max_rows=10,
  print_format='table'
) %}
  {% if not execute %}
    {{ return("") }}
  {% endif %}

  {% if where_clause is none or (where_clause | trim) == '' %}
    {% do exceptions.raise_compiler_error("trace_record_dag: 'where_clause' must be a non-empty SQL predicate.") %}
  {% endif %}

  {% set _valid_dirs = ['upstream', 'downstream'] %}
  {% set _dir = direction | lower %}
  {% if _dir not in _valid_dirs %}
    {% do exceptions.raise_compiler_error("trace_record_dag: invalid direction '" ~ direction ~ "'. Allowed: upstream, downstream") %}
  {% endif %}

  {% set _valid_formats = ['table','csv','json'] %}
  {% set _fmt = print_format | lower %}
  {% if _fmt not in _valid_formats %}
    {% do exceptions.raise_compiler_error("trace_record_dag: invalid print_format '" ~ print_format ~ "'. Allowed: table, csv, json") %}
  {% endif %}

  {% if (output_columns | length > 0) and (max_rows | int <= 0) %}
    {% do exceptions.raise_compiler_error("trace_record_dag: 'max_rows' must be positive when 'output_columns' is provided.") %}
  {% endif %}

  {# Resolve target model #}
  {% set model_candidates = graph.nodes.values()
      | selectattr('resource_type','equalto','model')
      | selectattr('name','equalto', target_model_name)
      | list %}
  {% if model_candidates | length == 1 %}
    {% set target_node = model_candidates[0] %}
    {% set target_uid  = target_node.unique_id %}
  {% elif model_candidates | length > 1 %}
    {% do exceptions.raise_compiler_error("trace_record_dag: multiple models named '" ~ target_model_name ~ "'. Disambiguate.") %}
  {% else %}
    {% set target_uid = "model." ~ project_name ~ "." ~ target_model_name %}
    {% if target_uid not in graph.nodes %}
      {% do exceptions.raise_compiler_error("trace_record_dag: model '" ~ target_model_name ~ "' not found.") %}
    {% endif %}
    {% set target_node = graph.nodes[target_uid] %}
  {% endif %}

  {# Build traversal #}
  {% set wanted_types = ['model', 'source', 'seed'] %}
  {% set all_nodes = graph.nodes.values() | list %}
  {% set all_sources = graph.sources.values() if graph.sources is defined else [] %}
  {% set all_nodes = all_nodes + (all_sources | list) %}

  {% if _dir == 'downstream' %}
    {% set child_map = {} %}
    {% for n in all_nodes %}
      {% if n.depends_on and n.depends_on.nodes %}
        {% for parent_uid in n.depends_on.nodes %}
          {% if parent_uid in child_map %}
            {% do child_map[parent_uid].append(n.unique_id) %}
          {% else %}
            {% do child_map.update({parent_uid: [n.unique_id]}) %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {% set seen = set([]) %}
  {% set queue = [target_uid] %}
  {% set ordered = [] %}
  {% for _ in range(0, 2000) %}
    {% if queue | length == 0 %}{% break %}{% endif %}
    {% set uid = queue.pop(0) %}
    {% if uid in seen %}{% continue %}{% endif %}
    {% do seen.add(uid) %}
    {% set n = graph.nodes.get(uid) or graph.sources.get(uid) %}
    {% if n and n.resource_type in wanted_types %}{% do ordered.append(n) %}{% endif %}
    {% if n %}
      {% if _dir == 'upstream' %}
        {% if n.depends_on and n.depends_on.nodes %}
          {% for parent_uid in n.depends_on.nodes %}
            {% if parent_uid not in seen %}{% do queue.append(parent_uid) %}{% endif %}
          {% endfor %}
        {% endif %}
      {% else %}
        {% set kids = child_map.get(uid) if child_map is defined else [] %}
        {% for child_uid in kids %}
          {% if child_uid not in seen %}{% do queue.append(child_uid) %}{% endif %}
        {% endfor %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {{ log("--- Record Trace Report ---", info=True) }}
  {{ log("Target Model: " ~ target_node.name ~ " (" ~ target_uid ~ ")", info=True) }}
  {{ log("Direction: " ~ _dir, info=True) }}
  {{ log("WHERE: " ~ where_clause, info=True) }}
  {{ log("Nodes to check: " ~ ordered | length, info=True) }}
  {% if output_columns | length > 0 %}
    {{ log("Output columns: " ~ (output_columns | join(', ')) ~ " | max_rows=" ~ (max_rows | string) ~ " | format=" ~ _fmt, info=True) }}
  {% endif %}
  {{ log("---------------------------", info=True) }}

  {# Precheck columns in WHERE #}
  {% if precheck_columns %}
    {% set _lc = where_clause | lower %}
    {% set _norm = _lc
      | replace('\n',' ') | replace('\r',' ') | replace('\t',' ')
      | replace('(',' ')  | replace(')',' ')  | replace('[',' ') | replace(']',' ')
      | replace('{',' ')  | replace('}',' ')  | replace(',',' ') | replace(';',' ')
      | replace("'",' ')  | replace('"',' ')  | replace(':',' ') | replace('?',' ')
      | replace('+',' ')  | replace('-',' ')  | replace('*',' ') | replace('/',' ')
      | replace('%',' ')  | replace('=',' ')  | replace('!',' ') | replace('<',' ') | replace('>',' ')
      | replace('|',' ')  | replace('&',' ')  | replace('.',' . ')
    %}
    {% set _tokens = _norm.split(' ') %}
    {% set _blacklist = [
      '','select','from','where','and','or','not','in','is','null','like','ilike','between','exists',
      'case','when','then','else','end','true','false','on','join','inner','left','right','full',
      'as','distinct','group','by','order','limit','offset','having','with','union','all','into',
      'cast','convert','date','timestamp','interval'
    ] %}
    {% set ids_in_where = [] %}
    {% for t in _tokens %}
      {% set t = t | trim %}
      {% if t not in _blacklist and (t.isdigit() == false) %}
        {% if '.' in t %}
          {% set last = (t.split('.') | last) | trim %}
          {% if last and last not in _blacklist and last not in ids_in_where %}
            {% do ids_in_where.append(last) %}
          {% endif %}
        {% else %}
          {% if t not in ids_in_where %}
            {% do ids_in_where.append(t) %}
          {% endif %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {% for node in ordered %}
    {% if node.resource_type == 'source' %}
      {% set identifier = node.identifier %}
    {% else %}
      {% set identifier = (node.alias if (node.alias is defined and node.alias) else node.name) %}
    {% endif %}
    {% set rel = api.Relation.create(database=node.database, schema=node.schema, identifier=identifier) %}
    {% set rel_obj = adapter.get_relation(database=rel.database, schema=rel.schema, identifier=rel.identifier) %}
    {% if rel_obj is none %}
      {% if verbose %}{{ log("[" ~ loop.index ~ "/" ~ loop.length ~ "] " ~ rel ~ ": SKIPPED (missing)", info=True) }}{% endif %}
      {% continue %}
    {% endif %}

    {% if include_schemas and (rel.schema | lower) not in (include_schemas | map('lower') | list) %}
      {% if verbose %}{{ log("[" ~ loop.index ~ "/" ~ loop.length ~ "] " ~ rel ~ ": SKIPPED (schema not included)", info=True) }}{% endif %}
      {% continue %}
    {% endif %}
    {% if exclude_schemas and (rel.schema | lower) in (exclude_schemas | map('lower') | list) %}
      {% if verbose %}{{ log("[" ~ loop.index ~ "/" ~ loop.length ~ "] " ~ rel ~ ": SKIPPED (schema excluded)", info=True) }}{% endif %}
      {% continue %}
    {% endif %}

    {% if precheck_columns %}
      {% set cols = adapter.get_columns_in_relation(rel) %}
      {% set colnames_lower = cols | map(attribute='name') | map('lower') | list %}
      {% set missing = [] %}
      {% for w in ids_in_where %}
        {% if (w | lower) not in colnames_lower %}
          {% do missing.append(w) %}
        {% endif %}
      {% endfor %}
      {% if missing | length > 0 %}
        {% if verbose %}{{ log("[" ~ loop.index ~ "/" ~ loop.length ~ "] " ~ rel ~ ": SKIPPED (columns not found: " ~ (missing | join(', ')) ~ ")", info=True) }}{% endif %}
        {% continue %}
      {% endif %}
    {% else %}
      {% set cols = adapter.get_columns_in_relation(rel) %}
    {% endif %}

    {% set check_sql %}select count(*) as record_count from {{ rel }} where {{ where_clause }}{% endset %}
    {% set run_res = run_query(check_sql) %}
    {% set cnt = (run_res.columns[0].values()[0] if run_res is not none else none) %}
    {{ log("[" ~ loop.index ~ "/" ~ loop.length ~ "] " ~ rel ~ ": " ~ (cnt if cnt is not none else 'NULL') ~ " record(s).", info=True) }}

    {% if output_columns | length > 0 and (cnt | int > 0) %}
      {% set requested_lower = output_columns | map('lower') | list %}
      {% set actual_by_lower = {} %}
      {% for c in cols %}{% do actual_by_lower.update({ (c.name | lower): c.name }) %}{% endfor %}
      {% set resolved = [] %}
      {% set missing_out = [] %}
      {% for oc in requested_lower %}
        {% if oc in actual_by_lower %}
          {% do resolved.append(actual_by_lower[oc]) %}
        {% else %}
          {% do missing_out.append(oc) %}
        {% endif %}
      {% endfor %}
      {% if missing_out | length > 0 %}
        {% if verbose %}{{ log("      └─ Skipping data print (missing: " ~ (missing_out | join(', ')) ~ ")", info=True) }}{% endif %}
      {% else %}
        {% set sample_sql %}
          select {{ resolved | join(', ') }}
          from {{ rel }}
          where {{ where_clause }}
          limit {{ max_rows | int }}
        {% endset %}
        {% set sample_res = run_query(sample_sql) %}
        {% if sample_res is not none and (sample_res.rows | length) > 0 %}
          {{ log("      └─ Sample rows (" ~ (sample_res.rows | length) ~ " of " ~ (cnt | string) ~ "):", info=True) }}
          {{ trace_record_dag__print_rows(rel|string, resolved, sample_res, _fmt) }}
        {% else %}
          {{ log("      └─ No rows returned by sample query.", info=True) }}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endfor %}
{% endmacro %}