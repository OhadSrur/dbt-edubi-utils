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

  Notes
    - Relation resolution:
        sources      → node.identifier
        models/seeds → node.alias if set, else node.name
    - Traversal is transitive (BFS) via dbt’s dependency graph.
    - Column precheck is best-effort (simple tokenizer, no regex). Complex SQL may not be fully parsed.

  Example
    edev dbt run-operation edubi_utils.trace_record_dag --args '{
      "target_model_name": "stg_syn__student_assessment_comments",
      "where_clause": "student_assessment_comments_seq_key = 388673",
      "direction": "downstream",
      "verbose": true
    }''
-#}

{% macro trace_record_dag(
  target_model_name,
  where_clause,
  include_schemas=[],
  exclude_schemas=[],
  direction='upstream',
  verbose=false,
  precheck_columns=true
) %}
  {# Only run during execution (not parse/compile) #}
  {% if not execute %}
    {{ return("") }}
  {% endif %}

  {# Basic validation #}
  {% if where_clause is none or (where_clause | trim) == '' %}
    {% do exceptions.raise_compiler_error("trace_record_dag: 'where_clause' must be a non-empty SQL predicate.") %}
  {% endif %}

  {% set _valid_dirs = ['upstream', 'downstream'] %}
  {% set _dir = direction | lower %}
  {% if _dir not in _valid_dirs %}
    {% do exceptions.raise_compiler_error("trace_record_dag: invalid direction '" ~ direction ~ "'. Allowed: upstream, downstream") %}
  {% endif %}

  {# Resolve target model by name (friendly). If ambiguous/missing, fail with guidance.
     Fallback to project-scoped unique_id for robustness. #}
  {% set model_candidates = graph.nodes.values()
      | selectattr('resource_type','equalto','model')
      | selectattr('name','equalto', target_model_name)
      | list %}

  {% if model_candidates | length == 1 %}
    {% set target_node = model_candidates[0] %}
    {% set target_uid  = target_node.unique_id %}
  {% elif model_candidates | length > 1 %}
    {% do exceptions.raise_compiler_error("trace_record_dag: multiple models named '" ~ target_model_name ~ "'. Disambiguate by package or rename.") %}
  {% else %}
    {% set target_uid = "model." ~ project_name ~ "." ~ target_model_name %}
    {% if target_uid not in graph.nodes %}
      {% do exceptions.raise_compiler_error("trace_record_dag: model '" ~ target_model_name ~ "' not found (tried name lookup and " ~ target_uid ~ ").") %}
    {% endif %}
    {% set target_node = graph.nodes[target_uid] %}
  {% endif %}

  {# Build traversal frontier depending on direction.
     For downstream traversal we precompute a child map. #}
  {% set wanted_types = ['model', 'source', 'seed'] %}
  {% set all_nodes = graph.nodes.values() | list %}
  {% set all_sources = graph.sources.values() if graph.sources is defined else [] %}
  {% set all_nodes = all_nodes + (all_sources | list) %}

  {% if _dir == 'downstream' %}
    {# child_map: parent_uid -> [child_uid, ...] #}
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

  {# BFS traversal with a safety cap; collect nodes in 'ordered' for reporting/querying. #}
  {% set seen = set([]) %}
  {% set queue = [target_uid] %}
  {% set ordered = [] %}

  {% for _ in range(0, 2000) %}
    {% if queue | length == 0 %}
      {% break %}
    {% endif %}

    {% set uid = queue.pop(0) %}
    {% if uid in seen %}
      {% continue %}
    {% endif %}
    {% do seen.add(uid) %}

    {% set n = graph.nodes.get(uid) or graph.sources.get(uid) %}
    {% if n and n.resource_type in wanted_types %}
      {% do ordered.append(n) %}
    {% endif %}

    {% if n %}
      {% if _dir == 'upstream' %}
        {# Enqueue parents (transitive ancestry) #}
        {% if n.depends_on and n.depends_on.nodes %}
          {% for parent_uid in n.depends_on.nodes %}
            {% if parent_uid not in seen %}
              {% do queue.append(parent_uid) %}
            {% endif %}
          {% endfor %}
        {% endif %}
      {% else %}
        {# Enqueue children using child_map #}
        {% set kids = child_map.get(uid) if child_map is defined else [] %}
        {% if kids %}
          {% for child_uid in kids %}
            {% if child_uid not in seen %}
              {% do queue.append(child_uid) %}
            {% endif %}
          {% endfor %}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {# Summary header #}
  {{ log("--- Record Trace Report ---", info=True) }}
  {{ log("Target Model: " ~ target_node.name ~ " (" ~ target_uid ~ ")", info=True) }}
  {{ log("Direction: " ~ _dir, info=True) }}
  {{ log("WHERE: " ~ where_clause, info=True) }}
  {{ log("Nodes to check: " ~ ordered | length, info=True) }}
  {{ log("---------------------------", info=True) }}

  {# ---------- OPTIONAL: precheck column names referenced in WHERE (no regex) ----------
     We:
       1) Lowercase the WHERE clause.
       2) Replace punctuation/operators with spaces.
       3) Split on spaces → tokens.
       4) Drop SQL keywords and numeric literals.
       5) Keep last segment of dotted names (table.col → col).
     This is conservative and won’t parse complex SQL. #}
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
        {# If token is dotted (table . col), keep the rightmost non-empty segment #}
        {% if '.' in t %}
          {% set parts = t.split('.') %}
          {% set last = (parts | last) | trim %}
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

  {# Iterate all visited nodes (including the target itself) #}
  {% for node in ordered %}

    {# Determine the physical relation (warehouse object) #}
    {% if node.resource_type == 'source' %}
      {% set identifier = node.identifier %}
    {% else %}
      {% set identifier = (node.alias if (node.alias is defined and node.alias) else node.name) %}
    {% endif %}

    {% set rel = api.Relation.create(
      database=node.database,
      schema=node.schema,
      identifier=identifier
    ) %}

    {# ---- Relation existence check to avoid "relation does not exist" errors ---- #}
    {% set rel_obj = adapter.get_relation(
      database=rel.database,
      schema=rel.schema,
      identifier=rel.identifier
    ) %}
    {% if rel_obj is none %}
      {% if verbose %}
        {{ log("[" ~ loop.index ~ "/" ~ loop.length ~ "] " ~ node.resource_type ~ " " ~ rel ~ ": SKIPPED (relation missing)", info=True) }}
      {% endif %}
      {% continue %}
    {% endif %}

    {# Optional schema filtering #}
    {% if include_schemas and (rel.schema | lower) not in (include_schemas | map('lower') | list) %}
      {% if verbose %}
        {{ log("[" ~ loop.index ~ "/" ~ loop.length ~ "] " ~ node.resource_type ~ " " ~ rel ~ ": SKIPPED (schema not included)", info=True) }}
      {% endif %}
      {% continue %}
    {% endif %}

    {% if exclude_schemas and (rel.schema | lower) in (exclude_schemas | map('lower') | list) %}
      {% if verbose %}
        {{ log("[" ~ loop.index ~ "/" ~ loop.length ~ "] " ~ node.resource_type ~ " " ~ rel ~ ": SKIPPED (schema excluded)", info=True) }}
      {% endif %}
      {% continue %}
    {% endif %}

    {# ---- Optional column precheck per relation ---- #}
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
        {% if verbose %}
          {{ log("[" ~ loop.index ~ "/" ~ loop.length ~ "] " ~ node.resource_type ~ " " ~ rel ~ ": SKIPPED (columns not found: " ~ (missing | join(', ')) ~ ")", info=True) }}
        {% endif %}
        {% continue %}
      {% endif %}
    {% endif %}

    {# ---- Execute count with the user WHERE clause ---- #}
    {% set check_sql %}
      select count(*) as record_count
      from {{ rel }}
      where {{ where_clause }}
    {% endset %}

    {% set run_res = run_query(check_sql) %}
    {% set cnt = (run_res.columns[0].values()[0] if run_res is not none else none) %}

    {{ log("[" ~ loop.index ~ "/" ~ loop.length ~ "] " ~ node.resource_type ~ " " ~ rel ~ ": " ~ (cnt if cnt is not none else 'NULL') ~ " record(s).", info=True) }}

  {% endfor %}

  {# No return value by design (logs only) #}
{% endmacro %}