# Documentation for macros

{% docs cte %}
[Simple CTE source](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/utils/macros.md) This is the original source code, which had some modfications.

A large portion of import statements in a SQL model are simple `SELECT * FROM table`. Writing pure SQL is verbose and this macro aims to simplify the imports.

**Args:**

* `tuple_list` The macro accepts one argument which is a list of tuples where each tuple has the alias name (1st tuple argument) and the table reference (2nd tuple argument), also by adding a 3rd tupe argument will add a where clause.
* `cte_ref_type` can be one of the three options ['ref', 'var', 'source']. `ref` is the defaulted option and does not need to be specified. Note: Selecting either of the options will apply if to `all` tables listed in the tuple
* `source_name` The name of the source that has the definition for the table name. This name is specified in the `yml` file as per below.

```yml
sources: 
  - name: <this_source_name>
    tables:
    - name: <this_table_source>
```

Below is an example and the expected output:

```sql
{% raw %}
{{ edubi_utils.cte([
    ('account','my_accounts'),
    ('users','source_users','is_active=1')])
}}

, excluded_accounts AS (

    SELECT DISTINCT
      account_id
    FROM {{ref('excluded_accounts')}}

)
{% endraw %}
```

**Output:**

```sql
{% raw %}
WITH account AS (

    SELECT * 
    FROM "PROD".my_schema.my_accounts

), users AS (

    SELECT * 
    FROM "PREP".my_source.source_users
    WHERE is_active = 1

)

, excluded_accounts AS (

    SELECT DISTINCT
      account_id
    FROM "PROD".legacy.excluded_accounts

)
{% endraw %}
```

***Examples for Using VAR***

```sql
{% raw %}
{{
    edubi_utils.cte([('source','seq_academic_achievement')],cte_ref_type='var')
}},

renames as (

    select *
    from source
)
{% endraw %}
```

dbt_project.yml

```yml
{% raw %}
vars:
  seqta_schema: dev_seed
  seqta_academic:
    seq_academic_achievement: "{{ source('seqta_sources','SEQTA_academicachievement') }}"
{% endraw %}
```

***Examples for Using Source***

```sql
{% raw %}
{{
    edubi_utils.cte([('data_test','data_case_when_is_not_null')],cte_ref_type='source', source_name='test_source')
}},

renames as (

    select *
    from data_test
)
{% endraw %}
```

schema.yml

```yml
sources:
- name: test_source
  tables:
  - name: data_case_when_is_not_null
```

{% enddocs %}

{% docs when_is_false %}

Use this to switch the logic direction. To use it pass '1::BOOLEAN' to the parameter

{% enddocs %}

{% docs predicate %}

Insert here the were clause condition for what need to be sum

{% enddocs %}
