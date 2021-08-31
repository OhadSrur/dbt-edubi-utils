# Documentation for macros

{% docs simple_cte %}
[Simple CTE source](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/utils/macros.md)
Used to simplify CTE imports in a model.

A large portion of import statements in a SQL model are simple `SELECT * FROM table`. Writing pure SQL is verbose and this macro aims to simplify the imports.

The macro accepts once argument which is a list of tuples where each tuple has the alias name and the table reference.

Below is an example and the expected output:

```sql
{% raw %}
{{ simple_cte([
    ('map_merged_crm_accounts','map_merged_crm_accounts'),
    ('zuora_account','zuora_account_source'),
    ('zuora_contact','zuora_contact_source')
]) }}

, excluded_accounts AS (

    SELECT DISTINCT
      account_id
    FROM {{ref('zuora_excluded_accounts')}}

)
{% endraw %}
```

```sql
WITH map_merged_crm_accounts AS (

    SELECT * 
    FROM "PROD".common.map_merged_crm_accounts

), zuora_account AS (

    SELECT * 
    FROM "PREP".zuora.zuora_account_source

), zuora_contact AS (

    SELECT * 
    FROM "PREP".zuora.zuora_contact_source

)

, excluded_accounts AS (

    SELECT DISTINCT
      account_id
    FROM "PROD".legacy.zuora_excluded_accounts

)
```

{% enddocs %}

{% docs when_is_false %}
Use this to switch the logic direction. To use it pass '1::BOOLEAN' to the parameter
{% enddocs %}

{% docs predicate %}
Insert here the were clause condition for what need to be sum
{% enddocs %}
