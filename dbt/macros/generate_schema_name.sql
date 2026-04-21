{#
    Multi-tenant schema router — Phase 6.

    Mirrors `datalink.tenancy.schema_for()` so Python and dbt agree on the
    exact schema name for every (client_id, layer) tuple.

    Contract:

        client_id (var) | custom_schema_name | generated schema
        ----------------+--------------------+-------------------------------
        'default'       | 'silver'           | {target.schema}_silver
        'default'       | 'gold_um'          | {target.schema}_gold_um
        'default'       | <none>             | {target.schema}
        'acme_health'   | 'silver'           | {target.schema}_silver_ACME_HEALTH
        'acme_health'   | 'gold_um'          | {target.schema}_gold_um_ACME_HEALTH
        'acme_health'   | <none>             | {target.schema}_ACME_HEALTH

    Invoke as:

        dbt run --vars '{"client_id": "acme_health"}'

    If `client_id` is not passed, it defaults to 'default' (backward
    compatible with every Phase 5.x DAG run).

    ### Why this macro — rather than multiple dbt targets

    dbt supports per-target profiles (via `profiles.yml`), but cycling
    through 10-15 targets per client is operationally painful: each target
    needs its own Snowflake / DuckDB connection creds, and CI-time tests
    would have to enumerate them. A single macro driven by a `--vars` flag
    keeps the profile count at 1 per environment (local / dev / stage /
    prod) and scales linearly with new tenants.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set client_id = var('client_id', 'default') | string | trim | lower -%}
    {%- set suffix = '' if client_id == 'default' else '_' ~ client_id | upper -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}{{ suffix }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}{{ suffix }}
    {%- endif -%}

{%- endmacro %}
