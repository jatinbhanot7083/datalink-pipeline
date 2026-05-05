{#
    Multi-tenant schema router — Phase 16.5 (Wave 5 dbt fix).

    Routes models to per-tenant + per-layer schemas based on:
      * dbt var ``client_id`` (default 'default' — back-compat with legacy)
      * model FQN — first non-package segment indicates layer (silver / gold)

    Contract:

        client_id (var) | model FQN segment   | generated schema
        ----------------+---------------------+----------------------
        'default'       | (any)               | (legacy behavior — see else branch)
        'aetna'         | datalink.silver.…   | SILVER_AETNA
        'aetna'         | datalink.gold.…     | GOLD_AETNA
        'bcbs'          | datalink.silver.…   | SILVER_BCBS
        'bcbs'          | datalink.gold.…     | GOLD_BCBS

    Invoke as:
        dbt run --vars '{"client_id": "aetna"}' --select silver.aetna.membership

    The CTAS targets BRONZE_AETNA / SILVER_AETNA / GOLD_AETNA in Snowflake,
    which is what Pipeline Architect's deploy step creates ahead of dbt run.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set client_id = var('client_id', 'default') | string | trim | lower -%}

    {%- if client_id == 'default' -%}
        {# Back-compat: legacy single-tenant flow. Preserve old behaviour
           (target.schema OR target.schema_<custom>). #}
        {%- if custom_schema_name is none -%}
            {{ target.schema }}
        {%- else -%}
            {{ target.schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- else -%}
        {# Per-tenant routing. fqn[1] is the top-level folder under models/. #}
        {%- set layer = node.fqn[1] | string | lower if node and node.fqn | length > 1 else '' -%}
        {%- if layer == 'silver' -%}
            SILVER_{{ client_id | upper }}
        {%- elif layer == 'gold' -%}
            GOLD_{{ client_id | upper }}
        {%- elif custom_schema_name is none -%}
            {{ target.schema }}_{{ client_id | upper }}
        {%- else -%}
            {{ target.schema }}_{{ custom_schema_name | trim }}_{{ client_id | upper }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}
