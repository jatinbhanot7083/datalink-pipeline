-- Data Vault 2.0 helpers — dialect-agnostic hashing.
-- MD5 is supported by both DuckDB and Snowflake and produces a 32-char
-- VARCHAR. We uppercase for Snowflake convention.
--
-- NULL handling: COALESCE to a sentinel so that a row with one NULL field
-- and a row with a different NULL field don't collide.

{% macro dv_hash_key(fields) %}
  UPPER(MD5(CONCAT_WS('|',
    {%- for field in fields %}
    COALESCE(CAST({{ field }} AS VARCHAR), '^^NULL^^')
    {%- if not loop.last %},{% endif %}
    {%- endfor %}
  )))
{% endmacro %}


-- Same implementation as dv_hash_key — semantically distinct (hash over
-- descriptive attributes for change detection in satellites) so we give
-- it its own name. Keeps SQL readable.
{% macro dv_hash_diff(fields) %}
  UPPER(MD5(CONCAT_WS('|',
    {%- for field in fields %}
    COALESCE(CAST({{ field }} AS VARCHAR), '^^NULL^^')
    {%- if not loop.last %},{% endif %}
    {%- endfor %}
  )))
{% endmacro %}
