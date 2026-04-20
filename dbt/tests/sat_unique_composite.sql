-- Custom test: every satellite must have a unique (hub_hk, load_dts) combination.
-- Returning rows = test failure.

{{ config(severity='error') }}

-- SAT_CLAIM_DETAILS
SELECT hub_claim_hk, load_dts, COUNT(*) AS n
FROM {{ ref('sat_claim_details') }}
GROUP BY hub_claim_hk, load_dts
HAVING COUNT(*) > 1

UNION ALL

-- SAT_MEMBER_DEMOGRAPHICS
SELECT hub_member_hk AS hub_claim_hk, load_dts, COUNT(*) AS n
FROM {{ ref('sat_member_demographics') }}
GROUP BY hub_member_hk, load_dts
HAVING COUNT(*) > 1

UNION ALL

-- SAT_PROVIDER_INFO
SELECT hub_provider_hk AS hub_claim_hk, load_dts, COUNT(*) AS n
FROM {{ ref('sat_provider_info') }}
GROUP BY hub_provider_hk, load_dts
HAVING COUNT(*) > 1
