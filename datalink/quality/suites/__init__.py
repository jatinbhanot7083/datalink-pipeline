"""GX expectation suites — one module per checkpoint.

Each module defines:
  - `NAME`               the suite identifier (also used as checkpoint name)
  - `SOURCE_TABLE`       qualified table the suite runs against
  - `build_suite()`      returns ExpectationSuite

Suites are the authoritative mapping of "what does Bronze/Silver/Gold
quality MEAN for this project". Modify with care — production thresholds
are derived from UM-Gold-v2 §8.2 + Medallion doc §3.4 / §4.3 / §5.2.
"""

from datalink.quality.suites.bronze_structural import (
    NAME as BRONZE_STRUCTURAL,
)
from datalink.quality.suites.bronze_structural import (
    build_suite as build_bronze_suite,
)
from datalink.quality.suites.gold_business import (
    NAME as GOLD_BUSINESS,
)
from datalink.quality.suites.gold_business import (
    build_suite as build_gold_suite,
)
from datalink.quality.suites.silver_clinical import (
    NAME as SILVER_CLINICAL,
)
from datalink.quality.suites.silver_clinical import (
    build_suite as build_silver_suite,
)

__all__ = [
    "BRONZE_STRUCTURAL",
    "GOLD_BUSINESS",
    "SILVER_CLINICAL",
    "build_bronze_suite",
    "build_gold_suite",
    "build_silver_suite",
]
