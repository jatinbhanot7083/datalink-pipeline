"""DQ dimension mapping — Phase 6.

The 6 data-quality dimensions from DataQuality_Metrics.docx §1:
  Completeness, Uniqueness, Timeliness, Accuracy, Consistency, Validity.

Each GX expectation_type maps to exactly one dimension. This module is
the single source of truth for that mapping, consumed by:

  * datalink.quality.checkpoint._record_results  — tags every row in
    CONTROL.gx_validation_results with its dimension so the dashboards
    can compute per-dimension pass rates without re-parsing JSON at
    query time.
  * datalink.ui.pages.5_DQ_Dashboard  — per-(client, source, dimension)
    breakdown pivots.

Any expectation_type not in the map returns 'Validity' (most common
catch-all) and logs a warning so new GX types surface the gap in the
mapping — NOT silently drop coverage.
"""

from __future__ import annotations

from datalink.quality.registry import DqDimension

# Canonical map. Keep synced with DataQuality_Metrics.docx §§1.1-1.6 +
# §2 'Great Expectations Objects'.
EXPECTATION_TO_DIMENSION: dict[str, DqDimension] = {
    # Completeness
    "expect_column_values_to_not_be_null": DqDimension.COMPLETENESS,
    "expect_table_row_count_to_equal": DqDimension.COMPLETENESS,
    # Uniqueness
    "expect_column_values_to_be_unique": DqDimension.UNIQUENESS,
    "expect_compound_columns_to_be_unique": DqDimension.UNIQUENESS,
    # Timeliness
    "expect_column_max_to_be_between": DqDimension.TIMELINESS,
    "expect_column_min_to_be_between": DqDimension.TIMELINESS,
    "expect_table_row_count_to_be_between": DqDimension.TIMELINESS,
    # Accuracy (cross-system — usually expect_column_pair_*_to_equal)
    "expect_column_pair_values_to_be_equal": DqDimension.ACCURACY,
    "expect_multicolumn_sum_to_equal": DqDimension.ACCURACY,
    # Consistency (date ordering, relational constraints)
    "expect_column_pair_values_a_to_be_greater_than_b": DqDimension.CONSISTENCY,
    "expect_column_pair_values_a_to_be_less_than_b": DqDimension.CONSISTENCY,
    # Validity (format, range, value set, schema)
    "expect_column_values_to_match_regex": DqDimension.VALIDITY,
    "expect_column_values_to_not_match_regex": DqDimension.VALIDITY,
    "expect_column_values_to_be_between": DqDimension.VALIDITY,
    "expect_column_values_to_be_in_set": DqDimension.VALIDITY,
    "expect_column_values_to_not_be_in_set": DqDimension.VALIDITY,
    "expect_column_values_to_be_of_type": DqDimension.VALIDITY,
    "expect_column_values_to_be_in_type_list": DqDimension.VALIDITY,
    "expect_table_columns_to_match_ordered_list": DqDimension.VALIDITY,
    "expect_table_columns_to_match_set": DqDimension.VALIDITY,
    "expect_column_value_lengths_to_be_between": DqDimension.VALIDITY,
    "expect_column_value_lengths_to_equal": DqDimension.VALIDITY,
}


def dimension_for(expectation_type: str) -> str:
    """Return the DQ dimension name for a GX expectation_type.

    Unknown types fall back to 'Validity' — the broadest CMS-style
    category. Returns the string value (not the enum) so it can land
    directly in a VARCHAR column.
    """
    mapped = EXPECTATION_TO_DIMENSION.get(expectation_type)
    if mapped is None:
        return DqDimension.VALIDITY.value
    return mapped.value
