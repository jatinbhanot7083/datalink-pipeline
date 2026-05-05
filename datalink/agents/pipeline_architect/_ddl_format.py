"""DDL line-formatting helpers — Phase 16.1 (Wave 1 Item 2).

Fixes two emitter bugs that caused runtime failures during Aetna Membership demo:

1. **Comma-in-comment** — old emitters used ``",\\n".join(col_lines)`` which
   put the column-separator comma at the END of each line. When the line had
   an inline ``-- comment``, the comma fell INSIDE the comment, so Snowflake's
   parser never saw a column separator → "SQL compilation error".

2. **Wrong nullability semantics on Bronze** — old Bronze emitter copied the
   catalog's ``requirement="Required"`` flag into ``NOT NULL``. Bronze is
   raw landing — vendor data legitimately has blanks (Medicaid-only members
   have no Medicare ID, etc.). NOT NULL belongs in Gold (or in GX
   expectations), never in Bronze.

This module provides a single ``render_create_table()`` function that handles
both correctly.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ColumnDef:
    """One column definition for a CREATE TABLE statement."""

    name: str
    sql_type: str
    nullable: bool = True  # default permissive
    comment: str = ""  # optional inline comment (no leading "--")


def render_create_table(
    *,
    fully_qualified_table: str,  # e.g. "BRONZE_AETNA.raw_membership"
    business_columns: list[ColumnDef],
    audit_columns: list[ColumnDef] | None = None,
    overflow_column: ColumnDef | None = None,
    header_comments: list[str] | None = None,
    business_section_label: str | None = None,  # e.g. "Business columns"
    audit_section_label: str = "DataLink audit columns (auto-injected)",
    overflow_section_label: str = "Variant overflow (any column not in agreed contract)",
    if_not_exists: bool = True,
) -> str:
    """Render a CREATE TABLE statement with correct comma placement.

    Comma rule (the bug we're fixing): the column-separator comma goes BEFORE
    any inline ``-- comment``, never after. Section-divider comments (whole-line
    comments that don't define a column) carry no comma at all. The final
    column has no trailing comma.

    Output structure:
        -- header_comments[0]
        -- header_comments[1]
        CREATE TABLE [IF NOT EXISTS] <fq> (
            -- ── <business_section_label> ──
            <col1>,  -- comment
            <col2>,
            <col3>,  -- comment
            -- ── <overflow_section_label> ──
            <overflow>,
            -- ── <audit_section_label> ──
            <audit1>,
            <audit2>
        );
    """
    out: list[str] = []
    for hc in header_comments or []:
        out.append(f"-- {hc}")
    create_kw = "CREATE TABLE IF NOT EXISTS" if if_not_exists else "CREATE TABLE"
    out.append(f"{create_kw} {fully_qualified_table} (")

    # Walk through columns in declaration order. We need to know which is the
    # absolute LAST column (no trailing comma on it). Build the full ordered
    # list of (kind, payload) tuples where kind is 'col' | 'divider'.
    items: list[tuple[str, ColumnDef | str]] = []
    if business_section_label:
        items.append(("divider", business_section_label))
    for c in business_columns:
        items.append(("col", c))
    if overflow_column is not None:
        items.append(("divider", overflow_section_label))
        items.append(("col", overflow_column))
    if audit_columns:
        items.append(("divider", audit_section_label))
        for c in audit_columns:
            items.append(("col", c))

    # Find the index of the LAST column (the one that gets no trailing comma).
    last_col_idx = max(
        (i for i, (k, _) in enumerate(items) if k == "col"),
        default=-1,
    )

    for i, (kind, payload) in enumerate(items):
        if kind == "divider":
            assert isinstance(payload, str)
            out.append(f"    -- ── {payload} ──")
        else:
            assert isinstance(payload, ColumnDef)
            base = f"    {payload.name:<34} {payload.sql_type:<14} "
            base += "NULL" if payload.nullable else "NOT NULL"
            is_last = i == last_col_idx
            if payload.comment:
                # Comma BEFORE the comment (the fix)
                if is_last:
                    out.append(f"{base}  -- {payload.comment}")
                else:
                    out.append(f"{base},  -- {payload.comment}")
            else:
                out.append(base if is_last else f"{base},")

    out.append(");")
    return "\n".join(out) + "\n"
