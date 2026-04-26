"""Phase 13.4 — codified optimization rules for On-Prem push scripts.

The agent in 13.7 generates Postgres + SQL Server MERGE scripts. These
rules:

  1. Define what "highly optimized" means concretely (Jatin's spec).
  2. Show up verbatim in the agent prompt so the LLM produces compliant
     output on the first try.
  3. Run as a post-validator so non-compliant output gets caught before
     the operator wastes a review cycle.

Rules captured here are deliberately specific — "use MERGE not INSERT"
is not "do something idempotent". The vocabulary maps 1:1 to the SQL
features each backend exposes (PostgreSQL ``ON CONFLICT``, SQL Server
``MERGE``).

The :class:`PushOptimizationReport` returned by :func:`validate_push_sql`
carries pass/fail per rule. Score = number passing / number applicable.
A score of 1.0 means every applicable rule passed; below 0.7 the
mapper UI will surface the failures for operator action before allowing
HITL submission.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum

# ============================================================================
# RULE VOCABULARY
# ============================================================================


class TargetBackend(StrEnum):
    """Which On-Prem engine the script targets. Determines which dialect
    rules apply."""

    POSTGRES = "POSTGRES"
    SQLSERVER = "SQLSERVER"


@dataclass(frozen=True)
class OptimizationRule:
    """One named optimization rule with backend applicability + a check.

    ``regex_required`` — at least one match must appear in the SQL for
                          the rule to pass.
    ``regex_forbidden`` — none of these may appear, else the rule fails.
    ``rationale``      — text shown to the operator when the rule fails.
    """

    rule_id: str
    name: str
    applies_to: tuple[TargetBackend, ...]
    rationale: str
    regex_required: tuple[str, ...] = ()
    regex_forbidden: tuple[str, ...] = ()


# Master list — these are the rules. Edit here, the agent prompt + UI
# both pick them up automatically.
RULES: tuple[OptimizationRule, ...] = (
    # ----- 1. Idempotent MERGE / UPSERT (not INSERT) ------------------------
    OptimizationRule(
        rule_id="POSTGRES_UPSERT",
        name="Postgres uses INSERT … ON CONFLICT DO UPDATE (idempotent UPSERT)",
        applies_to=(TargetBackend.POSTGRES,),
        rationale=(
            "Plain INSERT crashes on a re-pushed primary key. ON CONFLICT "
            "lets the same script run safely after a partial failure or "
            "watermark replay. Postgres-only feature."
        ),
        regex_required=(r"\bON\s+CONFLICT\b",),
        regex_forbidden=(),  # we don't ban INSERT — ON CONFLICT IS an INSERT
    ),
    OptimizationRule(
        rule_id="MSSQL_MERGE",
        name="SQL Server uses MERGE INTO ... USING ... WHEN MATCHED",
        applies_to=(TargetBackend.SQLSERVER,),
        rationale=(
            "MERGE is the idempotent UPSERT primitive on SQL Server 2008+. "
            "Plain INSERT/UPDATE pairs are not atomic and crash on PK "
            "collisions during replay."
        ),
        regex_required=(r"\bMERGE\s+INTO\b",),
        regex_forbidden=(),
    ),
    # ----- 2. Watermark-driven incremental (no full-table scans) -----------
    OptimizationRule(
        rule_id="WATERMARK_FILTER",
        name="Watermark-driven WHERE — only push rows since last batch",
        applies_to=(TargetBackend.POSTGRES, TargetBackend.SQLSERVER),
        rationale=(
            "Full-table push every batch crushes both source and target. "
            "Filter on `effective_start_date > {last_pushed_watermark}` "
            "(parameterized) so each run only ships deltas. Watermark "
            "lives in CONTROL.egress_batch_log per (entity, target)."
        ),
        regex_required=(r"effective_start_date\s*>", r":last_watermark|\$watermark|\?watermark"),
        regex_forbidden=(),
    ),
    # ----- 3. PK-aware MERGE — no soft-key joins ---------------------------
    OptimizationRule(
        rule_id="PK_AWARE_MERGE",
        name="MERGE / UPSERT keyed on the target's primary key columns",
        applies_to=(TargetBackend.POSTGRES, TargetBackend.SQLSERVER),
        rationale=(
            "Joining on natural attributes (member_id + dob + plan_id) "
            "instead of the actual PK leads to fan-out under data drift. "
            "The agent must read the target's declared PK (passed via the "
            "tool context) and key the MERGE on that exact tuple."
        ),
        # Match both:
        #   Postgres:    ON CONFLICT (auth_id)
        #   SQL Server:  ON (tgt.auth_id = src.auth_id)
        regex_required=(r"\bON\s+(?:CONFLICT\s+)?\(",),
    ),
    # ----- 4. Batched writes — bounded transaction size --------------------
    OptimizationRule(
        rule_id="BATCH_BOUND",
        name="Script declares a batch / chunk size for bounded transactions",
        applies_to=(TargetBackend.POSTGRES, TargetBackend.SQLSERVER),
        rationale=(
            "Single-transaction MERGE of millions of rows blows undo logs "
            "and locks the target table. Wrap the MERGE in a loop or "
            "declare a `:batch_size` parameter (default 10000) so the "
            "caller can chunk."
        ),
        regex_required=(r":batch_size|\$batch_size|\?batch_size|batch_size",),
    ),
    # ----- 5. No SELECT * — explicit column list ---------------------------
    OptimizationRule(
        rule_id="EXPLICIT_COLUMNS",
        name="Explicit column list — no SELECT * in source CTE",
        applies_to=(TargetBackend.POSTGRES, TargetBackend.SQLSERVER),
        rationale=(
            "SELECT * silently breaks when source columns drift. The "
            "Phase 11 schema contract gives us the column list; the script "
            "must enumerate it explicitly so any drift is caught at compile."
        ),
        regex_forbidden=(r"\bSELECT\s+\*",),
    ),
    # ----- 6. Audit / watermark update ------------------------------------
    OptimizationRule(
        rule_id="UPDATE_WATERMARK",
        name="Script updates CONTROL.egress_batch_log with new watermark",
        applies_to=(TargetBackend.POSTGRES, TargetBackend.SQLSERVER),
        rationale=(
            "After a successful MERGE the script must record the new "
            "watermark (max effective_start_date pushed) into "
            "CONTROL.egress_batch_log so the next run picks up the right "
            "delta. Without this, every run re-pushes from epoch."
        ),
        regex_required=(r"egress_batch_log",),
    ),
)


# ============================================================================
# VALIDATOR
# ============================================================================


@dataclass
class RuleResult:
    rule_id: str
    name: str
    passed: bool
    detail: str = ""


@dataclass
class PushOptimizationReport:
    backend: TargetBackend
    results: list[RuleResult] = field(default_factory=list)

    @property
    def applicable(self) -> int:
        return len(self.results)

    @property
    def passing(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def score(self) -> float:
        if not self.results:
            return 0.0
        return self.passing / self.applicable

    @property
    def failures(self) -> list[RuleResult]:
        return [r for r in self.results if not r.passed]

    def summary(self) -> str:
        return (
            f"{self.backend.value}: {self.passing}/{self.applicable} rules pass "
            f"(score={self.score:.2f})"
        )


def validate_push_sql(sql: str, *, backend: TargetBackend) -> PushOptimizationReport:
    """Run every applicable optimization rule against the SQL.

    Pure function — no DB, no LLM. Use this both:

      * Inside the mapper UI to gate operator-level approval.
      * As a post-LLM validation step that re-prompts the agent if the
        score is below threshold.
    """
    results: list[RuleResult] = []
    sql_no_comments = _strip_sql_comments(sql)

    for rule in RULES:
        if backend not in rule.applies_to:
            continue
        passed = True
        detail = ""

        for pattern in rule.regex_required:
            if not re.search(pattern, sql_no_comments, re.IGNORECASE):
                passed = False
                detail = f"required pattern not found: /{pattern}/"
                break

        if passed:
            for pattern in rule.regex_forbidden:
                if re.search(pattern, sql_no_comments, re.IGNORECASE):
                    passed = False
                    detail = f"forbidden pattern found: /{pattern}/"
                    break

        results.append(RuleResult(rule.rule_id, rule.name, passed, detail))

    return PushOptimizationReport(backend=backend, results=results)


def rules_prompt_block(backend: TargetBackend) -> str:
    """Render the applicable rules as a prompt fragment for the LLM.

    Used by 13.7's push-script generator so the LLM produces compliant
    output on the first try (and fewer revision cycles).
    """
    lines = [f"OPTIMIZATION RULES — your script MUST satisfy ALL of these for {backend.value}:"]
    for i, rule in enumerate(RULES, start=1):
        if backend not in rule.applies_to:
            continue
        lines.append(f"  {i}. {rule.name}")
        lines.append(f"     Rationale: {rule.rationale}")
    return "\n".join(lines)


# ============================================================================
# INTERNAL
# ============================================================================


_LINE_COMMENT = re.compile(r"--[^\n]*", re.MULTILINE)
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)


def _strip_sql_comments(sql: str) -> str:
    """Strip ``--`` and ``/* */`` comments so rule-text examples in
    docstrings don't accidentally satisfy a regex_required check."""
    return _BLOCK_COMMENT.sub("", _LINE_COMMENT.sub("", sql))
