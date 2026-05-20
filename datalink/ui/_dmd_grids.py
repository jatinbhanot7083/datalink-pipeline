"""DMD grids — Phase 22.

Replaces the two scrolling registry sections + the standalone "Inspect a
Global schema" picker with a unified pattern:

  * **One full-width grid per scope** (Global Medallion + Client Medallion).
  * No inner scroll bars — entire dataset list renders inline.
  * **Per-row action column** on the left: 🛠 Author / 📦 Clone icon
    buttons.  Layer-aware enable/disable (greyed out when nothing left
    to author / nothing to clone from).
  * **Per-row layer cells** double as clickable inspectors — clicking
    🥉/🥈/🥇 opens a modal with columns + AI proposal + DDL + dbt model.
    This replaces the "Inspect a Global schema" section entirely.

State machine — single source of truth in ``st.session_state["dmd_action"]``::

    {"kind": "author",   "scope": "GLOBAL_CORP", "dataset": "membership", "layer": "Silver"}
    {"kind": "clone",    "from_scope": "GLOBAL_CORP", "dataset": "membership"}
    {"kind": "view",     "scope": "GLOBAL_CORP", "dataset": "membership", "layer": "Silver"}

The "Pick a dataset" + "Cloning Center" sections lower on the DMD page read
this state to know which row was clicked.  When state is unset (no row
clicked), those sections render their default empty/help message — i.e. the
**event-driven flow** the user asked for.
"""

from __future__ import annotations

import json
import re
import uuid as _uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import streamlit as st

# Re-use colors that match the existing DMD palette so the new grids feel
# native, not bolted on.
_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_BLUE = "#2563eb"
_GREEN = "#15803d"
_AMBER = "#b45309"
_RED = "#b91c1c"
_GREY = "#6b7280"

_DMD_ACTION_KEY = "dmd_action"


# ---------------------------------------------------------------------------
# State helpers — every public function reads/writes through here.
# ---------------------------------------------------------------------------


def get_action() -> dict[str, Any] | None:
    """Return the currently-staged action, or None."""
    return st.session_state.get(_DMD_ACTION_KEY)


def clear_action() -> None:
    st.session_state.pop(_DMD_ACTION_KEY, None)


def _set_action(payload: dict[str, Any]) -> None:
    """Stage an action + force a scoped rerun.

    Scope is derived from the action ``kind``:

      * ``view`` — fragment-scoped rerun.  The grid re-renders, the modal
        opens (rendered inside the same fragment), and NOTHING ELSE on the
        page moves.  Downstream panels don't need to see a view action.
        This is what the operator expects when clicking 🥉/🥈/🥇 view —
        the page should stay put, only the modal should appear.

      * Anything else (``clone``, ``author``, …) — app-scoped rerun.  The
        downstream "Pick a dataset" / "Cloning Center" sections read the
        staged action to pre-fill themselves, so they need to re-run too.

    Earlier the helper unconditionally did app-scoped reruns, which made
    every view-click trigger a full page reflow.  Operators noticed.
    """
    st.session_state[_DMD_ACTION_KEY] = payload
    kind = str(payload.get("kind") or "").lower()
    # Modal-opening actions (view, anchor, clone) stay fragment-scoped so
    # the modal pops without a full-page reflow.  Only the legacy "author"
    # action (page-level picker, used by other surfaces) needs app scope.
    scope = "fragment" if kind in ("view", "anchor", "clone") else "app"
    try:
        st.rerun(scope=scope)  # Streamlit ≥ 1.37
    except TypeError:
        # Older Streamlit — fall back to a plain rerun (still works,
        # just loses the fragment optimization).
        st.rerun()


# ---------------------------------------------------------------------------
# Per-cell rendering helpers
# ---------------------------------------------------------------------------


def _layer_status_pill(layer_row: dict[str, Any] | None, layer_label: str) -> str:
    """Return inline HTML for a status pill — LIVE / DRAFT / — none."""
    if layer_row is None:
        return f"<span style='color:{_GREY}'>— none</span>"
    status = str(layer_row.get("status") or "")
    if status == "LIVE":
        v = layer_row.get("version") or "—"
        return (
            f"<span style='background:#dcfce7;color:{_GREEN};font-weight:600;"
            f"padding:1px 6px;border-radius:3px'>✓ LIVE v{v}</span>"
        )
    if status in ("DRAFT", "PENDING_REVIEW"):
        return (
            f"<span style='background:#fef3c7;color:{_AMBER};font-weight:600;"
            f"padding:1px 6px;border-radius:3px'>◌ {status[:1]}</span>"
        )
    if status == "ARCHIVED":
        return f"<span style='color:{_GREY}'>archived</span>"
    return f"<span style='color:{_GREY}'>{status or '—'}</span>"


def _product_lines_html(codes: list[str]) -> str:
    if not codes:
        return "<span style='color:#94a3b8;font-style:italic;font-size:.78rem'>All lines</span>"
    chips = []
    for c in codes:
        chips.append(
            f"<span style='background:#eff6ff;color:{_BLUE};font-size:.72rem;"
            f"padding:2px 7px;border-radius:10px;margin-right:3px;"
            f"border:1px solid #dbeafe;font-weight:600;'>{c}</span>"
        )
    return " ".join(chips)


# Category → (background, foreground, border) — keep palettes soft so the
# pill reads as a label, not a hard tag.  Falls back to neutral gray.
_CATEGORY_COLORS = {
    "member": ("#eff6ff", "#1d4ed8", "#bfdbfe"),  # blue
    "claims": ("#ecfdf5", "#047857", "#bbf7d0"),  # green
    "provider": ("#eef2ff", "#4338ca", "#c7d2fe"),  # indigo
    "regulatory": ("#fef2f2", "#b91c1c", "#fecaca"),  # red
    "cms": ("#fef2f2", "#b91c1c", "#fecaca"),  # red
    "quality": ("#f0fdfa", "#0f766e", "#99f6e4"),  # teal
    "hedis": ("#f0fdfa", "#0f766e", "#99f6e4"),  # teal
    "pharmacy": ("#fffbeb", "#a16207", "#fde68a"),  # amber
    "rx": ("#fffbeb", "#a16207", "#fde68a"),  # amber
    "operational": ("#fff7ed", "#c2410c", "#fed7aa"),  # orange
    "encounter": ("#fff7ed", "#c2410c", "#fed7aa"),  # orange
    "care": ("#faf5ff", "#7e22ce", "#e9d5ff"),  # purple
    "clinical": ("#faf5ff", "#7e22ce", "#e9d5ff"),  # purple
    "financial": ("#ecfdf5", "#047857", "#bbf7d0"),  # green
    "custom": ("#f8fafc", "#475569", "#e2e8f0"),  # neutral
}


def _category_pill_html(category: str) -> str:
    """Colored pill for the Category cell — palette is category-aware
    (Member=blue, Claims=green, Regulatory=red, etc.).  Falls back to a
    neutral gray pill for unknown / empty values."""
    cat = (category or "").strip()
    if not cat or cat == "—":
        return "<span style='color:#94a3b8;font-style:italic;font-size:.78rem'>—</span>"
    # Look up palette by best-effort keyword match
    cat_l = cat.lower()
    bg, fg, bd = ("#f1f5f9", "#475569", "#e2e8f0")  # default neutral
    for keyword, palette in _CATEGORY_COLORS.items():
        if keyword in cat_l:
            bg, fg, bd = palette
            break
    return (
        f"<span style='background:{bg};color:{fg};font-size:.74rem;"
        f"padding:2px 8px;border-radius:10px;border:1px solid {bd};"
        f"font-weight:600;letter-spacing:.01em;'>● {cat}</span>"
    )


def _fields_with_bar_html(count: int, max_count: int) -> str:
    """Right-aligned monospace count + mini progress bar showing relative
    size against the largest dataset in the grid.  Matches the mockup's
    'fields' column visual."""
    pct = int(round(100 * count / max_count)) if max_count else 0
    # Color tier: small (< 20% of max) = light blue, mid = blue, large = navy
    if pct < 30:
        bar_color = "#93c5fd"
    elif pct < 70:
        bar_color = "#3b82f6"
    else:
        bar_color = "#1e40af"
    return (
        f"<div style='text-align:right;'>"
        f"<div style='font-family:ui-monospace,Menlo,Consolas,monospace;"
        f"font-weight:700;font-size:.85rem;color:#0f172a;'>{count:,}</div>"
        f"<div style='width:100%;max-width:48px;height:3px;background:#f1f5f9;"
        f"border-radius:2px;margin:2px 0 0 auto;overflow:hidden;'>"
        f"<div style='width:{pct}%;height:100%;background:{bar_color};'></div>"
        f"</div></div>"
    )


# ---------------------------------------------------------------------------
# Schema inspector modal — replaces the standalone "Inspect a Global schema"
# section that used to live on the page.
# ---------------------------------------------------------------------------


def _render_view_dialog(
    *,
    inspector_fn: Callable[..., None],
    layer: str,
    row: dict[str, Any],
    cols: list[dict[str, Any]],
    entity_id: str,
) -> None:
    """Pop a modal with the existing inspector inside.  Streamlit's
    @st.dialog is the right primitive — it overlays the page without
    blowing away state."""

    @st.dialog(
        f"👁  {layer} schema — {row.get('dataset_code')}  (scope: {row.get('scope_owner')})",
        width="large",
    )
    def _modal():
        inspector_fn(layer=layer, row=row, cols=cols, entity_id=entity_id)
        if st.button("Close", key=f"close_view_{entity_id}", use_container_width=True):
            clear_action()
            # App-scoped — @st.dialog is internally a fragment, so
            # scope="fragment" reruns only the dialog body, not the outer
            # fragment that opened it → dialog never dismisses.
            st.rerun()

    _modal()


# ---------------------------------------------------------------------------
# Anchor authoring modal — DEAD CODE.  Kept temporarily for reference; not
# called anywhere.  Anchor button now triggers an in-page flow (existing
# "author" action handoff) instead of opening a modal popup.
# ---------------------------------------------------------------------------


def _render_anchor_modal(*, snap, scope: str, dataset_code: str, layer: str) -> None:
    """Modal that opens when the operator clicks ⚓ Anchor on a Silver or
    Gold cell.  Pre-fills Client / Anchor / Dataset (all DISABLED to avoid
    ambiguity, per operator request) and offers the 3 authoring modes.

    The modal is fragment-scoped (it's invoked from inside @st.fragment),
    so opening / closing / interacting does NOT trigger a page-level
    rerun — only the grid fragment re-renders.
    """

    def _on_dismiss():
        clear_action()

    @st.dialog(
        f"⚓  Author {layer} schema — {dataset_code}",
        width="large",
        on_dismiss=_on_dismiss,
    )
    def _modal():
        # ---- Pre-filled (locked) picker — Req 7 -----------------------
        st.markdown(
            "**Pick a dataset (drill into one for authoring).**  "
            "All three fields below are pre-selected for you — Client and "
            "Anchor are locked because this is the canonical baseline; "
            "Dataset is locked to the row you clicked Anchor on."
        )
        pcols = st.columns(3)
        with pcols[0]:
            st.selectbox(
                "Client",
                options=["GLOBAL_CORP"],
                index=0,
                disabled=True,
                key=f"anchor_modal_client_{dataset_code}_{layer}",
                help="Locked — the Anchor authoring flow only writes against the global "
                "(canonical) scope. Per-client variants happen via Clone.",
            )
        with pcols[1]:
            st.selectbox(
                "Anchor",
                options=["CATALOG_ANCHOR"],
                index=0,
                disabled=True,
                key=f"anchor_modal_anchor_{dataset_code}_{layer}",
                help="Locked — every canonical baseline dataset is anchored to "
                "CATALOG_ANCHOR. Cross-anchor authoring isn't supported here.",
            )
        with pcols[2]:
            st.selectbox(
                "Dataset",
                options=[dataset_code],
                index=0,
                disabled=True,
                key=f"anchor_modal_dataset_{dataset_code}_{layer}",
                help="Locked — pre-filled from the row you clicked Anchor on.",
            )

        st.markdown("---")
        st.markdown(f"### 🎨  Choose an authoring mode for **{layer}**")
        st.caption(
            f"All three modes write a DRAFT row to the global {layer} schema "
            "table.  Promote DRAFT → PENDING_REVIEW → LIVE from the main "
            "DMD page once the schema is ready."
        )

        mode_cols = st.columns(3)
        with mode_cols[0]:
            ai_clicked = st.button(
                "🤖  AI Construct",
                key=f"anchor_modal_mode_ai_{dataset_code}_{layer}",
                type="primary",
                use_container_width=True,
                help="LLM-driven schema proposal — Claude Haiku 4.5 + Voyage 3.5-lite RAG "
                "reads the Bronze field list and proposes a normalized "
                f"{layer} schema with column renames, type inference, and PHI flags.",
            )
        with mode_cols[1]:
            manual_clicked = st.button(
                "✍️  Manual Author",
                key=f"anchor_modal_mode_manual_{dataset_code}_{layer}",
                use_container_width=True,
                help="Hand-author every column.  Start with an empty DRAFT and add "
                "columns one at a time via the column editor on the main DMD page.",
            )
        with mode_cols[2]:
            upload_clicked = st.button(
                "📂  Upload Schema",
                key=f"anchor_modal_mode_upload_{dataset_code}_{layer}",
                use_container_width=True,
                help="Upload a CSV / DDL / JSON file describing the schema.  The "
                f"loader will create a DRAFT {layer} schema from your file.",
            )

        if ai_clicked or manual_clicked or upload_clicked:
            mode = "AI" if ai_clicked else ("MANUAL" if manual_clicked else "UPLOAD")
            # Stash the chosen mode + targets in session_state so the main
            # DMD page's authoring section picks it up + jumps into the
            # right flow.  Then close the modal.
            st.session_state["_dmd_pending_author"] = {
                "scope": "GLOBAL_CORP",
                "anchor": "CATALOG_ANCHOR",
                "dataset": dataset_code,
                "layer": layer,
                "mode": mode,
            }
            clear_action()
            # MUST be app-scoped (no scope= or scope="app") to dismiss
            # the dialog.  @st.dialog is INTERNALLY wrapped as a fragment
            # by Streamlit, so st.rerun(scope="fragment") only re-runs
            # the dialog's own fragment (re-rendering the SAME buttons,
            # making them appear non-responsive).  app-scoped rerun is
            # the documented way to programmatically close a dialog —
            # see streamlit/elements/dialog_decorator.py line 167-168.
            st.rerun()

        st.markdown("---")
        if st.button(
            "Cancel",
            key=f"anchor_modal_cancel_{dataset_code}_{layer}",
            use_container_width=True,
        ):
            clear_action()
            st.rerun()

    _modal()


# ---------------------------------------------------------------------------
# Clone modal — one-click clone of LIVE schemas to a chosen client
# ---------------------------------------------------------------------------


def _render_clone_modal(*, snap, dataset_code: str, from_scope: str) -> None:
    """Modal that opens when the operator clicks 📦 Clone on a row.  Pure
    one-click flow: source dataset locked + target client dropdown + Clone
    button.  Per operator Req 9: keep simple for now.

    Existing clients come from CONTROL.client_pipeline_instances (via the
    snap).  If none exist, the operator types a new client_id."""

    def _on_dismiss():
        clear_action()

    @st.dialog(
        f"📦  Clone — {dataset_code}",
        width="large",
        on_dismiss=_on_dismiss,
    )
    def _modal():
        st.markdown(
            f"**Clone the global LIVE Silver + Gold schemas for `{dataset_code}` "
            f"down to a specific client.**  The clone is concurrency-checked "
            f"per source row — if the source changes mid-flight, the clone "
            f"aborts so you don't end up with a half-cloned client."
        )

        # Source — locked
        src_cols = st.columns([1, 1])
        with src_cols[0]:
            st.text_input(
                "Source dataset",
                value=dataset_code,
                disabled=True,
                key=f"clone_modal_src_{dataset_code}",
                help="Locked — the row you clicked Clone on.",
            )
        with src_cols[1]:
            st.text_input(
                "Source scope",
                value=from_scope,
                disabled=True,
                key=f"clone_modal_scope_{dataset_code}",
                help="Locked — Global is the only valid source for canonical clones.",
            )

        # Target client — pick existing OR type new
        existing_clients = sorted(set(snap.distinct_clients or []))
        st.markdown("---")
        st.markdown("**Target client**")
        target_mode = st.radio(
            "Target",
            options=["Pick existing client", "Add NEW client"],
            horizontal=True,
            key=f"clone_modal_target_mode_{dataset_code}",
            label_visibility="collapsed",
        )
        if target_mode == "Pick existing client" and existing_clients:
            target_client = st.selectbox(
                "Existing client",
                options=existing_clients,
                key=f"clone_modal_existing_{dataset_code}",
                label_visibility="collapsed",
            )
        elif target_mode == "Pick existing client" and not existing_clients:
            st.info(
                "No clients registered yet — switch to **Add NEW client** to "
                "create the first one.",
                icon="ℹ️",
            )
            target_client = None
        else:
            target_client = st.text_input(
                "New client id (lowercase, alphanumeric + underscore)",
                placeholder="e.g. cano_health",
                key=f"clone_modal_new_{dataset_code}",
                label_visibility="collapsed",
            )
            if target_client:
                import re as _re

                if not _re.fullmatch(r"[a-z][a-z0-9_]*", target_client):
                    st.warning(
                        "Client id must start with a lowercase letter and "
                        "contain only lowercase letters, digits, and underscores."
                    )
                    target_client = None

        st.markdown("---")
        action_cols = st.columns([1, 1])
        with action_cols[0]:
            clone_clicked = st.button(
                "📦  Clone now",
                key=f"clone_modal_do_{dataset_code}",
                type="primary",
                disabled=not bool(target_client),
                use_container_width=True,
                help="Copies the LIVE Silver + Gold schemas for this dataset "
                "down to the chosen client.  Existing client schemas on "
                "this dataset are NOT touched — clone aborts if a row "
                "already exists (concurrency-safe).",
            )
        with action_cols[1]:
            cancel_clicked = st.button(
                "Cancel",
                key=f"clone_modal_cancel_{dataset_code}",
                use_container_width=True,
            )

        if cancel_clicked:
            clear_action()
            # App-scoped rerun — @st.dialog is internally wrapped as a
            # fragment, so fragment-scoped rerun only re-runs the dialog
            # body (button stays "clicked" forever).  See st.dialog docs.
            st.rerun()

        if clone_clicked and target_client:
            st.session_state["_dmd_pending_clone"] = {
                "dataset": dataset_code,
                "from_scope": from_scope,
                "to_client": target_client,
            }
            clear_action()
            st.rerun()

    _modal()


# ---------------------------------------------------------------------------
# Public: Global Medallion grid
# ---------------------------------------------------------------------------


@st.fragment
def render_global_medallion(
    *,
    snap,
    product_lines_by_dataset: dict[str, list[str]] | None = None,
    inspector_fn: Callable[..., None] | None = None,
) -> None:
    """Render the consolidated Global Medallion Schema grid.

    Replaces (a) the counter trio + (b) the scrolling dataframe + (c) the
    standalone "Inspect a Global schema" picker.

    Args:
        snap: dmd Snapshot (with .bronze_datasets / .silver_schemas / etc.)
        product_lines_by_dataset: optional override; falls back to
            ``snap.product_lines_by_dataset`` if present.
        inspector_fn: the existing ``_render_schema_inspector`` closure from
            the DMD page (kept there because it owns several closures over
            session state).  When None, layer cells still render but the
            modal won't open — clicking is a no-op.
    """
    if product_lines_by_dataset is None:
        product_lines_by_dataset = getattr(snap, "product_lines_by_dataset", {}) or {}

    _g_silver = [s for s in snap.silver_schemas if _is_global(s)]
    _g_gold = [g for g in snap.gold_schemas if _is_global(g)]

    # ---- Header --------------------------------------------------------
    st.markdown("## 🌍 Global Medallion Schema")
    st.caption(
        "Canonical templates that real clients clone from.  One row per "
        "Bronze dataset — Bronze is always present (read-only from the "
        "Product Catalogue); Silver + Gold are designable. "
        "🛠 Author works on a layer that doesn't exist yet.  📦 Clone "
        "copies the LIVE template down to a specific client.  "
        "Click 🥉 / 🥈 / 🥇 to view the schema."
    )

    # ---- 📊 KPI strip — 4 cards (dropped confusing "Catalog Health") ----
    bronze_count = len(snap.bronze_datasets)
    total_fields = sum(int(d.get("total_fields") or 0) for d in snap.bronze_datasets)
    g_silver_live = sum(1 for s in _g_silver if s.get("status") == "LIVE")
    g_silver_draft = sum(1 for s in _g_silver if s.get("status") in ("DRAFT", "PENDING_REVIEW"))
    g_gold_live = sum(1 for g in _g_gold if g.get("status") == "LIVE")
    g_gold_draft = sum(1 for g in _g_gold if g.get("status") in ("DRAFT", "PENDING_REVIEW"))

    silver_pct = int(round(100 * g_silver_live / bronze_count)) if bronze_count else 0
    gold_pct = int(round(100 * g_gold_live / bronze_count)) if bronze_count else 0

    st.markdown(
        f"""
        <style>
          .kpi-strip {{ display: flex; gap: 0.6rem; margin: 0.4rem 0 0.8rem 0; }}
          .kpi-card {{
            flex: 1; background: white;
            border: 1px solid #e2e8f0; border-radius: 8px;
            padding: 0.7rem 0.9rem 0.8rem 0.9rem;
            box-shadow: 0 1px 2px rgba(15,23,42,0.04);
            min-width: 0;
          }}
          .kpi-label {{
            font-size: 0.68rem; color: #64748b; font-weight: 600;
            letter-spacing: 0.08em; text-transform: uppercase;
            margin-bottom: 0.3rem;
          }}
          .kpi-value {{
            font-size: 1.65rem; color: #0f172a; font-weight: 700;
            line-height: 1.1; margin-bottom: 0.1rem;
            font-variant-numeric: tabular-nums;
          }}
          .kpi-value .kpi-denom {{
            font-size: 0.95rem; color: #94a3b8; font-weight: 500;
          }}
          .kpi-sub {{
            font-size: 0.72rem; color: #64748b; margin-top: 0.3rem;
          }}
          .kpi-sub .kpi-good {{ color: #16a34a; font-weight: 600; }}
          .kpi-bar-track {{
            width: 100%; height: 5px; background: #f1f5f9;
            border-radius: 3px; overflow: hidden; margin-top: 0.45rem;
          }}
          .kpi-bar-fill {{ height: 100%; border-radius: 3px; }}
        </style>
        <div class="kpi-strip">
          <div class="kpi-card">
            <div class="kpi-label">Total Datasets</div>
            <div class="kpi-value">{bronze_count:,}</div>
            <div class="kpi-sub">{total_fields:,} fields total</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">🥉 Bronze Coverage</div>
            <div class="kpi-value">{bronze_count:,}<span class="kpi-denom"> / {bronze_count:,}</span></div>
            <div class="kpi-bar-track"><div class="kpi-bar-fill"
              style="width:100%;background:linear-gradient(90deg,#f59e0b,#d97706);"></div></div>
            <div class="kpi-sub">100% landed from Product Catalogue</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">🥈 Silver Coverage</div>
            <div class="kpi-value">{g_silver_live:,}<span class="kpi-denom"> / {bronze_count:,}</span></div>
            <div class="kpi-bar-track"><div class="kpi-bar-fill"
              style="width:{silver_pct}%;background:linear-gradient(90deg,#64748b,#334155);"></div></div>
            <div class="kpi-sub">{silver_pct}% conformed{f" · {g_silver_draft} in draft" if g_silver_draft else ""}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">🥇 Gold Coverage</div>
            <div class="kpi-value">{g_gold_live:,}<span class="kpi-denom"> / {bronze_count:,}</span></div>
            <div class="kpi-bar-track"><div class="kpi-bar-fill"
              style="width:{gold_pct}%;background:linear-gradient(90deg,#eab308,#ca8a04);"></div></div>
            <div class="kpi-sub">{gold_pct}% productized{f" · {g_gold_draft} in draft" if g_gold_draft else ""}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("")
    # Banded section header with background + left-bar accent + 🔄 refresh
    # on the right.  The refresh button clears the snap cache and reruns
    # ONLY this fragment — so a Wipe/Upload elsewhere doesn't force a
    # full-page reload; operator pulls fresh data here on demand.
    hdr_cols = st.columns([10, 1.2])
    with hdr_cols[0]:
        st.markdown(
            f"""
            <div style="
              background: linear-gradient(90deg, #e0e7ff 0%, #f1f5f9 60%, transparent 100%);
              border-left: 4px solid {_NAVY};
              padding: 0.55rem 0.9rem;
              margin: 0.4rem 0 0.6rem 0;
              border-radius: 4px;
            ">
              <span style="font-size:1.05rem;font-weight:700;color:{_NAVY};
                           letter-spacing:.02em;">
                📋 Global Medallion Schema — datasets
              </span>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with hdr_cols[1]:
        if st.button(
            "🔄 Refresh",
            key="gms_section_refresh",
            help="Re-fetch from CONTROL — use after a wipe / upload to see fresh rows.  "
            "This is the ONE action that fully re-runs the page (so KPI cards + "
            "grid + filter counts all sync to the new data).  All other "
            "interactions (Arm wipe, Role, dialog open/close) stay scoped.",
            use_container_width=True,
        ):
            # Clear the snap cache + force a full rerun so the parent page
            # re-fetches and passes a fresh snap down.  This is the only
            # button on the page that intentionally does an app-scoped
            # rerun — because the user *asked for* fresh data.
            try:
                from datalink.ui._dmd_data import _fetch_snapshot_uncached

                _fetch_snapshot_uncached.clear()
            except Exception:
                pass
            st.rerun()

    if not snap.bronze_datasets:
        # Phase 22 — empty-state banner.  This is the default demo entry:
        # 0 / 0 / 0 until the operator clicks Upload catalogue above.
        st.markdown(
            """
            <div style='border: 2px dashed #cbd5e1; border-radius: 8px;
                        padding: 2rem; text-align: center; background: #f8fafc;
                        margin: 1rem 0;'>
                <div style='font-size: 2.5rem; opacity: 0.5;'>📭</div>
                <h4 style='color: #475569; margin: 0.5rem 0;'>No Global Schemas yet</h4>
                <p style='color: #64748b; margin: 0;'>
                    Use <strong>📂 Upload catalogue</strong> in the banner above to
                    seed the 33 canonical datasets in one click.
                    <br>
                    Or upload a single-sheet mapping spec / raw CSV/JSON to register
                    one dataset at a time.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    # ---- Pivot snap → per-dataset row ---------------------------------
    _by_code_silver: dict[str, dict[str, Any]] = {}
    for s in _g_silver:
        if str(s.get("status")) == "ARCHIVED":
            continue
        code = str(s.get("dataset_code"))
        cur = _by_code_silver.get(code)
        if cur is None or int(s.get("version") or 0) > int(cur.get("version") or 0):
            _by_code_silver[code] = s

    _by_code_gold: dict[str, dict[str, Any]] = {}
    for g in _g_gold:
        if str(g.get("status")) == "ARCHIVED":
            continue
        code = str(g.get("dataset_code"))
        cur = _by_code_gold.get(code)
        if cur is None or int(g.get("version") or 0) > int(cur.get("version") or 0):
            _by_code_gold[code] = g

    # ---- Filter bar: search + category + tier --------------------------
    # CSS to make Streamlit's filter widgets feel like a single bar
    st.markdown(
        """
        <style>
          .filter-bar-label {
            font-size: 0.7rem; color: #64748b; font-weight: 600;
            letter-spacing: 0.06em; text-transform: uppercase;
            margin-bottom: 0.2rem;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )
    fb1, fb2, fb3, fb4 = st.columns([2.8, 2, 2, 1.2])
    with fb1:
        st.markdown("<div class='filter-bar-label'>🔍 Search</div>", unsafe_allow_html=True)
        _filter_text = st.text_input(
            "Search",
            placeholder="Filter by name, code, or category…",
            key="med_filter_text",
            label_visibility="collapsed",
        )
    with fb2:
        st.markdown("<div class='filter-bar-label'>Category</div>", unsafe_allow_html=True)
        _all_cats = sorted(
            {str(d.get("category") or "—") for d in snap.bronze_datasets if d.get("category")}
        )
        _filter_cats = st.multiselect(
            "Category",
            options=_all_cats,
            default=[],
            placeholder="All categories",
            key="med_filter_cats",
            label_visibility="collapsed",
        )
    with fb3:
        st.markdown("<div class='filter-bar-label'>Tier completion</div>", unsafe_allow_html=True)
        _filter_tier = st.selectbox(
            "Tier",
            options=[
                "All datasets",
                "✓ Gold LIVE only",
                "✓ Silver LIVE only",
                "◌ Has any DRAFT",
                "○ Bronze-only (nothing above)",
            ],
            index=0,
            key="med_filter_tier",
            label_visibility="collapsed",
        )
    with fb4:
        st.markdown("<div class='filter-bar-label'>&nbsp;</div>", unsafe_allow_html=True)
        if st.button("Clear filters", key="med_clear_filters", use_container_width=True):
            for k in ("med_filter_text", "med_filter_cats", "med_filter_tier"):
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun(scope="fragment")

    # ---- Anchor focus mode (overrides all other filters) ---------------
    # When the operator clicked ⚓ Anchor on a row, collapse the grid to
    # show ONLY that one row and surface a banner with a "Show all"
    # button to exit anchor mode.  The picker + design-activities below
    # the grid pre-fill with that dataset (via the existing "author"
    # handoff in 14_Data_Model_Designer.py around line 1411).
    _anchor_focus = st.session_state.get("_dmd_anchor_focus")
    if isinstance(_anchor_focus, dict) and _anchor_focus.get("dataset"):
        _focus_ds = str(_anchor_focus.get("dataset"))
        _focus_layer = str(_anchor_focus.get("layer") or "Silver")
        st.info(
            f"⚓ **Anchor mode** — showing only **{_focus_ds}** · "
            f"target layer: **{_focus_layer}**.  The authoring picker below "
            f"is pre-filled with this dataset (Client = GLOBAL_CORP, Anchor "
            f"= CATALOG_ANCHOR).  Scroll down to **🎯 Pick a dataset (drill "
            f"into one for authoring)** section to continue authoring.",
            icon="⚓",
        )
        if st.button(
            "↩ Show all datasets",
            key="dmd_anchor_focus_exit",
            help="Exit anchor mode and show the full Global Medallion grid again.",
        ):
            st.session_state.pop("_dmd_anchor_focus", None)
            st.rerun()
        # Filter to ONLY the focused dataset row
        _filtered_datasets = [
            d for d in snap.bronze_datasets if str(d.get("dataset_code")) == _focus_ds
        ]
        # Skip the rest of the filter UI entirely while in anchor mode
        _render_grid_rows(
            snap=snap,
            datasets=_filtered_datasets,
            silver_by_code=_by_code_silver,
            gold_by_code=_by_code_gold,
            product_lines_by_dataset=product_lines_by_dataset,
            scope="GLOBAL_CORP",
            client_id_label="—",
            show_client_col=False,
            inspector_fn=inspector_fn,
            key_prefix="g",
        )
        return  # done — don't render the search/filter UI on top of anchor mode

    # ---- Apply filters --------------------------------------------------
    _filtered_datasets = list(snap.bronze_datasets)
    if _filter_text:
        q = _filter_text.lower().strip()
        _filtered_datasets = [
            d
            for d in _filtered_datasets
            if q in str(d.get("display_name") or "").lower()
            or q in str(d.get("dataset_code") or "").lower()
            or q in str(d.get("category") or "").lower()
        ]
    if _filter_cats:
        _filtered_datasets = [
            d for d in _filtered_datasets if str(d.get("category") or "—") in _filter_cats
        ]
    if _filter_tier != "All datasets":

        def _matches_tier(d):
            code = str(d.get("dataset_code"))
            s = _by_code_silver.get(code)
            g = _by_code_gold.get(code)
            s_status = (s or {}).get("status")
            g_status = (g or {}).get("status")
            if _filter_tier == "✓ Gold LIVE only":
                return g_status == "LIVE"
            if _filter_tier == "✓ Silver LIVE only":
                return s_status == "LIVE" and g_status != "LIVE"
            if _filter_tier == "◌ Has any DRAFT":
                return s_status in ("DRAFT", "PENDING_REVIEW") or g_status in (
                    "DRAFT",
                    "PENDING_REVIEW",
                )
            if _filter_tier == "○ Bronze-only (nothing above)":
                return s is None and g is None
            return True

        _filtered_datasets = [d for d in _filtered_datasets if _matches_tier(d)]

    # Counter row
    n_shown = len(_filtered_datasets)
    n_total = len(snap.bronze_datasets)
    if n_shown < n_total:
        st.caption(
            f"Showing **{n_shown:,}** of {n_total:,} datasets · "
            f"filters active. Click **Clear filters** to reset."
        )

    # ---- Render the grid -----------------------------------------------
    _render_grid_rows(
        snap=snap,
        datasets=_filtered_datasets,
        silver_by_code=_by_code_silver,
        gold_by_code=_by_code_gold,
        product_lines_by_dataset=product_lines_by_dataset,
        scope="GLOBAL_CORP",
        client_id_label="—",
        show_client_col=False,
        inspector_fn=inspector_fn,
        key_prefix="g",
    )


# ---------------------------------------------------------------------------
# Public: Client Medallion grid
# ---------------------------------------------------------------------------


@st.fragment
def render_client_medallion(
    *,
    snap,
    product_lines_by_dataset: dict[str, list[str]] | None = None,
    inspector_fn: Callable[..., None] | None = None,
    additional_clients: list[str] | None = None,
) -> None:
    """Render the Client Medallion Schema — all clients, expand/collapse,
    same per-row pattern as Global.

    HIDDEN when the operator is in ⚓ Anchor mode (focused on a single
    dataset in the Global grid).  Per operator request: when anchoring,
    the Pick-a-dataset section should appear IMMEDIATELY below the
    focused row, with no Client Medallion section in between.
    """
    if st.session_state.get("_dmd_anchor_focus"):
        return  # silently hide while in anchor mode

    if product_lines_by_dataset is None:
        product_lines_by_dataset = getattr(snap, "product_lines_by_dataset", {}) or {}

    # ---- Header — banded section + 🔄 Refresh button on the right ------
    cmh_cols = st.columns([10, 1.2])
    with cmh_cols[0]:
        st.markdown("## 🤝 Client Medallion Schema")
        st.caption(
            "Per-client schemas, including clients with NO authored content yet "
            "(so you can author from scratch or clone from Global).  Expand a "
            "client to see all datasets · 📦 Clone / ⚓ Anchor enabled where "
            "applicable for that (client, dataset) pair."
        )
    with cmh_cols[1]:
        st.markdown("&nbsp;")  # alignment spacer so button drops below the title
        if st.button(
            "🔄 Refresh",
            key="cmr_section_refresh",
            help="Re-fetch from CONTROL — pulls fresh client schemas after a "
            "Clone/Author/Wipe. ONE full rerun (operator-initiated, expected).",
            use_container_width=True,
        ):
            try:
                from datalink.ui._dmd_data import _fetch_snapshot_uncached

                _fetch_snapshot_uncached.clear()
            except Exception:
                pass
            st.rerun()

    # 📊 Client Medallion Status — counters strip (mirrors the Global section).
    st.markdown("### 📊 Client Medallion Status")

    # Per-client Bronze/Silver/Gold counts (LIVE only — that's what matters
    # for the headline counter).
    bronze_live_clients: set[str] = set(snap.distinct_clients or [])
    silver_live_clients: set[str] = set()
    gold_live_clients: set[str] = set()
    for s in snap.silver_schemas:
        scope = str(s.get("scope_owner") or "")
        if scope in ("GLOBAL_CORP", "__global__", "", "default"):
            continue
        if s.get("status") == "LIVE":
            silver_live_clients.add(scope)
    for g in snap.gold_schemas:
        scope = str(g.get("scope_owner") or "")
        if scope in ("GLOBAL_CORP", "__global__", "", "default"):
            continue
        if g.get("status") == "LIVE":
            gold_live_clients.add(scope)

    c1, c2, c3 = st.columns(3)
    c1.markdown(
        f'<div class="gd-card"><div class="gd-stat-emoji">🥉</div>'
        f'<div class="gd-stat">{len(bronze_live_clients)}</div>'
        f'<div class="gd-stat-label">Total Bronze LIVE clients</div></div>',
        unsafe_allow_html=True,
    )
    c2.markdown(
        f'<div class="gd-card"><div class="gd-stat-emoji">🥈</div>'
        f'<div class="gd-stat">{len(silver_live_clients)}</div>'
        f'<div class="gd-stat-label">Total Silver LIVE clients</div></div>',
        unsafe_allow_html=True,
    )
    c3.markdown(
        f'<div class="gd-card"><div class="gd-stat-emoji">🥇</div>'
        f'<div class="gd-stat">{len(gold_live_clients)}</div>'
        f'<div class="gd-stat-label">Total Gold LIVE clients</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown("")

    # Build per-client {dataset_code → latest non-archived silver/gold}
    all_clients: list[str] = sorted(
        set(snap.distinct_clients or []) | set(additional_clients or [])
    )
    if not all_clients:
        st.info(
            "No clients registered yet.  Clients appear here automatically when "
            "you clone a Global schema down to a client (use 📦 Clone on the "
            "Global grid above)."
        )
        return

    per_client_silver: dict[str, dict[str, dict[str, Any]]] = {c: {} for c in all_clients}
    per_client_gold: dict[str, dict[str, dict[str, Any]]] = {c: {} for c in all_clients}
    for s in snap.silver_schemas:
        scope = str(s.get("scope_owner") or "")
        if scope in ("GLOBAL_CORP", "__global__", "", "default"):
            continue
        if str(s.get("status")) == "ARCHIVED":
            continue
        d = per_client_silver.setdefault(scope, {})
        code = str(s.get("dataset_code"))
        cur = d.get(code)
        if cur is None or int(s.get("version") or 0) > int(cur.get("version") or 0):
            d[code] = s
    for g in snap.gold_schemas:
        scope = str(g.get("scope_owner") or "")
        if scope in ("GLOBAL_CORP", "__global__", "", "default"):
            continue
        if str(g.get("status")) == "ARCHIVED":
            continue
        d = per_client_gold.setdefault(scope, {})
        code = str(g.get("dataset_code"))
        cur = d.get(code)
        if cur is None or int(g.get("version") or 0) > int(cur.get("version") or 0):
            d[code] = g

    # Render one expander per client
    for client_id in all_clients:
        s_by_code = per_client_silver.get(client_id, {})
        g_by_code = per_client_gold.get(client_id, {})
        live_s = sum(1 for v in s_by_code.values() if v.get("status") == "LIVE")
        live_g = sum(1 for v in g_by_code.values() if v.get("status") == "LIVE")
        draft_s = sum(
            1 for v in s_by_code.values() if v.get("status") in ("DRAFT", "PENDING_REVIEW")
        )
        draft_g = sum(
            1 for v in g_by_code.values() if v.get("status") in ("DRAFT", "PENDING_REVIEW")
        )
        summary_bits = []
        if live_s:
            summary_bits.append(f"🥈 {live_s} LIVE")
        if draft_s:
            summary_bits.append(f"🥈 {draft_s} draft")
        if live_g:
            summary_bits.append(f"🥇 {live_g} LIVE")
        if draft_g:
            summary_bits.append(f"🥇 {draft_g} draft")
        summary = "  ·  ".join(summary_bits) if summary_bits else "no authored schemas yet"
        label = f"**{client_id.upper()}**  ·  {summary}"

        is_expanded_default = bool(live_s or live_g or draft_s or draft_g)
        with st.expander(label, expanded=is_expanded_default):
            _render_grid_rows(
                snap=snap,
                datasets=snap.bronze_datasets,
                silver_by_code=s_by_code,
                gold_by_code=g_by_code,
                product_lines_by_dataset=product_lines_by_dataset,
                scope=client_id,
                client_id_label=client_id,
                show_client_col=False,  # implicit — we're inside that client's expander
                inspector_fn=inspector_fn,
                key_prefix=f"c_{client_id}",
            )


# ---------------------------------------------------------------------------
# Internal: shared grid renderer used by both Global and Client
# ---------------------------------------------------------------------------


def _is_global(s: dict[str, Any]) -> bool:
    return str(s.get("scope_owner") or "") in ("GLOBAL_CORP", "__global__")


def _render_grid_rows(
    *,
    snap,
    datasets: list[dict[str, Any]],
    silver_by_code: dict[str, dict[str, Any]],
    gold_by_code: dict[str, dict[str, Any]],
    product_lines_by_dataset: dict[str, list[str]],
    scope: str,
    client_id_label: str,
    show_client_col: bool,
    inspector_fn: Callable[..., None] | None,
    key_prefix: str,
) -> None:
    """Render the per-row grid.  Same column layout for Global + Client.

    Columns: 🛠 Author · 📦 Clone · Dataset · Code · Category · Product Lines
             · 🥉 Bronze · 🥈 Silver · 🥇 Gold · Bronze fields

    Phase 22 — bordered rows: each data row is wrapped in
    ``st.container(border=True)`` so the layout reads as a proper table
    with gridlines, not a flexbox.  The header row gets a navy bar.
    """
    # Column widths (relative) — simplified 7-column layout per operator feedback:
    #   [Clone] | Dataset | Product lines | 🥉 Bronze | 🥈 Silver | 🥇 Gold | Fields
    # Removed: separate Author button (merged into Anchor click in layer cells),
    #          Code column (redundant with Dataset name), Category column
    #          (not needed — anchor metadata lives in CATALOG_ANCHOR semantics).
    widths = [0.9, 2.2, 1.5, 1.1, 1.2, 1.2, 0.7]
    headers = [
        ("", "header-actions", "Clone this dataset to a client (one-click)"),
        (
            "Dataset",
            "header-text",
            "Human-readable dataset name from the canonical Product Catalogue",
        ),
        (
            "Product lines",
            "header-text",
            "Which downstream products consume this dataset (CC, E360, RBN, …)",
        ),
        (
            "🥉 Bronze",
            "header-layer",
            "Click LIVE to inspect the canonical Bronze field list (read-only)",
        ),
        (
            "🥈 Silver",
            "header-layer",
            "Click Anchor to author this layer; LIVE pill if already authored",
        ),
        (
            "🥇 Gold",
            "header-layer",
            "Click Anchor to author this layer; LIVE pill if already authored",
        ),
        ("Fields", "header-num", "Total Bronze field count from the Product Catalogue"),
    ]

    # ---- Enterprise-grade table CSS — targets Streamlit's auto-generated
    # .st-key-<key> classes from st.container(key="..."), so the styling
    # actually nests properly (raw markdown <div>'s don't compose with
    # subsequent Streamlit components — that mistake was made and fixed).
    st.markdown(
        f"""
        <style>
          /* Outer table wrapper — single border around the whole grid */
          .st-key-{key_prefix}_med_table {{
            border: 1px solid #cbd5e1;
            border-radius: 6px;
            overflow: hidden;
            margin: 0.3rem 0 0.6rem 0;
            background: white;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06);
          }}
          /* Kill the inner gap Streamlit puts between stacked containers */
          .st-key-{key_prefix}_med_table div[data-testid="stVerticalBlock"] {{
            gap: 0 !important;
          }}

          /* Navy header band */
          .st-key-{key_prefix}_med_header {{
            background: {_NAVY} !important;
            padding: 0.6rem 0.5rem !important;
            border-bottom: 2px solid #1e3a5f;
          }}
          .st-key-{key_prefix}_med_header * {{
            color: white !important;
          }}
          .st-key-{key_prefix}_med_header p {{
            margin: 0 !important;
            font-weight: 700 !important;
            font-size: 0.92rem !important;
            letter-spacing: 0.05em !important;
            text-transform: uppercase !important;
          }}

          /* Body rows — zebra striping + hairline bottom border.
             Each row key looks like st-key-{key_prefix}_med_row_<parity>_<idx>
             so we use attribute-contains selectors to match the parity. */
          [class*="st-key-{key_prefix}_med_row_odd_"] {{
            background: white;
            border-bottom: 1px solid #e2e8f0;
            padding: 0.35rem 0.55rem !important;
            transition: background-color 0.12s ease;
          }}
          [class*="st-key-{key_prefix}_med_row_even_"] {{
            background: #f8fafc;
            border-bottom: 1px solid #e2e8f0;
            padding: 0.35rem 0.55rem !important;
            transition: background-color 0.12s ease;
          }}
          [class*="st-key-{key_prefix}_med_row_odd_"]:hover,
          [class*="st-key-{key_prefix}_med_row_even_"]:hover {{
            background: #eff6ff !important;
          }}
          /* Compact buttons inside table cells */
          .st-key-{key_prefix}_med_table button {{
            padding: 0.2rem 0.35rem !important;
            font-size: 0.78rem !important;
            min-height: 1.7rem !important;
            line-height: 1.15 !important;
          }}
          /* Silver LIVE button — green pill style (clickable inspect) */
          [class*="st-key-{key_prefix}_view_silver_live_"] button {{
            background: #dcfce7 !important;
            color: #15803d !important;
            border: 1px solid #bbf7d0 !important;
            font-weight: 700 !important;
            letter-spacing: .02em !important;
          }}
          [class*="st-key-{key_prefix}_view_silver_live_"] button:hover {{
            background: #bbf7d0 !important;
            border-color: #86efac !important;
          }}
          /* Gold LIVE button — amber pill style (clickable inspect) */
          [class*="st-key-{key_prefix}_view_gold_live_"] button {{
            background: #fef3c7 !important;
            color: #a16207 !important;
            border: 1px solid #fde68a !important;
            font-weight: 700 !important;
            letter-spacing: .02em !important;
          }}
          [class*="st-key-{key_prefix}_view_gold_live_"] button:hover {{
            background: #fde68a !important;
            border-color: #fcd34d !important;
          }}
          /* Bronze LIVE button — orange-amber pill (already clickable; same treatment) */
          [class*="st-key-{key_prefix}_view_bronze_"] button {{
            background: #fff7ed !important;
            color: #c2410c !important;
            border: 1px solid #fed7aa !important;
            font-weight: 700 !important;
            letter-spacing: .02em !important;
          }}
          [class*="st-key-{key_prefix}_view_bronze_"] button:hover {{
            background: #fed7aa !important;
            border-color: #fdba74 !important;
          }}
          /* Tighten paragraph spacing inside table cells */
          .st-key-{key_prefix}_med_table p {{
            margin: 0 !important;
            line-height: 1.35 !important;
          }}

          /* Cell typography classes (applied via inline <span class=...>) */
          .med-cell-name   {{ font-weight: 600; color: #0f172a; font-size: 0.88rem; }}
          .med-cell-code   {{ font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
                              color: #475569; font-size: 0.78rem;
                              background: #f1f5f9; padding: 2px 6px; border-radius: 3px;
                              display: inline-block; border: 1px solid #e2e8f0; }}
          .med-cell-cat    {{ color: #475569; font-size: 0.82rem; }}
          .med-cell-fields {{ text-align: right; font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
                              color: #0f172a; font-weight: 700; font-size: 0.88rem;
                              padding-right: 0.4rem; display: block; }}
          .med-cell-none   {{ color: #94a3b8; font-style: italic; font-size: 0.82rem;
                              text-align: center; display: block; }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ==== Outer table container (REAL nesting via st.container key=) =====
    table = st.container(key=f"{key_prefix}_med_table")
    with table:
        # ---- Header row (navy band) ------------------------------------
        with st.container(key=f"{key_prefix}_med_header"):
            hcols = st.columns(widths)
            for col, (label, _css_class, tooltip) in zip(hcols, headers, strict=False):
                col.markdown(
                    f"<span title='{tooltip}'>{label}</span>",
                    unsafe_allow_html=True,
                )

        # Pre-compute max field count for the in-grid progress-bar scaling
        max_fields = (
            max(
                (int(d.get("total_fields") or 0) for d in datasets),
                default=1,
            )
            or 1
        )

        # ---- Body rows — each in its own keyed container ---------------
        for row_idx, ds in enumerate(datasets):
            code = str(ds.get("dataset_code"))
            name = ds.get("display_name") or code
            category = ds.get("category") or "—"
            total_fields = ds.get("total_fields") or 0
            silver = silver_by_code.get(code)
            gold = gold_by_code.get(code)
            plines = product_lines_by_dataset.get(code, [])

            silver_live = silver is not None and silver.get("status") == "LIVE"
            gold_live = gold is not None and gold.get("status") == "LIVE"
            can_author = not (silver_live and gold_live)
            # Clone is only meaningful when the FULL canonical medallion is
            # LIVE (Bronze is implicitly LIVE from the catalog; we require
            # Silver AND Gold both LIVE).  Cloning a partial pipeline gives
            # the client a half-built model which they can't run end-to-end.
            can_clone = silver_live and gold_live

            # Real DOM nesting via st.container(key=...).  Parity in the key
            # so the CSS attribute-contains selector can stripe rows.
            parity = "even" if (row_idx % 2 == 0) else "odd"
            with st.container(key=f"{key_prefix}_med_row_{parity}_{row_idx}"):
                rcols = st.columns(widths)

                # 📦 Clone  (left button — only enabled when full medallion is LIVE)
                with rcols[0]:
                    # Build a precise disabled-state tooltip so the operator
                    # knows EXACTLY what's missing to unlock Clone.
                    if can_clone:
                        _clone_help = (
                            "Open a 1-click clone dialog to copy this "
                            "dataset's LIVE Silver+Gold schemas down to a "
                            "specific client."
                        )
                    else:
                        _missing = []
                        if not silver_live:
                            _missing.append("🥈 Silver")
                        if not gold_live:
                            _missing.append("🥇 Gold")
                        _clone_help = (
                            f"Clone disabled — needs ALL layers LIVE (Bronze ✓ + Silver + Gold). "
                            f"Still missing LIVE: {' + '.join(_missing)}.  "
                            f"Click ⚓ Anchor on the missing layer(s) to author them, "
                            f"then come back to Clone."
                        )
                    if st.button(
                        "📦 Clone",
                        key=f"{key_prefix}_clone_{code}",
                        disabled=not can_clone,
                        help=_clone_help,
                        use_container_width=True,
                    ):
                        _set_action({"kind": "clone", "from_scope": scope, "dataset": code})

                # Dataset name + product lines
                rcols[1].markdown(
                    f"<span class='med-cell-name'>{name}</span>",
                    unsafe_allow_html=True,
                )
                rcols[2].markdown(_product_lines_html(plines), unsafe_allow_html=True)

                # 🥉 Bronze — show ● LIVE pill, clickable to inspect field list
                with rcols[3]:
                    if st.button(
                        "● LIVE",
                        key=f"{key_prefix}_view_bronze_{code}",
                        help="Click to inspect the canonical Bronze field list (read-only).",
                        use_container_width=True,
                    ):
                        _set_action(
                            {"kind": "view", "scope": scope, "dataset": code, "layer": "Bronze"}
                        )

                # 🥈 Silver — LIVE clickable button (inspect like Bronze) /
                # DRAFT clickable / Anchor button when not authored
                with rcols[4]:
                    if silver_live:
                        # Hide "v1" (the default first version) — only show
                        # vN when N > 1 (i.e. the schema has evolved).
                        # Cleaner default UI; version detail still in inspector.
                        _sv = silver.get("version") or 1
                        _silver_label = "● LIVE" if int(_sv) <= 1 else f"● LIVE v{_sv}"
                        if st.button(
                            _silver_label,
                            key=f"{key_prefix}_view_silver_live_{code}",
                            help="Click to inspect this LIVE Silver schema "
                            "(columns, anchor, audit history).",
                            use_container_width=True,
                        ):
                            _set_action(
                                {"kind": "view", "scope": scope, "dataset": code, "layer": "Silver"}
                            )
                    elif silver is not None:
                        if st.button(
                            _layer_status_pill_short(silver),
                            key=f"{key_prefix}_view_silver_{code}",
                            help="DRAFT in progress — click to continue authoring.",
                            use_container_width=True,
                        ):
                            _set_action(
                                {"kind": "view", "scope": scope, "dataset": code, "layer": "Silver"}
                            )
                    else:
                        # Anchor button — IN-PAGE flow:
                        # 1) Set "author" action so the existing handoff at
                        #    line ~1411 in 14_Data_Model_Designer.py pre-fills
                        #    the picker (Client=scope, Dataset=code, Layer=Silver)
                        # 2) Set _dmd_anchor_focus so this grid collapses to
                        #    show ONLY the clicked row + a "Show all" button
                        # 3) App-scoped rerun (the picker on the main page
                        #    needs to re-render with the pre-filled values)
                        if st.button(
                            "⚓ Anchor",
                            key=f"{key_prefix}_anchor_silver_{code}",
                            help="Click to drill into authoring the Silver layer for this dataset. "
                            "The grid will collapse to just this row and the authoring "
                            "picker below will pre-fill (CATALOG_ANCHOR / Silver).",
                            use_container_width=True,
                        ):
                            st.session_state["_dmd_anchor_focus"] = {
                                "scope": scope,
                                "dataset": code,
                                "layer": "Silver",
                            }
                            _set_action(
                                {
                                    "kind": "author",
                                    "scope": scope,
                                    "dataset": code,
                                    "layer": "Silver",
                                }
                            )

                # 🥇 Gold — LIVE clickable button (inspect like Bronze/Silver) /
                # DRAFT clickable / Anchor button when not authored
                with rcols[5]:
                    if gold_live:
                        _gv = gold.get("version") or 1
                        _gold_label = "● LIVE" if int(_gv) <= 1 else f"● LIVE v{_gv}"
                        if st.button(
                            _gold_label,
                            key=f"{key_prefix}_view_gold_live_{code}",
                            help="Click to inspect this LIVE Gold schema "
                            "(fields, anchor, audit history).",
                            use_container_width=True,
                        ):
                            _set_action(
                                {"kind": "view", "scope": scope, "dataset": code, "layer": "Gold"}
                            )
                    elif gold is not None:
                        if st.button(
                            _layer_status_pill_short(gold),
                            key=f"{key_prefix}_view_gold_{code}",
                            help="DRAFT in progress — click to continue authoring.",
                            use_container_width=True,
                        ):
                            _set_action(
                                {"kind": "view", "scope": scope, "dataset": code, "layer": "Gold"}
                            )
                    else:
                        if st.button(
                            "⚓ Anchor",
                            key=f"{key_prefix}_anchor_gold_{code}",
                            help="Click to drill into authoring the Gold layer for this dataset. "
                            "The grid will collapse to just this row and the authoring "
                            "picker below will pre-fill (CATALOG_ANCHOR / Gold).",
                            use_container_width=True,
                        ):
                            st.session_state["_dmd_anchor_focus"] = {
                                "scope": scope,
                                "dataset": code,
                                "layer": "Gold",
                            }
                            _set_action(
                                {"kind": "author", "scope": scope, "dataset": code, "layer": "Gold"}
                            )

                # Right-aligned monospace field count + mini progress bar
                rcols[6].markdown(
                    _fields_with_bar_html(int(total_fields), max_fields),
                    unsafe_allow_html=True,
                )

    # Footer caption — outside the table wrapper
    st.caption(f"{len(datasets):,} datasets · scope: {scope} · key: {key_prefix}")

    # If an action was staged on THIS scope and it's a view action, open the
    # inline modal viewer.  (Pass 4 will swap in the rich 6-tab inspector;
    # for Pass 2 we ship a simple columns table.)
    action = get_action()
    if action:
        kind = action.get("kind")
        # view / clone modals — render only on the grid the click
        # originated from.  ANCHOR no longer opens a modal — it triggers
        # an in-page flow handled by the main DMD page (existing "author"
        # handoff at line ~1411 pre-fills the picker + the layer radio).
        same_scope = (kind == "view" and action.get("scope") == scope) or (
            kind == "clone" and action.get("from_scope") == scope
        )
        if same_scope and kind == "view":
            layer = action.get("layer")
            ds_code = action.get("dataset")
            if layer == "Bronze":
                _render_bronze_dialog(snap=snap, dataset_code=ds_code)
            elif layer in ("Silver", "Gold"):
                row, cols, entity_id = _resolve_view_target(
                    snap=snap,
                    scope=scope,
                    dataset_code=ds_code,
                    layer=layer,
                )
                if row is None:
                    st.warning(f"Couldn't load {layer} schema for {ds_code} (scope: {scope}).")
                    clear_action()
                elif inspector_fn is not None:
                    _render_view_dialog(
                        inspector_fn=inspector_fn,
                        layer=layer,
                        row=row,
                        cols=cols or [],
                        entity_id=entity_id or "",
                    )
                else:
                    _render_silver_gold_dialog(
                        layer=layer,
                        row=row,
                        cols=cols or [],
                        entity_id=entity_id or "",
                    )
        elif same_scope and kind == "anchor":
            _render_anchor_modal(
                snap=snap,
                scope=scope,
                dataset_code=action.get("dataset"),
                layer=action.get("layer"),
            )
        elif same_scope and kind == "clone":
            _render_clone_modal(
                snap=snap,
                dataset_code=action.get("dataset"),
                from_scope=action.get("from_scope") or scope,
            )


def _layer_status_pill_short(layer_row: dict[str, Any]) -> str:
    """Short button label — fits inside an st.button."""
    status = str(layer_row.get("status") or "")
    v = layer_row.get("version") or "—"
    if status == "LIVE":
        return f"✓ LIVE v{v}"
    if status == "DRAFT":
        return f"◌ DRAFT v{v}"
    if status == "PENDING_REVIEW":
        return f"◌ PR v{v}"
    return f"{status} v{v}"


def _resolve_view_target(
    *, snap, scope: str, dataset_code: str, layer: str
) -> tuple[dict[str, Any] | None, list[dict[str, Any]] | None, str | None]:
    """Find the latest non-archived (scope, dataset, layer) row + its columns."""
    if layer == "Silver":
        candidates = [
            s
            for s in snap.silver_schemas
            if str(s.get("scope_owner") or "") in (scope, _legacy(scope))
            and str(s.get("dataset_code") or "") == dataset_code
            and str(s.get("status") or "") != "ARCHIVED"
        ]
        if not candidates:
            return None, None, None
        row = max(candidates, key=lambda r: int(r.get("version") or 0))
        entity_id = str(row.get("silver_dataset_id"))
        cols = snap.silver_columns_by_dataset.get(entity_id, [])
        return row, cols, entity_id
    if layer == "Gold":
        candidates = [
            g
            for g in snap.gold_schemas
            if str(g.get("scope_owner") or "") in (scope, _legacy(scope))
            and str(g.get("dataset_code") or "") == dataset_code
            and str(g.get("status") or "") != "ARCHIVED"
        ]
        if not candidates:
            return None, None, None
        row = max(candidates, key=lambda r: int(r.get("version") or 0))
        entity_id = str(row.get("gold_dataset_id"))
        cols = snap.gold_fields_by_dataset.get(entity_id, [])
        return row, cols, entity_id
    return None, None, None


def _legacy(scope: str) -> str:
    return "__global__" if scope == "GLOBAL_CORP" else scope


def _render_silver_gold_dialog(
    *,
    layer: str,
    row: dict[str, Any],
    cols: list[dict[str, Any]],
    entity_id: str,
) -> None:
    """Simple Silver/Gold viewer.  Used when no rich inspector is supplied.

    Shows: header line · columns table · AI rationale (if present) · audit
    trail (created_by/at · approved_by/at).
    """
    layer_emoji = "🥈" if layer == "Silver" else "🥇"
    title = (
        f"{layer_emoji}  {layer} schema — {row.get('dataset_code')}  "
        f"v{row.get('version')}  ·  scope: {row.get('scope_owner')}"
    )

    @st.dialog(title, width="large")
    def _modal():
        # Header strip
        status = str(row.get("status") or "—")
        anchor_key = "silver_anchor" if layer == "Silver" else "gold_anchor"
        anchor = row.get(anchor_key) or "—"
        pattern = row.get("silver_pattern") or row.get("gold_table_name") or "—"
        st.markdown(
            f"**Status:** `{status}`  ·  **Anchor:** `{anchor}`  ·  "
            f"**Pattern/Table:** `{pattern}`  ·  "
            f"**Source:** `{row.get('source') or '—'}`"
        )
        if row.get("forked_from_global_version"):
            st.caption(f"Forked from Global v{row.get('forked_from_global_version')}")

        st.markdown("---")
        # Columns
        if cols:
            df = pd.DataFrame(cols)
            rename_map = {
                "column_order": "#",
                "column_name": "Column",
                "gold_column_name": "Column",
                "logical_type": "Type",
                "nullable": "Nullable",
                "is_business_key": "BK",
                "is_pii": "PII",
                "is_phi": "PHI",
                "description": "Description",
                "anchor_reference": "Anchor ref",
            }
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            # Drop technical IDs from display
            for tech in ("silver_dataset_id", "gold_dataset_id"):
                if tech in df.columns:
                    df = df.drop(columns=[tech])
            st.dataframe(df, use_container_width=True, hide_index=True, height=380)
        else:
            st.info("No columns registered yet.")

        # AI rationale (if present)
        rationale = row.get("ai_rationale")
        if rationale:
            with st.expander("🧠 AI rationale", expanded=False):
                st.markdown(str(rationale))

        # Audit trail
        with st.expander("🗒️  Audit", expanded=False):
            audit_pairs = [
                ("Created", f"{row.get('created_by') or '—'} · {row.get('created_at') or '—'}"),
                ("Approved", f"{row.get('approved_by') or '—'} · {row.get('approved_at') or '—'}"),
                ("Submitted", str(row.get("submitted_at") or "—")),
                ("Archived", str(row.get("archived_at") or "—")),
            ]
            for k, v in audit_pairs:
                st.markdown(f"- **{k}:** {v}")

        # Phase 22 — DBA-only edit affordances on LIVE schemas.
        # Backward-compat sacred: ADD column / DEPRECATE column only.
        # Never DELETE.  Hidden for USER role; visible for DBA.
        if status == "LIVE" and st.session_state.get("dmd_role") == "DBA":
            st.markdown("---")
            st.markdown("### 🛡️  DBA actions  (LIVE schema)")
            st.caption(
                "Backward-compat sacred: ADD column / DEPRECATE column only. "
                "Never DELETE.  Every action lands in "
                f"`CONTROL.{('gold' if layer == 'Gold' else 'silver')}_schema_audit_log`."
            )

            with st.expander("➕  Add new column to this LIVE schema", expanded=False):
                _render_dba_add_column(layer=layer, row=row, entity_id=entity_id)

            if cols:
                with st.expander("🗄️  Deprecate an existing column", expanded=False):
                    _render_dba_deprecate_column(
                        layer=layer, row=row, cols=cols, entity_id=entity_id
                    )

        if st.button("Close", key=f"close_view_{entity_id}", use_container_width=True):
            clear_action()
            # App-scoped — dialog body is internally a fragment; only
            # scope="app" (default) reruns the outer caller and dismisses.
            st.rerun()

    _modal()


# ---------------------------------------------------------------------------
# DBA-only ADD column / DEPRECATE column on LIVE schemas (Phase 22 · R4b).
# Wired up from inside the Silver/Gold viewer modal.  Audit log row written
# per action.  Pure schema-additive — backward-compatible by construction.
# ---------------------------------------------------------------------------


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (s or "").lower()).strip("_") or "col"


def _render_dba_add_column(*, layer: str, row: dict[str, Any], entity_id: str) -> None:
    """ADD column form.  Silver = needs Silver-table picker (HUB/SAT/LINK)."""
    from datalink.quality.control import CONTROL_SCHEMA
    from datalink.ui._query import warehouse_ctx

    new_name = st.text_input(
        "Column display name",
        key=f"dba_add_name_{entity_id}",
        placeholder="e.g. middle_initial",
        help="Will be slugified into a snake_case column identifier.",
    )
    c1, c2 = st.columns([1, 1])
    with c1:
        new_type = st.selectbox(
            "Logical type",
            options=["TEXT", "NUMBER", "BOOLEAN", "DATE", "TIMESTAMP_NTZ", "VARIANT"],
            key=f"dba_add_type_{entity_id}",
        )
    with c2:
        nullable = st.checkbox(
            "Nullable",
            value=True,
            key=f"dba_add_null_{entity_id}",
            help="New columns on LIVE schemas MUST be nullable for backward "
            "compat (existing rows have no value).",
        )

    new_desc = st.text_area(
        "Description",
        key=f"dba_add_desc_{entity_id}",
        placeholder="Why is this column being added?  Source spec change, "
        "new vendor field, regulatory requirement, etc.",
    )

    is_pii = st.checkbox(
        "PII (personally identifiable)", value=False, key=f"dba_add_pii_{entity_id}"
    )
    is_phi = st.checkbox("PHI (protected health info)", value=False, key=f"dba_add_phi_{entity_id}")

    silver_table_id: str | None = None
    if layer == "Silver":
        # Silver columns are nested under Silver tables.  Pick which.
        try:
            with warehouse_ctx(readonly=True) as wh:
                tables = list(
                    wh.query(
                        f"SELECT silver_table_id, table_name, table_kind "
                        f"FROM {CONTROL_SCHEMA}.global_silver_schema_tables "
                        f"WHERE silver_dataset_id = $sid ORDER BY table_order",
                        {"sid": entity_id},
                    )
                )
        except Exception as exc:
            st.error(f"Couldn't load Silver tables: {exc}")
            return
        if not tables:
            st.warning("No Silver tables registered yet for this dataset.")
            return
        labels = [f"{t['table_kind']} · {t['table_name']}" for t in tables]
        idx = st.selectbox(
            "Target Silver table",
            options=list(range(len(tables))),
            format_func=lambda i: labels[i],
            key=f"dba_add_tbl_{entity_id}",
            help="Pick which HUB / SAT / LINK to extend with this column.",
        )
        silver_table_id = str(tables[idx]["silver_table_id"])

    can_submit = bool((new_name or "").strip() and nullable is not False)  # nullability allowed
    if st.button(
        "➕  Add column to LIVE schema",
        type="primary",
        disabled=not (new_name or "").strip(),
        key=f"dba_add_btn_{entity_id}",
        use_container_width=True,
    ):
        try:
            with warehouse_ctx(readonly=False) as wh:
                col_name = _slug(new_name)
                actor = f"ui:dba:{st.session_state.get('client_id', 'operator')}"

                if layer == "Silver":
                    cur_max = list(
                        wh.query(
                            f"SELECT COALESCE(MAX(column_order),0) AS m "
                            f"FROM {CONTROL_SCHEMA}.global_silver_schema_columns "
                            f"WHERE silver_table_id = $tid",
                            {"tid": silver_table_id},
                        )
                    )[0]["m"]
                    next_order = int(cur_max) + 1
                    wh.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_columns "
                        f"(silver_column_id, silver_table_id, silver_dataset_id, "
                        f" column_order, column_name, logical_type, nullable, "
                        f" is_business_key, is_hash_key, is_hash_diff, is_pii, is_phi, "
                        f" description, registered_at) "
                        f"VALUES ($cid, $tid, $did, $ord, $name, $type, $nul, "
                        f"        FALSE, FALSE, FALSE, $pii, $phi, $desc, $ts)",
                        {
                            "cid": str(_uuid.uuid4()),
                            "tid": silver_table_id,
                            "did": entity_id,
                            "ord": next_order,
                            "name": col_name,
                            "type": new_type,
                            "nul": nullable,
                            "pii": is_pii,
                            "phi": is_phi,
                            "desc": new_desc,
                            "ts": datetime.now(UTC).replace(tzinfo=None),
                        },
                    )
                    wh.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.silver_schema_audit_log "
                        f"(audit_id, silver_dataset_id, dataset_code, action, "
                        f" actor, from_status, to_status, diff_summary, ts, notes) "
                        f"VALUES ($aid, $sid, $code, 'add_column', $by, 'LIVE', 'LIVE', "
                        f"        $diff, $ts, $notes)",
                        {
                            "aid": str(_uuid.uuid4()),
                            "sid": entity_id,
                            "code": str(row.get("dataset_code")),
                            "by": actor,
                            "diff": f"+ column `{col_name}` {new_type} {'NULL' if nullable else 'NOT NULL'}",
                            "ts": datetime.now(UTC).replace(tzinfo=None),
                            "notes": (new_desc or "")[:1000],
                        },
                    )
                else:  # Gold
                    cur_max = list(
                        wh.query(
                            f"SELECT COALESCE(MAX(column_order),0) AS m "
                            f"FROM {CONTROL_SCHEMA}.global_gold_schema_fields "
                            f"WHERE gold_dataset_id = $gid",
                            {"gid": entity_id},
                        )
                    )[0]["m"]
                    next_order = int(cur_max) + 1
                    wh.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.global_gold_schema_fields "
                        f"(gold_field_id, gold_dataset_id, dataset_code, column_order, "
                        f" gold_column_name, logical_type, nullable, is_business_key, "
                        f" is_pii, is_phi, description, version, registered_at) "
                        f"VALUES ($fid, $gid, $code, $ord, $name, $type, $nul, FALSE, "
                        f"        $pii, $phi, $desc, $ver, $ts)",
                        {
                            "fid": str(_uuid.uuid4()),
                            "gid": entity_id,
                            "code": str(row.get("dataset_code")),
                            "ord": next_order,
                            "name": col_name,
                            "type": new_type,
                            "nul": nullable,
                            "pii": is_pii,
                            "phi": is_phi,
                            "desc": new_desc,
                            "ver": int(row.get("version") or 1),
                            "ts": datetime.now(UTC).replace(tzinfo=None),
                        },
                    )
                    wh.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.gold_schema_audit_log "
                        f"(audit_id, gold_dataset_id, dataset_code, action, "
                        f" actor, from_status, to_status, diff_summary, ts, notes) "
                        f"VALUES ($aid, $gid, $code, 'add_column', $by, 'LIVE', 'LIVE', "
                        f"        $diff, $ts, $notes)",
                        {
                            "aid": str(_uuid.uuid4()),
                            "gid": entity_id,
                            "code": str(row.get("dataset_code")),
                            "by": actor,
                            "diff": f"+ column `{col_name}` {new_type} {'NULL' if nullable else 'NOT NULL'}",
                            "ts": datetime.now(UTC).replace(tzinfo=None),
                            "notes": (new_desc or "")[:1000],
                        },
                    )

            st.success(
                f"✓  Added column `{col_name}` ({new_type}) to {layer} v{row.get('version')}.  "
                "Audit row written.  Backward-compatible — existing rows have NULL."
            )
            # Bust snapshot so reopened modal shows the new column
            try:
                from datalink.ui import _dmd_data as _dmd

                _dmd.invalidate()
            except Exception:
                pass
            st.rerun()
        except Exception as exc:
            st.error(f"Add failed: {type(exc).__name__}: {exc}")


def _render_dba_deprecate_column(
    *, layer: str, row: dict[str, Any], cols: list[dict[str, Any]], entity_id: str
) -> None:
    """Mark a column DEPRECATED by prefixing its description.  No schema
    change, no row delete — keeps backward compat.  Audit row written."""
    from datalink.quality.control import CONTROL_SCHEMA
    from datalink.ui._query import warehouse_ctx

    name_key = "column_name" if any("column_name" in c for c in cols) else "gold_column_name"
    options = [
        str(c.get(name_key))
        for c in cols
        if c.get(name_key) and not str(c.get("description", "")).startswith("[DEPRECATED")
    ]
    if not options:
        st.info("Every column is already marked deprecated (or no columns to deprecate).")
        return
    target = st.selectbox(
        "Column to mark DEPRECATED",
        options=options,
        key=f"dba_dep_name_{entity_id}",
        help="Marks the column with a [DEPRECATED on …] prefix in its "
        "description.  Data continues to flow — consumers should migrate "
        "away.  Future BREAKING removal would require a new semver-major "
        "schema version, not a DELETE.",
    )
    reason = st.text_input(
        "Reason (recorded in audit log)",
        key=f"dba_dep_reason_{entity_id}",
        placeholder="e.g. Replaced by `member_external_id` v2.0",
    )
    if st.button(
        "🗄️  Mark DEPRECATED",
        type="primary",
        key=f"dba_dep_btn_{entity_id}",
        use_container_width=True,
    ):
        try:
            today = datetime.now(UTC).strftime("%Y-%m-%d")
            with warehouse_ctx(readonly=False) as wh:
                actor = f"ui:dba:{st.session_state.get('client_id', 'operator')}"
                prefix = f"[DEPRECATED on {today}] "
                if layer == "Silver":
                    wh.execute(
                        f"UPDATE {CONTROL_SCHEMA}.global_silver_schema_columns "
                        f"SET description = $prefix || COALESCE(description,'') "
                        f"WHERE silver_dataset_id = $sid AND column_name = $col "
                        f"  AND COALESCE(description,'') NOT LIKE '[DEPRECATED%'",
                        {"prefix": prefix, "sid": entity_id, "col": target},
                    )
                    wh.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.silver_schema_audit_log "
                        f"(audit_id, silver_dataset_id, dataset_code, action, "
                        f" actor, from_status, to_status, diff_summary, ts, notes) "
                        f"VALUES ($aid, $sid, $code, 'deprecate_column', $by, 'LIVE', 'LIVE', "
                        f"        $diff, $ts, $notes)",
                        {
                            "aid": str(_uuid.uuid4()),
                            "sid": entity_id,
                            "code": str(row.get("dataset_code")),
                            "by": actor,
                            "diff": f"~ deprecated `{target}`",
                            "ts": datetime.now(UTC).replace(tzinfo=None),
                            "notes": (reason or "")[:1000],
                        },
                    )
                else:  # Gold
                    wh.execute(
                        f"UPDATE {CONTROL_SCHEMA}.global_gold_schema_fields "
                        f"SET description = $prefix || COALESCE(description,'') "
                        f"WHERE gold_dataset_id = $gid AND gold_column_name = $col "
                        f"  AND COALESCE(description,'') NOT LIKE '[DEPRECATED%'",
                        {"prefix": prefix, "gid": entity_id, "col": target},
                    )
                    wh.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.gold_schema_audit_log "
                        f"(audit_id, gold_dataset_id, dataset_code, action, "
                        f" actor, from_status, to_status, diff_summary, ts, notes) "
                        f"VALUES ($aid, $gid, $code, 'deprecate_column', $by, 'LIVE', 'LIVE', "
                        f"        $diff, $ts, $notes)",
                        {
                            "aid": str(_uuid.uuid4()),
                            "gid": entity_id,
                            "code": str(row.get("dataset_code")),
                            "by": actor,
                            "diff": f"~ deprecated `{target}`",
                            "ts": datetime.now(UTC).replace(tzinfo=None),
                            "notes": (reason or "")[:1000],
                        },
                    )

            st.success(
                f"✓  Marked `{target}` DEPRECATED in {layer} v{row.get('version')}.  Audit row written."
            )
            try:
                from datalink.ui import _dmd_data as _dmd

                _dmd.invalidate()
            except Exception:
                pass
            st.rerun()
        except Exception as exc:
            st.error(f"Deprecate failed: {type(exc).__name__}: {exc}")


def _render_bronze_dialog(*, snap, dataset_code: str) -> None:
    """Inline Bronze field viewer.  Bronze has no Silver/Gold-style row —
    the source of truth is global_bronze_catalog_fields.  We pull from the
    snapshot (always loaded)."""

    @st.dialog(f"🥉  Bronze schema — {dataset_code}", width="large")
    def _modal():
        ds = next((d for d in snap.bronze_datasets if d.get("dataset_code") == dataset_code), None)
        if ds is None:
            st.error(f"No Bronze catalog row for {dataset_code}")
            return
        st.markdown(f"**Dataset:** {ds.get('display_name')}  ·  **Category:** {ds.get('category')}")
        try:
            used_by = json.loads(ds.get("used_by") or "[]")
        except Exception:
            used_by = []
        if used_by:
            st.markdown(f"**Used by:** {' · '.join(used_by)}")
        st.markdown(
            f"**Frequency:** {ds.get('default_frequency') or '—'}  ·  "
            f"**Total fields:** {ds.get('total_fields') or '—'}  ·  "
            f"**Required:** {ds.get('required_fields') or '—'}  ·  "
            f"**Optional:** {ds.get('optional_fields') or '—'}"
        )
        if ds.get("notes"):
            st.markdown(f"_{ds.get('notes')}_")
        st.markdown("---")
        # Bronze fields — pull from snapshot if it has them, else issue a
        # one-shot query (Streamlit page can pass a fields_by_dataset_code
        # map down later; for now, query inline).
        try:
            from datalink.quality.control import CONTROL_SCHEMA
            from datalink.ui._query import warehouse_ctx

            with warehouse_ctx(readonly=True) as wh:
                fields = list(
                    wh.query(
                        f"SELECT field_order, field_display_name, bronze_column_name, "
                        f"       requirement, logical_type, description, additional_notes, "
                        f"       example, is_pii, is_phi "
                        f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                        f"WHERE dataset_code = %s ORDER BY field_order",
                        (dataset_code,),
                    )
                )
            if fields:
                df = pd.DataFrame(fields)
                # rename for display
                df = df.rename(
                    columns={
                        "field_order": "#",
                        "field_display_name": "Field",
                        "bronze_column_name": "Column",
                        "requirement": "Req",
                        "logical_type": "Type",
                        "description": "Description",
                        "additional_notes": "Notes",
                        "example": "Example",
                        "is_pii": "PII",
                        "is_phi": "PHI",
                    }
                )
                st.dataframe(df, use_container_width=True, hide_index=True, height=420)
            else:
                st.info("No fields registered for this dataset.")
        except Exception as exc:
            st.error(f"Couldn't fetch Bronze fields: {exc}")
        if st.button("Close", key=f"close_bronze_view_{dataset_code}", use_container_width=True):
            clear_action()
            # App-scoped — see comment above; dialog dismissal requires it.
            st.rerun()

    _modal()
