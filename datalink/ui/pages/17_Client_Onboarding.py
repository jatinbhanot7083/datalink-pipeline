"""Client Onboarding · AI Mapping — Phase 23.

Operator drops a client data file (CSV / xlsx / JSON / docx / PDF /
fixed-width), the Bronze Inbound Mapping Agent (BIMA) proposes
per-field mappings to the canonical schema, and the operator reviews
+ approves before the mapping goes LIVE.

Lifecycle (HITL mandatory): DRAFT → PENDING_REVIEW → APPROVED → LIVE
                                    └─ REJECTED (with reviewer notes)
"""

from __future__ import annotations

import io
import json
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Bootstrap path so absolute imports work from a Streamlit page.
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="Client Onboarding · AI Mapping",
    page_icon="🚪",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.agents.inbound_mapping import (  # noqa: E402
    approve_proposal,
    get_proposal,
    list_live_mappings,
    list_proposals,
    propose_canonical_extension,
    propose_mapping,
    recommend_target_for_file,
    reject_proposal,
    submit_proposal,
)
from datalink.agents.inbound_mapping.bridge import edit_field_proposal  # noqa: E402
from datalink.agents.llm_router import get_llm  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Client Onboarding")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_GREEN = "#15803d"
_AMBER = "#b45309"
_RED = "#b91c1c"
_BLUE = "#2563eb"
_GREY = "#6b7280"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.4rem;}}
      .co-card {{background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px;
                padding: .8rem 1rem; margin: .5rem 0;}}
      .co-pill {{display: inline-block; padding: 1px 8px; border-radius: 12px;
                font-size: .78rem; font-weight: 600; margin-right: 4px;}}
      .co-pill-draft {{background: #f1f5f9; color: {_GREY};}}
      .co-pill-pending {{background: #fef3c7; color: {_AMBER};}}
      .co-pill-approved {{background: #dbeafe; color: {_BLUE};}}
      .co-pill-live {{background: #dcfce7; color: {_GREEN};}}
      .co-pill-rejected {{background: #fee2e2; color: {_RED};}}
      .co-pill-archived {{background: #e5e7eb; color: {_GREY};}}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🚪 Client Onboarding · AI Mapping")
st.caption(
    "Drop a client data file in **any format** (CSV · xlsx · JSON · docx · PDF) "
    "and the Bronze Inbound Mapping Agent proposes per-field mappings to your "
    "canonical schema.  **Every proposal requires HITL review before promotion to LIVE** — "
    "industry-grade governance + full audit trail.  Token costs land in the "
    "/AI Agents page automatically."
)


def _pill(status: str) -> str:
    key = (status or "").lower().replace("_", "")
    cls = {
        "draft": "co-pill-draft",
        "pendingreview": "co-pill-pending",
        "approved": "co-pill-approved",
        "live": "co-pill-live",
        "rejected": "co-pill-rejected",
        "archived": "co-pill-archived",
    }.get(key, "co-pill-draft")
    return f'<span class="co-pill {cls}">{status}</span>'


# ═════════════════════════════════════════════════════════════════════════════
# Section A — AI-driven mapping (Phase 23.2)
#
# The operator does NOT pick the target dataset — the AI inspects the file
# and recommends top-3 canonical datasets it most likely maps to.  Operator
# either accepts the top pick (one click) or switches to top-2 / top-3 via
# chip-style buttons.  Field-level mapping runs against the selected target.
# ═════════════════════════════════════════════════════════════════════════════
st.markdown("## 🤖 AI-driven mapping  (no dataset picker — AI figures it out)")
st.caption(
    "**Just two inputs needed: client name + the file.**  "
    "Click **🤖 Analyze** and the agent (Claude Haiku 4.5) reads the file, "
    "ranks all canonical datasets by similarity, picks the best match, and "
    "proposes per-field mappings.  Switch the target with one click if you "
    "disagree with the AI's pick."
)

with warehouse_ctx(readonly=True) as _wh_r:
    _datasets = list(
        _wh_r.query(
            f"SELECT dataset_code, display_name, total_fields "
            f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
            f"ORDER BY display_name"
        )
    )
    _existing_clients = sorted(
        {
            str(r["client_id"])
            for r in _wh_r.query(
                f"SELECT DISTINCT client_id FROM {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals"
            )
        }
        | {
            str(r["client_id"])
            for r in _wh_r.query(
                f"SELECT DISTINCT client_id FROM {CONTROL_SCHEMA}.bronze_inbound_mappings"
            )
        }
    )

if not _datasets:
    st.warning(
        "📦  No canonical datasets in `CONTROL.global_bronze_catalog_datasets` yet.  "
        "Go to **Data Model Designer**, click **📂 Upload catalogue**, drop your "
        "Product Catalogue xlsx, and come back."
    )
    st.stop()

# Inputs row: client + file uploader
p1, p2 = st.columns([1.0, 2.0])
with p1:
    client_options = ["— New client —", *_existing_clients]
    pick = st.selectbox(
        "Client",
        options=client_options,
        index=0,
        key="co_client_pick",
    )
    if pick == "— New client —":
        client_id_input = (
            st.text_input(
                "New client_id",
                key="co_new_client_text",
                placeholder="e.g. anthem · humana · centene",
            )
            .strip()
            .lower()
        )
    else:
        client_id_input = pick

with p2:
    # Source-type radio: file upload OR pasted spec text
    co_source_mode = st.radio(
        "Input source",
        options=["📂 Upload file", "📝 Paste spec text"],
        key="co_source_mode",
        horizontal=True,
        label_visibility="collapsed",
    )
    if co_source_mode == "📂 Upload file":
        uploaded = st.file_uploader(
            "Drop the client's data file or spec doc",
            type=["xlsx", "xls", "xlsm", "csv", "tsv", "psv", "json", "jsonl", "docx", "pdf"],
            accept_multiple_files=False,
            key="co_upload",
            help=(
                "Supported: structured data (xlsx · csv · tsv · psv · json · jsonl) "
                "AND spec documents (.docx · .pdf).  For specs, the agent reads "
                "any tables in the document AND the prose text; for data files, "
                "it samples columns + values directly."
            ),
        )
        pasted_spec = ""
    else:
        uploaded = None
        pasted_spec = st.text_area(
            "Paste the schema spec (field names, types, descriptions)",
            key="co_paste_spec",
            height=180,
            placeholder=(
                "Paste anything — CSV-style header rows, bullet-list of fields, "
                "or free-form prose describing the client's data shape.  Example:\n\n"
                "Member ID — required, unique per member\n"
                "Member First Name — required\n"
                "Date of Birth — required, YYYY-MM-DD\n"
                "Coverage Start — date\n"
                "Plan Code — required, 3-char alphanumeric (e.g. HM1, PPO)\n"
            ),
            help="The agent reads this text as a schema description and proposes "
            "a mapping to the canonical Bronze fields.  Same Claude Haiku 4.5 "
            "model as the file path — no special handling for prose vs table.",
        )

# Holder for the recommendation result + file bytes (so we don't re-read).
# Keyed on filename so re-uploading a different file resets the recommendation.
if "co_recommendation" not in st.session_state:
    st.session_state["co_recommendation"] = None

# Big AI button — works for both file upload and pasted spec text
_has_file = bool(uploaded)
_has_text = bool((pasted_spec or "").strip())
_can_analyze = bool(client_id_input) and (_has_file or _has_text)

if st.button(
    "🤖  Analyze with AI",
    type="primary",
    disabled=not _can_analyze,
    use_container_width=True,
    help=(
        "Calls Claude Haiku.  No fallback / stub — pure AI."
        if _can_analyze
        else "Pick a client + drop a file OR paste spec text to enable."
    ),
):
    settings = load_settings(env=os.environ.get("DL_ENV", "dev"))
    llm = get_llm(settings)
    model = getattr(settings.adapters.llm, "model", "")

    # Determine the input source — file_buffer OR pre-built SourceView from text
    file_bytes: bytes | None = None
    filename_label: str
    source_view_override = None
    if _has_file:
        file_bytes = uploaded.getvalue()
        filename_label = uploaded.name
        spinner_msg = f"🧠 Inspecting `{uploaded.name}` + ranking {len(_datasets)} datasets…"
    else:
        # Free-text path — build a SourceView from the pasted spec
        from datalink.agents.inbound_mapping import read_text

        source_view_override = read_text(pasted_spec, filename="pasted_spec.txt")
        filename_label = "pasted_spec.txt"
        spinner_msg = f"🧠 Reading pasted spec + ranking {len(_datasets)} datasets…"

    with st.spinner(spinner_msg):
        with warehouse_ctx(readonly=True) as _wh_rd:
            try:
                if source_view_override is not None:
                    rec = recommend_target_for_file(
                        wh=_wh_rd,
                        llm=llm,
                        source_view=source_view_override,
                        top_k=3,
                        temperature=0.0,
                    )
                else:
                    rec = recommend_target_for_file(
                        wh=_wh_rd,
                        llm=llm,
                        file_buffer=io.BytesIO(file_bytes),
                        filename=filename_label,
                        top_k=3,
                        temperature=0.0,
                    )
            except Exception as exc:
                st.error(f"❌ Analysis failed: {type(exc).__name__}: {exc}")
                st.exception(exc)
                rec = None
    if rec is not None:
        st.session_state["co_recommendation"] = {
            "filename": filename_label,
            "client_id": client_id_input.strip().lower(),
            "file_bytes": file_bytes,  # None for text-mode (re-uses source_view)
            "is_text_mode": _has_text and not _has_file,
            "recommendations": rec.get("recommendations", []),
            "top_pick": rec.get("top_pick"),
            "rationale": rec.get("rationale", ""),
            "tokens_used": rec.get("tokens_used", 0),
            "duration_ms": rec.get("duration_ms", 0),
            "source_view": rec.get("source_view"),
            "selected_target": rec.get("top_pick"),
            "model": str(model),
        }
        st.rerun()

# Render recommendation chips + field-mapping launcher
_rec_state = st.session_state.get("co_recommendation")
if _rec_state and _rec_state.get("filename") == (uploaded.name if uploaded else None):
    st.markdown("---")
    st.markdown("### 🎯 AI's target-dataset ranking")
    st.caption(
        f"🪙 {_rec_state['tokens_used']:,} tokens · {_rec_state['duration_ms']:,} ms "
        f"·  Model `{_rec_state['model']}` · Click a chip to switch target."
    )
    if _rec_state.get("rationale"):
        st.info(f"🧠 {_rec_state['rationale']}", icon="💡")

    recs = _rec_state.get("recommendations") or []
    if not recs:
        st.warning(
            "AI returned no recommendations.  Either the file is too unusual or "
            "the LLM had a parse hiccup — re-run **🤖 Analyze** or upload a "
            "different file."
        )
    else:
        # Chip row — one button per recommendation
        chip_cols = st.columns(len(recs))
        for i, r in enumerate(recs):
            with chip_cols[i]:
                code = r["dataset_code"]
                conf = float(r.get("confidence") or 0)
                display_name = r.get("display_name") or code
                is_selected = code == _rec_state.get("selected_target")
                emoji = "⭐" if is_selected else f"#{i+1}"
                bar = "█" * int(round(conf * 10)) + "░" * (10 - int(round(conf * 10)))
                if st.button(
                    f"{emoji}  **{display_name}**\n\n`{code}`\n\n{bar}  {conf:.2f}",
                    key=f"co_chip_{code}",
                    use_container_width=True,
                    type="primary" if is_selected else "secondary",
                    help=r.get("rationale", ""),
                ):
                    _rec_state["selected_target"] = code
                    st.session_state["co_recommendation"] = _rec_state
                    st.rerun()
                st.caption(r.get("rationale", "")[:140])

        # Propose-mapping button — for whichever target is currently selected
        sel = _rec_state.get("selected_target")
        st.markdown("")
        if st.button(
            f"✅  Propose field-level mapping for  **`{sel}`**",
            type="primary",
            use_container_width=True,
            disabled=not sel,
            help=(
                "Runs Claude on the selected target → produces editable per-field "
                "mappings → saves as DRAFT in CONTROL.bronze_inbound_mapping_proposals."
            ),
        ):
            settings = load_settings(env=os.environ.get("DL_ENV", "dev"))
            llm = get_llm(settings)
            model = getattr(settings.adapters.llm, "model", "")
            with st.spinner(
                f"🧠 Mapping fields → `{sel}`… (uses the SAME parsed file from Analyze — no re-read)"
            ):
                with warehouse_ctx(readonly=False) as _wh_w:
                    try:
                        # Text-mode: file_bytes is None — propose_mapping
                        # reuses the source_view from session state so no
                        # file_buffer is needed.  File-mode: pass file_buffer
                        # too as a belt-and-braces fallback in case re-read
                        # is needed.
                        _fb = (
                            io.BytesIO(_rec_state["file_bytes"])
                            if _rec_state.get("file_bytes")
                            else None
                        )
                        rpt = propose_mapping(
                            wh=_wh_w,
                            llm=llm,
                            client_id=_rec_state["client_id"],
                            dataset_code=sel,
                            file_buffer=_fb,
                            filename=_rec_state["filename"],
                            actor=f"ui:co:{_rec_state['client_id']}",
                            temperature=0.0,
                            grounding_mode="off",
                            model=str(model),
                            pre_read_source=_rec_state.get("source_view"),
                        )
                        total_tokens = _rec_state["tokens_used"] + rpt.get("tokens_used", 0)
                        total_ms = _rec_state["duration_ms"] + rpt.get("duration_ms", 0)
                        st.success(
                            f"✅ DRAFT saved.  `proposal_id={rpt['proposal_id']}`  ·  "
                            f"v{rpt.get('version', 1)}  ·  "
                            f"🪙 {total_tokens:,} tokens total (recommend + map)  ·  "
                            f"{total_ms:,} ms"
                        )
                        st.session_state["co_active_proposal_id"] = rpt["proposal_id"]
                        # Clear the recommendation cache after success so the
                        # next file's upload doesn't see stale state
                        st.session_state["co_recommendation"] = None
                        st.rerun()
                    except Exception as exc:
                        st.error(f"❌ Mapping failed: {type(exc).__name__}: {exc}")
                        st.exception(exc)

st.markdown("---")

# ═════════════════════════════════════════════════════════════════════════════
# Section B — Pending proposals (DRAFT / PENDING_REVIEW)
# ═════════════════════════════════════════════════════════════════════════════
st.markdown("## 📋 Pending proposals — awaiting HITL review")

with warehouse_ctx(readonly=True) as _wh_r:
    _pending = list_proposals(
        wh=_wh_r,
        status=["DRAFT", "PENDING_REVIEW", "APPROVED"],
        limit=50,
    )
    _historical = list_proposals(
        wh=_wh_r,
        status=["LIVE", "REJECTED", "ARCHIVED"],
        limit=100,
    )

if not _pending:
    st.info("No pending proposals.  Use **🆕 Propose new mapping** above to create one.")
else:
    pending_rows = []
    for p in _pending:
        pending_rows.append(
            {
                "📌": "▶"
                if p["proposal_id"] == st.session_state.get("co_active_proposal_id")
                else "",
                "Status": p["status"],
                "Client": p["client_id"],
                "Dataset": p["dataset_code"],
                "Version": f"v{p['version']}",
                "Source": f"{p.get('source_filename') or ''} ({p.get('source_format') or ''})",
                "Tokens": p.get("ai_token_count") or 0,
                "ms": p.get("ai_latency_ms") or 0,
                "Created by": p.get("created_by") or "",
                "Created at": str(p.get("created_at") or "")[:19],
                "proposal_id": p["proposal_id"],
            }
        )
    _df = pd.DataFrame(pending_rows)
    st.dataframe(_df.drop(columns=["proposal_id"]), use_container_width=True, hide_index=True)

    # Pick one to review
    _pick = st.selectbox(
        "Review proposal:",
        options=[r["proposal_id"] for r in pending_rows],
        format_func=lambda pid: next(
            (
                f"{r['Client']} / {r['Dataset']} · v{r['Version'][1:]} · {r['Status']}"
                for r in pending_rows
                if r["proposal_id"] == pid
            ),
            pid,
        ),
        key="co_active_proposal_id",
    )
    if _pick:
        st.markdown("---")
        st.markdown("## 🔬 Review proposal")
        with warehouse_ctx(readonly=True) as _wh_r:
            detail = get_proposal(wh=_wh_r, proposal_id=_pick)

        # Header
        hcols = st.columns([1.3, 1.3, 1, 1, 1, 2])
        hcols[0].markdown(f"**Client**  ·  `{detail['client_id']}`")
        hcols[1].markdown(f"**Dataset**  ·  `{detail['dataset_code']}`")
        hcols[2].markdown(f"**Version**  ·  v{detail['version']}")
        hcols[3].markdown(f"**Status**  ·  {_pill(detail['status'])}", unsafe_allow_html=True)
        hcols[4].markdown(f"**Tokens**  ·  {detail.get('ai_token_count', 0)}")
        hcols[5].markdown(
            f"**Source**  ·  `{detail.get('source_filename') or '—'}` "
            f"({detail.get('source_format') or '—'})"
        )

        # Rationale (AI's overall reasoning)
        if detail.get("ai_rationale"):
            with st.expander("🧠 AI rationale", expanded=False):
                st.markdown(detail["ai_rationale"])

        # Drift summary
        drift = detail.get("drift_summary")
        try:
            drift_d = json.loads(drift) if isinstance(drift, str) else (drift or {})
        except Exception:
            drift_d = {}
        if drift_d and drift_d.get("is_material"):
            st.warning(
                f"⚠ Schema drift detected vs prior LIVE mapping  ·  "
                f"+{len(drift_d.get('added_client_fields') or [])} new fields  ·  "
                f"-{len(drift_d.get('removed_client_fields') or [])} removed  ·  "
                f"{len(drift_d.get('changed_canonical_target') or [])} retargeted"
            )

        # Field-mapping table (editable per row)
        st.markdown("### 🔗 Field mappings  ·  edit before approving")
        st.caption(
            "Each row is editable: change the canonical target, override the transform SQL, "
            "or set review_status = REJECTED to drop a row from the LIVE mapping."
        )
        fps = detail.get("field_proposals") or []

        # Build editable table
        canon_codes_for_ds = [""] + sorted(
            [
                str(r["bronze_column_name"])
                for r in (
                    lambda: list(
                        _wh_r.query(
                            f"SELECT bronze_column_name FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                            f"WHERE dataset_code = $ds ORDER BY field_order",
                            {"ds": detail["dataset_code"]},
                        )
                    )
                )()
            ]
        )

        fp_rows_for_editor = []
        for fp in fps:
            fp_rows_for_editor.append(
                {
                    "fp_id": fp["field_proposal_id"],
                    "Client field": fp.get("client_field_name") or "",
                    "Path": fp.get("client_field_path") or "",
                    "Canonical target": fp.get("canonical_field_code") or "",
                    "Transform SQL": fp.get("transform_sql") or "",
                    "Kind": fp.get("transform_kind") or "direct",
                    "Confidence": float(fp.get("confidence") or 0),
                    "Required": bool(fp.get("is_required")),
                    "Review": fp.get("review_status") or "PENDING",
                    "Rationale": (fp.get("rationale") or "")[:200],
                }
            )
        fp_df = pd.DataFrame(fp_rows_for_editor)
        edited_df = st.data_editor(
            fp_df.drop(columns=["fp_id"]) if "fp_id" in fp_df.columns else fp_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Canonical target": st.column_config.SelectboxColumn(
                    "Canonical target",
                    options=canon_codes_for_ds,
                    help="Empty = unmap (will not land in LIVE).",
                ),
                "Kind": st.column_config.SelectboxColumn(
                    "Kind",
                    options=["direct", "cast", "split", "concat", "lookup", "regex", "parse_date"],
                ),
                "Review": st.column_config.SelectboxColumn(
                    "Review",
                    options=["PENDING", "ACCEPTED", "EDITED", "REJECTED"],
                ),
                "Confidence": st.column_config.ProgressColumn(
                    "Confidence",
                    min_value=0.0,
                    max_value=1.0,
                    format="%.2f",
                ),
                "Transform SQL": st.column_config.TextColumn(
                    "Transform SQL",
                    help="Snowflake SQL with :src as the source-column placeholder. NULL = direct copy.",
                ),
            },
            disabled=["Client field", "Path", "Confidence", "Rationale"],
            key=f"co_fp_editor_{_pick}",
        )

        # Save edits
        if st.button("💾  Save reviewer edits", key="co_save_edits"):
            n_edited = 0
            with warehouse_ctx(readonly=False) as _wh_w:
                # Re-key by client_field_name (Path is a tiebreaker for nested)
                fp_id_lookup = {
                    (r["Client field"], r.get("Path") or ""): r["fp_id"] for r in fp_rows_for_editor
                }
                for _, row in edited_df.iterrows():
                    key = (row["Client field"], row.get("Path") or "")
                    fp_id = fp_id_lookup.get(key)
                    if not fp_id:
                        continue
                    edit_field_proposal(
                        wh=_wh_w,
                        field_proposal_id=fp_id,
                        canonical_field_code=(row.get("Canonical target") or None) or None,
                        transform_sql=row.get("Transform SQL") or None,
                        transform_kind=row.get("Kind") or "direct",
                        review_status=row.get("Review") or "EDITED",
                        actor="ui:reviewer",
                    )
                    n_edited += 1
            st.success(f"✓ {n_edited} row(s) saved.")
            st.rerun()

        # Unmapped client fields + canonical-extension feedback loop (Pass 5)
        unmapped_codes = [
            r["Client field"]
            for r in fp_rows_for_editor
            if not (r["Canonical target"] or "").strip()
        ]
        if unmapped_codes:
            st.markdown("### 🟠 Unmapped client fields  ·  feedback loop")
            st.caption(
                "These client fields had no canonical match.  Click "
                "**↗ Propose to canonical** on any row to ask the AI to "
                "shape it into a canonical-field proposal.  A DBA reviews + "
                "approves it from **Data Model Designer** (DBA mode), and "
                "then the field becomes part of the global Bronze catalog "
                "for ALL clients."
            )
            # Pre-build a lookup of (client_field_name → row dict) from the
            # editor table so we can grab path/etc.
            row_by_name = {r["Client field"]: r for r in fp_rows_for_editor}
            for nm in unmapped_codes:
                row = row_by_name.get(nm, {})
                u1, u2, u3 = st.columns([1.6, 4, 1.5])
                with u1:
                    st.markdown(
                        f"<code>{nm}</code> · "
                        f"<span style='color:{_GREY};font-size:.78rem'>{row.get('Path') or '—'}</span>",
                        unsafe_allow_html=True,
                    )
                with u2:
                    st.caption(row.get("Rationale", "")[:200])
                with u3:
                    if st.button(
                        "↗ Propose to canonical",
                        key=f"co_promote_{detail['proposal_id']}_{nm}",
                        help=(
                            "Ask the AI to propose a canonical-field shape "
                            f"(display_name · slug · type · description · PII/PHI flags) "
                            f"for '{nm}'.  Writes a PENDING_REVIEW row to "
                            "bronze_canonical_extension_proposals."
                        ),
                        use_container_width=True,
                    ):
                        settings = load_settings(env=os.environ.get("DL_ENV", "dev"))
                        llm = get_llm(settings)
                        try:
                            with st.spinner(f"🧠 AI proposing canonical shape for '{nm}'…"):
                                with warehouse_ctx(readonly=False) as _wh_w:
                                    ext = propose_canonical_extension(
                                        wh=_wh_w,
                                        llm=llm,
                                        origin_proposal_id=detail["proposal_id"],
                                        origin_client_id=detail["client_id"],
                                        origin_field_name=nm,
                                        dataset_code=detail["dataset_code"],
                                        actor=f"ui:co:{detail['client_id']}",
                                        submit_immediately=True,
                                    )
                            st.success(
                                f"✓ Proposed `{ext['proposed_column_name']}` "
                                f"({ext['proposed_logical_type']}) — confidence "
                                f"{ext.get('ai_confidence', 0):.2f}  "
                                f"·  extension_id=`{ext['extension_id']}`  "
                                f"·  Pending DBA review on DMD."
                            )
                        except Exception as exc:
                            st.error(
                                f"❌ Propose-to-canonical failed: " f"{type(exc).__name__}: {exc}"
                            )

        # Reviewer notes + lifecycle buttons
        st.markdown("### 🛡️ HITL — submit / approve / reject")
        reviewer_notes = st.text_area(
            "Reviewer notes  (REQUIRED on Reject · recorded on Approve)",
            key=f"co_notes_{_pick}",
            placeholder="What did you change vs the AI proposal?  Why approve / why reject?",
            max_chars=4000,
        )
        b1, b2, b3, b4 = st.columns([1, 1, 1, 4])
        with b1:
            _can_submit = detail["status"] == "DRAFT"
            if st.button(
                "📤  Submit",
                key="co_submit",
                disabled=not _can_submit,
                help="DRAFT → PENDING_REVIEW.  Reviewer takes over from here.",
                use_container_width=True,
            ):
                with warehouse_ctx(readonly=False) as _wh_w:
                    submit_proposal(
                        wh=_wh_w, proposal_id=_pick, actor="ui:author", notes=reviewer_notes
                    )
                st.rerun()
        with b2:
            _can_approve = detail["status"] in ("PENDING_REVIEW", "APPROVED", "DRAFT")
            if st.button(
                "✅  Approve → LIVE",
                key="co_approve",
                type="primary",
                disabled=not _can_approve,
                help="Promote to LIVE.  Old LIVE for same (client, dataset) is auto-ARCHIVED.",
                use_container_width=True,
            ):
                with warehouse_ctx(readonly=False) as _wh_w:
                    new_mid = approve_proposal(
                        wh=_wh_w,
                        proposal_id=_pick,
                        actor="ui:reviewer",
                        notes=reviewer_notes,
                    )
                st.success(f"🎉 LIVE.  mapping_id=`{new_mid}`")
                st.rerun()
        with b3:
            _can_reject = detail["status"] in ("DRAFT", "PENDING_REVIEW")
            if st.button(
                "❌  Reject",
                key="co_reject",
                disabled=not _can_reject,
                help="Requires notes.",
                use_container_width=True,
            ):
                try:
                    with warehouse_ctx(readonly=False) as _wh_w:
                        reject_proposal(
                            wh=_wh_w, proposal_id=_pick, actor="ui:reviewer", notes=reviewer_notes
                        )
                    st.rerun()
                except ValueError as ve:
                    st.error(str(ve))

        # Audit trail
        with st.expander(
            f"🗒️ Audit trail ({len(detail.get('audit_log') or [])} entries)", expanded=False
        ):
            for a in detail.get("audit_log") or []:
                ts = str(a.get("ts") or "")[:19]
                st.markdown(
                    f"- `{ts}` · **{a.get('action')}** by `{a.get('actor')}`  "
                    f"· {a.get('from_status') or '—'} → {a.get('to_status') or '—'}  "
                    f"· {a.get('notes') or ''}"
                )


# ═════════════════════════════════════════════════════════════════════════════
# Section C — Historical proposals
# ═════════════════════════════════════════════════════════════════════════════
with st.expander(f"🗄️ Historical proposals ({len(_historical)})", expanded=False):
    if _historical:
        hist_rows = [
            {
                "Status": p["status"],
                "Client": p["client_id"],
                "Dataset": p["dataset_code"],
                "Version": f"v{p['version']}",
                "Source": p.get("source_filename") or "",
                "Tokens": p.get("ai_token_count") or 0,
                "Approved by": p.get("approved_by") or "—",
                "Approved at": str(p.get("approved_at") or "")[:19],
            }
            for p in _historical
        ]
        st.dataframe(pd.DataFrame(hist_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("No historical proposals yet.")


# ═════════════════════════════════════════════════════════════════════════════
# Section D — LIVE mappings — applied by Bronze ingest at run time
# ═════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown("## 🟢 LIVE inbound mappings")
st.caption(
    "These are the mappings the Bronze ingest task reads at run-time.  "
    "At most ONE LIVE per (client, dataset).  Promoting a new proposal automatically "
    "ARCHIVEs the prior LIVE."
)

with warehouse_ctx(readonly=True) as _wh_r:
    _live = list_live_mappings(wh=_wh_r)

if not _live:
    st.info("No LIVE mappings yet.  Approve a proposal above to activate one.")
else:
    live_rows = [
        {
            "Client": m["client_id"],
            "Dataset": m["dataset_code"],
            "Version": f"v{m['version']}",
            "Format": m.get("source_format") or "—",
            "Promoted by": m.get("promoted_by") or "—",
            "Promoted at": str(m.get("promoted_at") or "")[:19],
            "mapping_id": m["mapping_id"],
        }
        for m in _live
    ]
    st.dataframe(
        pd.DataFrame(live_rows).drop(columns=["mapping_id"]),
        use_container_width=True,
        hide_index=True,
    )
