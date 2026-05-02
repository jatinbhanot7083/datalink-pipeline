"""Data Contract Architect — Phase 14 — schema authoring page.

Two modes:

  Mode A (FILE_DRIVEN) — operator drops a sample CSV / EDI / JSON file.
                         Page extracts header + first ~5 rows, shows them
                         to the agent, gets a proposed Bronze contract.

  Mode B (CONTRACT_FIRST) — operator pastes a vendor mapping spec OR
                            describes the source in natural language.
                            Agent proposes a contract from scratch +
                            outputs a Vendor Spec PDF the operator can
                            hand to the client.

In both modes, the operator picks **anchored standards** (FHIR R4, X12,
NCPDP, CMS, DV2, HEDIS, or operator-uploaded custom standards). Top-k
chunks from those standards are retrieved via cosine similarity and
injected into Claude's context as the source-of-truth reference.

Approval emits five artifacts (Phase 14.6, NEXT):
  1. Bronze DDL committed under datalink/pipeline/bronze/ddl/
  2. SOURCE_SCHEMA_CONTRACTS row registered for drift detection
  3. DQ_SUITES row scaffolded with format/null/regex checks
  4. dbt model stub under dbt/models/silver/
  5. Vendor Data Contract Spec markdown (becomes PDF in 14.7)
"""

from __future__ import annotations

import io
import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

# bootstrap path
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="Data Contract Architect",
    page_icon="🏗",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.adapters.embeddings.router import get_embedder  # noqa: E402
from datalink.agents.contract_architect import (  # noqa: E402
    approve_design,
    persist_design,
    propose_contract,
)
from datalink.agents.llm_router import get_llm  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.memory import AgentMemoryStore  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._nav import render_sidebar, require_client  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Data Contract Architect")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_AGENT_BLUE = "#2563eb"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      h3 {{color: {_NAVY};}}
      .arch-hero {{background:linear-gradient(90deg,{_NAVY}11,{_GOLD}22);
                   padding:1rem 1.2rem;border-radius:8px;border-left:4px solid {_GOLD};
                   margin:.6rem 0 1.5rem 0}}
      .arch-prop {{background:#fff;border:1px solid #d4d4d8;border-radius:8px;
                   padding:1rem;margin:.5rem 0;box-shadow:0 1px 3px rgba(0,0,0,.04)}}
      .arch-pill {{display:inline-block;padding:.15rem .55rem;border-radius:12px;
                   font-size:.72rem;font-weight:700;letter-spacing:.04em;margin-right:.4rem}}
      .arch-pill-rename {{background:#fef3c7;color:#92400e}}
      .arch-pill-keep {{background:#dcfce7;color:#166534}}
      .arch-pill-new {{background:#dbeafe;color:#1e40af}}
      .arch-pill-retype {{background:#fee2e2;color:#991b1b}}
      .arch-meta {{font-size:.85rem;color:#475569;margin-top:.4rem}}
      .arch-cite {{font-size:.78rem;color:{_AGENT_BLUE};font-style:italic;}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _warehouse(*, readonly: bool = True) -> Iterator[Any]:
    with warehouse_ctx(readonly=readonly) as wh:
        yield wh


# ---------------------------------------------------------------------------
# HEADER
# ---------------------------------------------------------------------------

st.markdown("# 🏗 Data Contract Architect")
st.markdown(
    """
    <div class="arch-hero">
      <strong>Industry-standard-grounded schema authoring.</strong> The AI
      proposes Bronze table contracts anchored to HL7 FHIR R4, X12 EDI,
      NCPDP D.0, CMS dictionaries, Data Vault 2.0, NCQA HEDIS, or your
      org's own uploaded specs. You review, edit, approve. Output: a
      committed DDL + a printable Vendor Data Contract Spec the client
      team must adhere to before sending data.
      <div class="arch-meta">
        Backend: <strong>Claude Haiku 4.5 + Voyage 3.5-lite RAG</strong>
        &middot; PHI-safe: schema metadata only, no row data ever touches the LLM
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Phase 15 cross-page handoff — make the Pipeline Architect the default
# entry point. Data Contract Architect remains the right tool for one-off
# Bronze contracts on messy vendor files that don't fit the catalog yet.
st.markdown(
    f"""
    <div style="background:#fffbeb;border:1px solid #fde68a;
                padding:.85rem 1.1rem;border-radius:8px;
                border-left:4px solid {_GOLD};margin-bottom:1.2rem;">
      <strong>🏛 Looking for a full pipeline?</strong> The new
      <strong>Pipeline Architect</strong> (Phase 15) materialises
      Bronze→Silver→Gold pipelines straight from the Global Gold Catalog
      (33 datasets, 943 fields). One click emits 5 artifacts: Gold DDL,
      Silver+Gold dbt models, Airflow DAG, GX expectation suite, OnPrem
      routing rules. Use <em>this</em> page only for one-off Bronze
      contracts on messy vendor files that don't fit any catalog dataset.
      <div style="font-size:.82rem;color:#78350f;margin-top:.45rem;">
        <a href="/Pipeline_Architect" style="color:{_NAVY};font-weight:600;">→ Open Pipeline Architect</a>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

selected_client = require_client()


# ---------------------------------------------------------------------------
# Bootstrap CONTROL tables (idempotent — no-op after first run)
# ---------------------------------------------------------------------------

with _warehouse(readonly=False) as wh:
    create_control_tables(wh)


# ---------------------------------------------------------------------------
# Load registered standards from CONTROL.standard_registry
# ---------------------------------------------------------------------------


@st.cache_data(ttl=30)  # type: ignore[misc]
def _load_standards() -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh_q:
        rows = wh_q.query(
            f"SELECT standard_id, code, display_name, version, is_industry, "
            f"chunk_count FROM {CONTROL_SCHEMA}.standard_registry "
            f"WHERE is_active = TRUE ORDER BY is_industry DESC, code"
        )
        return list(rows)


standards = _load_standards()

if not standards:
    st.warning(
        "⚠️ No standards loaded yet. Run the corpus loader once:\n\n"
        "```bash\ndocker exec datalink-control-tower python3 "
        "scripts/load_industry_standards.py\n```"
    )
    st.stop()


# ---------------------------------------------------------------------------
# Top controls — Source type + Mode
# ---------------------------------------------------------------------------

c1, c2, c3 = st.columns([1.5, 1, 1])
with c1:
    source_type = st.selectbox(
        "Source type",
        options=["CLAIMS", "MEMBERSHIP", "PROVIDER", "PHARMACY_CLAIMS", "ENCOUNTER", "OTHER"],
        help="The business entity this contract describes. Drives initial RAG anchoring.",
    )
with c2:
    mode_label = st.radio(
        "Mode",
        options=["📁 File-driven", "📝 Contract-first"],
        horizontal=False,
        help=(
            "**File-driven**: vendor sent a sample file. AI profiles the headers + first rows "
            "and matches against standards.\n\n"
            "**Contract-first**: no file yet. You describe the source in NL or paste a vendor "
            "mapping spec. AI proposes a contract you hand back to the client team."
        ),
    )
    mode = "FILE_DRIVEN" if "File-driven" in mode_label else "CONTRACT_FIRST"
with c3:
    approval_mode = st.radio(
        "On Approve, save as:",
        options=["DRAFT", "HITL", "AUTO_APPROVE"],
        index=1,
        format_func=lambda m: {
            "DRAFT": "📝 DRAFT",
            "HITL": "👁️ HITL (review)",
            "AUTO_APPROVE": "⚡ AUTO_APPROVE",
        }[m],
        help="HITL is the audit-friendly default for AI-authored contracts.",
    )


# ---------------------------------------------------------------------------
# Anchored standards multi-select
# ---------------------------------------------------------------------------

st.markdown("### 🔗 Anchored standards")
st.caption(
    "Pick one or more reference corpora. The AI grounds every proposed column "
    "against these. Pinning multiple lets the agent cross-check (e.g. FHIR "
    "Claim.id ≈ X12 837P CLM01)."
)

std_options = {f"{s['display_name']} ({s['chunk_count']} chunks)": s["code"] for s in standards}
default_codes = (
    ["fhir-r4", "x12"]
    if {"fhir-r4", "x12"}.issubset(set(std_options.values()))
    else [next(iter(std_options.values()))]
)
default_labels = [k for k, v in std_options.items() if v in default_codes]

picked_labels = st.multiselect(
    "Standards",
    options=list(std_options.keys()),
    default=default_labels,
    help="At least one required.",
)
picked_codes: list[str] = [std_options[label] for label in picked_labels]


# ---------------------------------------------------------------------------
# AI controls (parity with DQ AI Architect + strictness slider)
# ---------------------------------------------------------------------------

with st.expander("⚙️ AI controls (temperature, RAG, strictness)", expanded=False):
    ai_t, ai_g, ai_s = st.columns([2, 1, 2])
    with ai_t:
        ai_temperature = st.slider(
            "Temperature",
            min_value=0.0,
            max_value=1.0,
            value=0.0,
            step=0.05,
            help=(
                "0.0 = deterministic — same input ⟹ same proposal. "
                "0.5 = mild variation. 0.7+ = creative — useful for "
                "exploring alternative schema designs."
            ),
        )
    with ai_g:
        ai_grounding_k = st.number_input(
            "Grounding k",
            min_value=1,
            max_value=10,
            value=4,
            help="Top-k chunks retrieved from each anchored standard.",
        )
    with ai_s:
        ai_strictness = st.slider(
            "🎯 Strictness",
            min_value=0.0,
            max_value=1.0,
            value=0.5,
            step=0.05,
            help=(
                "**Low** (0.0–0.2): honor vendor naming when reasonable. Document "
                "deviations as informational.\n\n"
                "**Medium** (0.3–0.7): rename when a clear standard-mandated name exists.\n\n"
                "**High** (0.8–1.0): aggressively rename non-standard fields. Force "
                "canonical names everywhere."
            ),
        )


# ---------------------------------------------------------------------------
# Mode-specific input
# ---------------------------------------------------------------------------

st.markdown("### 📥 Input")

input_payload: dict[str, Any] = {}
input_ready = False

if mode == "FILE_DRIVEN":
    uploaded = st.file_uploader(
        "Drop a sample file (CSV / TSV / pipe-delimited / TXT)",
        type=["csv", "tsv", "psv", "txt"],
        help=(
            "The file is parsed locally — only column names + first 5 rows are sent to the LLM. "
            "Auto-detects comma, tab, or pipe delimiters."
        ),
    )
    if uploaded is not None:
        try:
            raw = uploaded.read()
            text = raw.decode("utf-8", errors="replace")
            # Auto-detect the delimiter by inspecting the first line.
            first_line = text.split("\n", 1)[0] if text else ""
            if first_line.count("|") > first_line.count(",") and first_line.count(
                "|"
            ) > first_line.count("\t"):
                detected_delim = "|"
                detected_format_label = "PSV (pipe-delimited)"
            elif first_line.count("\t") > first_line.count(","):
                detected_delim = "\t"
                detected_format_label = "TSV (tab-delimited)"
            else:
                detected_delim = ","
                detected_format_label = "CSV (comma-delimited)"

            buf = io.StringIO(text)
            df = pd.read_csv(buf, nrows=200, low_memory=False, sep=detected_delim)

            st.caption(
                f"Detected: **{detected_format_label}** · "
                f"{len(df.columns)} columns × {len(df)} rows previewed · "
                f"first 5 rows sent to the LLM"
            )
            st.dataframe(df.head(5), use_container_width=True, hide_index=True)
            input_payload = {
                "file_name": uploaded.name,
                "headers": [str(c) for c in df.columns],
                "sample_values": df.head(5).fillna("").astype(str).values.tolist(),
                "detected_format": detected_format_label,
            }
            input_ready = True
        except Exception as e:
            st.error(f"Could not parse {uploaded.name}: {type(e).__name__}: {e}")
else:
    nl_description = st.text_area(
        "Describe the source",
        height=180,
        placeholder=(
            "Examples:\n"
            "  • Aetna will send us a daily 834 enrollment feed via SFTP. "
            "Each row is one membership state...\n"
            "  • CareSource ships a monthly claims extract — claim_id, "
            "member_id, NPI, CPT, ICD-10 primary + up to 3 secondary, dates of "
            "service, billed amount, allowed amount, place of service, status...\n"
            "  • [Or paste a vendor mapping spec table]"
        ),
        help=(
            "Be specific about: source name, transport, frequency, granularity "
            "(one row per what?), key fields, identifiers (NPI? MBI? plan ID?), "
            "and any specific format requirements."
        ),
    )
    if nl_description.strip():
        input_payload = {"nl_description": nl_description.strip()}
        input_ready = True

# Reset proposal when input or scope changes
scope_key = f"contract_arch::{selected_client}::{source_type}::{mode}"
if st.session_state.get("contract_arch_scope") != scope_key:
    st.session_state["contract_arch_scope"] = scope_key
    st.session_state.pop("contract_arch_proposal", None)


# ---------------------------------------------------------------------------
# Propose button
# ---------------------------------------------------------------------------

st.markdown("### 🧠 Propose contract")

go_disabled = not input_ready or not picked_codes
go_l, go_r = st.columns([1, 5])
with go_l:
    propose_clicked = st.button(
        "🧠 Propose",
        type="primary",
        disabled=go_disabled,
        use_container_width=True,
    )
with go_r:
    if not picked_codes:
        st.warning("Pick at least one anchored standard.")
    elif not input_ready:
        if mode == "FILE_DRIVEN":
            st.info("Upload a sample CSV to enable Propose.")
        else:
            st.info("Describe the source to enable Propose.")

if propose_clicked:
    settings = load_settings()
    llm = get_llm(settings)
    memory = AgentMemoryStore(embedder=get_embedder())
    try:
        memory.ensure_schema()
    except Exception as e:
        st.error(f"Memory store unavailable: {e}")
        st.stop()
    with st.spinner(
        f"Architecting contract via {llm.__class__.__name__} + RAG over "
        f"{len(picked_codes)} standard(s)…"
    ):
        try:
            with _warehouse(readonly=True) as wh_for_agent:
                proposal = propose_contract(
                    llm=llm,
                    warehouse=wh_for_agent,
                    memory=memory,
                    client_id=selected_client,
                    source_type=source_type,
                    mode=mode,
                    anchored_standards=picked_codes,
                    payload=input_payload,
                    temperature=float(ai_temperature),
                    grounding_k=int(ai_grounding_k),
                    strictness=float(ai_strictness),
                    actor=st.session_state.get("user", "anonymous@local"),
                )
            st.session_state["contract_arch_proposal"] = proposal.to_dict()
            st.session_state["contract_arch_input_payload"] = input_payload
            st.session_state["contract_arch_anchored"] = picked_codes
            st.success(
                f"✅ Proposed {len(proposal.proposed_columns)} columns in "
                f"{proposal.duration_ms / 1000:.1f}s · "
                f"{proposal.tokens_used} tokens · "
                f"{len(proposal.grounding)} chunks grounded"
            )
        except Exception as e:
            st.error(f"Proposal failed: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# Proposal display + edit + approve
# ---------------------------------------------------------------------------

proposal_dict = st.session_state.get("contract_arch_proposal")
if proposal_dict:
    st.markdown("---")
    st.markdown("## 📋 Proposal")

    # Standard match scores
    scores = proposal_dict.get("standard_match_scores", {})
    if scores:
        sc1, sc2, sc3, sc4 = st.columns(4)
        sorted_scores = sorted(scores.items(), key=lambda kv: -float(kv[1]))[:4]
        cols = [sc1, sc2, sc3, sc4]
        for col, (code, score) in zip(cols, sorted_scores, strict=False):
            col.metric(f"{code} match", f"{float(score):.2f}")

    # Headline + meta
    h1, h2, h3 = st.columns([3, 1, 1])
    h1.markdown(f"**Proposed table**: `{proposal_dict['proposed_table_name']}`")
    h2.metric("Tokens", proposal_dict.get("tokens_used", 0))
    h3.metric("Latency", f"{proposal_dict.get('duration_ms', 0) / 1000:.1f}s")

    # ---- Per-column table with edit capabilities ----------------------
    st.markdown("### 📐 Proposed columns")

    edited_columns_state = st.session_state.get("contract_arch_edited_columns")
    base_columns = edited_columns_state or proposal_dict["proposed_columns"]
    rows = []
    for c in base_columns:
        rows.append(
            {
                "Column": c["name"],
                "Type": c["type"],
                "Nullable": c["nullable"],
                "Deviation": c.get("deviation") or "",
                "Vendor field": c.get("vendor_field") or "",
                "Cited standard": c.get("matches_standard") or "",
                "Rationale": (c.get("rationale") or "")[:200],
            }
        )
    df_cols = pd.DataFrame(rows)
    edited_df = st.data_editor(
        df_cols,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "Column": st.column_config.TextColumn(width="medium", required=True),
            "Type": st.column_config.SelectboxColumn(
                width="small",
                options=[
                    "VARCHAR",
                    "TEXT",
                    "INTEGER",
                    "BIGINT",
                    "DECIMAL",
                    "NUMERIC",
                    "DATE",
                    "TIMESTAMP",
                    "BOOLEAN",
                    "JSON",
                ],
                required=True,
            ),
            "Nullable": st.column_config.CheckboxColumn(width="small"),
            "Deviation": st.column_config.SelectboxColumn(
                width="small", options=["", "RENAME", "RETYPE", "KEEP", "NEW"]
            ),
            "Vendor field": st.column_config.TextColumn(width="small"),
            "Cited standard": st.column_config.TextColumn(width="medium"),
            "Rationale": st.column_config.TextColumn(width="large"),
        },
        key=f"col_editor::{scope_key}",
    )

    # Re-shape edited rows back to the agent's column dict format
    edited_columns: list[dict[str, Any]] = []
    for _i, r in edited_df.iterrows():
        if not str(r["Column"]).strip():
            continue
        edited_columns.append(
            {
                "name": str(r["Column"]).strip(),
                "type": str(r["Type"]).strip().upper(),
                "nullable": bool(r["Nullable"]),
                "rationale": str(r["Rationale"] or ""),
                "matches_standard": str(r["Cited standard"] or "") or None,
                "vendor_field": str(r["Vendor field"] or "") or None,
                "deviation": str(r["Deviation"] or "") or None,
            }
        )

    # ---- DDL preview --------------------------------------------------
    with st.expander("📄 Proposed DDL", expanded=False):
        st.code(proposal_dict["proposed_ddl"], language="sql")

    # ---- Deviation log ------------------------------------------------
    devs = proposal_dict.get("deviation_log", [])
    if devs:
        with st.expander(f"🆚 Deviation log ({len(devs)} entries)", expanded=False):
            for d in devs:
                action = d.get("action", "?")
                cls = {
                    "RENAME": "arch-pill-rename",
                    "KEEP": "arch-pill-keep",
                    "NEW": "arch-pill-new",
                    "RETYPE": "arch-pill-retype",
                }.get(action, "arch-pill-keep")
                st.markdown(
                    f'<span class="arch-pill {cls}">{action}</span> '
                    f"`{d.get('vendor_field', '')}` → `{d.get('standard_field', '')}` — "
                    f"{d.get('reason', '')}",
                    unsafe_allow_html=True,
                )

    # ---- GX scaffold preview -----------------------------------------
    gx = proposal_dict.get("gx_scaffold", [])
    if gx:
        with st.expander(f"🛡 GX scaffold ({len(gx)} expectations)", expanded=False):
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "Expectation": e.get("expectation_type"),
                            "Column": e.get("column", ""),
                            "Dimension": e.get("dimension", ""),
                            "Severity": e.get("severity", ""),
                            "kwargs": json.dumps(e.get("kwargs", {})),
                        }
                        for e in gx
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )

    # ---- dbt scaffold ------------------------------------------------
    dbt_text = proposal_dict.get("dbt_scaffold", "")
    if dbt_text:
        with st.expander("🔧 dbt model scaffold", expanded=False):
            st.code(dbt_text, language="sql")

    # ---- Vendor spec markdown (Mode B output) ------------------------
    vendor_spec = proposal_dict.get("vendor_spec_md", "")
    if vendor_spec:
        with st.expander("📜 Vendor Data Contract Spec (markdown)", expanded=False):
            st.markdown(vendor_spec)

    # ---- Rationale ----------------------------------------------------
    if proposal_dict.get("rationale"):
        with st.expander("🧠 Agent rationale", expanded=False):
            st.markdown(proposal_dict["rationale"])

    # ---- Grounding chunks --------------------------------------------
    grounding = proposal_dict.get("grounding", [])
    if grounding:
        with st.expander(f"🔗 RAG grounding ({len(grounding)} chunks retrieved)", expanded=False):
            for g in grounding:
                st.markdown(
                    f'<span class="arch-cite">'
                    f"[{g.get('standard_code')}] {g.get('section_path')} "
                    f"(distance={float(g.get('distance', 0)):.3f})"
                    f"</span>",
                    unsafe_allow_html=True,
                )

    # ---- Approve / Save Draft ----------------------------------------
    st.markdown("---")
    notes = st.text_input(
        "Approval notes (audit trail)",
        value=f"approved by {st.session_state.get('user', 'operator')} via Data Contract Architect",
        key="contract_arch_notes",
    )
    a_left, a_right = st.columns([1, 5])
    if a_left.button(
        f"✅ Approve as {approval_mode}",
        type="primary",
        use_container_width=True,
    ):
        try:
            actor = st.session_state.get("user", "anonymous@local")
            settings = load_settings()
            llm = get_llm(settings)
            memory = AgentMemoryStore(embedder=get_embedder())
            # Re-hydrate ContractProposal from session for persist_design
            from datalink.agents.contract_architect import (
                ContractMode,
                ContractProposal,
                ProposedColumn,
            )

            cols_for_persist = [
                ProposedColumn(
                    name=c["name"],
                    type=c["type"],
                    nullable=bool(c["nullable"]),
                    rationale=c.get("rationale", ""),
                    matches_standard=c.get("matches_standard"),
                    vendor_field=c.get("vendor_field"),
                    deviation=c.get("deviation"),
                )
                for c in proposal_dict["proposed_columns"]
            ]
            re_proposal = ContractProposal(
                proposed_table_name=proposal_dict["proposed_table_name"],
                proposed_columns=cols_for_persist,
                proposed_ddl=proposal_dict["proposed_ddl"],
                standard_match_scores={
                    k: float(v) for k, v in proposal_dict.get("standard_match_scores", {}).items()
                },
                rationale=proposal_dict.get("rationale", ""),
                deviation_log=proposal_dict.get("deviation_log", []),
                gx_scaffold=proposal_dict.get("gx_scaffold", []),
                dbt_scaffold=proposal_dict.get("dbt_scaffold", ""),
                vendor_spec_md=proposal_dict.get("vendor_spec_md", ""),
                grounding=proposal_dict.get("grounding", []),
                tokens_used=int(proposal_dict.get("tokens_used", 0)),
                duration_ms=int(proposal_dict.get("duration_ms", 0)),
                proposer_model=proposal_dict.get("proposer_model", ""),
                embedding_model=proposal_dict.get("embedding_model", ""),
                temperature=float(proposal_dict.get("temperature", 0.0)),
                strictness=float(proposal_dict.get("strictness", 0.5)),
                mode=ContractMode(proposal_dict.get("mode", "FILE_DRIVEN")),
            )

            with _warehouse(readonly=False) as wh_w:
                design_id = persist_design(
                    warehouse=wh_w,
                    proposal=re_proposal,
                    client_id=selected_client,
                    source_type=source_type,
                    anchored_standards=st.session_state.get("contract_arch_anchored", picked_codes),
                    payload=st.session_state.get("contract_arch_input_payload", input_payload),
                    edited_columns=edited_columns,
                    actor=actor,
                    approval_mode=approval_mode,
                    notes=notes,
                )
                if approval_mode == "AUTO_APPROVE":
                    approve_design(
                        warehouse=wh_w,
                        design_id=design_id,
                        actor=actor,
                        notes="auto-approved at submit time",
                    )
            if approval_mode == "AUTO_APPROVE":
                st.success(
                    f"✅ Design `{design_id[:8]}…` APPROVED. All 5 artifacts emitted:\n\n"
                    f"  - 📄 `datalink/pipeline/bronze/ddl/{selected_client}_{source_type.lower()}_*.sql`\n"
                    f"  - 📌 `CONTROL.source_schema_contracts` (drift detection now active)\n"
                    f"  - 🛡 `CONTROL.dq_suites` (PENDING_REVIEW — review on DQ Review page)\n"
                    f"  - 🔧 `dbt/models/silver/{selected_client}/silver_*.sql`\n"
                    f"  - 📜 `docs/specs/{selected_client}_{source_type.lower()}_v1.md` + `.html` "
                    f"(open `.html` in browser, Cmd/Ctrl+P → Save as PDF for the vendor)"
                )
            else:
                st.success(
                    f"✅ Design saved as `{design_id[:8]}…`. Status: "
                    f"**PENDING_REVIEW** — approve in DQ Review to emit the 5 artifacts."
                )
        except Exception as e:
            st.error(f"Save failed: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# Pending designs panel
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown("### 📋 Pending designs for this client")

with _warehouse(readonly=True) as wh_p:
    pending = wh_p.query(
        f"""
        SELECT design_id, source_type, status, mode, created_at, created_by,
               array_size(parse_json(anchored_standards)) AS std_count
        FROM {CONTROL_SCHEMA}.contract_designs
        WHERE client_id = $cid AND status IN ('DRAFT', 'PENDING_REVIEW')
        ORDER BY created_at DESC
        LIMIT 20
        """,
        {"cid": selected_client},
    )

if not pending:
    st.caption(f"_No pending designs for `{selected_client}`._")
else:
    df_pending = pd.DataFrame(
        [
            {
                "design_id": r["design_id"][:8] + "…",
                "source": r["source_type"],
                "status": r["status"],
                "mode": r["mode"],
                "created": (
                    r["created_at"].strftime("%Y-%m-%d %H:%M")
                    if hasattr(r["created_at"], "strftime")
                    else str(r["created_at"])
                ),
                "by": r["created_by"],
                "#stds": r["std_count"],
            }
            for r in pending
        ]
    )
    st.dataframe(df_pending, use_container_width=True, hide_index=True)
