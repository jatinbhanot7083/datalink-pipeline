"""AI Reasoning Transparency — Phase 16.4 (Wave 4 #18).

For every AI proposal in CONTROL.agent_proposals, surface:
  * The exact prompt that was sent (agent_input.prompt)
  * The RAG context chunks that were injected
  * The full structured output (proposal_payload)
  * Token + cost breakdown
  * Confidence scoring (extracted from payload where present)

Auditor-grade transparency for the "AI black box" critique.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="AI Reasoning",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.proposals import store as p_store  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="AI Reasoning")

st.title("🔍 AI Reasoning Transparency")
st.caption(
    "What did the agent see? What did it decide? Why? Full chain-of-context "
    "for every proposal. EU AI Act and HHS-AI-guidance ready."
)

# Pick a proposal
with warehouse_ctx(readonly=True) as wh:
    proposals = list(
        wh.query(
            f"""
        SELECT proposal_id, agent_type, scope_key, version, status,
               total_tokens, estimated_cost_usd, latency_ms, model_id,
               created_at, created_by
          FROM {CONTROL_SCHEMA}.agent_proposals
          ORDER BY created_at DESC LIMIT 100
        """
        )
    )

if not proposals:
    st.info(
        "No AI proposals yet. Generate one in **Pipeline Architect** or "
        "**Data Model Designer** to see reasoning."
    )
    st.stop()

filter_agent = st.selectbox(
    "Agent type filter",
    options=["(all)", *sorted({p["agent_type"] for p in proposals})],
)
filtered = [p for p in proposals if filter_agent == "(all)" or p["agent_type"] == filter_agent]

opts = [
    f"{p['agent_type']} · {p['scope_key']} · v{p['version']} · "
    f"{p['status']} · ${float(p['estimated_cost_usd']):.4f} · "
    f"{p['created_at']}"
    for p in filtered
]
sel_idx = st.selectbox(
    "Proposal",
    options=range(len(opts)),
    format_func=lambda i: opts[i],
)
sel = filtered[sel_idx]

# Load full record
prop = p_store.get(sel["proposal_id"])
if not prop:
    st.error("Proposal vanished from store.")
    st.stop()

# Header card
st.markdown("---")
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric(
        "Tokens",
        f"{prop.total_tokens:,}",
        delta=f"prompt {prop.prompt_tokens:,} + completion {prop.completion_tokens:,}",
    )
with c2:
    st.metric("Cost", f"${prop.estimated_cost_usd:.4f}", delta=prop.model_id)
with c3:
    st.metric("Latency", f"{prop.latency_ms} ms")
with c4:
    st.metric("Status", prop.status.value)

# Tabs: prompt / RAG context / structured output / reasoning
tab_input, tab_output, tab_reason, tab_meta = st.tabs(
    [
        "📥 Agent input (prompt + RAG)",
        "📤 Agent output (structured)",
        "🤖 Chain-of-thought / reasoning",
        "📊 Metadata + lineage",
    ]
)

with tab_input:
    st.markdown("**The exact prompt + context the agent received:**")
    st.json(prop.agent_input, expanded=False)

with tab_output:
    st.markdown("**Full structured output the agent returned:**")
    st.json(prop.proposal_payload, expanded=False)

with tab_reason:
    # Look for typical reasoning fields in the payload
    payload = prop.proposal_payload
    reasoning_fields = [
        ("Executive summary", payload.get("executive_summary")),
        ("Clone vs Build rationale", payload.get("clone_recommendation")),
        ("Override review", payload.get("override_review")),
        ("Rationale", payload.get("rationale")),
        ("AI reasoning", payload.get("ai_reasoning")),
    ]
    for label, val in reasoning_fields:
        if val:
            st.markdown(f"**{label}:**")
            if isinstance(val, str):
                st.markdown(val)
            else:
                st.json(val, expanded=False)
            st.markdown("---")
    if not any(v for _, v in reasoning_fields):
        st.caption(
            "No structured reasoning fields in this payload. (Different agents "
            "expose reasoning differently — pipeline_architect always carries "
            "executive_summary + rationale.)"
        )

with tab_meta:
    st.markdown("**Lineage stamps:**")
    lineage = {
        "scope_type": prop.scope_type,
        "scope_key": prop.scope_key,
        "version": prop.version,
        "supersedes": prop.supersedes_proposal_id,
        "parent_client_id": prop.parent_client_id,
        "cloned_from_proposal_id": prop.cloned_from_proposal_id,
        "linked_artifact": (
            f"{prop.linked_artifact_type}:{prop.linked_artifact_id}"
            if prop.linked_artifact_id
            else None
        ),
        "tags": prop.tags,
        "pinned": prop.pinned,
    }
    st.json(lineage, expanded=True)

    # Downstream — what got created from this proposal?
    if prop.linked_artifact_id and prop.linked_artifact_type == "pipeline_instance":
        with warehouse_ctx(readonly=True) as wh:
            inst = list(
                wh.query(
                    f"SELECT * FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                    f"WHERE instance_id = $iid",
                    {"iid": prop.linked_artifact_id},
                )
            )
        if inst:
            st.markdown("**Linked pipeline instance:**")
            st.dataframe(pd.DataFrame([dict(inst[0])]), use_container_width=True, hide_index=True)
