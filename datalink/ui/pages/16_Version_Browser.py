"""Version Browser — Phase 16.2 (Wave 2, Items 7+8).

Universal version timeline + diff viewer + restore for ANY artifact:
  * Silver schemas
  * Gold schemas
  * pipeline_instances
  * dq_suites

Pick artifact_type + artifact_id → see history (newest first) + side-by-side
semantic diff between any two versions + one-click restore.
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
    page_title="Version Browser",
    page_icon="🕒",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402
from datalink.versioning import store as v_store  # noqa: E402

render_sidebar(active="Version Browser")

st.title("🕒 Version Browser")
st.caption(
    "Universal version timeline + side-by-side diff + one-click restore for "
    "every versioned artifact. Auditor-friendly trail of who-changed-what-when."
)

# Artifact-type picker
ARTIFACT_TYPES = [
    ("silver_schema", "🥈 Silver schemas"),
    ("gold_schema", "🥇 Gold schemas"),
    ("pipeline_instance", "🏛 Pipeline instances"),
    ("dq_suite", "🧪 DQ suites"),
]

at_col, aid_col = st.columns([1, 2])
with at_col:
    artifact_type = st.selectbox(
        "Artifact type",
        options=[a for a, _ in ARTIFACT_TYPES],
        format_func=lambda x: dict(ARTIFACT_TYPES)[x],
    )

# List artifact_ids for this type from version_history
with warehouse_ctx(readonly=True) as wh:
    distinct_ids = list(
        wh.query(
            f"SELECT DISTINCT artifact_id, artifact_scope_key, "
            f"       MAX(version) AS latest_version "
            f"FROM {CONTROL_SCHEMA}.version_history "
            f"WHERE artifact_type = %(at)s "
            f"GROUP BY artifact_id, artifact_scope_key "
            f"ORDER BY artifact_scope_key",
            {"at": artifact_type},
        )
    )

with aid_col:
    if not distinct_ids:
        st.warning(
            f"No versioned `{artifact_type}` artifacts yet. Versions are "
            "recorded automatically when you author Silver/Gold or deploy "
            "pipelines (Phase 16.2 hooks pending wire-up — record_version "
            "is called from the bridges)."
        )
        st.stop()
    options = [
        f"{d['artifact_scope_key']}  ({d['artifact_id'][:12]}…)  → v{d['latest_version']}"
        for d in distinct_ids
    ]
    sel_idx = st.selectbox(
        "Artifact",
        options=range(len(options)),
        format_func=lambda i: options[i],
    )
    artifact = distinct_ids[sel_idx]

artifact_id = str(artifact["artifact_id"])
scope_key = str(artifact["artifact_scope_key"])

# ---------- History timeline -------------------------------------------------
st.markdown("---")
st.markdown(f"### 📜 Version timeline for `{scope_key}`")

events = v_store.history(artifact_type=artifact_type, artifact_id=artifact_id)
hist_rows = []
for e in events:
    pin_marker = "📌" if e.pinned else ""
    cur_marker = "✅ CURRENT" if e.is_current else ""
    tags = ", ".join(e.tags) if e.tags else "—"
    diff_meta = "—"
    if e.diff_summary:
        d = e.diff_summary
        bits = []
        if d.get("added_columns"):
            bits.append(f"+{len(d['added_columns'])} cols")
        if d.get("removed_columns"):
            bits.append(f"-{len(d['removed_columns'])} cols")
        if d.get("changed_columns"):
            bits.append(f"~{len(d['changed_columns'])} cols")
        diff_meta = " · ".join(bits) if bits else "no changes"
    hist_rows.append(
        {
            "v": f"v{e.version}",
            "Status": cur_marker,
            "Pinned": pin_marker,
            "Change": e.change_kind,
            "Diff": diff_meta,
            "Tags": tags,
            "By": e.created_by,
            "When": e.created_at.strftime("%Y-%m-%d %H:%M"),
            "Reason": (e.change_reason or "")[:50],
        }
    )
st.dataframe(pd.DataFrame(hist_rows), use_container_width=True, hide_index=True)

# ---------- Side-by-side diff viewer ----------------------------------------
st.markdown("### 🔍 Side-by-side diff")
ev_options = [
    f"v{e.version} · {e.change_kind} · {e.created_at.strftime('%Y-%m-%d %H:%M')}" for e in events
]
diff_l, diff_r = st.columns([1, 1])
with diff_l:
    a_idx = st.selectbox(
        "Version A",
        options=range(len(ev_options)),
        format_func=lambda i: ev_options[i],
        index=min(1, len(events) - 1),
    )
with diff_r:
    b_idx = st.selectbox(
        "Version B", options=range(len(ev_options)), format_func=lambda i: ev_options[i], index=0
    )

if 0 <= a_idx < len(events) and 0 <= b_idx < len(events) and a_idx != b_idx:
    a = events[a_idx]
    b = events[b_idx]
    try:
        d = v_store.diff(a.version_event_id, b.version_event_id)
    except Exception as exc:
        d = None
        st.error(f"Diff failed: {exc}")

    if d:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"#### v{a.version} (A)")
            st.caption(f"By: `{a.created_by}` · {a.created_at}")
            st.json(a.snapshot, expanded=False)
        with c2:
            st.markdown(f"#### v{b.version} (B)")
            st.caption(f"By: `{b.created_by}` · {b.created_at}")
            st.json(b.snapshot, expanded=False)
        st.markdown("#### Semantic diff")
        if not (
            d.get("added_keys")
            or d.get("removed_keys")
            or d.get("changed_keys")
            or d.get("added_columns")
            or d.get("removed_columns")
            or d.get("changed_columns")
        ):
            st.success("No semantic differences.")
        else:
            if d.get("added_columns"):
                st.success(
                    f"➕ Added columns ({len(d['added_columns'])}): "
                    + ", ".join(d["added_columns"])
                )
            if d.get("removed_columns"):
                st.error(
                    f"➖ Removed columns ({len(d['removed_columns'])}): "
                    + ", ".join(d["removed_columns"])
                )
            if d.get("changed_columns"):
                st.warning(f"🔄 Changed columns ({len(d['changed_columns'])}):")
                for ch in d["changed_columns"]:
                    st.text(f"  {ch['key']}")
                    cca, ccb = st.columns(2)
                    with cca:
                        st.json(ch["old"], expanded=False)
                    with ccb:
                        st.json(ch["new"], expanded=False)
            if d.get("added_keys"):
                st.info(f"➕ Added top-level keys: {', '.join(d['added_keys'])}")
            if d.get("removed_keys"):
                st.info(f"➖ Removed top-level keys: {', '.join(d['removed_keys'])}")
            if d.get("changed_keys"):
                st.info(
                    "🔄 Changed top-level keys: " + ", ".join([k["key"] for k in d["changed_keys"]])
                )

# ---------- Inspect / Restore / Tag ------------------------------------------
st.markdown("### 🔧 Inspect / Restore / Tag")
sel_idx_inspect = st.selectbox(
    "Pick a version",
    options=range(len(ev_options)),
    format_func=lambda i: ev_options[i],
    key="inspect_select",
)
sel_event = events[sel_idx_inspect]
st.caption(f"`{sel_event.version_event_id}` · `{sel_event.created_by}` · `{sel_event.created_at}`")

ic1, ic2, ic3, ic4, ic5 = st.columns([1, 1, 1, 1, 4])
with ic1:
    if st.button(
        "⏪ Restore as current",
        key=f"restore_{sel_event.version_event_id}",
        disabled=sel_event.is_current,
        help="Creates a new RESTORE event with this snapshot. Audit-trailed.",
    ):
        v_store.restore(
            sel_event.version_event_id,
            restored_by="ui:operator",
            change_reason=f"Restored from v{sel_event.version}",
        )
        st.toast(f"⏪ Restored v{sel_event.version}", icon="✅")
        st.rerun()
with ic2:
    if sel_event.pinned:
        if st.button(
            "📌 Unpin", key=f"unpin_{sel_event.version_event_id}", use_container_width=True
        ):
            v_store.unpin(sel_event.version_event_id)
            st.rerun()
    else:
        if st.button("📌 Pin", key=f"pin_{sel_event.version_event_id}", use_container_width=True):
            v_store.pin(sel_event.version_event_id)
            st.rerun()
with ic3:
    if st.button(
        "✅ Set current",
        key=f"setcur_{sel_event.version_event_id}",
        disabled=sel_event.is_current,
        help="Promote this version to current without recording RESTORE.",
        use_container_width=True,
    ):
        v_store.set_current(sel_event.version_event_id, by="ui:operator")
        st.rerun()

# Tag editor
with st.expander(f"🏷  Manage tags on v{sel_event.version}"):
    try:
        with warehouse_ctx(readonly=True) as wh:
            catalog = list(
                wh.query(
                    f"SELECT tag_name, tag_category, color_hex FROM {CONTROL_SCHEMA}.proposal_tags "
                    f"ORDER BY tag_category, tag_name"
                )
            )
    except Exception:
        catalog = []
    current = set(sel_event.tags)
    new_set = st.multiselect(
        "Tags",
        options=[t["tag_name"] for t in catalog],
        default=list(current),
        key=f"tags_{sel_event.version_event_id}",
    )
    if st.button("Apply tag changes", key=f"taga_{sel_event.version_event_id}", type="primary"):
        for t in set(new_set) - current:
            try:
                v_store.add_tag(
                    version_event_id=sel_event.version_event_id,
                    tag_name=t,
                    assigned_by="ui:operator",
                )
            except Exception as exc:
                st.error(f"add {t}: {exc}")
        for t in current - set(new_set):
            v_store.remove_tag(
                version_event_id=sel_event.version_event_id, tag_name=t, removed_by="ui:operator"
            )
        st.rerun()

# Upstream notifications panel
st.markdown("---")
st.markdown("### 📦 Upstream-update notifications")
notifs = v_store.list_open_notifications()
if not notifs:
    st.caption("No open upstream-update notifications.")
else:
    rows = [
        {
            "Severity": n.severity,
            "Downstream": f"{n.artifact_type}: {n.artifact_scope_key}",
            "Upstream": f"{n.upstream_artifact_type}",
            "Consumed v": n.consumed_version,
            "Available v": n.available_version,
            "Created": n.created_at.strftime("%Y-%m-%d %H:%M"),
        }
        for n in notifs
    ]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
