"""DMD top-of-page admin banner — Phase 22.

Renders a horizontal banner with two operator actions:

  💥 **Wipe Everything** — TRUNCATEs Snowflake CONTROL.* + Postgres + SQL
      Server + drops per-client schemas + deletes generated DAG/dbt/DDL
      files + wipes Airflow metadata.  Idempotent.  Two-step confirmation
      (checkbox + button) so a stray click can't nuke the demo.

  📂 **Upload Product Catalogue** — accepts:
      * the canonical 2-sheet xlsx (33-dataset master + 943-field catalogue)
      * any single-sheet mapping spec (Field name / Required / Description)
      * any CSV/PSV/TSV/JSON data file (schema inferred from headers + dtypes)
      Routes via :func:`datalink.admin.smart_load`.

Both actions clear ``st.cache_data`` after running so the page-snapshot
re-fetches on the next rerun.

Designed to render in a single call from a Streamlit page:

    from datalink.ui._dmd_admin_banner import render_admin_banner
    render_admin_banner()
"""

from __future__ import annotations

import io

import streamlit as st


def _section_color(section_name: str) -> str:
    return {
        "snowflake": "#1d4ed8",
        "postgres": "#0d9488",
        "sqlserver": "#7c3aed",
        "files": "#b45309",
        "airflow": "#9333ea",
    }.get(section_name, "#475569")


def _render_wipe_result(rpt) -> None:
    """Pretty-print a WipeReport into the page."""
    if rpt.succeeded:
        st.success(rpt.summary_line())
    else:
        st.error(rpt.summary_line())

    cols = st.columns(len(rpt.sections) or 1)
    for col, (sec_name, body) in zip(cols, rpt.sections.items(), strict=False):
        counts = body.get("counts", {})
        warnings = body.get("warnings", [])
        errors = body.get("errors", [])
        color = _section_color(sec_name)
        with col:
            count_str = " · ".join(f"{k}={v}" for k, v in counts.items() if v) or "—"
            st.markdown(
                f"<div style='border-left:4px solid {color}; padding:.4rem .6rem; "
                f"background:#f8fafc; margin-bottom:.4rem;'>"
                f"<strong>{sec_name}</strong><br><span style='font-size:.85rem;'>{count_str}</span></div>",
                unsafe_allow_html=True,
            )
            if warnings:
                with st.expander(f"⚠ {len(warnings)} warning(s)", expanded=False):
                    for w in warnings[:20]:
                        st.markdown(f"- {w}")
                    if len(warnings) > 20:
                        st.caption(f"…and {len(warnings) - 20} more")
            if errors:
                with st.expander(f"❌ {len(errors)} error(s)", expanded=True):
                    for e in errors:
                        st.markdown(f"- `{e}`")


def _render_load_result(rpt) -> None:
    """Render a LoadReport with per-dataset categorization:
       • datasets_added            — net-new (green success banner)
       • datasets_unchanged        — exact-hash match (blue info banner)
       • datasets_schema_changed   — diff detected and re-applied (amber alert + per-dataset diff)

    Designed for the v2 UPSERT loader — NEVER wipes datasets not in the
    upload.  Each dataset is classified into exactly one bucket and the
    bucket counts are surfaced clearly in the result modal.
    """
    diffs = getattr(rpt, "schema_diffs", {}) or {}

    n_add = int(getattr(rpt, "datasets_added", 0))
    n_unch = int(
        getattr(rpt, "datasets_unchanged", 0) or getattr(rpt, "datasets_skipped_duplicate", 0)
    )
    n_chg = int(getattr(rpt, "datasets_schema_changed", 0))
    f_add = int(getattr(rpt, "fields_added", 0))
    f_rep = int(getattr(rpt, "fields_replaced", 0))

    # ---- Top-line outcome banner --------------------------------------
    if rpt.errors:
        st.error(rpt.summary())
    elif n_add == 0 and n_chg == 0 and n_unch > 0:
        # Exact-hash match across every dataset — the "this is identical" case
        st.info(
            f"🔁 **Schema already exists** — all {n_unch} dataset(s) in this "
            f"upload have IDENTICAL schemas to what's already in the catalogue.  "
            f"**No changes applied.**  If you intended to update a schema, "
            f"check that your file actually differs from the existing one.",
            icon="🔁",
        )
    elif n_chg > 0:
        # Schema diff detected — surface it loudly so the operator reviews
        st.warning(
            f"⚠️ **Schema changes detected** — {n_chg} dataset(s) had different "
            f"schemas than the existing catalogue.  Changes have been applied; "
            f"please review the per-dataset diff below.",
            icon="⚠️",
        )
        if n_add:
            st.success(f"➕ Also added {n_add} brand-new dataset(s) ({f_add} new fields).")
    elif n_add > 0:
        st.success(rpt.summary())
    else:
        st.success(rpt.summary())

    # ---- Bucket counter strip -----------------------------------------
    c1, c2, c3 = st.columns(3)
    c1.metric("➕ Added", n_add, help="Net-new datasets inserted")
    c2.metric("🔁 Unchanged", n_unch, help="Exact-hash match — no write performed")
    c3.metric(
        "⚠️ Schema-changed",
        n_chg,
        help="Existing dataset(s) had different schema; re-applied with diff",
    )

    # ---- Per-dataset diff details (only show when interesting) --------
    interesting = [
        (code, d) for code, d in diffs.items() if d.get("status") in ("added", "changed")
    ]
    if interesting:
        with st.expander(f"📋 Per-dataset details ({len(interesting)})", expanded=(n_chg > 0)):
            for code, d in interesting:
                status = d.get("status")
                if status == "added":
                    n_fields = len(d.get("added") or [])
                    st.markdown(
                        f"### ➕ `{code}` &nbsp; "
                        f"<span style='color:#16a34a;font-weight:600;font-size:.85em'>"
                        f"NEW DATASET · {n_fields} fields</span>",
                        unsafe_allow_html=True,
                    )
                    if d.get("added"):
                        st.caption(
                            "Fields: "
                            + ", ".join(f"`{f}`" for f in d["added"][:12])
                            + (f" _+{len(d['added'])-12} more_" if len(d["added"]) > 12 else "")
                        )
                elif status == "changed":
                    a, r, c = d.get("added") or [], d.get("removed") or [], d.get("changed") or []
                    delta_summary = " · ".join(
                        filter(
                            None,
                            [
                                f"+{len(a)} added" if a else None,
                                f"−{len(r)} removed" if r else None,
                                f"~{len(c)} changed" if c else None,
                            ],
                        )
                    )
                    st.markdown(
                        f"### ⚠️ `{code}` &nbsp; "
                        f"<span style='color:#b45309;font-weight:600;font-size:.85em'>"
                        f"SCHEMA CHANGED · {delta_summary}</span>",
                        unsafe_allow_html=True,
                    )
                    if a:
                        st.markdown(
                            f"**Added fields ({len(a)}):** "
                            + ", ".join(
                                f"<span style='background:#dcfce7;color:#15803d;padding:1px 6px;"
                                f"border-radius:3px;font-family:monospace;font-size:.78em'>{f}</span>"
                                for f in a[:20]
                            )
                            + (f" <em>+{len(a)-20} more</em>" if len(a) > 20 else ""),
                            unsafe_allow_html=True,
                        )
                    if r:
                        st.markdown(
                            f"**Removed fields ({len(r)}):** "
                            + ", ".join(
                                f"<span style='background:#fee2e2;color:#b91c1c;padding:1px 6px;"
                                f"border-radius:3px;font-family:monospace;font-size:.78em;"
                                f"text-decoration:line-through'>{f}</span>"
                                for f in r[:20]
                            )
                            + (f" <em>+{len(r)-20} more</em>" if len(r) > 20 else ""),
                            unsafe_allow_html=True,
                        )
                    if c:
                        st.markdown(
                            f"**Req/Opt changed ({len(c)}):** "
                            + ", ".join(
                                f"<span style='background:#fef3c7;color:#a16207;padding:1px 6px;"
                                f"border-radius:3px;font-family:monospace;font-size:.78em'>{f}</span>"
                                for f in c[:20]
                            )
                            + (f" <em>+{len(c)-20} more</em>" if len(c) > 20 else ""),
                            unsafe_allow_html=True,
                        )

    # ---- Warnings + errors --------------------------------------------
    if rpt.warnings:
        with st.expander(f"⚠ {len(rpt.warnings)} warning(s)"):
            for w in rpt.warnings[:30]:
                st.markdown(f"- {w}")
    if rpt.errors:
        with st.expander(f"❌ {len(rpt.errors)} error(s)", expanded=True):
            for e in rpt.errors:
                if " — FIX: " in e:
                    head, fix = e.split(" — FIX: ", 1)
                    st.markdown(f"❌ **{head}**")
                    st.markdown(f"   🔧  _How to fix:_ {fix}")
                    st.markdown("")
                else:
                    st.markdown(f"- `{e}`")


@st.fragment
def render_admin_banner() -> None:
    """Render the wipe + upload + role-toggle banner.  Call once at the top of DMD.

    Wrapped in ``@st.fragment`` so every interaction inside the banner
    (Arm-wipe checkbox toggle, Role dropdown change, Wipe button click,
    Upload dialog open/close) reruns ONLY this banner — NOT the rest of
    the page.  Without this, every checkbox tick caused the whole DMD page
    (33-row grid, 5 KPI cards, filter bar) to re-render and Streamlit-flash,
    which the operator hated (rightly so).
    """
    # Container style — gives the banner a distinct visual identity at the
    # top of an already-dense page.
    st.markdown(
        """
        <style>
          .dmd-admin-banner {
            background: linear-gradient(90deg, #fef3c7 0%, #f0f9ff 50%, #ecfdf5 100%);
            border-left: 5px solid #d4af37;
            padding: 0.6rem 1rem 0.3rem 1rem;
            margin-bottom: 1.2rem;
            border-radius: 4px;
          }
          .dmd-admin-banner h4 {
            margin: 0 0 .15rem 0;
            color: #0a1a3e;
            font-size: 0.95rem;
            font-weight: 600;
          }
          .dmd-admin-banner p {
            margin: 0;
            color: #475569;
            font-size: 0.82rem;
          }
          /* ============================================================
             Earlier attempt: hide ALL Streamlit progress indicators to
             "kill the spinning" during dialog dismissal.  That was wrong —
             it made every progress indicator invisible, so Save / Approve /
             AI Construct / Anchor click all looked dead to the operator
             ("nothing happened when I clicked").
             Now: show ALL progress indicators.  The brief "Running" flash
             during reruns is the documented Streamlit feedback that an
             interaction registered — operators NEED that feedback.
             ============================================================ */
          /* (All hiding rules removed — progress indicators are visible) */
        </style>
        <div class="dmd-admin-banner">
          <h4>⚙️  Admin actions — wipe / upload catalogue / role</h4>
          <p>Repeatable demo state.  Wipe = nukes Snowflake + Postgres + SQL Server data.  Upload = xlsx / mapping spec / CSV-JSON.  Role gates LIVE-schema edits (DBA-only: ADD column · DEPRECATE column · never DELETE).</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 4-column action row: Arm-wipe · Wipe button · Upload popover · Role.
    # Result banners render BELOW this row at full page width (Phase 22 fix
    # — previously they were squeezed into a 5th column ~200 px wide which
    # made the per-section cards illegible).
    cols = st.columns([1.2, 1.6, 1.3, 1.6])

    # -----------------------------------------------------------------------
    # WIPE  (Phase 22 — on_click callback pattern)
    #
    # Streamlit forbids writing to ``st.session_state[key]`` AFTER the
    # widget with that key has been rendered.  An on_click callback runs
    # BEFORE the next rerun, so it's the canonical place to reset widget
    # state (here: auto-disarm the checkbox after a successful wipe).
    # -----------------------------------------------------------------------
    def _do_wipe() -> None:
        from datalink.admin import wipe_everything

        rpt = wipe_everything(actor="dmd:ui:wipe_button")
        # Safe to write here — runs BEFORE the next render of the checkbox.
        st.session_state["dmd_admin_wipe_armed"] = False
        st.session_state["_dmd_admin_last_wipe"] = rpt
        try:
            st.cache_data.clear()
        except Exception:
            pass

    with cols[0]:
        wipe_armed = st.checkbox(
            "Arm wipe",
            value=False,
            key="dmd_admin_wipe_armed",
            help="Tick this to enable the 💥 Wipe button.  Two-step on purpose.",
        )

    with cols[1]:
        st.button(
            "💥  Wipe Everything",
            disabled=not wipe_armed,
            type="primary" if wipe_armed else "secondary",
            use_container_width=True,
            key="dmd_admin_wipe_btn",
            on_click=_do_wipe,
            help="TRUNCATE Snowflake CONTROL.* + Postgres + SQL Server + drop "
            "per-client schemas + delete generated DAG/dbt/DDL files + wipe "
            "Airflow metadata.  Keeps infra (warehouse / role / DB / API keys).",
        )

    # -----------------------------------------------------------------------
    # UPLOAD  (gated on Role selection — Upload Catalogue requires an explicit
    # USER/DBA pick before the dialog can open)
    # -----------------------------------------------------------------------
    def _open_upload_dialog() -> None:
        """on_click handler — validates Role is selected before opening
        the upload dialog.  If no Role chosen, raises a transient warning
        flag that renders below the action row (auto-clears after one
        display)."""
        role_value = st.session_state.get("dmd_role")
        if not role_value:
            st.session_state["_dmd_admin_role_required_warning"] = True
            return
        st.session_state["_dmd_admin_upload_open"] = True

    with cols[2]:
        st.button(
            "📂  Upload catalogue",
            use_container_width=True,
            on_click=_open_upload_dialog,
            key="dmd_admin_upload_open_btn",
            help="Pick a Role first (right), then drop a Product Catalogue "
            "xlsx OR a single-sheet mapping spec OR a raw CSV/PSV/TSV/JSON "
            "data file.  Encoding auto-detected (UTF-8 / UTF-8-BOM / "
            "CP1252 / Latin-1).",
        )

    # -----------------------------------------------------------------------
    # ROLE TOGGLE  (label collapsed for alignment with the buttons; default
    # is "no selection" so user must explicitly pick — see _open_upload_dialog
    # which gates Upload Catalogue on this being non-empty)
    # -----------------------------------------------------------------------
    with cols[3]:
        # Map current session state → selectbox index (or None if unset)
        _current_role = st.session_state.get("dmd_role")
        _role_options = ["USER", "DBA"]
        _role_index = _role_options.index(_current_role) if _current_role in _role_options else None
        role = st.selectbox(
            "Role",
            options=_role_options,
            index=_role_index,
            placeholder="Select Role",
            key="dmd_role",
            label_visibility="collapsed",  # ← removes the "Role" label so the
            # selectbox top-edge aligns with the
            # adjacent buttons
            help="USER: read-only on LIVE schemas; can author DRAFT.  "
            "DBA: can ADD column or DEPRECATE column on LIVE schemas (never "
            "DELETE — backward-compat sacred).  Required to enable Upload "
            "Catalogue.  Demo-grade RBAC; production swap to OIDC.",
        )
        if role == "DBA":
            st.caption("🛡️ DBA mode")
        elif role == "USER":
            st.caption("👤 read-only on LIVE")
        else:
            st.caption("⚠ Select a role to enable Upload")

    # Transient warning shown when user clicked Upload Catalogue without
    # picking a Role first.  Auto-clears on next render.
    if st.session_state.pop("_dmd_admin_role_required_warning", False):
        st.warning(
            "⚠️ **Please pick a Role first** (right-most dropdown — USER or DBA) "
            "before clicking 📂 Upload catalogue.",
            icon="🛡️",
        )

    # -----------------------------------------------------------------------
    # Upload dialog — renders when the flag is set; closes itself on action
    # -----------------------------------------------------------------------
    # Rotating-key trick: each open of the dialog gets a fresh file_uploader
    # key so the widget never carries stale state across closes.
    _upload_seq = int(st.session_state.get("_dmd_admin_upload_seq", 0))
    _upload_widget_key = f"dmd_admin_upload__{_upload_seq}"

    # Callback fired when user dismisses the upload dialog via the × close
    # button or click-outside.  WITHOUT this, the session_state flag stays
    # True and any later rerun (role change, checkbox toggle, etc.) would
    # re-open the dialog — the exact bug operators reported.
    def _on_upload_dismiss() -> None:
        st.session_state["_dmd_admin_upload_open"] = False

    if st.session_state.get("_dmd_admin_upload_open"):

        @st.dialog("📂  Upload catalogue", width="large", on_dismiss=_on_upload_dismiss)
        def _upload_dialog():
            st.markdown(
                "**Canonical Product Catalogue uploader.**  "
                "Drop a file in one of the supported formats.  Validation runs "
                "before any Snowflake write — bad files surface a structured "
                "error with the fix.  Need a starter template? Download one below."
            )

            # ----- Sample template downloads ----------------------------------
            from pathlib import Path as _P

            _SAMPLES_DIR = _P("/opt/datalink") / "data" / "sample" / "canonical_templates"
            if not _SAMPLES_DIR.is_dir():
                # When running outside the container, fall back to repo root
                _SAMPLES_DIR = (
                    _P(__file__).resolve().parents[2] / "data" / "sample" / "canonical_templates"
                )

            with st.expander("📥  Download canonical sample template", expanded=False):
                st.caption(
                    "Each file shows the EXACT shape the loader expects.  "
                    "Open it, replace the data, save, upload.  All samples "
                    "contain the same 5 datasets · 20 fields — just in "
                    "different formats."
                )
                sample_files = [
                    (
                        "xlsx",
                        "canonical_catalogue.xlsx",
                        "**Single sheet** `08_Field_Catalogue` — one row per field · Dataset filled on first row of each group",
                    ),
                    (
                        "csv",
                        "canonical_catalogue.csv",
                        "Flat — one row per field · Dataset repeated on every row",
                    ),
                    ("tsv", "canonical_catalogue.tsv", "Tab-separated equivalent of CSV"),
                    ("psv", "canonical_catalogue.psv", "Pipe-separated equivalent of CSV"),
                    (
                        "json",
                        "canonical_catalogue.json",
                        '`{"field_catalogue": [...]}` — single array · dataset metadata derived',
                    ),
                    (
                        "jsonl",
                        "canonical_catalogue.jsonl",
                        "One JSON field-object per line · no `_kind` discriminator needed",
                    ),
                ]
                for label, fname, desc in sample_files:
                    fpath = _SAMPLES_DIR / fname
                    if not fpath.exists():
                        st.caption(
                            f"⚠ `{fname}` missing on disk · run `scripts/build_canonical_samples.py`"
                        )
                        continue
                    sd_cols = st.columns([1.4, 4.6, 1.6])
                    with sd_cols[0]:
                        st.markdown(f"**.{label}**")
                    with sd_cols[1]:
                        st.caption(desc)
                    with sd_cols[2]:
                        try:
                            with fpath.open("rb") as fh:
                                st.download_button(
                                    f"⬇ {label.upper()}",
                                    data=fh.read(),
                                    file_name=fname,
                                    mime="application/octet-stream",
                                    key=f"dmd_admin_dl_{label}",
                                    use_container_width=True,
                                )
                        except Exception as exc:
                            st.caption(f"err: {exc}")

            # ----- Single-dataset template downloads (Req 6) ----------------
            with st.expander(
                "📥  Download SINGLE-dataset template (additive upload)", expanded=False
            ):
                st.caption(
                    "Use these to upload ONE new dataset additively without "
                    "touching the existing catalogue.  Same column convention "
                    "as the multi-dataset templates above — just one dataset's "
                    "rows.  Rename `New Dataset` to your real dataset name "
                    "before upload."
                )
                single_files = [
                    ("xlsx", "single_dataset.xlsx", "Single sheet · 4 sample fields"),
                    ("csv", "single_dataset.csv", "Flat CSV · 4 sample fields"),
                    ("tsv", "single_dataset.tsv", "Tab-separated"),
                    ("psv", "single_dataset.psv", "Pipe-separated"),
                    ("json", "single_dataset.json", '`{"field_catalogue": [...]}`'),
                    ("jsonl", "single_dataset.jsonl", "One JSON object per line"),
                ]
                for label, fname, desc in single_files:
                    fpath = _SAMPLES_DIR / fname
                    if not fpath.exists():
                        st.caption(
                            f"⚠ `{fname}` missing — run `scripts/build_canonical_samples.py`"
                        )
                        continue
                    sd_cols = st.columns([1.4, 4.6, 1.6])
                    with sd_cols[0]:
                        st.markdown(f"**.{label}**")
                    with sd_cols[1]:
                        st.caption(desc)
                    with sd_cols[2]:
                        try:
                            with fpath.open("rb") as fh:
                                st.download_button(
                                    f"⬇ {label.upper()}",
                                    data=fh.read(),
                                    file_name=fname,
                                    mime="application/octet-stream",
                                    key=f"dmd_admin_dl_single_{label}",
                                    use_container_width=True,
                                )
                        except Exception as exc:
                            st.caption(f"err: {exc}")

            st.markdown("---")

            up = st.file_uploader(
                "Drop file here",
                type=["xlsx", "xls", "xlsm", "csv", "tsv", "psv", "json", "jsonl"],
                accept_multiple_files=False,
                key=_upload_widget_key,
                label_visibility="visible",
                help="Encoding auto-detected (UTF-8 / UTF-8-BOM / CP1252 / Latin-1).",
            )
            st.text_input(
                "Dataset name (only used for single-sheet / data-file uploads)",
                placeholder="e.g. provider_demographics",
                key="dmd_admin_upload_dataset_hint",
                help="Optional. Ignored for the canonical single-sheet `08_Field_Catalogue` workbook.",
            )

            dlg_cols = st.columns([1.2, 1, 4])
            with dlg_cols[0]:
                process_clicked = st.button(
                    "⬆  Process upload",
                    type="primary",
                    disabled=up is None,
                    use_container_width=True,
                    key="dmd_admin_do_upload",
                )
            with dlg_cols[1]:
                cancel_clicked = st.button(
                    "Cancel",
                    use_container_width=True,
                    key="dmd_admin_upload_cancel",
                )

            if cancel_clicked:
                st.session_state["_dmd_admin_upload_open"] = False
                # Fragment-scoped rerun — only the admin banner re-renders,
                # the rest of the page (grid, KPI cards) stays put.
                # App-scoped — @st.dialog is internally wrapped as a
                # fragment by Streamlit (see streamlit/elements/
                # dialog_decorator.py line 99-107).  scope="fragment"
                # only re-runs the dialog's body, not the outer code
                # that gated the dialog open.  Only scope="app" (default)
                # reruns the outer admin-banner fragment so the dialog
                # conditional re-evaluates to False and the modal closes.
                # The brief parent re-render is the documented cost of
                # programmatic dialog dismissal — Streamlit limitation.
                st.rerun()

            if process_clicked and up is not None:
                from datalink.admin import smart_load

                try:
                    with st.spinner(f"Processing `{up.name}`…"):
                        rpt = smart_load(
                            io.BytesIO(up.getvalue()),
                            filename_hint=up.name,
                            actor=f"dmd:ui:upload:{up.name}",
                        )
                    st.session_state["_dmd_admin_last_load"] = rpt
                except Exception as exc:
                    st.error(f"❌ Upload failed: `{type(exc).__name__}: {exc}`")
                    return
                # Success — bump widget seq, close the upload form, and
                # let the result-modal block below show the outcome.  NO
                # app-scoped st.rerun: the calling fragment will re-render
                # on its own and the grid section stays put with a 🔄
                # refresh button the operator clicks when ready.
                st.session_state["_dmd_admin_upload_seq"] = _upload_seq + 1
                st.session_state["_dmd_admin_upload_open"] = False
                # Invalidate the DMD snapshot cache so the next 🔄 click
                # on the grid fetches fresh rows.
                try:
                    from datalink.ui._dmd_data import _fetch_snapshot_uncached

                    _fetch_snapshot_uncached.clear()
                except Exception:
                    pass
                # App-scoped — @st.dialog is internally wrapped as a
                # fragment by Streamlit (see streamlit/elements/
                # dialog_decorator.py line 99-107).  scope="fragment"
                # only re-runs the dialog's body, not the outer code
                # that gated the dialog open.  Only scope="app" (default)
                # reruns the outer admin-banner fragment so the dialog
                # conditional re-evaluates to False and the modal closes.
                # The brief parent re-render is the documented cost of
                # programmatic dialog dismissal — Streamlit limitation.
                st.rerun()

        _upload_dialog()

    # -----------------------------------------------------------------------
    # RESULT MODALS — open a @st.dialog when a recent wipe/upload completed.
    # Replaces the in-page expander banners (which forced full-page renders
    # and clutter).  Each modal has an explicit Close button that clears
    # the relevant session_state key and reruns the fragment.
    # -----------------------------------------------------------------------
    last_wipe = st.session_state.get("_dmd_admin_last_wipe")
    last_load = st.session_state.get("_dmd_admin_last_load")

    # Same dismiss-clearing pattern for the result modals — without it the
    # × close on the modal would visually go away but the state flag would
    # survive, causing the modal to reappear on the next rerun.
    def _on_wipe_dismiss() -> None:
        st.session_state.pop("_dmd_admin_last_wipe", None)

    def _on_load_dismiss() -> None:
        st.session_state.pop("_dmd_admin_last_load", None)

    if last_wipe is not None and not st.session_state.get("_dmd_admin_upload_open"):

        @st.dialog("💥  Wipe complete", width="large", on_dismiss=_on_wipe_dismiss)
        def _wipe_result_modal():
            _render_wipe_result(last_wipe)
            st.markdown("---")
            st.caption(
                "Grids on the page may still show stale data until you click "
                "🔄 Refresh on a section.  This is by design — no surprise "
                "full-page reloads."
            )
            if st.button(
                "Close",
                key="dmd_admin_dismiss_wipe",
                type="primary",
                use_container_width=True,
            ):
                st.session_state.pop("_dmd_admin_last_wipe", None)
                # App-scoped — @st.dialog is internally wrapped as a
                # fragment by Streamlit (see streamlit/elements/
                # dialog_decorator.py line 99-107).  scope="fragment"
                # only re-runs the dialog's body, not the outer code
                # that gated the dialog open.  Only scope="app" (default)
                # reruns the outer admin-banner fragment so the dialog
                # conditional re-evaluates to False and the modal closes.
                # The brief parent re-render is the documented cost of
                # programmatic dialog dismissal — Streamlit limitation.
                st.rerun()

        _wipe_result_modal()

    if last_load is not None and not st.session_state.get("_dmd_admin_upload_open"):

        @st.dialog("📂  Upload result", width="large", on_dismiss=_on_load_dismiss)
        def _load_result_modal():
            _render_load_result(last_load)
            st.markdown("---")
            st.caption(
                "Click **🔄 Refresh** on the Global Medallion Schema grid "
                "to pull the new rows.  The grid won't auto-reload — "
                "that's intentional to avoid full-page churn."
            )
            if st.button(
                "Close",
                key="dmd_admin_dismiss_load",
                type="primary",
                use_container_width=True,
            ):
                st.session_state.pop("_dmd_admin_last_load", None)
                # App-scoped — @st.dialog is internally wrapped as a
                # fragment by Streamlit (see streamlit/elements/
                # dialog_decorator.py line 99-107).  scope="fragment"
                # only re-runs the dialog's body, not the outer code
                # that gated the dialog open.  Only scope="app" (default)
                # reruns the outer admin-banner fragment so the dialog
                # conditional re-evaluates to False and the modal closes.
                # The brief parent re-render is the documented cost of
                # programmatic dialog dismissal — Streamlit limitation.
                st.rerun()

        _load_result_modal()
