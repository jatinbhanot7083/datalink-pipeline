"""Shared sidebar navigation — one unified, enterprise-styled sidebar per page.

Phase 6 consolidation (revised):

  * ONE nav — Streamlit's auto page-nav is hidden via CSS; this module
    renders the complete nav so there's no duplication.
  * Dark navy sidebar with gold accents — matches the Control Tower brand.
  * WCAG-AA contrast (off-white text on navy, 12:1).
  * Three sections: Dashboards · External Tools · Databases.

Usage from any page (must be called AFTER st.set_page_config):

    from datalink.ui._nav import render_sidebar
    render_sidebar(active="Control Tower")
"""

from __future__ import annotations

import streamlit as st

# Brand palette — kept identical to control_tower.py so the sidebar feels
# like part of the app, not a third-party widget.
_SIDEBAR_BG = "#0b1629"  # deepest navy
_SIDEBAR_BG_HOVER = "#132140"  # one shade up for row hover
_BRAND_GOLD = "#d4af37"
_BRAND_GOLD_DIM = "#a88827"  # for section rules
_TEXT_PRIMARY = "#e5e7eb"  # off-white — 12.1:1 contrast on _SIDEBAR_BG
_TEXT_MUTED = "#94a3b8"  # slate-400 — for section labels + icons


# -----------------------------------------------------------------------------
# Link tables — flat + explicit. Add/remove a row = one line diff.
# -----------------------------------------------------------------------------

INTERNAL_PAGES: list[tuple[str, str]] = [
    ("Control Tower", "/"),
    ("DQ Author", "/DQ_Author"),
    ("DQ Review", "/DQ_Review"),
    ("Executive Dashboard", "/Executive_Dashboard"),
    ("CrewAI Dashboard", "/CrewAI_Dashboard"),
    ("DQ Dashboard", "/DQ_Dashboard"),
    ("DQ Suite Registry", "/DQ_Suite_Registry"),
    ("Warehouse Explorer", "/Warehouse_Explorer"),
]

EXTERNAL_TOOLS: list[tuple[str, str]] = [
    ("Airflow", "http://localhost:8088"),
    ("Adminer (generic DB admin)", "http://localhost:8081"),
    ("pgAdmin", "http://localhost:5050"),
    ("Grafana", "http://localhost:3000"),
    ("Prometheus", "http://localhost:9090"),
    ("GX Data Docs", "http://localhost:8090"),
    ("Webhook Inbox", "http://localhost:9000"),
    ("File Browser (container FS)", "http://localhost:8082"),
    ("Portainer", "https://localhost:9443"),
]

# Adminer accepts driver/server/username/db via query-string so the login
# form is pre-populated; user just types the password.
DATABASES: list[tuple[str, str]] = [
    (
        "DuckDB (Bronze/Silver/Gold)",
        "/Warehouse_Explorer",  # in-app schema+data explorer; DuckDB has no server UI
    ),
    (
        "SQL Server — DataLinkUM",
        "http://localhost:8081/?mssql=sqlserver&username=sa&db=DataLinkUM",
    ),
    (
        "Postgres — datalink_um",
        "http://localhost:8081/?pgsql=postgres&username=datalink&db=datalink_um",
    ),
    (
        "Postgres Replica — datalink_um_replica",
        "http://localhost:8081/?pgsql=postgres_replica&username=datalink&db=datalink_um_replica",
    ),
]


# One big CSS block — scoped to [data-testid="stSidebar"] so nothing leaks
# into the main page content. Written in a single f-string so we can
# interpolate the palette constants without polluting the inline HTML below.
_SIDEBAR_CSS = f"""
<style>
  /* Hide Streamlit's auto page-nav so our custom nav is the only one.
     This removes the duplicated list at the top of the sidebar. */
  [data-testid="stSidebarNav"] {{ display: none !important; }}

  /* Paint the sidebar dark navy, full height. */
  [data-testid="stSidebar"] {{
    background: {_SIDEBAR_BG} !important;
    border-right: 1px solid #1f2937;
  }}
  [data-testid="stSidebar"] > div:first-child {{
    padding-top: 1.2rem;
    padding-left: .9rem;
    padding-right: .9rem;
  }}

  /* Default text inside the sidebar — off-white, high contrast. */
  [data-testid="stSidebar"],
  [data-testid="stSidebar"] p,
  [data-testid="stSidebar"] span,
  [data-testid="stSidebar"] div {{
    color: {_TEXT_PRIMARY};
  }}

  /* Brand title row. */
  .dl-nav-title {{
    color: {_BRAND_GOLD};
    font-weight: 700;
    font-size: 1.05rem;
    letter-spacing: .02em;
    margin: 0 0 1.1rem 0;
    padding-bottom: .55rem;
    border-bottom: 2px solid {_BRAND_GOLD};
  }}

  /* Section header (DASHBOARDS / EXTERNAL TOOLS / DATABASES). */
  .dl-nav-section {{
    color: {_TEXT_MUTED};
    font-size: .68rem;
    font-weight: 700;
    letter-spacing: .12em;
    text-transform: uppercase;
    margin: 1.25rem 0 .4rem 0;
    padding-bottom: .25rem;
    border-bottom: 1px solid {_BRAND_GOLD_DIM};
  }}
  .dl-nav-section:first-of-type {{ margin-top: .4rem; }}

  /* Link base style. */
  .dl-nav-link {{
    display: flex;
    align-items: center;
    gap: .5rem;
    padding: .42rem .6rem;
    margin: .12rem 0;
    border-radius: 4px;
    text-decoration: none !important;
    color: {_TEXT_PRIMARY} !important;
    font-size: .86rem;
    font-weight: 500;
    border-left: 3px solid transparent;
    transition: background .12s ease, border-color .12s ease;
  }}
  .dl-nav-link:hover {{
    background: {_SIDEBAR_BG_HOVER};
    border-left-color: {_BRAND_GOLD_DIM};
  }}
  .dl-nav-link.active {{
    background: {_SIDEBAR_BG_HOVER};
    border-left-color: {_BRAND_GOLD};
    color: {_BRAND_GOLD} !important;
    font-weight: 700;
  }}

  /* Icon prefix on external / database rows. */
  .dl-nav-ico {{
    color: {_TEXT_MUTED};
    font-size: .78rem;
    min-width: 1rem;
    text-align: center;
  }}
  .dl-nav-link:hover .dl-nav-ico {{ color: {_BRAND_GOLD}; }}

  /* Footer at the very bottom. */
  .dl-nav-footer {{
    color: #64748b;
    font-size: .68rem;
    text-align: center;
    margin: 1.5rem 0 .2rem 0;
    padding-top: .6rem;
    border-top: 1px solid #1f2937;
  }}
</style>
"""


def _render_link(label: str, url: str, *, external: bool, is_active: bool, icon: str) -> str:
    """Render a single anchor row — returns the HTML string."""
    target = 'target="_blank" rel="noopener"' if external else 'target="_self"'
    cls = "dl-nav-link active" if is_active else "dl-nav-link"
    return (
        f'<a href="{url}" {target} class="{cls}">'
        f'<span class="dl-nav-ico">{icon}</span>'
        f"<span>{label}</span>"
        "</a>"
    )


def _resolve_current_client() -> str | None:
    """Reconcile URL query param and session_state, return the active client.

    The Control Tower's client selector writes to ``session_state["client_id"]``
    via the widget's ``key=`` binding. The custom ``<a href>`` sidebar causes
    full-page navigations that wipe session_state, so we keep the URL
    (``?client=aetna``) as the cross-page source of truth.

    On every page render this function:
      1. If URL has ``?client=X`` → persist X into session_state so the
         Control Tower selectbox shows it when the user lands there.
      2. Else if session_state has a real client → push it to the URL so
         the sidebar's outgoing links carry it forward.

    Sentinel values (anything starting with '—') and the legacy
    pseudo-tenant "default" are filtered out — we only propagate real
    tenants.
    """

    def _is_real(x: object) -> bool:
        return bool(x) and isinstance(x, str) and x != "default" and not x.startswith("—")

    url_client: str | None = st.query_params.get("client")
    ss_client = st.session_state.get("client_id")

    if _is_real(url_client):
        # URL wins — persist to session_state so widgets pick it up
        st.session_state["client_id"] = url_client
        return str(url_client)

    if _is_real(ss_client):
        # Session has a real client, push to URL for nav persistence
        if st.query_params.get("client") != ss_client:
            st.query_params["client"] = str(ss_client)
        return str(ss_client)

    # No real client anywhere — clear stale URL param if present
    if "client" in st.query_params:
        del st.query_params["client"]
    return None


# Sentinel shown as the first option in the unified client selector.
# When it's the current selection, pages that need a client halt
# rendering via require_client().
CLIENT_SENTINEL = "— Select a client —"


def _list_real_clients() -> list[str]:
    """Read distinct real tenants from CONTROL.dq_suites (excludes 'default')."""
    try:
        from datalink.ui._query import query_silent

        df = query_silent(
            "SELECT DISTINCT client_id FROM CONTROL.dq_suites "
            "WHERE client_id <> 'default' ORDER BY client_id"
        )
        if df.empty:
            return []
        return [str(c) for c in df["client_id"].tolist()]
    except Exception:
        return []


def _render_client_selector() -> str | None:
    """Render the one-and-only client selector at the top of the sidebar.

    State is persisted via ``session_state["client_id"]`` (intra-session)
    and ``?client=X`` URL param (cross-page navigation). Returns the
    currently-selected real client, or ``None`` if the sentinel is active.
    """
    clients = _list_real_clients()
    options = [CLIENT_SENTINEL, *clients]

    # Stale-value guard: if the remembered client was removed from the
    # registry, reset to the sentinel so the selectbox doesn't blow up.
    if st.session_state.get("client_id") not in options:
        st.session_state["client_id"] = CLIENT_SENTINEL

    st.selectbox(
        "Client",
        options=options,
        key="client_id",
        help=(
            "One client at a time. Drives DQ suite versions + per-client "
            "schemas (BRONZE_AETNA, …). Selection persists across every "
            "page via URL query param."
        ),
    )

    sel = st.session_state["client_id"]

    # Keep URL in sync so cross-page navigation preserves the choice.
    if sel != CLIENT_SENTINEL:
        if st.query_params.get("client") != sel:
            st.query_params["client"] = str(sel)
    elif "client" in st.query_params:
        del st.query_params["client"]

    return None if sel == CLIENT_SENTINEL else str(sel)


def require_client() -> str:
    """Halt page rendering until a real client is selected in the sidebar.

    Call this at the top of any page whose content is client-scoped.
    When no selection is active, shows a prompt and st.stop()s. When a
    real client is active, returns it.

    MUST be called AFTER ``render_sidebar(...)`` so the selector has
    been drawn first.
    """
    sel = _resolve_current_client()
    if sel is None:
        st.info(
            "👆 **Pick a client from the sidebar.** "
            "Every page renders content scoped to the selected tenant."
        )
        st.stop()
        # mypy doesn't know st.stop() halts — appease the return-type check.
        raise RuntimeError("unreachable: st.stop() halts execution")
    return sel


def render_sidebar(active: str | None = None) -> None:
    """Render the enterprise sidebar. Call once per page, after set_page_config."""
    # Reconcile URL <-> session_state BEFORE rendering any hrefs so
    # sidebar links always carry the active client forward.
    current_client = _resolve_current_client()
    client_suffix = f"?client={current_client}" if current_client else ""

    with st.sidebar:
        st.markdown(_SIDEBAR_CSS, unsafe_allow_html=True)

        st.markdown(
            '<div class="dl-nav-title">🏛️ DataLink Navigation</div>',
            unsafe_allow_html=True,
        )

        # ---- CLIENT SELECTOR (global, one per session) ----
        _render_client_selector()

        # ---- DASHBOARDS ----
        st.markdown(
            '<div class="dl-nav-section">Dashboards</div>',
            unsafe_allow_html=True,
        )
        for label, path in INTERNAL_PAGES:
            href = f"{path}{client_suffix}"
            st.markdown(
                _render_link(
                    label,
                    href,
                    external=False,
                    is_active=(label == active),
                    icon="▸",
                ),
                unsafe_allow_html=True,
            )

        # ---- EXTERNAL TOOLS ----
        st.markdown(
            '<div class="dl-nav-section">External Tools</div>',
            unsafe_allow_html=True,
        )
        for label, url in EXTERNAL_TOOLS:
            st.markdown(
                _render_link(label, url, external=True, is_active=False, icon="↗"),
                unsafe_allow_html=True,
            )

        # ---- DATABASES ----
        st.markdown(
            '<div class="dl-nav-section">Databases</div>',
            unsafe_allow_html=True,
        )
        for label, url in DATABASES:
            # In-app paths (start with "/") open in the same tab; Adminer
            # deep-links open in a new tab.
            is_external = url.startswith(("http://", "https://"))
            st.markdown(
                _render_link(
                    label,
                    url,
                    external=is_external,
                    is_active=False,
                    icon="🗄",
                ),
                unsafe_allow_html=True,
            )

        # ---- Footer ----
        st.markdown(
            '<div class="dl-nav-footer">DataLink · EvokeConnectCare™</div>',
            unsafe_allow_html=True,
        )
