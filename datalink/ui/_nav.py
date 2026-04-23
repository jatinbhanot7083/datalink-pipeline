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


def render_sidebar(active: str | None = None) -> None:
    """Render the enterprise sidebar. Call once per page, after set_page_config."""
    with st.sidebar:
        st.markdown(_SIDEBAR_CSS, unsafe_allow_html=True)

        st.markdown(
            '<div class="dl-nav-title">🏛️ DataLink Navigation</div>',
            unsafe_allow_html=True,
        )

        # ---- DASHBOARDS ----
        st.markdown(
            '<div class="dl-nav-section">Dashboards</div>',
            unsafe_allow_html=True,
        )
        for label, path in INTERNAL_PAGES:
            st.markdown(
                _render_link(
                    label,
                    path,
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
