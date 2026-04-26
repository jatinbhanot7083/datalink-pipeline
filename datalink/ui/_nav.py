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

import base64
from pathlib import Path

import streamlit as st

# ---- Brand assets ---------------------------------------------------------
# DataLink logos are bundled into the repo at datalink/ui/static/ so the UI
# renders them without runtime dependency on datalinksoftware.com's CDN.
# Two variants:
#   datalink-logo-white.png   white/grey  — for dark backgrounds (sidebar)
#   datalink-logo-color.png   blue/full   — for light backgrounds (page heroes)
# Encoded once at import time so each Streamlit rerun reuses the cached
# base64 string instead of re-reading the file from disk.
_STATIC_DIR = Path(__file__).resolve().parent / "static"


def _b64_image(filename: str) -> str:
    """Return a data: URI for the named static image, or '' if missing."""
    p = _STATIC_DIR / filename
    if not p.exists():
        return ""
    mime = "image/png" if p.suffix.lower() == ".png" else "image/svg+xml"
    return f"data:{mime};base64,{base64.b64encode(p.read_bytes()).decode('ascii')}"


_LOGO_WHITE_DATA = _b64_image("datalink-logo-white.png")
_LOGO_COLOR_DATA = _b64_image("datalink-logo-color.png")

# Brand palette — light-slate sidebar so brand-blue logo + gold accents +
# section dividers all read clearly. Earlier dark-navy palette absorbed
# every border, hover-tint and gold accent — the visual hierarchy was
# effectively invisible. Light slate is the industry-standard enterprise
# sidebar look (Notion, Linear, GitHub) and lets DataLink blue + gold do
# the heavy lifting for branding.
# Industry-grade Command Center palette — references Datadog, Vercel,
# Linear, Grafana sidebar treatments. Medium-tone slate base so white
# link-cards stand out as crisp, button-like elements; prominent section
# headers in brand-navy give the menu its operational feel.
_SIDEBAR_BG = "#cbd5e1"  # slate-300 — darker, lets cards pop
_LINK_BG = "#ffffff"  # white card per link
_LINK_BG_ALT = "#f8fafc"  # subtle zebra tint
_LINK_BG_HOVER = "#dbeafe"  # soft brand-blue on hover
_SIDEBAR_BG_HOVER = "#dbeafe"  # legacy alias
_SIDEBAR_BORDER = "#94a3b8"  # slate-400 — borders read clearly on slate-300
_BRAND_GOLD = "#d4af37"
_BRAND_GOLD_DIM = "#b9952f"
_BRAND_NAVY = "#0a1a3e"
_TEXT_PRIMARY = "#0a1a3e"  # navy — high contrast on slate bg
_TEXT_MUTED = "#475569"  # slate-600 — for body text
_SECTION_HEADER = "#0a1a3e"  # navy — section labels are loud, not muted
_NAV_INPUT_INK = "#0a1a3e"


# -----------------------------------------------------------------------------
# Link tables — grouped by lifecycle stage so the sidebar reads as a story:
#
#   Operate  → run pipelines, watch live status
#   Author   → write / edit / review / register DQ checks
#   Observe  → see what happened (KPIs, trends, agent activity)
#   Inspect  → drill into raw rows
#
# Add a page: append a tuple to the right group. Move a page: swap groups.
# Reorder within a group: change the inner list order. Single source of
# truth — every page that calls render_sidebar(active=...) picks up the
# new arrangement on next reload.
# -----------------------------------------------------------------------------

# Each group: (section_label, css_slug, [(page_label, route, icon), ...])
# The css_slug feeds a per-category CSS class on every link card so each
# lifecycle stage carries its own tint — the eye groups same-category
# links by color the same way Notion/Linear/GitHub colour-code their
# sidebar sections.
INTERNAL_PAGE_GROUPS: list[tuple[str, str, list[tuple[str, str, str]]]] = [
    (
        "Operate",
        "cat-operate",  # amber wash — "active ops"
        [
            ("Control Tower", "/", "🏛️"),
        ],
    ),
    (
        "Author DQ",
        "cat-author",  # indigo wash — "build / author"
        [
            ("DQ AI Architect", "/DQ_AI_Architect", "🧪"),  # NL-driven entry point
            ("DQ Author", "/DQ_Author", "📝"),  # grid editor for power users
            ("DQ Review", "/DQ_Review", "👁️"),  # reviewer queue
            ("DQ Suite Registry", "/DQ_Suite_Registry", "📚"),  # read-only inventory
        ],
    ),
    (
        "Observe",
        "cat-observe",  # emerald wash — "monitor"
        [
            ("Executive Dashboard", "/Executive_Dashboard", "📈"),
            ("DQ Dashboard", "/DQ_Dashboard", "📊"),
            ("AI Agents", "/AI_Agents", "🤖"),
        ],
    ),
    (
        "Inspect",
        "cat-inspect",  # pink wash — "investigate"
        [
            ("Warehouse Explorer", "/Warehouse_Explorer", "🔎"),
        ],
    ),
]

# Flat alias kept for callers that still expect the (label, route) shape
# (tests, legacy code). Drops the icon + css_slug columns.
INTERNAL_PAGES: list[tuple[str, str]] = [
    (label, path) for _, _slug, pages in INTERNAL_PAGE_GROUPS for (label, path, _icon) in pages
]

EXTERNAL_TOOLS: list[tuple[str, str]] = [
    # The bare-login Adminer entry was removed — every real DB the team
    # touches is surfaced in the DB Admin (Adminer) section below with
    # driver/host/user pre-filled, so a generic Adminer link only gives
    # operators a worse UX (have to retype every field).
    ("Airflow", "http://localhost:8088"),
    ("pgAdmin", "http://localhost:5050"),
    ("Grafana", "http://localhost:3000"),
    ("Prometheus", "http://localhost:9090"),
    ("GX Data Docs", "http://localhost:8090"),
    ("Webhook Inbox", "http://localhost:9000"),
    ("File Browser (container FS)", "http://localhost:8082"),
    ("Portainer", "https://localhost:9443"),
]

# Adminer accepts driver/server/username/db via query-string so the login
# form is pre-populated; user just types the password. Each entry below
# deep-links into Adminer for one specific operational DB. The "DuckDB"
# entry was removed — DuckDB has no server UI, so the only sensible
# browser is the in-app Warehouse Explorer (Inspect section).
DATABASES: list[tuple[str, str]] = [
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

  /* Default text inside the sidebar — navy on slate-50 for high contrast.
     Light bg means the selectbox renders natively with no overrides
     needed (it's already dark-on-light). */
  [data-testid="stSidebar"],
  [data-testid="stSidebar"] p,
  [data-testid="stSidebar"] span,
  [data-testid="stSidebar"] div,
  [data-testid="stSidebar"] label {{
    color: {_TEXT_PRIMARY};
  }}
  /* The "Client" widget label sits above the selectbox — slate-600 mute
     so it reads as supporting text, not a primary heading. */
  [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {{
    color: {_TEXT_MUTED};
    font-size: .82rem;
    font-weight: 600;
    letter-spacing: .03em;
  }}

  /* === Client selectbox — undo the off-white inheritance ============== */
  /* Streamlit renders the selectbox with a white internal background, so
     the broad ``color: off-white`` rule above produced unreadable
     white-on-white text. Force dark navy ink inside the closed widget AND
     the dropdown popup (which Streamlit/BaseWeb portals OUTSIDE the
     sidebar at the document root, so it needs a body-level rule). The
     "Client" label above the widget stays off-white because it's a
     stWidgetLabel sibling, not a [data-baseweb="select"] descendant. */
  [data-testid="stSidebar"] [data-baseweb="select"] {{
    color: {_NAV_INPUT_INK} !important;
  }}
  [data-testid="stSidebar"] [data-baseweb="select"] *,
  [data-testid="stSidebar"] [data-baseweb="select"] div,
  [data-testid="stSidebar"] [data-baseweb="select"] span,
  [data-testid="stSidebar"] [data-baseweb="select"] input {{
    color: {_NAV_INPUT_INK} !important;
  }}
  /* Dropdown popup — rendered at body root via React portal. */
  [data-baseweb="popover"] [role="listbox"],
  [data-baseweb="popover"] [role="option"],
  [data-baseweb="popover"] [role="option"] *,
  [data-baseweb="menu"] [role="option"],
  [data-baseweb="menu"] [role="option"] * {{
    color: {_NAV_INPUT_INK} !important;
    background-color: #ffffff !important;
  }}
  [data-baseweb="popover"] [role="option"]:hover,
  [data-baseweb="menu"] [role="option"]:hover {{
    background-color: #f1f5f9 !important;
  }}
  [data-baseweb="popover"] [role="option"][aria-selected="true"],
  [data-baseweb="menu"] [role="option"][aria-selected="true"] {{
    background-color: #e0e7ff !important;
    font-weight: 600;
  }}

  /* Brand block — logo and wordmark stacked + centered at the top of
     the sidebar. Gold underline separates it from the navigation. */
  .dl-nav-brand {{
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: .4rem;
    margin: 0 auto 1.2rem auto;
    padding: .25rem 0 .75rem 0;
    border-bottom: 2px solid {_BRAND_GOLD};
    text-align: center;
  }}
  .dl-nav-logo-plaque {{
    /* Plaque kept as a no-op wrapper for backwards compat with markup
       that still includes it — it's invisible on the light sidebar. */
    background: transparent;
    border: none;
    box-shadow: none;
    padding: .25rem 0;
    align-self: center;
    width: 100%;
    max-width: 200px;
    text-align: center;
  }}
  .dl-nav-logo {{
    width: 100%;
    max-width: 170px;
    height: auto;
    display: inline-block;
  }}
  /* Wordmark — navy + tracked-out caps. Gold underline on the parent
     block carries the brand accent without needing the title to repeat it. */
  .dl-nav-title {{
    color: {_BRAND_NAVY};
    font-weight: 700;
    font-size: .95rem;
    letter-spacing: .14em;
    text-transform: uppercase;
    text-align: center;
    margin: 0;
    padding-bottom: 0;
    border-bottom: none;
  }}

  /* Section header — Operate / Author DQ / Observe / Inspect / External /
     Databases. Loud-and-proud: bigger than link rows, brand-navy, bold,
     spaced caps. Gold accent bar to the left + thin gold rule below so
     the menu reads as a clear hierarchy. */
  .dl-nav-section {{
    color: {_SECTION_HEADER};
    font-size: .92rem;
    font-weight: 800;
    letter-spacing: .12em;
    text-transform: uppercase;
    margin: 1.35rem 0 .5rem 0;
    padding: 0 0 .35rem .8rem;
    position: relative;
    border-bottom: 2px solid {_BRAND_GOLD};
  }}
  .dl-nav-section::before {{
    content: '';
    position: absolute;
    left: 0;
    top: .15rem;
    width: 4px;
    height: 1.05rem;
    background: {_BRAND_GOLD};
    border-radius: 2px;
  }}
  .dl-nav-section:first-of-type {{ margin-top: .5rem; }}

  /* The "Client" widget label sits above the selectbox — make it visually
     match a section header (it IS a section header in the user's mental
     model: client picker is the global filter). Use !important to win
     over Streamlit's own stWidgetLabel default styling. */
  [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {{
    color: {_SECTION_HEADER} !important;
    font-size: .92rem !important;
    font-weight: 800 !important;
    letter-spacing: .12em !important;
    text-transform: uppercase !important;
    margin-bottom: .25rem !important;
    padding-left: .8rem !important;
    position: relative !important;
  }}
  [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p::before {{
    content: '';
    position: absolute;
    left: 0;
    top: .25rem;
    width: 4px;
    height: .95rem;
    background: {_BRAND_GOLD};
    border-radius: 2px;
  }}

  /* Link card — compact white card, smaller than section header so the
     hierarchy reads correctly: SECTION (loud) > link (compact). Tighter
     padding + smaller font keeps the menu dense without feeling cramped. */
  .dl-nav-link {{
    display: flex;
    align-items: center;
    gap: .5rem;
    padding: .35rem .6rem;
    margin: .14rem 0;
    border-radius: 5px;
    text-decoration: none !important;
    color: {_TEXT_PRIMARY} !important;
    font-size: .82rem;
    font-weight: 500;
    background: {_LINK_BG};
    border: 1px solid {_SIDEBAR_BORDER};
    border-left: 3px solid {_SIDEBAR_BORDER};
    box-shadow: 0 1px 2px rgba(15, 23, 42, .08);
    transition: background .14s ease, border-color .14s ease,
                transform .14s ease, box-shadow .14s ease,
                color .14s ease;
  }}
  /* Per-category GRADIENTS — same left→right fade texture as the Control
     Tower active link, but each section gets its own hue. The eye groups
     same-category links by color while still seeing the unified gradient
     treatment across the whole sidebar. Active link's gold gradient
     OVERRIDES the category gradient (defined further down with .active). */
  .dl-nav-link.cat-operate {{
    background: linear-gradient(
        90deg,
        rgba(245,158,11,.32) 0%,
        rgba(245,158,11,.10) 70%,
        {_LINK_BG} 100%);
    border-color: #fcd34d;            /* amber-300 */
    border-left-color: #f59e0b;       /* amber-500 — bold left rail */
  }}
  .dl-nav-link.cat-author {{
    background: linear-gradient(
        90deg,
        rgba(99,102,241,.28) 0%,
        rgba(99,102,241,.08) 70%,
        {_LINK_BG} 100%);
    border-color: #a5b4fc;            /* indigo-300 */
    border-left-color: #6366f1;       /* indigo-500 */
  }}
  .dl-nav-link.cat-observe {{
    background: linear-gradient(
        90deg,
        rgba(14,165,233,.28) 0%,       /* sky — neutral, fresh */
        rgba(14,165,233,.08) 70%,
        {_LINK_BG} 100%);
    border-color: #7dd3fc;            /* sky-300 */
    border-left-color: #0ea5e9;       /* sky-500 */
  }}
  .dl-nav-link.cat-inspect {{
    background: linear-gradient(
        90deg,
        rgba(180,83,9,.28) 0%,         /* bronze/copper — warm, neutral */
        rgba(180,83,9,.08) 70%,
        {_LINK_BG} 100%);
    border-color: #fdba74;            /* orange-300 */
    border-left-color: #b45309;       /* amber-700 (bronze) */
  }}
  /* External Tools & Databases — distinct hues so they're visually
     separated from the lifecycle stages above. Teal = external portals
     (Airflow, Grafana, …); purple = data stores. */
  .dl-nav-link.cat-tools {{
    background: linear-gradient(
        90deg,
        rgba(20,184,166,.28) 0%,
        rgba(20,184,166,.08) 70%,
        {_LINK_BG} 100%);
    border-color: #5eead4;            /* teal-300 */
    border-left-color: #14b8a6;       /* teal-500 */
  }}
  .dl-nav-link.cat-data {{
    background: linear-gradient(
        90deg,
        rgba(100,116,139,.32) 0%,      /* slate — neutral, professional */
        rgba(100,116,139,.10) 70%,
        {_LINK_BG} 100%);
    border-color: #cbd5e1;            /* slate-300 */
    border-left-color: #475569;       /* slate-600 (deeper rail for contrast) */
  }}
  /* Hover — soft brand-blue tint, navy left-border accent, slight slide.
     Cards lift via a bigger drop shadow so the row pops above its
     neighbours. */
  .dl-nav-link:hover {{
    background: {_LINK_BG_HOVER};
    border-color: #93c5fd;          /* blue-300 */
    border-left-color: {_BRAND_NAVY};
    transform: translateX(2px);
    box-shadow: 0 4px 10px rgba(10, 26, 62, .12);
  }}
  /* Active — strong gold gradient, gold left-border, gold-dim bold text,
     and a subtle inset glow so the active row reads from across the
     screen as "you are here". */
  .dl-nav-link.active {{
    background: linear-gradient(
        90deg,
        rgba(212,175,55,.40) 0%,
        rgba(212,175,55,.12) 70%,
        {_LINK_BG} 100%);
    border-color: {_BRAND_GOLD};
    border-left-color: {_BRAND_GOLD};
    color: {_BRAND_GOLD_DIM} !important;
    font-weight: 700;
    box-shadow: 0 2px 6px rgba(212, 175, 55, .25),
                inset 0 0 0 1px rgba(212, 175, 55, .35);
  }}

  /* Icon slot — wider than before so emoji glyphs render at a comfortable
     size. Active link recolors the icon to gold via inheritance from the
     row's color (emoji ignore color, but the underlying tint is correct
     for any future SVG icons we swap in). */
  .dl-nav-ico {{
    color: {_TEXT_MUTED};
    font-size: 1.02rem;
    min-width: 1.4rem;
    text-align: center;
    line-height: 1;
    transition: transform .14s ease;
  }}
  .dl-nav-link:hover .dl-nav-ico {{
    color: {_BRAND_GOLD};
    transform: scale(1.08);
  }}
  .dl-nav-link.active .dl-nav-ico {{ color: {_BRAND_GOLD}; }}

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


def _render_link(
    label: str,
    url: str,
    *,
    external: bool,
    is_active: bool,
    icon: str,
    category_class: str = "",
) -> str:
    """Render a single anchor row — returns the HTML string.

    ``category_class`` (e.g. ``cat-author``) is appended to the link's
    class list so the per-category background tint kicks in. Empty string
    for external/database rows that don't belong to a lifecycle stage.
    """
    target = 'target="_blank" rel="noopener"' if external else 'target="_self"'
    base = "dl-nav-link active" if is_active else "dl-nav-link"
    cls = f"{base} {category_class}".strip()
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
    full-page navigations that wipe session_state, so we use the URL
    (``?client=aetna``) as a **bootstrap** mechanism — only when session_state
    is empty (e.g. right after a full-page reload).

    Priority (live session_state must win over stale URL, otherwise the
    selectbox cannot change away from whatever the URL was seeded with):

      1. If session_state has a real client → that's the truth. Push it
         to the URL so the sidebar's outgoing ``<a href>`` links carry it
         forward to the next page.
      2. Else if URL has ``?client=X`` → bootstrap: seed session_state from
         the URL so the selectbox and gated pages pick it up. This is the
         recovery path after a full-page reload wiped session_state.
      3. Else → no real client anywhere; clear any stale URL param.

    Sentinel values (anything starting with '—') and the legacy
    pseudo-tenant "default" are filtered out — we only propagate real
    tenants.
    """

    def _is_real(x: object) -> bool:
        return bool(x) and isinstance(x, str) and x != "default" and not x.startswith("—")

    url_client: str | None = st.query_params.get("client")
    ss_client = st.session_state.get("client_id")

    # session_state wins — it reflects the user's latest selectbox interaction
    if _is_real(ss_client):
        if st.query_params.get("client") != ss_client:
            st.query_params["client"] = str(ss_client)
        return str(ss_client)

    # Bootstrap from URL after a full-page reload wiped session_state
    if _is_real(url_client):
        st.session_state["client_id"] = url_client
        return str(url_client)

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

    MUST be called AFTER ``render_sidebar(...)`` so the selector widget
    has already been drawn. This function ONLY READS session_state —
    it does not attempt to write, because Streamlit forbids external
    modification of a key that's bound to an instantiated widget.
    """
    sel = st.session_state.get("client_id")
    if not sel or not isinstance(sel, str) or sel == CLIENT_SENTINEL or sel == "default":
        st.info(
            "👆 **Pick a client from the sidebar.** "
            "Every page renders content scoped to the selected tenant."
        )
        st.stop()
        # mypy doesn't know st.stop() halts — appease the return-type check.
        raise RuntimeError("unreachable: st.stop() halts execution")
    return str(sel)


def render_sidebar(active: str | None = None) -> None:
    """Render the enterprise sidebar. Call once per page, after set_page_config."""
    # Reconcile URL <-> session_state BEFORE rendering any hrefs so
    # sidebar links always carry the active client forward.
    current_client = _resolve_current_client()
    client_suffix = f"?client={current_client}" if current_client else ""

    with st.sidebar:
        st.markdown(_SIDEBAR_CSS, unsafe_allow_html=True)

        # Brand block — official blue DataLink logo on a soft white plaque
        # so the brand renders in its native colors against the dark navy
        # sidebar. Falls back to the emoji marker if the static asset is
        # missing (fresh checkout).
        if _LOGO_COLOR_DATA:
            st.markdown(
                f'<div class="dl-nav-brand">'
                f'  <div class="dl-nav-logo-plaque">'
                f'    <img src="{_LOGO_COLOR_DATA}" alt="DataLink" class="dl-nav-logo" />'
                f"  </div>"
                f'  <div class="dl-nav-title">Command Center</div>'
                f"</div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="dl-nav-title">🏛️ DataLink Command Center</div>',
                unsafe_allow_html=True,
            )

        # ---- CLIENT SELECTOR (global, one per session) ----
        _render_client_selector()

        # ---- DASHBOARDS — grouped by lifecycle stage ----
        # Each group renders its own section label + a stack of link cards
        # tinted by category (cat-operate / cat-author / cat-observe /
        # cat-inspect). Per-category tint = visual grouping at a glance.
        for group_label, css_slug, group_pages in INTERNAL_PAGE_GROUPS:
            st.markdown(
                f'<div class="dl-nav-section">{group_label}</div>',
                unsafe_allow_html=True,
            )
            for label, path, icon in group_pages:
                href = f"{path}{client_suffix}"
                st.markdown(
                    _render_link(
                        label,
                        href,
                        external=False,
                        is_active=(label == active),
                        icon=icon,
                        category_class=css_slug,
                    ),
                    unsafe_allow_html=True,
                )

        # ---- EXTERNAL TOOLS ----
        # Teal-tinted gradient (cat-tools) so external portals are visually
        # distinct from the lifecycle stages above.
        st.markdown(
            '<div class="dl-nav-section">External Tools</div>',
            unsafe_allow_html=True,
        )
        for label, url in EXTERNAL_TOOLS:
            st.markdown(
                _render_link(
                    label,
                    url,
                    external=True,
                    is_active=False,
                    icon="↗",
                    category_class="cat-tools",
                ),
                unsafe_allow_html=True,
            )

        # ---- DB ADMIN (Adminer deep-links) ----
        # Purple-tinted gradient (cat-data). Each link opens Adminer with
        # driver/host/user pre-populated — operator only types the password.
        st.markdown(
            '<div class="dl-nav-section">DB Admin (Adminer)</div>',
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
                    category_class="cat-data",
                ),
                unsafe_allow_html=True,
            )

        # ---- Footer ----
        st.markdown(
            '<div class="dl-nav-footer">DataLink · EvokeConnectCare™</div>',
            unsafe_allow_html=True,
        )
