"""Generate the DataLink Data Quality demo deck (8 slides).

Output:
  docs/demo/DataLink_DQ_Demo.pptx      (committed copy)
  /mnt/c/Users/Jatin/Downloads/...     (Windows side, when run from WSL)

Uses python-pptx built-in MSO_SHAPE stencils — CAN (storage layers),
GEAR_6 (orchestration containers), HEXAGON (DAG tasks), FLOWCHART_DECISION
(halt/continue branches), CLOUD (external services), FLOWCHART_DOCUMENT
(reports).  No external icon packs needed — these render as proper
enterprise architecture stencils, same as Visio's "Networking and
Containers" set.

Run:
    python scripts/build_dq_demo_pptx.py
"""

from __future__ import annotations

from pathlib import Path

from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Inches, Pt

# ---------------------------------------------------------------------------
# Brand palette
# ---------------------------------------------------------------------------
NAVY = RGBColor(0x0F, 0x17, 0x2A)
BLUE = RGBColor(0x1E, 0x3A, 0x8A)
BLUE_LIGHT = RGBColor(0xDB, 0xEA, 0xFE)
SLATE = RGBColor(0x47, 0x55, 0x69)
MUTED = RGBColor(0x64, 0x74, 0x8B)
BG = RGBColor(0xF8, 0xFA, 0xFC)
BORDER = RGBColor(0xCB, 0xD5, 0xE1)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)

BRONZE = RGBColor(0xB4, 0x53, 0x09)
BRONZE_LIGHT = RGBColor(0xFD, 0xE6, 0x8A)  # warm tan
SILVER = RGBColor(0x94, 0xA3, 0xB8)
SILVER_LIGHT = RGBColor(0xE2, 0xE8, 0xF0)
GOLD = RGBColor(0xD9, 0x77, 0x06)
GOLD_LIGHT = RGBColor(0xFE, 0xF3, 0xC7)

GREEN = RGBColor(0x10, 0xB9, 0x81)
GREEN_LIGHT = RGBColor(0xD1, 0xFA, 0xE5)
RED = RGBColor(0xDC, 0x26, 0x26)
RED_LIGHT = RGBColor(0xFE, 0xE2, 0xE2)
AMBER = RGBColor(0xF5, 0x9E, 0x0B)
AMBER_LIGHT = RGBColor(0xFE, 0xF3, 0xC7)
PURPLE = RGBColor(0x7C, 0x3A, 0xED)
PURPLE_LIGHT = RGBColor(0xED, 0xE9, 0xFE)
TEAL = RGBColor(0x0D, 0x94, 0x88)

# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------
SLIDE_W = Inches(13.333)
SLIDE_H = Inches(7.5)


def set_fill(shape, color: RGBColor) -> None:
    shape.fill.solid()
    shape.fill.fore_color.rgb = color


def set_no_fill(shape) -> None:
    shape.fill.background()


def set_line(shape, color: RGBColor | None, width_pt: float = 1.0) -> None:
    if color is None:
        shape.line.fill.background()
        return
    shape.line.color.rgb = color
    shape.line.width = Pt(width_pt)


def set_text(
    shape,
    text: str,
    *,
    size: int = 14,
    bold: bool = False,
    color: RGBColor = NAVY,
    align: int = PP_ALIGN.CENTER,
    anchor: int = MSO_ANCHOR.MIDDLE,
    italic: bool = False,
    font: str = "Segoe UI",
) -> None:
    tf = shape.text_frame
    tf.word_wrap = True
    tf.margin_left = Inches(0.05)
    tf.margin_right = Inches(0.05)
    tf.margin_top = Inches(0.03)
    tf.margin_bottom = Inches(0.03)
    tf.vertical_anchor = anchor
    tf.text = ""  # clear
    lines = text.split("\n")
    for i, line in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align
        run = p.add_run()
        run.text = line
        run.font.name = font
        run.font.size = Pt(size)
        run.font.bold = bold
        run.font.italic = italic
        run.font.color.rgb = color


def add_textbox(
    slide,
    left: float,
    top: float,
    width: float,
    height: float,
    text: str,
    **kw,
) -> object:
    tb = slide.shapes.add_textbox(Inches(left), Inches(top), Inches(width), Inches(height))
    set_text(tb, text, **kw)
    return tb


def add_shape(
    slide,
    shape_type,
    left: float,
    top: float,
    width: float,
    height: float,
    *,
    fill: RGBColor | None = None,
    line: RGBColor | None = BORDER,
    line_w: float = 1.0,
    text: str = "",
    text_kw: dict | None = None,
):
    shp = slide.shapes.add_shape(
        shape_type, Inches(left), Inches(top), Inches(width), Inches(height)
    )
    if fill is None:
        set_no_fill(shp)
    else:
        set_fill(shp, fill)
    set_line(shp, line, line_w)
    if text:
        set_text(shp, text, **(text_kw or {}))
    return shp


def add_connector(
    slide,
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    color: RGBColor = SLATE,
    width_pt: float = 1.75,
):
    from pptx.enum.shapes import MSO_CONNECTOR

    conn = slide.shapes.add_connector(
        MSO_CONNECTOR.STRAIGHT, Inches(x1), Inches(y1), Inches(x2), Inches(y2)
    )
    conn.line.color.rgb = color
    conn.line.width = Pt(width_pt)
    # arrowhead via XML hack
    line_el = conn.line._get_or_add_ln()
    from lxml import etree

    nsmap = {"a": "http://schemas.openxmlformats.org/drawingml/2006/main"}
    tail = etree.SubElement(
        line_el,
        "{http://schemas.openxmlformats.org/drawingml/2006/main}tailEnd",
    )
    tail.set("type", "triangle")
    tail.set("w", "med")
    tail.set("h", "med")
    return conn


# ---------------------------------------------------------------------------
# Common slide chrome
# ---------------------------------------------------------------------------
def add_header(slide, title: str, subtitle: str = "", *, accent: RGBColor = BLUE) -> None:
    # accent ribbon top-left
    add_shape(slide, MSO_SHAPE.RECTANGLE, 0, 0, 0.35, 7.5, fill=accent, line=None)
    add_textbox(
        slide, 0.6, 0.25, 12.5, 0.55, title, size=28, bold=True, color=NAVY, align=PP_ALIGN.LEFT
    )
    if subtitle:
        add_textbox(
            slide, 0.6, 0.78, 12.5, 0.35, subtitle, size=14, color=MUTED, align=PP_ALIGN.LEFT
        )
    # bottom divider
    add_shape(slide, MSO_SHAPE.RECTANGLE, 0.6, 1.22, 12.1, 0.025, fill=BORDER, line=None)


def add_footer(slide, page: int, total: int) -> None:
    add_textbox(
        slide,
        0.6,
        7.15,
        6.0,
        0.3,
        "DataLink Command Center · DQ Demo · 11 May 2026",
        size=9,
        color=MUTED,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide,
        7.0,
        7.15,
        5.7,
        0.3,
        f"{page} / {total}",
        size=9,
        color=MUTED,
        align=PP_ALIGN.RIGHT,
    )


# ---------------------------------------------------------------------------
# Visio-style stencil composites
# ---------------------------------------------------------------------------
def stencil_storage(
    slide, left, top, *, label: str, fill: RGBColor, w: float = 1.7, h: float = 1.3
):
    """3D cylinder = data layer (Bronze/Silver/Gold)."""
    shp = add_shape(slide, MSO_SHAPE.CAN, left, top, w, h, fill=fill, line=SLATE, line_w=1.0)
    set_text(shp, label, size=13, bold=True, color=WHITE)
    return shp


def stencil_container(
    slide,
    left,
    top,
    *,
    label: str,
    sub: str = "",
    w: float = 1.8,
    h: float = 1.0,
    fill: RGBColor = BLUE_LIGHT,
):
    """Rounded rectangle with a small gear in the corner = orchestration container."""
    box = add_shape(
        slide, MSO_SHAPE.ROUNDED_RECTANGLE, left, top, w, h, fill=fill, line=BLUE, line_w=1.5
    )
    gear = add_shape(
        slide, MSO_SHAPE.GEAR_6, left + 0.05, top + 0.05, 0.32, 0.32, fill=BLUE, line=None
    )
    set_no_fill(gear)
    set_fill(gear, BLUE)
    txt = f"{label}\n{sub}" if sub else label
    add_textbox(
        slide,
        left + 0.05,
        top + 0.32,
        w - 0.1,
        h - 0.4,
        txt,
        size=11,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.CENTER,
    )
    return box


def stencil_dag_task(
    slide,
    left,
    top,
    *,
    label: str,
    fill: RGBColor = WHITE,
    line: RGBColor = SLATE,
    w: float = 1.55,
    h: float = 0.55,
    text_size: int = 10,
):
    """Hexagon = Airflow DAG task."""
    shp = add_shape(slide, MSO_SHAPE.HEXAGON, left, top, w, h, fill=fill, line=line, line_w=1.25)
    set_text(shp, label, size=text_size, bold=True, color=NAVY)
    return shp


def stencil_gx_checkpoint(slide, left, top, *, label: str, w: float = 1.55, h: float = 0.55):
    """GX checkpoint = green hexagon (highlighted)."""
    return stencil_dag_task(slide, left, top, label=label, fill=GREEN_LIGHT, line=GREEN, w=w, h=h)


def stencil_decision(slide, left, top, *, label: str, w: float = 1.4, h: float = 0.9):
    """Diamond = decision."""
    shp = add_shape(
        slide,
        MSO_SHAPE.FLOWCHART_DECISION,
        left,
        top,
        w,
        h,
        fill=AMBER_LIGHT,
        line=AMBER,
        line_w=1.5,
    )
    set_text(shp, label, size=10, bold=True, color=NAVY)
    return shp


def stencil_ai(slide, left, top, *, label: str, sub: str = "", w: float = 2.0, h: float = 1.1):
    """AI agent = purple rounded rect with lightning bolt."""
    box = add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        left,
        top,
        w,
        h,
        fill=PURPLE_LIGHT,
        line=PURPLE,
        line_w=1.5,
    )
    bolt = add_shape(
        slide, MSO_SHAPE.LIGHTNING_BOLT, left + 0.1, top + 0.1, 0.25, 0.4, fill=PURPLE, line=None
    )
    txt = f"{label}\n{sub}" if sub else label
    add_textbox(
        slide,
        left + 0.05,
        top + 0.35,
        w - 0.1,
        h - 0.4,
        txt,
        size=11,
        bold=True,
        color=PURPLE,
        align=PP_ALIGN.CENTER,
    )
    return box


def stencil_cloud(
    slide,
    left,
    top,
    *,
    label: str,
    w: float = 1.5,
    h: float = 0.9,
    fill: RGBColor = BLUE_LIGHT,
    color: RGBColor = BLUE,
):
    """Cloud icon."""
    shp = add_shape(slide, MSO_SHAPE.CLOUD, left, top, w, h, fill=fill, line=color, line_w=1.5)
    set_text(shp, label, size=10, bold=True, color=color)
    return shp


def stencil_report(slide, left, top, *, label: str, w: float = 1.3, h: float = 1.0):
    """Document = report."""
    shp = add_shape(
        slide, MSO_SHAPE.FLOWCHART_DOCUMENT, left, top, w, h, fill=WHITE, line=SLATE, line_w=1.25
    )
    set_text(shp, label, size=10, bold=True, color=NAVY)
    return shp


# ---------------------------------------------------------------------------
# Slides
# ---------------------------------------------------------------------------
def slide_title(prs: Presentation) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])  # blank

    # full-bleed navy background
    bg = add_shape(slide, MSO_SHAPE.RECTANGLE, 0, 0, 13.333, 7.5, fill=NAVY, line=None)

    # accent stripe
    add_shape(slide, MSO_SHAPE.RECTANGLE, 0, 6.85, 13.333, 0.15, fill=GOLD, line=None)
    add_shape(slide, MSO_SHAPE.RECTANGLE, 0, 7.0, 13.333, 0.05, fill=BLUE, line=None)

    # tag
    tag = add_shape(slide, MSO_SHAPE.ROUNDED_RECTANGLE, 0.7, 1.0, 1.6, 0.35, fill=GOLD, line=None)
    set_text(tag, "PHASE 21 · DEMO", size=11, bold=True, color=NAVY)

    add_textbox(
        slide,
        0.7,
        1.6,
        12.0,
        1.4,
        "DataLink Data Quality",
        size=60,
        bold=True,
        color=WHITE,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide,
        0.7,
        3.0,
        12.0,
        1.0,
        "From Bronze to Gold with AI-Authored Expectations",
        size=28,
        color=BLUE_LIGHT,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide,
        0.7,
        4.1,
        12.0,
        0.6,
        "How Great Expectations, the DataLink Architect agents, and pipeline\n"
        "control plug into your existing Airflow + dbt + Snowflake stack.",
        size=16,
        color=SILVER_LIGHT,
        align=PP_ALIGN.LEFT,
    )

    # decorative stencil row
    stencil_storage(slide, 1.5, 5.4, label="Bronze", fill=BRONZE, w=1.4, h=1.05)
    add_shape(slide, MSO_SHAPE.RIGHT_ARROW, 3.0, 5.7, 0.5, 0.45, fill=WHITE, line=None)
    stencil_storage(slide, 3.6, 5.4, label="Silver", fill=SILVER, w=1.4, h=1.05)
    add_shape(slide, MSO_SHAPE.RIGHT_ARROW, 5.1, 5.7, 0.5, 0.45, fill=WHITE, line=None)
    stencil_storage(slide, 5.7, 5.4, label="Gold", fill=GOLD, w=1.4, h=1.05)

    # AI badge
    stencil_ai(slide, 8.5, 5.4, label="DataLink", sub="AI Architects", w=2.2, h=1.05)

    add_textbox(
        slide,
        0.7,
        7.05,
        12.0,
        0.4,
        "Jatin Bhanot · VP Data Operations · datalink-pipeline @ phase-15.5-gold-schema-designer",
        size=10,
        color=MUTED,
        align=PP_ALIGN.LEFT,
    )


# ---------------------------------------------------------------------------
def slide_before(prs: Presentation, page: int, total: int) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    add_header(
        slide,
        "Where most data pipelines are today — DQ is an afterthought",
        "Bronze → Silver → Gold with no inline quality checks. Failures surface in dashboards, not in the DAG.",
    )

    # Source clouds
    stencil_cloud(slide, 0.7, 2.0, label="SQL Server\n(Claims)", w=1.5, h=0.9)
    stencil_cloud(slide, 0.7, 3.1, label="HL7 / EDI\nFeeds", w=1.5, h=0.9)
    stencil_cloud(slide, 0.7, 4.2, label="External\nFile Drops", w=1.5, h=0.9)

    # Airflow container wrapping the DAG
    add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        2.7,
        1.65,
        8.6,
        4.4,
        fill=BG,
        line=BORDER,
        line_w=1.25,
    )
    add_textbox(
        slide,
        2.85,
        1.75,
        4.0,
        0.35,
        "  Airflow DAG · membership_pipeline",
        size=12,
        bold=True,
        color=SLATE,
        align=PP_ALIGN.LEFT,
    )
    # small Airflow gear icon
    add_shape(slide, MSO_SHAPE.GEAR_6, 2.85, 1.78, 0.28, 0.28, fill=SLATE, line=None)

    # Tasks
    task_y = 3.0
    stencil_dag_task(slide, 3.0, task_y, label="bronze_ingest")
    stencil_dag_task(slide, 5.0, task_y, label="silver_transform")
    stencil_dag_task(slide, 7.1, task_y, label="gold_load")
    stencil_dag_task(slide, 9.2, task_y, label="publish_gold")

    # arrows
    for x in (4.55, 6.55, 8.65):
        add_shape(slide, MSO_SHAPE.RIGHT_ARROW, x, task_y + 0.18, 0.45, 0.2, fill=SLATE, line=None)

    # Storage layers below
    stencil_storage(slide, 3.0, 4.4, label="Bronze", fill=BRONZE, w=1.55, h=1.05)
    stencil_storage(slide, 5.0, 4.4, label="Silver", fill=SILVER, w=1.55, h=1.05)
    stencil_storage(slide, 7.1, 4.4, label="Gold", fill=GOLD, w=1.55, h=1.05)

    # dotted lines from task to layer
    for tx in (3.78, 5.78, 7.88):
        add_connector(slide, tx, task_y + 0.55, tx, 4.4, color=BORDER, width_pt=1.0)

    # No-DQ banner
    banner = add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        2.85,
        5.65,
        8.4,
        0.35,
        fill=RED_LIGHT,
        line=RED,
        line_w=1.0,
    )
    set_text(
        banner,
        "⚠   No DQ checkpoints in the pipeline — bad rows reach Gold before anyone notices",
        size=11,
        bold=True,
        color=RED,
    )

    # Right callouts — pain points
    pain_x = 11.5
    add_textbox(
        slide,
        pain_x,
        1.7,
        1.7,
        0.35,
        "Pain today",
        size=12,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )
    pains = [
        ("•", "DQ runs out-of-band, not in the DAG"),
        ("•", "Bad data already in Gold by Tuesday"),
        ("•", "Engineers hand-write expectations"),
        ("•", "No suite versioning or lineage"),
        ("•", "Drift detected by ticket, not code"),
    ]
    py = 2.1
    for _, t in pains:
        add_textbox(
            slide, pain_x, py, 1.75, 0.4, f"•  {t}", size=10, color=SLATE, align=PP_ALIGN.LEFT
        )
        py += 0.45

    add_footer(slide, page, total)


# ---------------------------------------------------------------------------
def slide_after(prs: Presentation, page: int, total: int) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    add_header(
        slide,
        "With DataLink — DQ checkpoints live inside the DAG",
        "Three GX checkpoints (bronze_validate, silver_dq, gold_dq) become first-class Airflow tasks. Critical fail = pipeline halts.",
        accent=GREEN,
    )

    # Sources
    stencil_cloud(slide, 0.55, 2.4, label="Sources", w=1.4, h=0.85)

    # Airflow container
    add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        2.2,
        1.55,
        10.6,
        4.6,
        fill=BG,
        line=BORDER,
        line_w=1.25,
    )
    add_textbox(
        slide,
        2.35,
        1.65,
        6.0,
        0.35,
        "  Airflow DAG · factory-generated per (client, dataset)",
        size=12,
        bold=True,
        color=SLATE,
        align=PP_ALIGN.LEFT,
    )
    add_shape(slide, MSO_SHAPE.GEAR_6, 2.35, 1.68, 0.28, 0.28, fill=SLATE, line=None)

    # Task chain with DQ checkpoints interleaved
    ty = 2.4
    tasks = [
        ("bronze_ingest", "task", 2.45),
        ("bronze_validate", "gx", 4.0),
        ("silver_transform", "task", 5.55),
        ("silver_dq", "gx", 7.1),
        ("gold_load", "task", 8.65),
        ("gold_dq", "gx", 10.2),
        ("publish_gold", "task", 11.75),
    ]
    for name, kind, x in tasks:
        if kind == "gx":
            stencil_gx_checkpoint(slide, x - 0.65, ty, label=name, w=1.3, h=0.55)
        else:
            stencil_dag_task(slide, x - 0.65, ty, label=name, w=1.3, h=0.55)

    # Arrows between tasks
    for i in range(len(tasks) - 1):
        x1 = tasks[i][2] + 0.65
        x2 = tasks[i + 1][2] - 0.65
        add_connector(slide, x1, ty + 0.27, x2, ty + 0.27, color=SLATE, width_pt=1.5)

    # Storage layers
    sy = 4.0
    stencil_storage(slide, 2.45, sy, label="Bronze", fill=BRONZE, w=1.4, h=1.0)
    stencil_storage(slide, 5.55, sy, label="Silver", fill=SILVER, w=1.4, h=1.0)
    stencil_storage(slide, 8.65, sy, label="Gold", fill=GOLD, w=1.4, h=1.0)

    # GX container under the row of checkpoints
    gxbox = add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        3.3,
        5.3,
        8.2,
        0.75,
        fill=GREEN_LIGHT,
        line=GREEN,
        line_w=1.5,
    )
    add_textbox(
        slide,
        3.4,
        5.35,
        8.0,
        0.3,
        "  Great Expectations · checkpoint runner (containerised)",
        size=11,
        bold=True,
        color=GREEN,
        align=PP_ALIGN.LEFT,
    )
    add_shape(slide, MSO_SHAPE.GEAR_6, 3.4, 5.36, 0.26, 0.26, fill=GREEN, line=None)
    add_textbox(
        slide,
        3.4,
        5.65,
        8.0,
        0.35,
        "Loads suite_id from CONTROL.dq_suites · writes results to dq_validation_runs · emits OpenTelemetry metrics",
        size=9,
        color=SLATE,
        align=PP_ALIGN.LEFT,
    )

    # connector from GX box up to checkpoints
    for x in (4.0, 7.1, 10.2):
        add_connector(slide, x, 5.3, x, ty + 0.55, color=GREEN, width_pt=1.2)

    # Right rail — control mechanism
    pain_x = 11.5
    add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        pain_x - 0.05,
        6.4,
        1.55,
        0.7,
        fill=RED_LIGHT,
        line=RED,
        line_w=1.25,
    )
    add_textbox(slide, pain_x, 6.45, 1.5, 0.3, "HALT on critical", size=10, bold=True, color=RED)
    add_textbox(slide, pain_x, 6.7, 1.5, 0.3, "fail · pipeline pauses", size=9, color=RED)

    # Source-to-bronze arrow
    add_connector(slide, 1.95, 2.7, 2.45, ty + 0.27, color=SLATE, width_pt=1.5)

    add_footer(slide, page, total)


# ---------------------------------------------------------------------------
def slide_ai_authoring(prs: Presentation, page: int, total: int) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    add_header(
        slide,
        "AI-authored expectations — DqSuiteArchitectAgent",
        "Claude Haiku proposes ≤30 expectations per (dataset, layer), balanced across 6 quality dimensions. Engineer reviews → promotes.",
        accent=PURPLE,
    )

    # Flow: schema sample → AI → suite proposal → review → promote
    fy = 1.7
    box_h = 1.05

    # Schema sample
    s1 = add_shape(
        slide,
        MSO_SHAPE.FLOWCHART_DOCUMENT,
        0.7,
        fy,
        1.7,
        box_h,
        fill=WHITE,
        line=SLATE,
        line_w=1.25,
    )
    set_text(s1, "Layer schema\n+ sample rows\n(PHI-redacted)", size=10, color=NAVY)

    add_shape(slide, MSO_SHAPE.RIGHT_ARROW, 2.45, fy + 0.42, 0.4, 0.2, fill=SLATE, line=None)

    # AI agent
    stencil_ai(
        slide, 2.95, fy - 0.05, label="DqSuiteArchitect", sub="Claude Haiku 4.5", w=2.4, h=1.2
    )

    add_shape(slide, MSO_SHAPE.RIGHT_ARROW, 5.4, fy + 0.42, 0.4, 0.2, fill=SLATE, line=None)

    # Suite proposal
    s3 = add_shape(
        slide,
        MSO_SHAPE.FLOWCHART_DOCUMENT,
        5.9,
        fy,
        2.0,
        box_h,
        fill=PURPLE_LIGHT,
        line=PURPLE,
        line_w=1.25,
    )
    set_text(
        s3, "Draft suite\n≤30 expectations\nJSON + rationale", size=10, bold=True, color=PURPLE
    )

    add_shape(slide, MSO_SHAPE.RIGHT_ARROW, 7.95, fy + 0.42, 0.4, 0.2, fill=SLATE, line=None)

    # Review
    s4 = add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        8.45,
        fy,
        1.9,
        box_h,
        fill=BLUE_LIGHT,
        line=BLUE,
        line_w=1.25,
    )
    set_text(
        s4, "Engineer review\nDQ Author page\n(approve / tweak)", size=10, bold=True, color=BLUE
    )

    add_shape(slide, MSO_SHAPE.RIGHT_ARROW, 10.4, fy + 0.42, 0.4, 0.2, fill=SLATE, line=None)

    # Promote
    s5 = add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        10.9,
        fy,
        1.9,
        box_h,
        fill=GREEN_LIGHT,
        line=GREEN,
        line_w=1.25,
    )
    set_text(s5, "Promote\nDRAFT → LIVE\nFactory DAG picks up", size=10, bold=True, color=GREEN)

    # 6 dimensions header
    add_textbox(
        slide,
        0.7,
        3.15,
        12.0,
        0.4,
        "Six quality dimensions — every suite must touch all six",
        size=16,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )

    dims = [
        ("Completeness", "expect_column_values_to_not_be_null", GREEN, "✓"),
        ("Uniqueness", "expect_column_values_to_be_unique", BLUE, "★"),
        ("Validity", "expect_column_values_to_match_regex", AMBER, "✎"),
        ("Consistency", "expect_column_pair_values_A_to_be_equal_to_B", PURPLE, "≡"),
        ("Timeliness", "expect_column_max_to_be_between (today − 7)", TEAL, "⏱"),
        ("Accuracy", "expect_column_values_to_be_in_set (ref lookup)", RED, "◎"),
    ]

    dx = 0.7
    dy = 3.75
    card_w = 2.0
    card_h = 1.6
    gap = 0.15
    for i, (name, expectation, color, glyph) in enumerate(dims):
        col = i % 6
        x = dx + col * (card_w + gap)
        card = add_shape(
            slide,
            MSO_SHAPE.ROUNDED_RECTANGLE,
            x,
            dy,
            card_w,
            card_h,
            fill=WHITE,
            line=color,
            line_w=1.5,
        )
        # color band on top
        add_shape(slide, MSO_SHAPE.RECTANGLE, x, dy, card_w, 0.18, fill=color, line=None)
        add_textbox(
            slide, x, dy + 0.22, card_w, 0.35, f"{glyph}  {name}", size=12, bold=True, color=color
        )
        add_textbox(
            slide,
            x + 0.1,
            dy + 0.6,
            card_w - 0.2,
            card_h - 0.7,
            expectation,
            size=9,
            color=SLATE,
            align=PP_ALIGN.LEFT,
            italic=True,
        )

    # callout
    cb = add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        0.7,
        5.6,
        12.1,
        0.85,
        fill=PURPLE_LIGHT,
        line=PURPLE,
        line_w=1.0,
    )
    add_textbox(
        slide,
        0.9,
        5.7,
        11.8,
        0.7,
        "Guardrails  ·   ≤ 30 expectations per suite   ·   PHI/PII columns excluded from sample payload   ·   "
        "every proposal includes severity + rationale   ·   token + latency emitted as OTLP metrics",
        size=11,
        color=PURPLE,
        align=PP_ALIGN.LEFT,
    )

    add_footer(slide, page, total)


# ---------------------------------------------------------------------------
def slide_lifecycle(prs: Presentation, page: int, total: int) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    add_header(
        slide,
        "Suite lifecycle & pipeline-control mechanism",
        "Versioned suites · explicit promote step · DQ failures and schema drift drive the same control plane.",
        accent=AMBER,
    )

    # Top half: state machine
    add_textbox(
        slide,
        0.7,
        1.4,
        12.0,
        0.35,
        "Suite state machine",
        size=14,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )

    states = [
        ("DRAFT", 1.0, 1.95, SLATE, BG),
        ("PENDING_REVIEW", 3.6, 1.95, AMBER, AMBER_LIGHT),
        ("LIVE", 6.3, 1.95, GREEN, GREEN_LIGHT),
        ("ARCHIVED", 8.7, 1.95, MUTED, BG),
    ]
    state_h = 0.65
    state_w = 2.0
    for label, x, y, color, fill in states:
        box = add_shape(
            slide,
            MSO_SHAPE.ROUNDED_RECTANGLE,
            x,
            y,
            state_w,
            state_h,
            fill=fill,
            line=color,
            line_w=1.5,
        )
        set_text(box, label, size=12, bold=True, color=color)

    # arrows between states
    transitions = [
        (3.0, 2.28, 3.6, 2.28, "submit"),
        (5.6, 2.28, 6.3, 2.28, "approve"),
        (8.3, 2.28, 8.7, 2.28, "supersede"),
    ]
    for x1, y1, x2, y2, lbl in transitions:
        add_connector(slide, x1, y1, x2, y2, color=SLATE, width_pt=1.5)
        add_textbox(
            slide,
            (x1 + x2) / 2 - 0.35,
            y1 - 0.35,
            0.8,
            0.25,
            lbl,
            size=9,
            italic=True,
            color=MUTED,
            align=PP_ALIGN.CENTER,
        )

    # rejection loop back from PENDING to DRAFT
    add_connector(slide, 4.6, 2.6, 4.6, 3.0, color=RED, width_pt=1.25)
    add_connector(slide, 4.6, 3.0, 2.0, 3.0, color=RED, width_pt=1.25)
    add_connector(slide, 2.0, 3.0, 2.0, 2.6, color=RED, width_pt=1.25)
    add_textbox(
        slide,
        2.6,
        3.05,
        2.0,
        0.3,
        "reject (with comment)",
        size=9,
        italic=True,
        color=RED,
        align=PP_ALIGN.CENTER,
    )

    # Right side: versioning note
    vbox = add_shape(
        slide, MSO_SHAPE.ROUNDED_RECTANGLE, 11.05, 1.85, 1.75, 1.4, fill=BG, line=BORDER, line_w=1.0
    )
    add_textbox(
        slide,
        11.1,
        1.9,
        1.65,
        0.3,
        "Pinned versions",
        size=11,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide,
        11.1,
        2.2,
        1.65,
        1.0,
        "Each LIVE row is the\ncurrent suite for a\n(dataset, layer) tuple.\nHistory kept in\ndq_suite_versions.",
        size=9,
        color=SLATE,
        align=PP_ALIGN.LEFT,
    )

    # Bottom half: pipeline control decision tree
    add_textbox(
        slide,
        0.7,
        3.6,
        12.0,
        0.35,
        "What happens when a checkpoint fails",
        size=14,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )

    # GX checkpoint
    stencil_gx_checkpoint(
        slide, 0.7, 4.2, label="bronze_validate /\nsilver_dq / gold_dq", w=2.0, h=0.9
    )

    add_shape(slide, MSO_SHAPE.RIGHT_ARROW, 2.78, 4.55, 0.4, 0.2, fill=SLATE, line=None)

    # decision diamond
    stencil_decision(slide, 3.3, 4.1, label="Severity?", w=1.6, h=1.05)

    # three branches
    # critical -> HALT
    add_connector(slide, 4.95, 4.4, 5.7, 4.4, color=RED, width_pt=1.5)
    halt = add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        5.7,
        4.15,
        2.0,
        0.55,
        fill=RED_LIGHT,
        line=RED,
        line_w=1.5,
    )
    set_text(halt, "CRITICAL → HALT", size=11, bold=True, color=RED)
    add_textbox(
        slide,
        7.75,
        4.2,
        4.0,
        0.45,
        "DAG marked failed · UI banner · ops paged · pipeline_control_state row written",
        size=10,
        color=SLATE,
        align=PP_ALIGN.LEFT,
    )

    # high -> PAUSE
    add_connector(slide, 4.95, 4.9, 5.7, 4.9, color=AMBER, width_pt=1.5)
    pause = add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        5.7,
        4.7,
        2.0,
        0.55,
        fill=AMBER_LIGHT,
        line=AMBER,
        line_w=1.5,
    )
    set_text(pause, "HIGH → PAUSE", size=11, bold=True, color=AMBER)
    add_textbox(
        slide,
        7.75,
        4.75,
        4.0,
        0.45,
        "Downstream tasks blocked · awaits engineer ack in Pipeline Control page",
        size=10,
        color=SLATE,
        align=PP_ALIGN.LEFT,
    )

    # low/medium -> continue
    add_connector(slide, 4.95, 5.4, 5.7, 5.4, color=GREEN, width_pt=1.5)
    cont = add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        5.7,
        5.25,
        2.0,
        0.55,
        fill=GREEN_LIGHT,
        line=GREEN,
        line_w=1.5,
    )
    set_text(cont, "LOW/MED → ALERT", size=11, bold=True, color=GREEN)
    add_textbox(
        slide,
        7.75,
        5.3,
        4.0,
        0.45,
        "Pipeline continues · Grafana panel turns amber · entry in dq_validation_runs",
        size=10,
        color=SLATE,
        align=PP_ALIGN.LEFT,
    )

    # Bottom: schema drift parallel track
    add_shape(
        slide, MSO_SHAPE.ROUNDED_RECTANGLE, 0.7, 6.0, 12.1, 1.0, fill=BG, line=BORDER, line_w=1.0
    )
    add_textbox(
        slide,
        0.9,
        6.05,
        12.0,
        0.3,
        "Schema drift is its own control track",
        size=12,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide,
        0.9,
        6.35,
        12.0,
        0.6,
        "Additive change (new nullable column)  →  log + auto-allow      ·      "
        "Breaking change (drop / type-narrow)  →  HALTED + manual override required      ·      "
        "All events streamed to schema_drift_events_total counter (Grafana)",
        size=10,
        color=SLATE,
        align=PP_ALIGN.LEFT,
    )

    add_footer(slide, page, total)


# ---------------------------------------------------------------------------
def slide_sample_suite(prs: Presentation, page: int, total: int) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    add_header(
        slide,
        "Sample suite — membership_bronze · 19 expectations",
        "Auto-generated by DqSuiteArchitectAgent · reviewed by ops · LIVE since 2026-05-09 (suite_id=412).",
        accent=BLUE,
    )

    # Left: expectation table
    table_left = 0.6
    table_top = 1.5
    table_w = 8.7
    add_textbox(
        slide,
        table_left,
        table_top,
        table_w,
        0.35,
        "Top 8 expectations (full suite has 19)",
        size=13,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )

    rows = [
        ("Column", "Expectation", "Dim", "Sev", True),
        ("member_id", "expect_column_values_to_not_be_null", "Completeness", "critical", False),
        ("member_id", "expect_column_values_to_be_unique", "Uniqueness", "critical", False),
        (
            "member_id",
            "expect_column_value_lengths_to_be_between (8,12)",
            "Validity",
            "high",
            False,
        ),
        ("dob", "expect_column_values_to_be_dateutil_parseable", "Validity", "high", False),
        (
            "dob",
            "expect_column_values_to_be_between (1900-01-01, today)",
            "Validity",
            "medium",
            False,
        ),
        (
            "effective_date",
            "expect_column_max_to_be_between (today−7, today)",
            "Timeliness",
            "high",
            False,
        ),
        ("plan_code", "expect_column_values_to_be_in_set (plan_ref)", "Accuracy", "medium", False),
        ("gender", "expect_column_values_to_be_in_set ['M','F','U','X']", "Validity", "low", False),
    ]

    rh = 0.38
    col_w = [1.7, 4.2, 1.4, 1.4]
    for ri, row in enumerate(rows):
        y = table_top + 0.4 + ri * rh
        col, exp, dim, sev, is_header = row[0], row[1], row[2], row[3], row[4]
        fill = NAVY if is_header else (BG if ri % 2 == 0 else WHITE)
        line = NAVY if is_header else BORDER
        text_color = WHITE if is_header else NAVY
        # full row background
        x = table_left
        for i, w in enumerate(col_w):
            cell = add_shape(
                slide, MSO_SHAPE.RECTANGLE, x, y, w, rh, fill=fill, line=line, line_w=0.5
            )
            text = [col, exp, dim, sev][i]
            set_text(
                cell,
                text,
                size=10,
                bold=is_header,
                color=text_color,
                align=PP_ALIGN.LEFT if i in (0, 1) else PP_ALIGN.CENTER,
            )
            x += w

    # Severity color chip on the sev column
    sev_color_map = {"critical": RED, "high": AMBER, "medium": BLUE, "low": MUTED}
    for ri, row in enumerate(rows):
        if row[4]:  # header
            continue
        y = table_top + 0.4 + ri * rh
        sev = row[3]
        x = table_left + col_w[0] + col_w[1] + col_w[2] + 0.15
        chip = add_shape(
            slide,
            MSO_SHAPE.OVAL,
            x,
            y + 0.12,
            0.14,
            0.14,
            fill=sev_color_map.get(sev, MUTED),
            line=None,
        )

    # Bottom-left: link to suite page
    add_textbox(
        slide,
        table_left,
        table_top + 0.4 + len(rows) * rh + 0.2,
        table_w,
        0.35,
        "→ See DQ Suite Registry · /6_DQ_Suite_Registry · suite_id=412 for the full 19-expectation list",
        size=10,
        italic=True,
        color=BLUE,
        align=PP_ALIGN.LEFT,
    )

    # Right: dimension breakdown pie chart
    chart_left = Inches(9.4)
    chart_top = Inches(1.85)
    chart_w = Inches(3.6)
    chart_h = Inches(3.2)
    chart_data = CategoryChartData()
    chart_data.categories = [
        "Completeness",
        "Uniqueness",
        "Validity",
        "Consistency",
        "Timeliness",
        "Accuracy",
    ]
    chart_data.add_series("Expectations", (5, 2, 6, 2, 2, 2))
    chart = slide.shapes.add_chart(
        XL_CHART_TYPE.DOUGHNUT, chart_left, chart_top, chart_w, chart_h, chart_data
    ).chart
    chart.has_title = True
    chart.chart_title.text_frame.text = "Dimension coverage"
    for p in chart.chart_title.text_frame.paragraphs:
        for r in p.runs:
            r.font.size = Pt(12)
            r.font.bold = True
            r.font.color.rgb = NAVY
    chart.has_legend = True
    chart.legend.position = XL_LEGEND_POSITION.BOTTOM
    chart.legend.include_in_layout = False
    chart.legend.font.size = Pt(8)

    # Right: stats below chart
    stats_y = 5.25
    add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        9.4,
        stats_y,
        3.5,
        1.65,
        fill=BG,
        line=BORDER,
        line_w=1.0,
    )
    add_textbox(
        slide,
        9.55,
        stats_y + 0.05,
        3.3,
        0.3,
        "Last 24 h run",
        size=11,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )
    stats = [
        ("Pass rate", "98.4 %", GREEN),
        ("Rows evaluated", "1.2 M", NAVY),
        ("Critical fails", "0", GREEN),
        ("High fails", "2", AMBER),
    ]
    ry = stats_y + 0.35
    for label, value, color in stats:
        add_textbox(slide, 9.55, ry, 1.8, 0.3, label, size=10, color=MUTED, align=PP_ALIGN.LEFT)
        add_textbox(
            slide, 11.2, ry, 1.6, 0.3, value, size=11, bold=True, color=color, align=PP_ALIGN.RIGHT
        )
        ry += 0.32

    add_footer(slide, page, total)


# ---------------------------------------------------------------------------
def slide_adoption(prs: Presentation, page: int, total: int) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    add_header(
        slide,
        "Introducing DQ into an existing pipeline — 5 steps · ~10 min per dataset",
        "What Data Engineering actually has to do · zero changes to existing dbt models or ingestion code.",
        accent=GREEN,
    )

    steps = [
        (
            "1",
            "Register dataset",
            "Add one row to CONTROL.datasets\n(dataset_code, source, layers)",
            "Data Engineer · Streamlit page  /14_Data_Model_Designer",
            BLUE,
        ),
        (
            "2",
            "Run AI Architect",
            "/8_DQ_AI_Architect → pick\n(dataset, layer) → Propose",
            "AI generates ≤30 expectations\nin ~6 seconds",
            PURPLE,
        ),
        (
            "3",
            "Review & promote",
            "/1_DQ_Author → tweak severities →\nSubmit · /2_DQ_Review → Approve",
            "Engineer · Reviewer (4 eyes)",
            AMBER,
        ),
        (
            "4",
            "Factory picks up suite",
            "dags/_factory_<dataset>.py auto-\nwires bronze_validate / silver_dq /\ngold_dq tasks · no DAG edits",
            "Zero engineer touch",
            GREEN,
        ),
        (
            "5",
            "Dashboards light up",
            "Grafana datalink_dq_quality panels\nrender pass-rate + dim breakdown.\nDrift counter starts ticking.",
            "Live in next DAG run",
            TEAL,
        ),
    ]

    sw = 2.4
    sh = 4.4
    gap = 0.15
    sx = 0.55
    sy = 1.55
    for i, (num, title, body, who, color) in enumerate(steps):
        x = sx + i * (sw + gap)
        card = add_shape(
            slide, MSO_SHAPE.ROUNDED_RECTANGLE, x, sy, sw, sh, fill=WHITE, line=color, line_w=1.5
        )
        # number circle
        circ = add_shape(
            slide, MSO_SHAPE.OVAL, x + 0.15, sy + 0.18, 0.55, 0.55, fill=color, line=None
        )
        set_text(circ, num, size=22, bold=True, color=WHITE)
        # title
        add_textbox(
            slide,
            x + 0.8,
            sy + 0.22,
            sw - 0.9,
            0.5,
            title,
            size=13,
            bold=True,
            color=color,
            align=PP_ALIGN.LEFT,
        )
        # body
        add_textbox(
            slide, x + 0.2, sy + 1.0, sw - 0.4, 2.2, body, size=10, color=NAVY, align=PP_ALIGN.LEFT
        )
        # who/footer
        add_shape(
            slide,
            MSO_SHAPE.RECTANGLE,
            x + 0.2,
            sy + sh - 0.95,
            sw - 0.4,
            0.01,
            fill=color,
            line=None,
        )
        add_textbox(
            slide,
            x + 0.2,
            sy + sh - 0.85,
            sw - 0.4,
            0.75,
            who,
            size=9,
            italic=True,
            color=MUTED,
            align=PP_ALIGN.LEFT,
        )

        # arrow to next
        if i < len(steps) - 1:
            arrow_x = x + sw + 0.005
            add_shape(
                slide,
                MSO_SHAPE.RIGHT_ARROW,
                arrow_x,
                sy + sh / 2 - 0.1,
                0.12,
                0.2,
                fill=color,
                line=None,
            )

    # Bottom: what does NOT change
    add_shape(
        slide, MSO_SHAPE.ROUNDED_RECTANGLE, 0.55, 6.2, 12.2, 0.75, fill=BG, line=BORDER, line_w=1.0
    )
    add_textbox(
        slide,
        0.7,
        6.25,
        11.0,
        0.3,
        "What does NOT change",
        size=11,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide,
        0.7,
        6.5,
        12.0,
        0.5,
        "Your dbt models · your ingestion code · your Airflow scheduler config · your Snowflake schemas · "
        "your CI / CD.  DQ slots in as new Airflow tasks — nothing existing has to move.",
        size=10,
        color=SLATE,
        align=PP_ALIGN.LEFT,
    )

    add_footer(slide, page, total)


# ---------------------------------------------------------------------------
def slide_reports(prs: Presentation, page: int, total: int) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    add_header(
        slide,
        "Live reports — GX Data Docs + Grafana",
        "Two surfaces · one source of truth · both backed by dq_validation_runs in Snowflake.",
        accent=BLUE,
    )

    # Left: GX Data Docs mockup
    add_textbox(
        slide,
        0.6,
        1.45,
        6.2,
        0.35,
        "GX Data Docs · per-run HTML report",
        size=14,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )
    # frame
    add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        0.55,
        1.85,
        6.2,
        5.0,
        fill=WHITE,
        line=BORDER,
        line_w=1.5,
    )
    # browser bar
    add_shape(slide, MSO_SHAPE.RECTANGLE, 0.55, 1.85, 6.2, 0.35, fill=NAVY, line=None)
    for ci, c in enumerate([RED, AMBER, GREEN]):
        add_shape(slide, MSO_SHAPE.OVAL, 0.7 + ci * 0.18, 1.95, 0.13, 0.13, fill=c, line=None)
        # url
    add_textbox(
        slide,
        1.4,
        1.88,
        5.2,
        0.3,
        "  http://localhost:8089/data_docs/...",
        size=9,
        color=BLUE_LIGHT,
        align=PP_ALIGN.LEFT,
    )

    # title
    add_textbox(
        slide,
        0.75,
        2.3,
        6.0,
        0.35,
        "Suite: membership_bronze · Run 412 · 2026-05-11 06:42 UTC",
        size=11,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )

    # status pills
    pills = [
        ("19 evaluated", BLUE),
        ("17 passed", GREEN),
        ("2 failed", AMBER),
        ("0 critical", GREEN),
    ]
    px = 0.75
    for txt, color in pills:
        chip = add_shape(
            slide,
            MSO_SHAPE.ROUNDED_RECTANGLE,
            px,
            2.7,
            1.35,
            0.35,
            fill=WHITE,
            line=color,
            line_w=1.0,
        )
        set_text(chip, txt, size=10, bold=True, color=color)
        px += 1.45

    # table-ish rows
    docs_rows = [
        ("✓", "member_id  · not null", "100 %"),
        ("✓", "member_id  · unique", "100 %"),
        ("⚠", "dob  · between (1900, today)", "99.7 %"),
        ("✓", "effective_date  · max within 7d", "100 %"),
        ("⚠", "plan_code  · in plan_ref", "98.4 %"),
        ("✓", "gender  · in set [M,F,U,X]", "100 %"),
    ]
    dy = 3.25
    for icon, name, pct in docs_rows:
        color = GREEN if icon == "✓" else AMBER
        # row bg
        add_shape(slide, MSO_SHAPE.RECTANGLE, 0.7, dy, 5.95, 0.42, fill=BG, line=None)
        add_textbox(
            slide,
            0.78,
            dy + 0.05,
            0.3,
            0.3,
            icon,
            size=14,
            bold=True,
            color=color,
            align=PP_ALIGN.LEFT,
        )
        add_textbox(
            slide, 1.15, dy + 0.06, 4.0, 0.3, name, size=10, color=NAVY, align=PP_ALIGN.LEFT
        )
        add_textbox(
            slide,
            5.4,
            dy + 0.06,
            1.2,
            0.3,
            pct,
            size=10,
            bold=True,
            color=color,
            align=PP_ALIGN.RIGHT,
        )
        dy += 0.5

    # Right: Grafana dashboard mockup with mini panels
    add_textbox(
        slide,
        7.0,
        1.45,
        5.8,
        0.35,
        "Grafana · datalink_dq_quality",
        size=14,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )
    add_shape(
        slide,
        MSO_SHAPE.ROUNDED_RECTANGLE,
        6.95,
        1.85,
        5.85,
        5.0,
        fill=NAVY,
        line=BORDER,
        line_w=1.5,
    )

    # 4 mini panels (top-left: pass rate big-number, top-right: dim breakdown bar, bottom-left: line of pass rate over time, bottom-right: drift counter)
    panel_w = 2.75
    panel_h = 2.3
    px1 = 7.05
    px2 = 7.05 + panel_w + 0.05
    py1 = 1.95
    py2 = 1.95 + panel_h + 0.05

    # Panel 1: stat big number
    add_shape(
        slide,
        MSO_SHAPE.RECTANGLE,
        px1,
        py1,
        panel_w,
        panel_h,
        fill=RGBColor(0x18, 0x1B, 0x1F),
        line=NAVY,
        line_w=1.0,
    )
    add_textbox(
        slide,
        px1,
        py1 + 0.05,
        panel_w,
        0.3,
        "DQ pass rate (24h)",
        size=10,
        color=SILVER_LIGHT,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide,
        px1,
        py1 + 0.45,
        panel_w,
        1.3,
        "98.4 %",
        size=58,
        bold=True,
        color=GREEN,
        align=PP_ALIGN.CENTER,
    )
    add_textbox(
        slide,
        px1,
        py1 + 1.75,
        panel_w,
        0.3,
        "▲ 0.6 pp vs prior 24h",
        size=9,
        color=GREEN,
        align=PP_ALIGN.CENTER,
    )

    # Panel 2: stacked bars by dimension
    add_shape(
        slide,
        MSO_SHAPE.RECTANGLE,
        px2,
        py1,
        panel_w,
        panel_h,
        fill=RGBColor(0x18, 0x1B, 0x1F),
        line=NAVY,
        line_w=1.0,
    )
    add_textbox(
        slide,
        px2,
        py1 + 0.05,
        panel_w,
        0.3,
        "Fails by dimension (24h)",
        size=10,
        color=SILVER_LIGHT,
        align=PP_ALIGN.LEFT,
    )
    bar_data = [
        ("Compl", 0, GREEN),
        ("Uniq", 0, GREEN),
        ("Valid", 12, AMBER),
        ("Cons", 1, BLUE),
        ("Time", 0, GREEN),
        ("Acc", 4, AMBER),
    ]
    bar_x = px2 + 0.2
    bar_max = 12
    bar_w_each = (panel_w - 0.4) / len(bar_data)
    bar_base = py1 + panel_h - 0.35
    for label, val, color in bar_data:
        h = max(0.05, (val / bar_max) * 1.5)
        x = bar_x + 0.05
        add_shape(
            slide, MSO_SHAPE.RECTANGLE, x, bar_base - h, bar_w_each - 0.1, h, fill=color, line=None
        )
        add_textbox(
            slide,
            bar_x,
            bar_base + 0.02,
            bar_w_each,
            0.25,
            label,
            size=8,
            color=SILVER_LIGHT,
            align=PP_ALIGN.CENTER,
        )
        add_textbox(
            slide,
            bar_x,
            bar_base - h - 0.25,
            bar_w_each,
            0.22,
            str(val),
            size=8,
            color=WHITE,
            align=PP_ALIGN.CENTER,
        )
        bar_x += bar_w_each

    # Panel 3: line chart placeholder
    add_shape(
        slide,
        MSO_SHAPE.RECTANGLE,
        px1,
        py2,
        panel_w,
        panel_h,
        fill=RGBColor(0x18, 0x1B, 0x1F),
        line=NAVY,
        line_w=1.0,
    )
    add_textbox(
        slide,
        px1,
        py2 + 0.05,
        panel_w,
        0.3,
        "Pass rate over time (7d)",
        size=10,
        color=SILVER_LIGHT,
        align=PP_ALIGN.LEFT,
    )
    # draw a pseudo line by polyline of small rectangles
    points = [0.5, 0.55, 0.48, 0.62, 0.7, 0.66, 0.74, 0.8, 0.82, 0.88, 0.92, 0.96, 0.98]
    line_x = px1 + 0.2
    line_y_base = py2 + panel_h - 0.4
    step = (panel_w - 0.4) / (len(points) - 1)
    from pptx.enum.shapes import MSO_CONNECTOR as _MC

    prev = (line_x, line_y_base - points[0] * 1.4)
    for i in range(1, len(points)):
        cur = (line_x + i * step, line_y_base - points[i] * 1.4)
        conn = slide.shapes.add_connector(
            _MC.STRAIGHT, Inches(prev[0]), Inches(prev[1]), Inches(cur[0]), Inches(cur[1])
        )
        conn.line.color.rgb = GREEN
        conn.line.width = Pt(2.0)
        prev = cur
    add_textbox(
        slide,
        px1,
        py2 + panel_h - 0.3,
        panel_w,
        0.25,
        "May 4    May 6    May 8    May 10",
        size=8,
        color=SILVER_LIGHT,
        align=PP_ALIGN.CENTER,
    )

    # Panel 4: drift counter
    add_shape(
        slide,
        MSO_SHAPE.RECTANGLE,
        px2,
        py2,
        panel_w,
        panel_h,
        fill=RGBColor(0x18, 0x1B, 0x1F),
        line=NAVY,
        line_w=1.0,
    )
    add_textbox(
        slide,
        px2,
        py2 + 0.05,
        panel_w,
        0.3,
        "Schema drift events (7d)",
        size=10,
        color=SILVER_LIGHT,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide,
        px2,
        py2 + 0.4,
        panel_w,
        1.0,
        "3",
        size=58,
        bold=True,
        color=AMBER,
        align=PP_ALIGN.CENTER,
    )
    add_textbox(
        slide,
        px2,
        py2 + 1.4,
        panel_w,
        0.3,
        "2 additive · 1 breaking (halted)",
        size=9,
        color=SILVER_LIGHT,
        align=PP_ALIGN.CENTER,
    )
    add_textbox(
        slide,
        px2,
        py2 + 1.7,
        panel_w,
        0.3,
        "→ aetna_claims · column 'tax_id' dropped",
        size=8,
        italic=True,
        color=AMBER,
        align=PP_ALIGN.CENTER,
    )

    add_footer(slide, page, total)


# ---------------------------------------------------------------------------
def slide_close(prs: Presentation, page: int, total: int) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])

    # full navy background
    add_shape(slide, MSO_SHAPE.RECTANGLE, 0, 0, 13.333, 7.5, fill=NAVY, line=None)
    # gold band
    add_shape(slide, MSO_SHAPE.RECTANGLE, 0, 6.85, 13.333, 0.15, fill=GOLD, line=None)
    add_shape(slide, MSO_SHAPE.RECTANGLE, 0, 7.0, 13.333, 0.05, fill=BLUE, line=None)

    add_textbox(
        slide,
        0.7,
        0.7,
        12.0,
        0.7,
        "Recap & next steps",
        size=36,
        bold=True,
        color=WHITE,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide,
        0.7,
        1.45,
        12.0,
        0.5,
        "Three things to take away — and what we want from Data Engineering.",
        size=16,
        color=BLUE_LIGHT,
        align=PP_ALIGN.LEFT,
    )

    # three takeaways
    takes = [
        (
            "DQ lives in the DAG",
            "Three GX checkpoints become first-class Airflow tasks.\nFails halt or pause — never silently leak to Gold.",
            GREEN,
        ),
        (
            "AI authors the suite",
            "DqSuiteArchitectAgent (Claude Haiku) drafts ≤30\nexpectations balanced across 6 dimensions in ~6 seconds.",
            PURPLE,
        ),
        (
            "Zero pipeline rewrite",
            "Factory DAGs auto-wire the new tasks once a suite is LIVE.\ndbt models, ingestion, schedules untouched.",
            BLUE,
        ),
    ]
    cy = 2.4
    cw = 4.0
    cgap = 0.15
    cx = 0.7
    for i, (title, body, color) in enumerate(takes):
        x = cx + i * (cw + cgap)
        card = add_shape(
            slide, MSO_SHAPE.ROUNDED_RECTANGLE, x, cy, cw, 2.5, fill=WHITE, line=color, line_w=2.0
        )
        add_shape(slide, MSO_SHAPE.RECTANGLE, x, cy, cw, 0.15, fill=color, line=None)
        add_textbox(
            slide,
            x + 0.3,
            cy + 0.3,
            cw - 0.6,
            0.5,
            title,
            size=18,
            bold=True,
            color=color,
            align=PP_ALIGN.LEFT,
        )
        add_textbox(
            slide, x + 0.3, cy + 0.9, cw - 0.6, 1.5, body, size=12, color=NAVY, align=PP_ALIGN.LEFT
        )

    # ask box
    ask = add_shape(slide, MSO_SHAPE.ROUNDED_RECTANGLE, 0.7, 5.3, 12.0, 1.3, fill=GOLD, line=None)
    add_textbox(
        slide,
        0.95,
        5.4,
        11.5,
        0.45,
        "Ask from Data Engineering",
        size=16,
        bold=True,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide,
        0.95,
        5.85,
        11.5,
        0.75,
        "Pick one production dataset (membership_bronze is the obvious one).  Give us 30 min to walk you "
        "through the 5-step adoption.  We'll have its suite LIVE and panels rendering same session.",
        size=12,
        color=NAVY,
        align=PP_ALIGN.LEFT,
    )

    add_textbox(
        slide,
        0.7,
        7.1,
        12.0,
        0.3,
        "DataLink Command Center · jatin.bhanot@datalink.com",
        size=10,
        color=MUTED,
        align=PP_ALIGN.LEFT,
    )
    add_textbox(
        slide, 0.7, 7.1, 12.0, 0.3, f"{page} / {total}", size=10, color=MUTED, align=PP_ALIGN.RIGHT
    )


# ---------------------------------------------------------------------------
def main() -> Path:
    prs = Presentation()
    prs.slide_width = SLIDE_W
    prs.slide_height = SLIDE_H

    # 9 slides: title + 7 content + recap close.  User asked for 6–10.
    total = 9
    slide_title(prs)
    slide_before(prs, 2, total)
    slide_after(prs, 3, total)
    slide_ai_authoring(prs, 4, total)
    slide_lifecycle(prs, 5, total)
    slide_sample_suite(prs, 6, total)
    slide_adoption(prs, 7, total)
    slide_reports(prs, 8, total)
    slide_close(prs, 9, total)

    # Output locations
    repo_root = Path(__file__).resolve().parents[1]
    out_repo = repo_root / "docs" / "demo" / "DataLink_DQ_Demo.pptx"
    out_repo.parent.mkdir(parents=True, exist_ok=True)
    prs.save(str(out_repo))

    # Windows-side copy (use the platform-appropriate path).  We just copy
    # the bytes — no point regenerating the same deck twice.
    import shutil
    import sys

    if sys.platform.startswith("win"):
        win_target = Path(r"C:/Users/Jatin/Downloads/DataLink_DQ_Demo.pptx")
    else:
        win_target = Path("/mnt/c/Users/Jatin/Downloads/DataLink_DQ_Demo.pptx")
    try:
        win_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(out_repo), str(win_target))
        print(f"wrote: {win_target}")
    except Exception as e:
        print(f"skip {win_target}: {e}")

    print(f"wrote: {out_repo}")
    return out_repo


if __name__ == "__main__":
    main()
