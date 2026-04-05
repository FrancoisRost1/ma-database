"""
Generate a clean schema diagram for the M&A Database relational schema.
Saved to docs/schema_diagram.png.
Uses matplotlib patches and annotations — no graphviz dependency required.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import os

# ---------------------------------------------------------------------------
# Colour palette — Bloomberg dark
# ---------------------------------------------------------------------------
BG = "#0A0A0A"
BOX_BG = "#1A1A2E"
BOX_BORDER = "#00D4AA"
VIEW_BORDER = "#45B7D1"
HEADER_BG = "#00D4AA"
HEADER_TEXT = "#0A0A0A"
BODY_TEXT = "#CCCCCC"
PK_COLOR = "#F7DC6F"
FK_COLOR = "#F0B27A"
ARROW_COLOR = "#888888"
TITLE_COLOR = "#E8E8E8"
CAPTION_COLOR = "#666666"

fig, ax = plt.subplots(figsize=(20, 14))
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)
ax.set_xlim(0, 20)
ax.set_ylim(0, 14)
ax.axis("off")

# ---------------------------------------------------------------------------
# Helper: draw a table box
# ---------------------------------------------------------------------------
def draw_table(ax, x, y, width, height, title, fields,
               border_color=BOX_BORDER, header_bg=HEADER_BG, is_view=False):
    """Draw a styled table box with title and field rows."""
    row_h = (height - 0.55) / max(len(fields), 1)

    # Box background
    box = FancyBboxPatch(
        (x, y), width, height,
        boxstyle="round,pad=0.05",
        facecolor=BOX_BG, edgecolor=border_color, linewidth=1.8,
    )
    ax.add_patch(box)

    # Header strip
    hdr = FancyBboxPatch(
        (x, y + height - 0.55), width, 0.55,
        boxstyle="round,pad=0.03",
        facecolor=header_bg if not is_view else VIEW_BORDER,
        edgecolor="none",
    )
    ax.add_patch(hdr)

    # Title
    ax.text(
        x + width / 2, y + height - 0.275,
        title,
        ha="center", va="center",
        fontsize=9, fontweight="bold",
        color=HEADER_TEXT, fontfamily="monospace",
    )

    # Fields
    for i, (fname, ftype, annotation) in enumerate(fields):
        fy = y + height - 0.55 - (i + 0.5) * row_h
        # Annotation colour
        if annotation == "PK":
            ann_color = PK_COLOR
        elif annotation == "FK":
            ann_color = FK_COLOR
        elif annotation == "PK,FK":
            ann_color = "#BB8FCE"
        else:
            ann_color = BODY_TEXT

        ax.text(
            x + 0.15, fy,
            fname,
            ha="left", va="center",
            fontsize=7.5, color=ann_color, fontfamily="monospace",
        )
        ax.text(
            x + width - 0.12, fy,
            ftype,
            ha="right", va="center",
            fontsize=6.5, color="#555555", fontfamily="monospace",
        )

    return x + width / 2, y + height  # top-center anchor


def draw_arrow(ax, x1, y1, x2, y2, label=""):
    """Draw a simple FK relationship arrow."""
    ax.annotate(
        "", xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(
            arrowstyle="-|>",
            color=ARROW_COLOR,
            lw=1.2,
            connectionstyle="arc3,rad=0.0",
        ),
    )
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(mx, my + 0.08, label, ha="center", va="bottom",
                fontsize=6, color=ARROW_COLOR, fontstyle="italic")


# ---------------------------------------------------------------------------
# Table definitions: (field_name, type_hint, annotation)
# ---------------------------------------------------------------------------
deals_fields = [
    ("deal_id", "VARCHAR PK", "PK"),
    ("announcement_date", "DATE", ""),
    ("closing_date", "DATE", ""),
    ("deal_type", "VARCHAR", ""),
    ("deal_status", "VARCHAR", ""),
    ("deal_value_usd", "DOUBLE", ""),
    ("target_name", "VARCHAR", ""),
    ("acquirer_party_id", "VARCHAR FK", "FK"),
    ("target_party_id", "VARCHAR FK", "FK"),
    ("sector_id", "VARCHAR FK", "FK"),
    ("geography", "VARCHAR", ""),
    ("data_origin", "VARCHAR", ""),
    ("...more fields", "", ""),
]

parties_fields = [
    ("party_id", "VARCHAR PK", "PK"),
    ("party_name", "VARCHAR", ""),
    ("party_type", "VARCHAR", ""),
    ("headquarters", "VARCHAR", ""),
    ("description", "VARCHAR", ""),
]

sectors_fields = [
    ("sector_id", "VARCHAR PK", "PK"),
    ("sector_name", "VARCHAR", ""),
    ("sub_industry", "VARCHAR", ""),
]

valuation_fields = [
    ("deal_id", "PK, FK→deals", "PK,FK"),
    ("ev_to_ebitda", "DOUBLE", ""),
    ("ev_to_revenue", "DOUBLE", ""),
    ("premium_paid_pct", "DOUBLE", ""),
    ("leverage_multiple", "DOUBLE", ""),
    ("target_revenue", "DOUBLE", ""),
    ("target_ebitda", "DOUBLE", ""),
    ("target_ebitda_margin", "DOUBLE", ""),
]

metadata_fields = [
    ("deal_id", "PK, FK→deals", "PK,FK"),
    ("data_source", "VARCHAR", ""),
    ("source_url", "VARCHAR", ""),
    ("citation", "VARCHAR", ""),
    ("completeness_score", "DOUBLE", ""),
    ("confidence_score", "DOUBLE", ""),
    ("last_reviewed", "DATE", ""),
    ("reviewed_by", "VARCHAR", ""),
]

view_fields = [
    ("All deal fields + party names + sector names", "", ""),
    ("+ valuation metrics + quality scores", "", ""),
    ("Used by all analytics, dashboard, and exports", "", ""),
]

# ---------------------------------------------------------------------------
# Layout: positions (x, y, w, h)
# ---------------------------------------------------------------------------
# deals — center, top
dx, dy, dw, dh = 7.5, 5.8, 5.0, 7.4
draw_table(ax, dx, dy, dw, dh, "deals", deals_fields)

# parties — top right
px2, py2, pw2, ph2 = 14.0, 8.0, 5.5, 3.4
draw_table(ax, px2, py2, pw2, ph2, "parties", parties_fields)

# sectors — top left
sx, sy, sw, sh = 1.0, 8.0, 5.5, 2.6
draw_table(ax, sx, sy, sw, sh, "sectors", sectors_fields)

# valuation_metrics — bottom left
vx, vy, vw, vh = 1.0, 1.5, 5.5, 5.2
draw_table(ax, vx, vy, vw, vh, "valuation_metrics", valuation_fields)

# deal_metadata — bottom right
mx2, my2, mw2, mh2 = 13.0, 1.5, 6.0, 5.2
draw_table(ax, mx2, my2, mw2, mh2, "deal_metadata", metadata_fields)

# v_deals_flat VIEW — bottom spanning
draw_table(ax, 3.5, 0.05, 13.0, 1.2, "v_deals_flat  (VIEW — analytical join of all 5 tables)",
           view_fields, border_color=VIEW_BORDER, header_bg=VIEW_BORDER, is_view=True)

# ---------------------------------------------------------------------------
# Arrows (FK relationships)
# ---------------------------------------------------------------------------
# deals.acquirer_party_id → parties
draw_arrow(ax, 12.5, 10.2, 14.0, 10.5, "FK: acquirer_party_id")

# deals.sector_id → sectors
draw_arrow(ax, 7.5, 10.5, 6.5, 9.8, "FK: sector_id")

# valuation_metrics → deals (1:1)
draw_arrow(ax, 6.5, 7.2, 7.5, 7.0, "1:1")

# deal_metadata → deals (1:1)
draw_arrow(ax, 13.0, 7.2, 12.5, 7.0, "1:1")

# deals → v_deals_flat
draw_arrow(ax, 10.0, 5.8, 10.0, 1.25, "")

# ---------------------------------------------------------------------------
# Title and caption
# ---------------------------------------------------------------------------
ax.text(10, 13.7, "M&A Database — Relational Schema",
        ha="center", va="center",
        fontsize=15, fontweight="bold", color=TITLE_COLOR, fontfamily="monospace")

ax.text(10, 13.35,
        "5 normalized tables + 2 analytical views · DuckDB columnar engine",
        ha="center", va="center",
        fontsize=9, color=CAPTION_COLOR, fontfamily="monospace")

# Legend
legend_elements = [
    mpatches.Patch(facecolor=PK_COLOR, label="Primary Key (PK)"),
    mpatches.Patch(facecolor=FK_COLOR, label="Foreign Key (FK)"),
    mpatches.Patch(facecolor="#BB8FCE", label="PK + FK (child table)"),
    mpatches.Patch(facecolor=VIEW_BORDER, label="View"),
]
ax.legend(handles=legend_elements, loc="lower right",
          fontsize=8, framealpha=0.15, labelcolor=BODY_TEXT,
          facecolor=BOX_BG, edgecolor=BOX_BORDER)

plt.tight_layout(pad=0.2)

out_path = os.path.join(os.path.dirname(__file__), "schema_diagram.png")
plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=BG)
print(f"Saved: {out_path}")
