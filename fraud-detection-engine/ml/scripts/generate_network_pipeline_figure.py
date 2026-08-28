"""generate_network_pipeline_figure.py — Flowchart figure for the report's
"Analyse de réseau" section: OpenG2P data -> relation extraction -> graph
construction -> edge weighting -> PageRank -> network risk score -> hybrid
score. The graph-construction step embeds a small real NetworkX graph
(shared phone / shared account edges) rather than a purely decorative icon.

Usage:
    python ml/scripts/generate_network_pipeline_figure.py
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
from matplotlib.patches import FancyBboxPatch

OUT_DIR = Path(__file__).resolve().parents[3] / "docs" / "Rapport" / "architecture"

EY_DARK = "#2E2E38"
EY_PURPLE = "#71639e"
RED = "#C62828"
GRAY = "#adb5bd"
LIGHT_BG = "#F6F6F6"

STAGES = [
    ("Données OpenG2P", "oval"),
    ("Extraction des relations\n(téléphones et comptes partagés)", "box"),
    ("Construction du graphe\n(NetworkX)", "graphbox"),
    ("Pondération des arêtes\nCompte partagé = 0.8   Téléphone partagé = 0.5", "box"),
    ("Calcul du PageRank", "box"),
    ("Network Risk Score", "box"),
    ("Score Hybride", "oval"),
]


def draw_box(ax, cx, cy, w, h, text, kind):
    if kind == "oval":
        box = FancyBboxPatch(
            (cx - w / 2, cy - h / 2), w, h,
            boxstyle="round,pad=0.02,rounding_size=0.3",
            linewidth=1.6, edgecolor=EY_PURPLE, facecolor=EY_PURPLE, alpha=0.15,
        )
        text_color = EY_DARK
    else:
        box = FancyBboxPatch(
            (cx - w / 2, cy - h / 2), w, h,
            boxstyle="round,pad=0.02,rounding_size=0.08",
            linewidth=1.4, edgecolor=EY_DARK, facecolor="white",
        )
        text_color = EY_DARK
    ax.add_patch(box)
    if kind != "graphbox":
        ax.text(cx, cy, text, ha="center", va="center", fontsize=10.5,
                 color=text_color, wrap=True, linespacing=1.5)
    else:
        ax.text(cx, cy + h / 2 - 0.14, text, ha="center", va="top", fontsize=10.5,
                 color=text_color)
    return box


def draw_mini_graph(fig, ax_parent, cx, cy, w, h):
    # A small illustrative fraud network: 2 beneficiaries share a phone,
    # 2 (overlapping) share a bank account — mirrors GraphAnalyzer's real
    # edge types (shared_phone / shared_account), not arbitrary shapes.
    G = nx.Graph()
    G.add_edges_from([
        ("B1", "B2"),  # shared phone
        ("B2", "B3"),  # shared account
        ("B3", "B4"),  # shared phone
        ("B1", "B4"),  # shared account (closes a small fraud ring)
    ])
    pos = {
        "B1": (-0.8, 0.35), "B2": (0.0, 0.7),
        "B3": (0.8, 0.35), "B4": (0.0, -0.1),
    }

    # Inset axes positioned inside the parent box, in figure-fraction coords
    bbox = ax_parent.get_position()
    fig_w, fig_h = bbox.width, bbox.height
    # Convert data coords (cx, cy, w, h) of the box to an inset axes rect
    x0, y0 = ax_parent.transData.transform((cx - w / 2 + 0.15, cy - h / 2 + 0.05))
    x1, y1 = ax_parent.transData.transform((cx + w / 2 - 0.15, cy + h / 2 - 0.35))
    inv = fig.transFigure.inverted()
    fx0, fy0 = inv.transform((x0, y0))
    fx1, fy1 = inv.transform((x1, y1))
    inset = fig.add_axes([fx0, fy0, fx1 - fx0, fy1 - fy0])
    inset.set_xlim(-1.2, 1.2)
    inset.set_ylim(-0.4, 1.0)
    inset.axis("off")

    nx.draw_networkx_edges(G, pos, ax=inset, width=2, edge_color=GRAY)
    nx.draw_networkx_nodes(G, pos, ax=inset, node_size=420,
                            node_color=EY_DARK, edgecolors="white", linewidths=1.5)
    nx.draw_networkx_labels(G, pos, ax=inset, font_size=8, font_color="white")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(5.5, 11))
    ax.set_xlim(-2.6, 2.6)
    n = len(STAGES)
    step = 2.0
    top = (n - 1) * step / 2
    ax.set_ylim(-top - 1.2, top + 1.2)
    ax.axis("off")

    centers = []
    for i, (text, kind) in enumerate(STAGES):
        cy = top - i * step
        w, h = (2.6, 1.0) if kind == "oval" else (4.6, 1.5 if kind == "graphbox" else 1.3)
        centers.append((cy, w, h, kind))
        draw_box(ax, 0, cy, w, h, text, kind)
        if kind == "graphbox":
            draw_mini_graph(fig, ax, 0, cy, w, h)

    for i in range(n - 1):
        cy1, _, h1, _ = centers[i]
        cy2, _, h2, _ = centers[i + 1]
        ax.annotate(
            "", xy=(0, cy2 + h2 / 2), xytext=(0, cy1 - h1 / 2),
            arrowprops=dict(arrowstyle="-|>", color=EY_DARK, lw=1.6),
        )

    fig.tight_layout()
    out = OUT_DIR / "network_analysis_pipeline.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"✅ {out}")


if __name__ == "__main__":
    main()
