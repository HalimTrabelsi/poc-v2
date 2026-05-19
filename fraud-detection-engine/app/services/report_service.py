"""PDF + CSV audit report generation for fraud cases.

Produces a professional PDF investigation report per case, including:
  - Case header (ID, beneficiary, date, risk label)
  - Score breakdown bar chart (rule / ML / graph / final)
  - Rules triggered table
  - SHAP feature contributions bar chart
  - Network graph image (if connections exist)
  - Full feature vector table (appendix)

Also produces a lightweight CSV summary for bulk compliance exports.
"""
import io
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ── PDF ───────────────────────────────────────────────────────────────────────

def generate_pdf_report(case: dict) -> bytes:
    """Return PDF bytes for a single fraud case dict."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.platypus import (
            HRFlowable, Image, Paragraph, SimpleDocTemplate,
            Spacer, Table, TableStyle,
        )
    except ImportError:
        raise RuntimeError("reportlab is not installed — cannot generate PDF")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
    )
    styles  = getSampleStyleSheet()
    story   = []

    RISK_COLOR = {
        "CRITICAL": colors.HexColor("#c0392b"),
        "HIGH":     colors.HexColor("#e67e22"),
        "MEDIUM":   colors.HexColor("#f1c40f"),
        "LOW":      colors.HexColor("#27ae60"),
    }

    risk_level = case.get("risk_level", "LOW")
    risk_color = RISK_COLOR.get(risk_level, colors.black)

    title_style = ParagraphStyle(
        "Title", parent=styles["Title"],
        fontSize=18, textColor=colors.HexColor("#2c3e50"),
    )
    h2_style = ParagraphStyle(
        "H2", parent=styles["Heading2"],
        fontSize=13, textColor=colors.HexColor("#2c3e50"), spaceAfter=4,
    )
    body_style = styles["BodyText"]

    # ── Header ────────────────────────────────────────────────────────────────
    story.append(Paragraph("Fraud Investigation Report", title_style))
    story.append(Paragraph(
        "OpenG2P Fraud Detection Engine — Confidential", body_style
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#2c3e50")))
    story.append(Spacer(1, 0.3*cm))

    meta_data = [
        ["Case ID",        (case.get("case_id") or "N/A")[:36]],
        ["Beneficiary ID", case.get("beneficiary_id", "N/A")],
        ["Generated",      datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")],
        ["Status",         case.get("status", "OPEN")],
        ["Recommendation", case.get("recommendation", "—")],
    ]
    meta_table = Table(meta_data, colWidths=[4.5*cm, 12*cm])
    meta_table.setStyle(TableStyle([
        ("FONTNAME",    (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 9),
        ("TEXTCOLOR",   (0, 0), (0, -1), colors.HexColor("#555555")),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 3),
    ]))
    story.append(meta_table)
    story.append(Spacer(1, 0.4*cm))

    # ── Risk Banner ───────────────────────────────────────────────────────────
    banner = Table(
        [[f"RISK LEVEL: {risk_level}   |   FINAL SCORE: {case.get('final_score', 0):.1%}"]],
        colWidths=[16.5*cm],
    )
    banner.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (-1, -1), risk_color),
        ("TEXTCOLOR",   (0, 0), (-1, -1), colors.white),
        ("FONTNAME",    (0, 0), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 14),
        ("ALIGN",       (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING",  (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 8),
    ]))
    story.append(banner)
    story.append(Spacer(1, 0.5*cm))

    # ── Score Breakdown Chart ─────────────────────────────────────────────────
    story.append(Paragraph("Score Breakdown", h2_style))
    chart_img = _score_breakdown_chart(case)
    if chart_img:
        story.append(Image(chart_img, width=14*cm, height=5*cm))
    story.append(Spacer(1, 0.4*cm))

    # ── Rules Triggered ───────────────────────────────────────────────────────
    rules = case.get("rules_triggered") or []
    if isinstance(rules, str):
        import json
        try:
            rules = json.loads(rules)
        except Exception:
            rules = []

    if rules:
        story.append(Paragraph("Rules Triggered", h2_style))
        rule_rows = [["Rule ID", "Name", "Weight", "Evidence"]]
        for r in rules:
            rule_rows.append([
                r.get("rule_id", ""),
                r.get("name", ""),
                f"{r.get('weight', 0):.2f}",
                (r.get("explanation") or r.get("evidence", ""))[:80],
            ])
        rule_table = Table(rule_rows, colWidths=[2*cm, 4*cm, 2*cm, 8.5*cm])
        rule_table.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, 0),  colors.HexColor("#2c3e50")),
            ("TEXTCOLOR",    (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",     (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",     (0, 0), (-1, -1), 8),
            ("ROWBACKGROUNDS",(0, 1),(-1, -1), [colors.HexColor("#f8f9fa"), colors.white]),
            ("GRID",         (0, 0), (-1, -1), 0.3, colors.HexColor("#dee2e6")),
            ("TOPPADDING",   (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
        ]))
        story.append(rule_table)
        story.append(Spacer(1, 0.4*cm))

    # ── SHAP Feature Contributions ────────────────────────────────────────────
    top_features = case.get("top_features") or []
    if isinstance(top_features, str):
        import json
        try:
            top_features = json.loads(top_features)
        except Exception:
            top_features = []

    if top_features:
        story.append(Paragraph("Feature Contributions (SHAP)", h2_style))
        shap_img = _shap_bar_chart(top_features)
        if shap_img:
            story.append(Image(shap_img, width=14*cm, height=6*cm))
        story.append(Spacer(1, 0.4*cm))

    # ── Features Appendix ─────────────────────────────────────────────────────
    features = case.get("features") or {}
    if isinstance(features, str):
        import json
        try:
            features = json.loads(features)
        except Exception:
            features = {}

    if features:
        story.append(Paragraph("Feature Vector (Appendix)", h2_style))
        feat_rows = [["Feature", "Value"]]
        for k, v in sorted(features.items()):
            if isinstance(v, float):
                feat_rows.append([k, f"{v:.4f}"])
            else:
                feat_rows.append([k, str(v)])
        feat_table = Table(feat_rows, colWidths=[9*cm, 7.5*cm])
        feat_table.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0),  colors.HexColor("#2c3e50")),
            ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 7),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.HexColor("#f8f9fa"), colors.white]),
            ("GRID",          (0, 0), (-1, -1), 0.3, colors.HexColor("#dee2e6")),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        story.append(feat_table)

    # ── Footer ────────────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.6*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.grey))
    story.append(Paragraph(
        "Generated by OpenG2P Fraud Detection Engine v2.0 — CONFIDENTIAL",
        ParagraphStyle("footer", parent=body_style, fontSize=7, textColor=colors.grey),
    ))

    doc.build(story)
    return buf.getvalue()


def _score_breakdown_chart(case: dict) -> io.BytesIO | None:
    """Horizontal bar chart of rule/ML/graph/final scores."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        labels  = ["Final Score", "ML Score", "Rule Score", "Graph Score"]
        values  = [
            case.get("final_score", 0),
            case.get("ml_score",    0),
            case.get("rule_score",  0),
            case.get("graph_score", 0),
        ]
        bar_colors = ["#c0392b", "#2980b9", "#27ae60", "#8e44ad"]

        fig, ax = plt.subplots(figsize=(7, 2.5))
        bars = ax.barh(labels, values, color=bar_colors, height=0.5)
        ax.set_xlim(0, 1)
        ax.set_xlabel("Score (0–1)")
        ax.set_title("Fraud Score Components", fontsize=11, fontweight="bold")
        for bar, val in zip(bars, values):
            ax.text(val + 0.01, bar.get_y() + bar.get_height()/2,
                    f"{val:.2%}", va="center", fontsize=9)
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return buf
    except Exception as exc:
        logger.warning("Score chart generation failed: %s", exc)
        return None


def _shap_bar_chart(top_features: list) -> io.BytesIO | None:
    """Horizontal bar chart of top SHAP contributions."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        items = sorted(top_features, key=lambda f: abs(f.get("shap_value", 0)), reverse=True)[:10]
        names  = [f.get("feature", "")   for f in items]
        shaps  = [f.get("shap_value", 0) for f in items]
        clrs   = ["#c0392b" if s > 0 else "#27ae60" for s in shaps]

        fig, ax = plt.subplots(figsize=(7, 3))
        ax.barh(names[::-1], shaps[::-1], color=clrs[::-1], height=0.6)
        ax.axvline(0, color="black", linewidth=0.8)
        ax.set_xlabel("SHAP Value  (positive = increases fraud risk)")
        ax.set_title("Top Feature Contributions", fontsize=11, fontweight="bold")
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return buf
    except Exception as exc:
        logger.warning("SHAP chart generation failed: %s", exc)
        return None


# ── CSV ───────────────────────────────────────────────────────────────────────

def generate_csv_report(cases: list[dict]) -> bytes:
    """Return UTF-8 CSV bytes for a list of case dicts."""
    import csv
    buf = io.StringIO()
    fieldnames = [
        "case_id", "beneficiary_id", "final_score", "rule_score",
        "ml_score", "graph_score", "risk_level", "recommendation",
        "status", "created_at", "explanation",
    ]
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for case in cases:
        row = {k: case.get(k, "") for k in fieldnames}
        row["final_score"] = f"{float(row['final_score'] or 0):.4f}"
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")
