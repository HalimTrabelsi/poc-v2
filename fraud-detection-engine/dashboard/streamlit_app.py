"""Professional Streamlit dashboard for the Fraud Detection Engine."""
import hashlib
import hmac
import os
import time

import altair as alt
import pandas as pd
import requests
import streamlit as st

_engine_url = os.getenv("FRAUD_ENGINE_URL", "http://localhost:8000")
API_BASE = f"{_engine_url.rstrip('/')}/api/v1"
API_KEY = os.getenv("FRAUD_API_KEY", "dev-secret-change-in-prod")
HEADERS = {"X-API-Key": API_KEY}

DASHBOARD_TOKEN_SECRET = os.getenv("DASHBOARD_TOKEN_SECRET", "")


def _check_access_token() -> bool:
    """Verify the ?token= minted by Odoo's /fraud/open_dashboard route.

    Identity is delegated to Odoo (only Fraud Officers/Supervisors get a
    token there) since this app has no login of its own — anyone reaching
    this port without a valid, unexpired token is refused.
    """
    token = st.query_params.get("token", "")
    if not token:
        return False
    parts = token.split(":")
    if len(parts) != 3:
        return False
    user_id, expiry, signature = parts
    payload = f"{user_id}:{expiry}"
    expected = hmac.new(
        DASHBOARD_TOKEN_SECRET.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return False
    try:
        if int(expiry) < time.time():
            return False
    except ValueError:
        return False
    return True

STATUS_OPTIONS = ["OPEN", "UNDER_REVIEW", "CLOSED", "FALSE_POSITIVE"]

# ── Design tokens (validated status + categorical palette) ────────────────────
# Status colors are reserved for risk states and always ship with a text label.
RISK_STATUS = {
    "LOW":      {"color": "#0ca30c", "label": "Low"},
    "MEDIUM":   {"color": "#fab219", "label": "Medium"},
    "HIGH":     {"color": "#ec835a", "label": "High"},
    "CRITICAL": {"color": "#d03b3b", "label": "Critical"},
}
RISK_ORDER = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
RISK_COLORS = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}  # table cells

ACCENT    = "#22d3ee"   # electric cyan — primary/brand
ACCENT_2  = "#8b5cf6"   # violet — gradient partner
INK_MUTED = "#8b98ad"   # blue-gray chart chrome, readable on light and dark


# Futuristic glass/neon theme. Surfaces and text still come from Streamlit's
# theme variables (so the Light/Dark switcher in the ⋮ menu keeps working);
# the glass overlays, glows and gradients are translucent layers on top.
_CSS = """
<style>
/* ── global ──────────────────────────────────────────────────────────── */
html, body, [class*="css"] {
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
}
.stApp {
  background-image:
    radial-gradient(1100px 500px at 15% -10%, rgba(34,211,238,0.10), transparent 60%),
    radial-gradient(900px 480px at 95% -5%, rgba(139,92,246,0.10), transparent 60%),
    radial-gradient(700px 700px at 50% 120%, rgba(34,211,238,0.05), transparent 60%);
  background-attachment: fixed;
}
.block-container { padding-top: 1.5rem; padding-bottom: 3rem; max-width: 1220px; }
h1, h2, h3 { letter-spacing: -0.01em; }
h2, h3 { position: relative; }

/* ── glass card recipe (shared) ──────────────────────────────────────── */
.kpi, .hero-score, div[data-testid="stForm"], [data-testid="stMetric"] {
  background:
    linear-gradient(180deg, rgba(255,255,255,0.055), rgba(255,255,255,0.015)),
    var(--secondary-background-color, transparent);
  border: 1px solid rgba(148,163,184,0.20);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
}

/* ── sidebar ─────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
  border-right: 1px solid rgba(148,163,184,0.18);
  background-image: linear-gradient(180deg, rgba(34,211,238,0.05), transparent 30%);
}
[data-testid="stSidebar"] .block-container { padding-top: 1.2rem; }
.sb-brand { display:flex; align-items:center; gap:11px; margin-bottom:2px; }
.sb-brand .logo {
  width:40px; height:40px; border-radius:12px; flex:0 0 40px;
  background: linear-gradient(135deg, #22d3ee, #8b5cf6);
  box-shadow: 0 0 18px rgba(34,211,238,0.45);
  display:flex; align-items:center; justify-content:center;
  color:#fff; font-size:20px;
}
.sb-brand .name { font-weight:800; font-size:15px; line-height:1.15;
                  color: var(--text-color, inherit); letter-spacing:0.01em; }
.sb-brand .sub  { font-size:11.5px; opacity:0.6; letter-spacing:0.06em;
                  text-transform:uppercase; }
.sb-link a {
  display:block; font-size:12.5px; text-decoration:none; font-weight:600;
  color: var(--primary-color, #22d3ee);
  padding:7px 11px; border:1px solid rgba(34,211,238,0.35); border-radius:9px;
  background: rgba(34,211,238,0.06);
  margin:12px 0 4px 0; transition: box-shadow .2s, border-color .2s;
}
.sb-link a:hover { border-color:#22d3ee; box-shadow: 0 0 14px rgba(34,211,238,0.35); }

/* ── metric cards ────────────────────────────────────────────────────── */
[data-testid="stMetric"] { border-radius: 13px; padding: 14px 16px; }

/* ── KPI tiles (custom) ──────────────────────────────────────────────── */
.kpi-row { display:flex; gap:14px; flex-wrap:wrap; margin: 6px 0 20px 0; }
.kpi {
  flex:1 1 160px; min-width:150px;
  border-radius:14px; padding:15px 17px 13px 17px;
  border-top:2px solid var(--accent, #22d3ee);
  box-shadow: 0 10px 30px -18px var(--accent, #22d3ee);
  transition: transform .15s ease, box-shadow .15s ease;
}
.kpi:hover { transform: translateY(-2px);
             box-shadow: 0 14px 34px -16px var(--accent, #22d3ee); }
.kpi .k-label { font-size:11.5px; opacity:0.6; text-transform:uppercase;
                letter-spacing:0.09em; margin-bottom:5px; }
.kpi .k-value { font-size:28px; font-weight:800; line-height:1.05;
                color: var(--text-color, inherit);
                font-variant-numeric: tabular-nums; }
.kpi .k-sub   { font-size:12px; opacity:0.5; margin-top:5px; }

/* ── page header ─────────────────────────────────────────────────────── */
.page-header { display:flex; align-items:center; gap:15px; margin-bottom: 6px; }
.page-header .ph-icon {
  width:46px; height:46px; border-radius:13px; flex:0 0 46px;
  background: linear-gradient(135deg, #22d3ee, #8b5cf6);
  box-shadow: 0 0 22px rgba(34,211,238,0.40);
  display:flex; align-items:center; justify-content:center; font-size:22px;
}
.page-header h1 {
  font-size:27px; margin:0; font-weight:800;
  background: linear-gradient(90deg, #22d3ee, #8b5cf6);
  -webkit-background-clip: text; background-clip: text;
  -webkit-text-fill-color: transparent; color: transparent;
}
.page-header p  { font-size:13px; opacity:0.65; margin:3px 0 0 0;
                  letter-spacing:0.02em; }

/* ── risk pill ───────────────────────────────────────────────────────── */
.pill { display:inline-flex; align-items:center; gap:7px; padding:4px 13px;
        border-radius:999px; font-size:13px; font-weight:700;
        border:1px solid currentColor; }
.pill .dot { width:8px; height:8px; border-radius:50%;
             box-shadow: 0 0 8px currentColor; }

/* ── hero score ──────────────────────────────────────────────────────── */
.hero-score {
  display:flex; align-items:center; gap:24px;
  border-radius:16px; padding:20px 24px; margin:8px 0 18px 0;
  box-shadow: 0 12px 40px -22px var(--ring, #22d3ee);
}
.hero-score .ring {
  width:98px; height:98px; border-radius:50%; flex:0 0 98px;
  display:flex; align-items:center; justify-content:center;
  font-size:22px; font-weight:800; color: var(--text-color, inherit);
  font-variant-numeric: tabular-nums;
  background:
    radial-gradient(closest-side,
      var(--secondary-background-color, #131b2e) 76%, transparent 77% 100%),
    conic-gradient(var(--ring) calc(var(--pct)*1%), rgba(148,163,184,0.18) 0);
  filter: drop-shadow(0 0 14px var(--ring));
}
.hero-score .h-title { font-size:12px; opacity:0.6; text-transform:uppercase;
                       letter-spacing:0.10em; }
.hero-score .h-rec   { font-size:14px; margin-top:7px;
                       color: var(--text-color, inherit); }

/* ── buttons / tabs / inputs ─────────────────────────────────────────── */
.stButton > button { border-radius: 10px; font-weight:700;
                     transition: box-shadow .2s, transform .1s; }
.stButton > button[kind="primary"] {
  background: linear-gradient(90deg, #06b6d4, #8b5cf6);
  border: none; color: #fff;
}
.stButton > button[kind="primary"]:hover {
  box-shadow: 0 0 18px rgba(34,211,238,0.45); transform: translateY(-1px);
}
.stTabs [data-baseweb="tab-list"] { gap: 8px; }
.stTabs [data-baseweb="tab"] {
  border-radius: 9px 9px 0 0; padding: 8px 18px; font-weight:700;
}
.stTabs [aria-selected="true"] {
  background: linear-gradient(180deg, rgba(34,211,238,0.12), transparent);
}
div[data-testid="stForm"] { border-radius:14px; padding:20px; }
[data-testid="stDataFrame"] {
  border-radius: 12px; overflow:hidden;
  border: 1px solid rgba(148,163,184,0.18);
}
[data-testid="stExpander"] { border-radius: 12px; }
</style>
"""


def _inject_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def page_header(title: str, subtitle: str = "", icon: str = "📊") -> None:
    sub = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(
        f'<div class="page-header"><div class="ph-icon">{icon}</div>'
        f"<div><h1>{title}</h1>{sub}</div></div>",
        unsafe_allow_html=True,
    )
    st.markdown("")


def kpi_row(items: list[dict]) -> None:
    """Render a row of KPI tiles. Each item: label, value, sub (opt), accent (opt)."""
    tiles = "".join(
        f'<div class="kpi" style="--accent:{it.get("accent", ACCENT)}">'
        f'<div class="k-label">{it["label"]}</div>'
        f'<div class="k-value">{it["value"]}</div>'
        f'<div class="k-sub">{it.get("sub", "")}</div></div>'
        for it in items
    )
    st.markdown(f'<div class="kpi-row">{tiles}</div>', unsafe_allow_html=True)


def risk_pill(level: str) -> str:
    s = RISK_STATUS.get(level, RISK_STATUS["LOW"])
    return (
        f'<span class="pill" style="background:{s["color"]}1a;color:{s["color"]};">'
        f'<span class="dot" style="background:{s["color"]}"></span>{level}</span>'
    )


# ── Altair chart helpers (thin marks, hairline grid, direct labels) ───────────

def _axis(title: str = "") -> alt.Axis:
    # Mid-gray chrome reads on both the light and dark theme surfaces.
    return alt.Axis(
        title=title, labelColor=INK_MUTED, titleColor=INK_MUTED,
        gridColor=INK_MUTED, gridOpacity=0.25,
        domainColor=INK_MUTED, tickColor=INK_MUTED,
        labelFontSize=12, titleFontSize=12,
    )


def risk_distribution_chart(counts: pd.DataFrame) -> alt.Chart:
    """Horizontal bars for case counts by risk level, in reserved status colors."""
    counts = counts.copy()
    counts["risk_level"] = pd.Categorical(counts["risk_level"], RISK_ORDER)
    counts = counts.sort_values("risk_level")
    base = alt.Chart(counts).encode(
        y=alt.Y("risk_level:N", sort=RISK_ORDER, axis=_axis(""),
                scale=alt.Scale(paddingInner=0.45)),
        x=alt.X("count:Q", axis=_axis("Cases")),
        color=alt.Color(
            "risk_level:N", legend=None,
            scale=alt.Scale(domain=RISK_ORDER,
                            range=[RISK_STATUS[r]["color"] for r in RISK_ORDER]),
        ),
    )
    bars = base.mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
    labels = base.mark_text(align="left", dx=6, fontSize=12).encode(
        text="count:Q", color=alt.value(INK_MUTED))
    return (bars + labels).properties(height=190, background="transparent")


def count_bar_chart(df: pd.DataFrame, cat_col: str, count_col: str,
                    x_title: str = "Cases") -> alt.Chart:
    """Single-hue horizontal count bars with direct labels (magnitude job)."""
    base = alt.Chart(df).encode(
        y=alt.Y(f"{cat_col}:N", sort="-x", axis=_axis(""),
                scale=alt.Scale(paddingInner=0.45)),
        x=alt.X(f"{count_col}:Q", axis=_axis(x_title)),
    )
    bars = base.mark_bar(color=ACCENT, cornerRadiusTopRight=4,
                         cornerRadiusBottomRight=4)
    labels = base.mark_text(align="left", dx=6, color=INK_MUTED, fontSize=12).encode(
        text=f"{count_col}:Q")
    return (bars + labels).properties(height=190, background="transparent")


# ── SHAP rendering ─────────────────────────────────────────────────────────────

def _shap_value(f: dict) -> float:
    """Read the SHAP contribution under either key ('shap_value' or 'impact')."""
    val = f.get("shap_value")
    if val is None:
        val = f.get("impact", 0)
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


def render_shap(features: list, title: str = "Top Feature Contributions (SHAP)") -> None:
    """Render SHAP factors as a signed horizontal bar chart + detail table.

    Features whose raw value is 0 are dropped — a non-zero SHAP on an absent
    feature is a missing-data artefact, not a real driver, and shows up as a
    misleading bar otherwise.
    """
    if not features:
        return
    rows = []
    for f in features:
        try:
            value = float(f.get("value") or 0)
        except (TypeError, ValueError):
            value = 0.0
        if value == 0.0:
            continue
        rows.append({"feature": f.get("feature", "?"), "value": value,
                     "shap": _shap_value(f)})
    if not rows:
        return

    st.subheader(title)
    rows.sort(key=lambda r: abs(r["shap"]), reverse=True)
    chart_df = pd.DataFrame(rows)
    chart_df["direction"] = chart_df["shap"].map(
        lambda v: "increases risk" if v > 0 else "decreases risk")

    # Diverging encoding: red = increases risk, blue = decreases risk.
    base = alt.Chart(chart_df).encode(
        y=alt.Y("feature:N", sort=None, axis=_axis(""),
                scale=alt.Scale(paddingInner=0.45)),
        x=alt.X("shap:Q", axis=_axis("SHAP impact on fraud score")),
        color=alt.Color(
            "direction:N",
            scale=alt.Scale(domain=["increases risk", "decreases risk"],
                            range=["#f87171", "#38bdf8"]),
            legend=alt.Legend(title="", orient="top", labelColor=INK_MUTED),
        ),
    )
    bars = base.mark_bar(cornerRadius=4)
    zero = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color=INK_MUTED).encode(x="x:Q")
    st.altair_chart((bars + zero).properties(height=32 * len(rows) + 60,
                                             background="transparent"),
                    use_container_width=True)

    st.dataframe(pd.DataFrame([{
        "Feature":     r["feature"],
        "Value":       f"{r['value']:.3f}",
        "SHAP Impact": f"{r['shap']:+.4f}",
        "Direction":   "increases_risk" if r["shap"] > 0 else "decreases_risk",
    } for r in rows]), use_container_width=True, hide_index=True)


# ── API helpers ───────────────────────────────────────────────────────────────

def _get(path, params=None):
    try:
        r = requests.get(f"{API_BASE}{path}", headers=HEADERS, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(f"API error: {exc}")
        return None


def _post(path, json=None):
    try:
        r = requests.post(f"{API_BASE}{path}", headers=HEADERS, json=json, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(f"API error: {exc}")
        return None


def _patch(path, json):
    try:
        r = requests.patch(f"{API_BASE}{path}", headers=HEADERS, json=json, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(f"API error: {exc}")
        return None


def _delete(path):
    try:
        r = requests.delete(f"{API_BASE}{path}", headers=HEADERS, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(f"API error: {exc}")
        return None


# ── Cases page ────────────────────────────────────────────────────────────────

def show_cases_page() -> None:
    page_header("Fraud Cases", "Review, triage and close detected cases", "🗂️")

    col1, col2, col3 = st.columns(3)
    risk_filter   = col1.selectbox("Risk Level", ["All", "LOW", "MEDIUM", "HIGH", "CRITICAL"])
    status_filter = col2.selectbox("Status", ["All"] + STATUS_OPTIONS)
    limit         = col3.slider("Max rows", 10, 500, 50)

    params: dict = {"limit": limit}
    if risk_filter   != "All": params["risk_level"] = risk_filter
    if status_filter != "All": params["status"]     = status_filter

    data = _get("/cases", params=params)
    if data is None:
        return

    cases: list[dict] = data.get("cases", [])

    n_crit = sum(1 for c in cases if c.get("risk_level") == "CRITICAL")
    n_high = sum(1 for c in cases if c.get("risk_level") == "HIGH")
    n_open = sum(1 for c in cases if c.get("status") == "OPEN")
    kpi_row([
        {"label": "Cases returned", "value": data.get("total", len(cases)),
         "sub": "matching current filters"},
        {"label": "Critical", "value": n_crit,
         "accent": RISK_STATUS["CRITICAL"]["color"], "sub": "block payment"},
        {"label": "High", "value": n_high,
         "accent": RISK_STATUS["HIGH"]["color"], "sub": "manual review"},
        {"label": "Open", "value": n_open, "sub": "awaiting triage"},
    ])

    if not cases:
        st.info("No cases match the selected filters.")
        return

    rows = []
    for c in cases:
        rl = c.get("risk_level", "LOW")
        rows.append({
            "Case ID":       c.get("case_id", "")[:8] + "…",
            "Beneficiary":   c.get("beneficiary_id"),
            "Score":         float(c.get("final_score", 0) or 0),
            "Risk":          f"{RISK_COLORS.get(rl, '')} {rl}",
            "Recommendation":c.get("recommendation"),
            "Status":        c.get("status"),
            "Created":       c.get("created_at", "")[:10],
            "_case_id":      c.get("case_id"),
        })

    df = pd.DataFrame(rows)
    st.dataframe(
        df.drop(columns=["_case_id"]),
        use_container_width=True, hide_index=True,
        column_config={
            "Score": st.column_config.ProgressColumn(
                "Score", format="percent", min_value=0.0, max_value=1.0),
        },
    )

    # ── case actions ─────────────────────────────────────────────────────────
    st.subheader("Case Actions")
    case_ids    = [r["_case_id"] for r in rows]
    selected_id = st.selectbox("Select Case", case_ids, format_func=lambda x: x[:12] + "…")

    action_tab, feedback_tab = st.tabs(["Update Status", "Submit Verdict"])

    with action_tab:
        new_status = st.selectbox("New Status", STATUS_OPTIONS)
        notes      = st.text_area("Agent Notes", key="status_notes")
        if st.button("Update Status"):
            result = _patch(f"/cases/{selected_id}/status",
                            {"status": new_status, "notes": notes})
            if result:
                st.success(f"Case updated to {new_status}")
                st.rerun()

    with feedback_tab:
        st.markdown(
            "Submit an investigator verdict to train the model. "
            "Confirmed labels feed the **weekly XGBoost retraining** pipeline."
        )
        verdict = st.radio(
            "Verdict",
            ["confirmed_fraud", "false_positive", "uncertain"],
            horizontal=True,
            format_func=lambda v: {
                "confirmed_fraud": "Confirm Fraud",
                "false_positive":  "False Positive",
                "uncertain":       "Uncertain",
            }[v],
        )
        investigator = st.text_input("Investigator name / ID", value="investigator")
        fb_notes     = st.text_area("Notes (optional)", key="feedback_notes")
        if st.button("Submit Verdict", type="primary"):
            result = _post(f"/cases/{selected_id}/feedback", {
                "verdict": verdict, "notes": fb_notes, "investigator": investigator,
            })
            if result:
                st.success(f"Verdict '{verdict}' recorded — feedback_id: {result.get('feedback_id')}")

    # ── PDF download ──────────────────────────────────────────────────────────
    st.subheader("Download Report")
    c1, c2 = st.columns(2)
    with c1:
        if selected_id and st.button("Download PDF Report"):
            try:
                r = requests.get(
                    f"{API_BASE}/cases/{selected_id}/report/pdf",
                    headers=HEADERS, timeout=30
                )
                r.raise_for_status()
                st.download_button(
                    label="Save PDF",
                    data=r.content,
                    file_name=f"fraud_case_{selected_id[:8]}.pdf",
                    mime="application/pdf",
                )
            except Exception as exc:
                st.error(f"PDF generation failed: {exc}")
    with c2:
        if st.button("Export All Cases (CSV)"):
            try:
                r = requests.get(
                    f"{API_BASE}/cases/export/csv",
                    headers=HEADERS, params={"limit": 5000}, timeout=30
                )
                r.raise_for_status()
                st.download_button(
                    label="Save CSV",
                    data=r.content,
                    file_name="fraud_cases_export.csv",
                    mime="text/csv",
                )
            except Exception as exc:
                st.error(f"CSV export failed: {exc}")


# ── Score / Scan page ─────────────────────────────────────────────────────────

def _render_decision(result: dict) -> None:
    rl    = result.get("risk_level", "LOW")
    score = float(result.get("final_score", 0) or 0)
    s     = RISK_STATUS.get(rl, RISK_STATUS["LOW"])

    st.markdown(
        f'<div class="hero-score" style="--pct:{score * 100:.1f};--ring:{s["color"]}">'
        f'<div class="ring">'
        f"{score:.0%}</div>"
        f'<div><div class="h-title">Final fraud score</div>'
        f'<div style="margin:6px 0">{risk_pill(rl)}</div>'
        f'<div class="h-rec">Recommendation — '
        f'<b>{result.get("recommendation", "")}</b></div></div></div>',
        unsafe_allow_html=True,
    )

    st.info(result.get("explanation", "No explanation available."))

    if result.get("rules_triggered"):
        st.subheader("Triggered Rules")
        st.dataframe(pd.DataFrame([{
            "Rule":        r.get("rule_id"),
            "Name":        r.get("name"),
            "Weight":      r.get("weight"),
            "Explanation": r.get("explanation"),
        } for r in result["rules_triggered"]]),
            use_container_width=True, hide_index=True)

    render_shap(result.get("top_features") or [])

    st.caption(
        f"Processed in {result.get('processing_ms', 0):.0f} ms | "
        f"Case ID: {result.get('case_id', 'N/A')}"
    )


def show_scoring_page() -> None:
    page_header("Score a Beneficiary",
                "Run the hybrid pipeline — rules, ML, anomaly and graph", "⚡")
    tab_single, tab_batch, tab_csv = st.tabs(["Single Beneficiary", "Scan All from OpenG2P", "Batch CSV Upload"])

    with tab_single:
        bid = st.text_input("Beneficiary ID (partner_id)", placeholder="e.g. 42")
        if st.button("Run Fraud Score", type="primary"):
            if not bid.strip():
                st.warning("Please enter a Beneficiary ID.")
                return
            with st.spinner(f"Scoring beneficiary {bid}…"):
                result = _post(f"/score/beneficiary/{bid.strip()}")
            if result:
                _render_decision(result)

    with tab_batch:
        limit = st.number_input("Limit (0 = all)", 0, 10000, 100, 10)
        if st.button("Scan All Beneficiaries", type="primary"):
            with st.spinner("Fetching beneficiary list…"):
                params = {"limit": int(limit)} if limit > 0 else {}
                bene_list = _get("/beneficiaries", params=params or None)

            if not bene_list:
                st.warning("No beneficiaries returned.")
                return

            ids = [str(b.get("partner_id") or b.get("beneficiary_id")) for b in bene_list if b]
            st.info(f"Found {len(ids)} beneficiaries. Scoring now…")
            progress = st.progress(0)
            results  = []
            for i, b in enumerate(ids):
                r = _post(f"/score/beneficiary/{b}")
                if r:
                    results.append({
                        "Beneficiary ID": b,
                        "Score":          float(r.get("final_score", 0) or 0),
                        "Risk":           f"{RISK_COLORS.get(r.get('risk_level','LOW'),'')} {r.get('risk_level','LOW')}",
                        "Recommendation": r.get("recommendation"),
                        "Case ID":        (r.get("case_id") or "")[:12],
                    })
                progress.progress((i + 1) / len(ids))
            if results:
                st.success(f"Scored {len(results)} beneficiaries.")
                st.dataframe(
                    pd.DataFrame(results), use_container_width=True, hide_index=True,
                    column_config={
                        "Score": st.column_config.ProgressColumn(
                            "Score", format="percent", min_value=0.0, max_value=1.0),
                    },
                )

    with tab_csv:
        st.markdown(
            "Upload a CSV file with a **`beneficiary_id`** column. "
            "All listed beneficiaries are scored in parallel and results returned as CSV."
        )
        uploaded = st.file_uploader("Choose CSV file", type=["csv"])
        if uploaded and st.button("Score Batch", type="primary"):
            with st.spinner(f"Scoring beneficiaries in {uploaded.name}…"):
                try:
                    r = requests.post(
                        f"{API_BASE}/score/batch",
                        headers=HEADERS,
                        files={"file": (uploaded.name, uploaded.getvalue(), "text/csv")},
                        timeout=300,
                    )
                    r.raise_for_status()
                    st.success("Batch scoring complete!")
                    st.download_button(
                        label="Download Results CSV",
                        data=r.content,
                        file_name="batch_scores.csv",
                        mime="text/csv",
                    )
                    # Preview
                    import io
                    preview_df = pd.read_csv(io.BytesIO(r.content))
                    st.dataframe(preview_df.head(50), use_container_width=True)
                except Exception as exc:
                    st.error(f"Batch scoring failed: {exc}")

        st.caption("Supports up to 10,000 beneficiaries per upload. Uses up to 8 parallel workers.")


# ── Explainability page ───────────────────────────────────────────────────────

def show_explainability_page() -> None:
    page_header("Decision Explainability",
                "SHAP factors, triggered rules and the AI-written summary", "💡")
    bid = st.text_input("Beneficiary ID", placeholder="e.g. 12345", key="explain_id")

    if st.button("Get Explanation", type="primary"):
        if not bid:
            st.warning("Please enter a Beneficiary ID.")
            return
        with st.spinner("Fetching explanation…"):
            result = _get(f"/explain/{bid}")
        if result is None:
            return

        st.subheader("Summary")
        st.write(result.get("summary", ""))

        if result.get("top_reasons"):
            st.subheader("Top Reasons")
            for reason in result["top_reasons"]:
                st.markdown(f"- {reason}")

        if result.get("rule_explanations"):
            st.subheader("Rule Explanations")
            for expl in result["rule_explanations"]:
                st.markdown(f"- {expl}")

        render_shap(result.get("feature_contributions") or [],
                    title="Feature Contributions (SHAP)")

        raw = result.get("raw_scores", {})
        if raw:
            st.subheader("Raw Scores")
            kpi_row([
                {"label": "Final", "value": f"{raw.get('final_score', 0):.3f}"
                 if raw.get("final_score") else "N/A"},
                {"label": "Rules", "value": f"{raw.get('rule_score', 0):.3f}"
                 if raw.get("rule_score") else "N/A", "accent": "#34d399"},
                {"label": "ML", "value": f"{raw.get('ml_score', 0):.3f}"
                 if raw.get("ml_score") else "N/A", "accent": "#8b5cf6"},
                {"label": "Graph", "value": f"{raw.get('graph_score', 0):.3f}"
                 if raw.get("graph_score") else "N/A", "accent": "#fb923c"},
            ])


# ── Geo Hotspots page ─────────────────────────────────────────────────────────

def show_geo_page() -> None:
    page_header("Geospatial Fraud Hotspots",
                "DBSCAN clusters weighted by fraud score", "🗺️")
    st.markdown(
        "Beneficiaries are clustered geographically using **DBSCAN**. "
        "The heatmap weight is the fraud score. "
        "Darker/taller areas = higher fraud density in that region."
    )

    with st.spinner("Loading geo data…"):
        heatmap_data = _get("/geo/heatmap")
        hotspots     = _get("/geo/hotspots")

    if not heatmap_data:
        st.warning("No scored beneficiaries with coordinates found yet. Score some beneficiaries first.")
        return

    df_map = pd.DataFrame(heatmap_data)

    # Deep-link from Odoo: ?beneficiary=ID centers + highlights that partner
    focus_bid = st.query_params.get("beneficiary", "")
    if focus_bid:
        focus = df_map[df_map["partner_id"].astype(str) == str(focus_bid)]
        if not focus.empty:
            st.info(f"Showing location of beneficiary #{focus_bid} (highlighted in blue).")
        else:
            st.warning(f"Beneficiary #{focus_bid} has no geo data yet.")

    # ── pydeck heatmap ────────────────────────────────────────────────────────
    try:
        import pydeck as pdk

        heatmap_layer = pdk.Layer(
            "HeatmapLayer",
            data=df_map,
            get_position=["lon", "lat"],
            get_weight="weight",
            aggregation="MEAN",
            radius_pixels=60,
            intensity=1.5,
            threshold=0.05,
            color_range=[
                [12,  163, 12,  180],   # green  = low
                [250, 178, 25,  200],   # amber  = medium
                [236, 131, 90,  220],   # orange = high
                [208, 59,  59,  255],   # red    = critical
            ],
        )

        scatter_layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_map[df_map["fraud_score"] >= 0.60],
            get_position=["lon", "lat"],
            get_radius=5000,
            get_fill_color=[208, 59, 59, 160],
            pickable=True,
            tooltip=True,
        )

        layers = [heatmap_layer, scatter_layer]
        # If a focus beneficiary is requested, drop a bright blue ring + zoom in
        if focus_bid and not focus.empty:
            highlight_layer = pdk.Layer(
                "ScatterplotLayer",
                data=focus,
                get_position=["lon", "lat"],
                get_radius=8000,
                get_fill_color=[42, 120, 214, 220],
                stroked=True,
                get_line_color=[255, 255, 255, 255],
                line_width_min_pixels=3,
                pickable=True,
            )
            layers.append(highlight_layer)
            view = pdk.ViewState(
                latitude=float(focus["lat"].iloc[0]),
                longitude=float(focus["lon"].iloc[0]),
                zoom=10,
                pitch=40,
            )
        else:
            view = pdk.ViewState(
                latitude=df_map["lat"].mean(),
                longitude=df_map["lon"].mean(),
                zoom=6,
                pitch=40,
            )

        st.pydeck_chart(pdk.Deck(
            layers=layers,
            initial_view_state=view,
            map_provider="carto",
            map_style="light",
            tooltip={"text": "Partner {partner_id}\nScore: {fraud_score}"},
        ))

    except Exception as exc:
        st.warning(f"pydeck chart unavailable ({exc}) — showing table instead")
        st.dataframe(df_map.sort_values("fraud_score", ascending=False), use_container_width=True)

    # ── hotspot table ─────────────────────────────────────────────────────────
    if hotspots:
        high_risk = [h for h in hotspots if h.get("risk_label") in ("HIGH", "CRITICAL")]
        kpi_row([
            {"label": "Total clusters", "value": len(hotspots)},
            {"label": "High-risk clusters", "value": len(high_risk),
             "accent": RISK_STATUS["HIGH"]["color"]},
            {"label": "Beneficiaries on map", "value": len(df_map)},
        ])

        st.subheader(f"Detected Clusters ({len(hotspots)})")
        hs_rows = []
        for h in hotspots:
            rl = h.get("risk_label", "LOW")
            hs_rows.append({
                "Cluster":       h.get("cluster_id"),
                "Center":        f"{h['center_lat']:.4f}, {h['center_lon']:.4f}",
                "Radius km":     h.get("radius_km"),
                "Beneficiaries": h.get("count"),
                "HIGH+CRITICAL": h.get("fraud_count"),
                "Fraud Rate":    f"{h.get('fraud_rate', 0):.1%}",
                "Avg Score":     f"{h.get('avg_score', 0):.3f}",
                "Risk":          f"{RISK_COLORS.get(rl, '')} {rl}",
            })
        st.dataframe(pd.DataFrame(hs_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No dense clusters detected yet. More data needed for DBSCAN to find hotspots.")


# ── Monitoring page ───────────────────────────────────────────────────────────

def show_monitoring_page() -> None:
    page_header("System Monitoring",
                "Service health, risk distribution and model lifecycle", "📈")

    health = _get("/health")
    if health:
        status_ok = health.get("status", "") == "healthy" or health.get("status", "") == "ok"
        kpi_row([
            {"label": "Service status", "value": health.get("status", "unknown").upper(),
             "accent": "#0ca30c" if status_ok else "#d03b3b",
             "sub": f"v{health.get('version', '')}"},
            {"label": "Models ready", "value": "Yes" if health.get("models_ready") else "No",
             "accent": "#0ca30c" if health.get("models_ready") else "#d03b3b"},
            {"label": "Rules loaded", "value": health.get("rules_loaded", 0)},
        ])
        st.caption(f"Last check: {health.get('timestamp', '')[:19]}")

    st.subheader("Risk Distribution")
    data = _get("/cases", params={"limit": 500})
    if data:
        cases = data.get("cases", [])
        if cases:
            df = pd.DataFrame(cases)
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Cases by risk level**")
                risk_counts = df["risk_level"].value_counts().reset_index()
                risk_counts.columns = ["risk_level", "count"]
                st.altair_chart(risk_distribution_chart(risk_counts),
                                use_container_width=True)
            with c2:
                st.markdown("**Cases by recommendation**")
                rec_counts = df["recommendation"].value_counts().reset_index()
                rec_counts.columns = ["recommendation", "count"]
                st.altair_chart(
                    count_bar_chart(rec_counts, "recommendation", "count"),
                    use_container_width=True)

    # ── feedback & retraining ─────────────────────────────────────────────────
    st.subheader("Investigator Feedback & Model Retraining")
    stats = _get("/feedback/stats")
    if stats:
        kpi_row([
            {"label": "Total verdicts", "value": stats.get("total_feedback", 0)},
            {"label": "Confirmed fraud", "value": stats.get("confirmed_fraud", 0),
             "accent": RISK_STATUS["CRITICAL"]["color"]},
            {"label": "False positives", "value": stats.get("false_positive", 0),
             "accent": RISK_STATUS["MEDIUM"]["color"]},
            {"label": "Est. precision", "value": f"{stats.get('estimated_precision', 0):.1%}",
             "accent": "#0ca30c"},
        ])
        if stats.get("last_retrain"):
            st.caption(f"Last retrain: {stats['last_retrain'][:19]}")

    col_btn, col_info = st.columns([1, 3])
    with col_btn:
        if st.button("Retrain Model Now", type="primary"):
            result = _post("/retrain")
            if result:
                st.success(result.get("message", "Retraining started"))
    with col_info:
        st.info(
            "Retraining uses confirmed fraud / false-positive verdicts from investigators "
            "as ground-truth labels. It runs automatically every 7 days. "
            f"Minimum {10} verdicts required."
        )

    # ── MLflow model versions ─────────────────────────────────────────────────
    st.subheader("Model Version History (MLflow)")
    versions = _get("/models/versions")
    if versions:
        ver_rows = []
        for v in versions:
            ver_rows.append({
                "Run ID":     v.get("run_id", ""),
                "Status":     v.get("status", ""),
                "Started":    v.get("started", ""),
                "Accuracy":   f"{v.get('accuracy', 0):.3f}",
                "Samples":    v.get("n_samples", 0),
                "Feedback":   v.get("n_feedback", 0),
            })
        df_ver = pd.DataFrame(ver_rows)
        st.dataframe(df_ver, use_container_width=True, hide_index=True)

        st.markdown("**Rollback to a previous run:**")
        run_ids = [v.get("run_id") for v in versions]
        rollback_id = st.selectbox("Select run", run_ids)
        if st.button("Rollback Model", type="secondary"):
            result = _post(f"/models/rollback/{rollback_id}")
            if result:
                if result.get("status") == "success":
                    st.success(f"Rolled back to run {rollback_id}")
                else:
                    st.error(result.get("error", "Rollback failed"))
    else:
        st.info("No MLflow runs found. Trigger a retraining to populate this table.")


# ── Rules Management page ───────────────────────────────────────────────────────

ALERT_LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]


def show_rules_page() -> None:
    page_header("Rule Engine — Management",
                "Create, tune, enable or retire detection rules", "⚙️")
    st.caption(
        "Rules are stored as YAML files on the fraud-engine and hot-reloaded after every "
        "change — no restart needed. Conditions are evaluated through a restricted "
        "AST evaluator (only arithmetic/boolean/comparison expressions), so arbitrary "
        "Python code cannot be injected here."
    )

    data = _get("/rules/admin")
    rules = data.get("rules", []) if data else []

    # ── summary ──────────────────────────────────────────────────────────────
    kpi_row([
        {"label": "Total rules", "value": len(rules)},
        {"label": "Enabled", "value": sum(1 for r in rules if r.get("enabled", True)),
         "accent": "#0ca30c"},
        {"label": "Disabled", "value": sum(1 for r in rules if not r.get("enabled", True)),
         "accent": "#898781"},
    ])

    if st.button("Reload from disk"):
        result = _post("/rules/reload")
        if result:
            st.success(f"Reloaded — {result.get('count', 0)} active rules")
            st.rerun()

    tab_list, tab_edit, tab_new = st.tabs(["Active Rules", "Edit / Delete", "New Rule"])

    # ── Active rules table ───────────────────────────────────────────────────
    with tab_list:
        if not rules:
            st.info("No rules found.")
        else:
            rows = [{
                "ID": r["id"],
                "Name": r["name"],
                "Scenario": r.get("scenario"),
                "Weight": r.get("weight"),
                "Alert Level": r.get("alert_level"),
                "Condition": r.get("condition"),
                "Status": "🟢 Enabled" if r.get("enabled", True) else "⚪ Disabled",
            } for r in rules]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ── Edit / enable-disable / delete ───────────────────────────────────────
    with tab_edit:
        if not rules:
            st.info("No rules to edit yet — create one in the 'New Rule' tab.")
        else:
            rule_ids = [r["id"] for r in rules]
            selected_id = st.selectbox("Select a rule", rule_ids, key="edit_select")
            rule = next(r for r in rules if r["id"] == selected_id)

            with st.form("edit_rule_form"):
                name = st.text_input("Name", value=rule["name"])
                description = st.text_area("Description", value=rule.get("description", ""))
                weight = st.slider("Weight", 0.0, 1.0, float(rule.get("weight", 0.3)), 0.01)
                alert_level = st.selectbox(
                    "Alert Level", ALERT_LEVELS,
                    index=ALERT_LEVELS.index(rule.get("alert_level", "MEDIUM")),
                )
                condition = st.text_input(
                    "Condition", value=rule.get("condition", ""),
                    help="Boolean expression over feature names, e.g. "
                         "'shared_account_count >= 2 and income < 500'",
                )
                evidence_template = st.text_input(
                    "Evidence template", value=rule.get("evidence_template", ""),
                    help="Explanation shown to agents, e.g. "
                         "'Shared with {shared_account_count} other beneficiaries'",
                )
                submitted = st.form_submit_button("Save changes", type="primary")

            if submitted:
                payload = {
                    "id": rule["id"],
                    "name": name,
                    "description": description,
                    "weight": weight,
                    "alert_level": alert_level,
                    "condition": condition,
                    "evidence_template": evidence_template,
                    "enabled": rule.get("enabled", True),
                    "scenario_file": rule.get("file"),
                }
                result = _post("/rules", json=payload)
                if result:
                    st.success(f"Rule '{selected_id}' updated")
                    st.rerun()

            st.divider()
            col_toggle, col_delete = st.columns(2)
            with col_toggle:
                is_enabled = rule.get("enabled", True)
                label = "Disable rule" if is_enabled else "Enable rule"
                if st.button(label):
                    result = _patch(f"/rules/{selected_id}/enabled", {"enabled": not is_enabled})
                    if result:
                        st.success(f"Rule '{selected_id}' {'disabled' if is_enabled else 'enabled'}")
                        st.rerun()
            with col_delete:
                confirm = st.checkbox(f"Confirm permanent deletion of {selected_id}")
                if st.button("Delete rule", type="secondary", disabled=not confirm):
                    result = _delete(f"/rules/{selected_id}")
                    if result:
                        st.success(f"Rule '{selected_id}' deleted")
                        st.rerun()

    # ── New rule ─────────────────────────────────────────────────────────────
    with tab_new:
        scenarios_data = _get("/rules/scenarios")
        scenario_files = scenarios_data.get("files", []) if scenarios_data else []

        with st.form("new_rule_form", clear_on_submit=True):
            col_a, col_b = st.columns(2)
            with col_a:
                new_id = st.text_input("Rule ID", placeholder="e.g. NF007")
                new_name = st.text_input("Name", placeholder="e.g. Large Cash Withdrawal Pattern")
                new_weight = st.slider("Weight", 0.0, 1.0, 0.30, 0.01)
                new_alert_level = st.selectbox("Alert Level", ALERT_LEVELS, index=2)
            with col_b:
                scenario_choice = st.selectbox(
                    "Scenario file",
                    scenario_files + ["+ New file…"],
                    help="Rules are grouped by scenario YAML file under app/rules/rules/",
                )
                if scenario_choice == "+ New file…":
                    scenario_file = st.text_input(
                        "New scenario filename", placeholder="e.g. custom_rules.yaml"
                    )
                else:
                    scenario_file = scenario_choice
                new_description = st.text_area("Description", placeholder="What does this rule detect?")

            new_condition = st.text_input(
                "Condition",
                placeholder="e.g. payment_gap_ratio > 0.5 and nb_programs >= 3",
                help="Only arithmetic, boolean and comparison operators are allowed "
                     "(no function calls) — enforced by a restricted AST evaluator.",
            )
            new_evidence = st.text_input(
                "Evidence template",
                placeholder="e.g. 'Payment gap ratio {payment_gap_ratio:.2f} across {nb_programs} programs'",
            )
            create_submitted = st.form_submit_button("Create rule", type="primary")

        if create_submitted:
            if not new_id or not new_name or not new_condition or not scenario_file:
                st.error("ID, name, condition and scenario file are required.")
            else:
                payload = {
                    "id": new_id,
                    "name": new_name,
                    "description": new_description,
                    "weight": new_weight,
                    "alert_level": new_alert_level,
                    "condition": new_condition,
                    "evidence_template": new_evidence or "Rule triggered",
                    "enabled": True,
                    "scenario_file": scenario_file if scenario_file.endswith(".yaml")
                                     else f"{scenario_file}.yaml",
                }
                result = _post("/rules", json=payload)
                if result:
                    st.success(f"Rule '{new_id}' created in {payload['scenario_file']}")
                    st.rerun()


# ── Sidebar ───────────────────────────────────────────────────────────────────

def _show_sidebar_scanner() -> None:
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Auto-Scanner**")
    status = _get("/scan/status")
    if status:
        st.sidebar.metric("OpenG2P Beneficiaries", status.get("total_in_openg2p", 0))
        st.sidebar.metric("Scored",                status.get("already_scored",  0))
        pending = status.get("pending", 0)
        if pending > 0:
            st.sidebar.warning(f"{pending} pending")
        else:
            st.sidebar.success("All scored")

    if st.sidebar.button("Scan Now"):
        with st.sidebar:
            with st.spinner("Scanning…"):
                result = _post("/scan/now")
            if result:
                s = result.get("summary", {})
                st.sidebar.success(
                    f"Scored {s.get('scored', 0)} new | "
                    f"CRITICAL: {s.get('CRITICAL', 0)}  HIGH: {s.get('HIGH', 0)}"
                )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="Fraud Detection System",
        layout="wide",
        page_icon="🛡️",
    )

    if not _check_access_token():
        st.error(
            "🔒 Access denied — open this dashboard from OpenG2P (Odoo) as a "
            "Fraud Officer or Fraud Supervisor."
        )
        st.stop()

    _inject_css()

    st.sidebar.markdown(
        '<div class="sb-brand"><div class="logo">🛡️</div>'
        '<div><div class="name">Fraud Detection</div>'
        '<div class="sub">OpenG2P — Fraud Engine v2</div></div></div>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown(
        '<div class="sb-link"><a href="http://localhost:8069/odoo/'
        'action-g2p_fraud_detection.action_fraud_dashboard" target="_blank">'
        "↗ Open in OpenG2P (Odoo)</a></div>",
        unsafe_allow_html=True,
    )

    pages = ["Cases", "Score / Scan / Batch", "Explainability", "Geo Hotspots",
             "Monitoring", "Rules Management"]
    # Respect ?page=… deep-linking from Odoo so cross-app navigation lands on
    # the right tab. Falls back to Cases if the param is missing or invalid.
    _page_aliases = {
        "cases": "Cases", "score": "Score / Scan / Batch",
        "explain": "Explainability", "explainability": "Explainability",
        "geo": "Geo Hotspots", "heatmap": "Geo Hotspots", "hotspots": "Geo Hotspots",
        "monitor": "Monitoring", "monitoring": "Monitoring",
        "rules": "Rules Management", "rules_management": "Rules Management",
    }
    _qp = st.query_params.get("page", "").lower()
    default_idx = pages.index(_page_aliases[_qp]) if _qp in _page_aliases else 0

    page = st.sidebar.radio("Navigation", pages, index=default_idx)
    _show_sidebar_scanner()

    if   page == "Cases":                show_cases_page()
    elif page == "Score / Scan / Batch": show_scoring_page()
    elif page == "Explainability":       show_explainability_page()
    elif page == "Geo Hotspots":         show_geo_page()
    elif page == "Monitoring":           show_monitoring_page()
    elif page == "Rules Management":     show_rules_page()


if __name__ == "__main__":
    main()
