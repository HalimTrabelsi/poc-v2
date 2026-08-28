"""Professional Streamlit dashboard for the Fraud Detection Engine."""
import base64
import hashlib
import hmac
import os
import time
from contextlib import contextmanager

import altair as alt
import pandas as pd
import requests
import streamlit as st

try:
    from dashboard._brand import OPENG2P_LOGO_URI, OPENG2P_ICON_URI
except ImportError:  # when run as `streamlit run dashboard/streamlit_app.py`
    from _brand import OPENG2P_LOGO_URI, OPENG2P_ICON_URI

_engine_url = os.getenv("FRAUD_ENGINE_URL", "http://localhost:8000")
API_BASE = f"{_engine_url.rstrip('/')}/api/v1"
API_KEY = os.getenv("FRAUD_API_KEY", "dev-secret-change-in-prod")
HEADERS = {"X-API-Key": API_KEY}

DASHBOARD_TOKEN_SECRET = os.getenv("DASHBOARD_TOKEN_SECRET", "")


def _check_access_token() -> dict | None:
    """Verify the ?token= minted by Odoo's /fraud/open_dashboard route.

    Identity is delegated to Odoo (only Fraud Officers/Supervisors get a
    token there) since this app has no login of its own — anyone reaching
    this port without a valid, unexpired token is refused.

    Two token layouts are accepted:
      * legacy 3-part  ``user_id:expiry:signature``
      * current 4-part ``user_id:expiry:name_b64:signature`` — carries the
        display name so the dashboard can greet the connected analyst.
    Returns the identity dict on success, else ``None``.
    """
    token = st.query_params.get("token", "")
    if not token:
        return None
    parts = token.split(":")
    if len(parts) == 3:
        user_id, expiry, signature = parts
        payload = f"{user_id}:{expiry}"
        name = ""
    elif len(parts) == 4:
        user_id, expiry, name_b64, signature = parts
        payload = f"{user_id}:{expiry}:{name_b64}"
        try:
            pad = "=" * (-len(name_b64) % 4)
            name = base64.urlsafe_b64decode(name_b64 + pad).decode("utf-8")
        except Exception:
            name = ""
    else:
        return None

    expected = hmac.new(
        DASHBOARD_TOKEN_SECRET.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        if int(expiry) < time.time():
            return None
    except ValueError:
        return None
    return {"user_id": user_id, "name": name or f"Utilisateur #{user_id}"}

STATUS_OPTIONS = ["OPEN", "UNDER_REVIEW", "CLOSED", "FALSE_POSITIVE"]
STATUS_LABELS_FR = {
    "OPEN": "Ouvert", "UNDER_REVIEW": "En révision",
    "CLOSED": "Clôturé", "FALSE_POSITIVE": "Faux positif",
}

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

# French icon + label badges — icon AND text together (never color alone).
RISK_BADGES_FR = {
    "CRITICAL": "🔴 Critique",
    "HIGH":     "🟠 Élevé",
    "MEDIUM":   "🟡 Moyen",
    "LOW":      "🟢 Faible",
}


def risk_badge(level: str) -> str:
    """Icon + French label for a raw risk_level string (never the raw code)."""
    return RISK_BADGES_FR.get(level, f"⚪ {level}")


# Human-readable French labels for every technical column/feature name
# currently displayed anywhere in a table, chart axis, or tooltip.
COLUMN_LABELS = {
    "final_score": "Score de risque",
    "fraud_score": "Score de risque",
    "risk_level": "Niveau de risque",
    "recommendation": "Recommandation",
    "status": "Statut",
    "case_id": "N° de dossier",
    "beneficiary_id": "Bénéficiaire",
    "beneficiary_name": "Bénéficiaire",
    "partner_id": "Bénéficiaire",
    "created_at": "Créé le",
    "payment_gap_ratio": "Taux d'échec de paiement",
    "payment_success_rate": "Taux de réussite des paiements",
    "shared_account_count": "Comptes partagés",
    "shared_phone_count": "Téléphones partagés",
    "nb_programs": "Programmes inscrits",
    "nb_active_programs": "Programmes actifs",
    "income": "Revenu",
    "income_per_person": "Revenu par personne",
    "household_size": "Taille du ménage",
    "network_risk": "Risque réseau",
    "network_risk_score": "Risque réseau",
    "pmt_score": "Score PMT",
    "country_code": "Pays",
    "city": "Ville",
    "region": "Région",
    "lat": "Latitude",
    "lon": "Longitude",
    "cluster_id": "Cluster",
    "count": "Nombre",
    "fraud_count": "Cas à risque",
    "fraud_rate": "Taux de fraude",
    "avg_score": "Score moyen",
    "risk_label": "Niveau",
    "radius_km": "Rayon (km)",
    "weight": "Poids",
    "duplicate_national_id_count": "Doublons de pièce d'identité",
    "age": "Âge",
    "gender": "Genre",
    "dependency_ratio": "Taux de dépendance",
    "high_amount_flag": "Montant anormalement élevé",
    "income_program_inconsistency": "Revenu incohérent avec les programmes",
}

# Feature names as they appear in SHAP output (English snake_case) — French,
# non-technical labels for the chart axis and the detail table.
FEATURE_LABELS = {
    "income_per_person": "Revenu par personne",
    "shared_account_count": "Comptes bancaires partagés",
    "shared_phone_count": "Téléphones partagés",
    "nb_programs": "Nombre de programmes",
    "nb_active_programs": "Programmes actifs",
    "network_risk": "Risque réseau",
    "payment_gap_ratio": "Taux d'échec de paiement",
    "payment_success_rate": "Taux de réussite des paiements",
    "pmt_score": "Score d'éligibilité (PMT)",
    "household_size": "Taille du ménage",
    "income": "Revenu",
    "age": "Âge",
    "dependency_ratio": "Taux de dépendance",
    "high_amount_flag": "Montant anormalement élevé",
    "income_program_inconsistency": "Revenu incohérent avec les programmes",
}
DIRECTION_LABELS_FR = {
    "increases_risk": "Augmente le risque",
    "decreases_risk": "Diminue le risque",
}


def apply_column_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Rename any snake_case columns still present to their French label
    (via COLUMN_LABELS) before any st.dataframe()/st.table() call. Columns
    not in the dict are left as-is (already-friendly headers, e.g. hand-
    built 'Case ID'/'Score', pass through unchanged)."""
    return df.rename(columns=COLUMN_LABELS)

# ── EY brand palette (yellow + black, professional — no neon) ─────────────────
BRAND_YELLOW = "#FFE600"   # EY signature yellow — accents, highlights, hovers
EY_DARK      = "#2E2E38"   # near-black ink — headers, chart marks, hero band
EY_GRAY      = "#F4F4F5"   # page background
EY_BORDER    = "#E2E2E6"   # hairline card borders

ACCENT    = EY_DARK        # chart marks read on white (yellow bars would not)
ACCENT_2  = BRAND_YELLOW   # secondary highlight
INK_MUTED = "#6B6B76"      # muted gray chart chrome / captions on light surface


# EY-inspired professional theme: yellow (#FFE600) + near-black on a clean
# light surface. No neon, no glow — flat cards, hairline borders, and simple
# hover lifts. The OpenG2P logo animates (pulse + spinning yellow ring) during
# loading. Surfaces/text still track Streamlit's theme variables.
_CSS = f"""
<style>
/* ── global ──────────────────────────────────────────────────────────── */
html, body, [class*="css"] {{
  font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
}}
.stApp {{ background-color: {EY_GRAY}; }}
.block-container {{ padding-top: 1.4rem; padding-bottom: 3rem; max-width: 1240px; }}
h1, h2, h3 {{ letter-spacing: -0.01em; color: {EY_DARK}; }}

/* ── shared flat card recipe ─────────────────────────────────────────── */
.kpi, .hero-score, div[data-testid="stForm"], [data-testid="stMetric"] {{
  background: #FFFFFF;
  border: 1px solid {EY_BORDER};
  box-shadow: 0 1px 2px rgba(26,26,36,0.04);
}}

/* ── sidebar ─────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {{
  background: #FFFFFF;
  border-right: 1px solid {EY_BORDER};
}}
[data-testid="stSidebar"] .block-container {{ padding-top: 1.2rem; }}
.sb-brand {{ display:flex; align-items:center; gap:11px; margin-bottom:4px; }}
.sb-brand .logo {{
  width:44px; height:44px; border-radius:11px; flex:0 0 44px;
  background: {EY_DARK}; padding:6px;
  display:flex; align-items:center; justify-content:center;
}}
.sb-brand .logo img {{ width:100%; height:auto; display:block; }}
.sb-brand .name {{ font-weight:800; font-size:15px; line-height:1.15;
                  color:{EY_DARK}; letter-spacing:0.01em; }}
.sb-brand .sub  {{ font-size:11px; color:{INK_MUTED}; letter-spacing:0.07em;
                  text-transform:uppercase; }}
.sb-link a {{
  display:block; font-size:12.5px; text-decoration:none; font-weight:700;
  color:{EY_DARK};
  padding:8px 12px; border:1px solid {EY_BORDER}; border-radius:9px;
  background:#FFFFFF; margin:12px 0 4px 0;
  transition: border-color .18s ease, background .18s ease;
}}
.sb-link a:hover {{ border-color:{BRAND_YELLOW}; background:#FFFDF0; }}

/* connected-user chip (sidebar + landing) */
.user-chip {{
  display:flex; align-items:center; gap:10px;
  padding:9px 12px; border:1px solid {EY_BORDER}; border-radius:11px;
  background:#FFFFFF; margin:6px 0 2px 0;
}}
.user-chip .ava {{
  width:32px; height:32px; border-radius:50%; flex:0 0 32px;
  background:{BRAND_YELLOW}; color:{EY_DARK};
  display:flex; align-items:center; justify-content:center;
  font-weight:800; font-size:14px;
}}
.user-chip .u-name {{ font-weight:700; font-size:13px; color:{EY_DARK}; line-height:1.1; }}
.user-chip .u-role {{ font-size:11px; color:{INK_MUTED};
                     text-transform:uppercase; letter-spacing:0.06em; }}

/* ── metric cards ────────────────────────────────────────────────────── */
[data-testid="stMetric"] {{ border-radius: 13px; padding: 14px 16px; }}

/* ── KPI tiles ───────────────────────────────────────────────────────── */
.kpi-row {{ display:flex; gap:14px; flex-wrap:wrap; margin: 6px 0 20px 0; }}
.kpi {{
  flex:1 1 160px; min-width:150px;
  border-radius:13px; padding:15px 17px 13px 17px;
  border-top:3px solid var(--accent, {BRAND_YELLOW});
  transition: transform .15s ease, box-shadow .15s ease;
}}
.kpi:hover {{ transform: translateY(-2px);
             box-shadow: 0 8px 22px -12px rgba(26,26,36,0.28); }}
.kpi .k-label {{ font-size:11.5px; color:{INK_MUTED}; text-transform:uppercase;
                letter-spacing:0.09em; margin-bottom:5px; }}
.kpi .k-value {{ font-size:28px; font-weight:800; line-height:1.05;
                color:{EY_DARK}; font-variant-numeric: tabular-nums; }}
.kpi .k-sub   {{ font-size:12px; color:{INK_MUTED}; margin-top:5px; }}

/* ── page header ─────────────────────────────────────────────────────── */
.page-header {{ display:flex; align-items:center; gap:15px; margin-bottom: 6px;
               padding-left:14px; border-left:4px solid {BRAND_YELLOW}; }}
.page-header .ph-icon {{
  width:44px; height:44px; border-radius:11px; flex:0 0 44px;
  background:{EY_DARK}; color:{BRAND_YELLOW};
  display:flex; align-items:center; justify-content:center; font-size:21px;
}}
.page-header h1 {{ font-size:26px; margin:0; font-weight:800; color:{EY_DARK}; }}
.page-header p  {{ font-size:13px; color:{INK_MUTED}; margin:3px 0 0 0;
                  letter-spacing:0.02em; }}

/* ── risk pill ───────────────────────────────────────────────────────── */
.pill {{ display:inline-flex; align-items:center; gap:7px; padding:4px 13px;
        border-radius:999px; font-size:13px; font-weight:700;
        border:1px solid currentColor; }}
.pill .dot {{ width:8px; height:8px; border-radius:50%; }}

/* ── hero score ──────────────────────────────────────────────────────── */
.hero-score {{
  display:flex; align-items:center; gap:24px;
  border-radius:16px; padding:20px 24px; margin:8px 0 18px 0;
}}
.hero-score .ring {{
  width:98px; height:98px; border-radius:50%; flex:0 0 98px;
  display:flex; align-items:center; justify-content:center;
  font-size:22px; font-weight:800; color:{EY_DARK};
  font-variant-numeric: tabular-nums;
  background:
    radial-gradient(closest-side, #FFFFFF 76%, transparent 77% 100%),
    conic-gradient(var(--ring, {EY_DARK}) calc(var(--pct)*1%), {EY_BORDER} 0);
}}
.hero-score .h-title {{ font-size:12px; color:{INK_MUTED}; text-transform:uppercase;
                       letter-spacing:0.10em; }}
.hero-score .h-rec   {{ font-size:14px; margin-top:7px; color:{EY_DARK}; }}

/* ── buttons: EY yellow with black text, simple hover ────────────────── */
.stButton > button {{ border-radius: 9px; font-weight:700;
                     border:1px solid {EY_BORDER}; color:{EY_DARK};
                     background:#FFFFFF;
                     transition: border-color .18s ease, background .18s ease,
                                 transform .1s ease; }}
.stButton > button:hover {{ border-color:{BRAND_YELLOW}; background:#FFFDF0;
                           color:{EY_DARK}; }}
.stButton > button[kind="primary"] {{
  background:{BRAND_YELLOW}; border:1px solid {BRAND_YELLOW}; color:{EY_DARK};
}}
.stButton > button[kind="primary"]:hover {{
  background:#FFEF4D; border-color:#FFEF4D; transform: translateY(-1px);
}}
.stDownloadButton > button {{ border-radius:9px; font-weight:700; }}

/* ── tabs / inputs / tables ──────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {{ gap: 6px; }}
.stTabs [data-baseweb="tab"] {{
  border-radius: 9px 9px 0 0; padding: 8px 18px; font-weight:700;
  color:{INK_MUTED};
}}
.stTabs [aria-selected="true"] {{ color:{EY_DARK};
  border-bottom:3px solid {BRAND_YELLOW}; background:#FFFFFF; }}
div[data-testid="stForm"] {{ border-radius:14px; padding:20px; }}
[data-testid="stDataFrame"] {{ border-radius: 12px; overflow:hidden;
  border: 1px solid {EY_BORDER}; }}
[data-testid="stExpander"] {{ border-radius: 12px; border:1px solid {EY_BORDER}; }}

/* ── landing page ────────────────────────────────────────────────────── */
.ey-hero {{
  display:flex; align-items:center; gap:26px;
  background:{EY_DARK}; border-radius:18px; padding:30px 34px;
  margin: 2px 0 22px 0; position:relative; overflow:hidden;
}}
.ey-hero::after {{
  content:""; position:absolute; right:-40px; top:-40px;
  width:220px; height:220px; border-radius:50%;
  background:{BRAND_YELLOW}; opacity:0.10;
}}
.ey-hero .hero-logo {{ width:112px; flex:0 0 112px; z-index:1;
  background:#FFFFFF; border-radius:16px; padding:12px; }}
.ey-hero .hero-logo img {{ width:100%; display:block; }}
.ey-hero .h-eyebrow {{ color:{BRAND_YELLOW}; font-size:12px; font-weight:700;
  letter-spacing:0.16em; text-transform:uppercase; margin-bottom:6px; }}
.ey-hero h1 {{ color:#FFFFFF; font-size:30px; font-weight:800; margin:0 0 6px 0; }}
.ey-hero p  {{ color:#C9C9D2; font-size:14px; margin:0; max-width:560px; }}
.ey-hero .hero-user {{ margin-top:14px; display:inline-flex; align-items:center;
  gap:10px; background:rgba(255,255,255,0.08); border:1px solid rgba(255,230,0,0.35);
  padding:7px 13px; border-radius:999px; }}
.ey-hero .hero-user .ava {{ width:26px; height:26px; border-radius:50%;
  background:{BRAND_YELLOW}; color:{EY_DARK}; font-weight:800; font-size:12px;
  display:flex; align-items:center; justify-content:center; }}
.ey-hero .hero-user .txt {{ color:#FFFFFF; font-size:13px; font-weight:600; }}
.ey-hero .hero-user .txt small {{ color:{BRAND_YELLOW}; display:block;
  font-size:10px; letter-spacing:0.08em; text-transform:uppercase; }}

.mod-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(240px,1fr));
  gap:16px; margin-top:6px; }}
.mod-card {{ background:#FFFFFF; border:1px solid {EY_BORDER}; border-radius:14px;
  padding:20px; height:100%; transition: border-color .18s ease,
  transform .12s ease, box-shadow .18s ease; }}
.mod-card:hover {{ border-color:{BRAND_YELLOW}; transform:translateY(-3px);
  box-shadow:0 12px 26px -16px rgba(26,26,36,0.32); }}
.mod-card .m-ico {{ width:44px; height:44px; border-radius:11px;
  background:{EY_GRAY}; display:flex; align-items:center; justify-content:center;
  font-size:22px; margin-bottom:12px; }}
.mod-card:hover .m-ico {{ background:{BRAND_YELLOW}; }}
.mod-card .m-title {{ font-weight:800; font-size:15px; color:{EY_DARK}; margin-bottom:4px; }}
.mod-card .m-desc {{ font-size:12.5px; color:{INK_MUTED}; line-height:1.45; }}

/* ── animated-logo loading overlay ───────────────────────────────────── */
.ey-load {{ display:flex; flex-direction:column; align-items:center;
  justify-content:center; gap:16px; padding:44px 0; }}
.ey-load .spin {{ position:relative; width:104px; height:104px; }}
.ey-load .spin .track {{ position:absolute; inset:0; border-radius:50%;
  background: conic-gradient({BRAND_YELLOW} 0 25%, {EY_BORDER} 0 100%);
  -webkit-mask: radial-gradient(closest-side, transparent 72%, #000 73%);
          mask: radial-gradient(closest-side, transparent 72%, #000 73%);
  animation: ey-spin 1s linear infinite; }}
.ey-load .spin .mark {{ position:absolute; inset:16px; border-radius:14px;
  background:#FFFFFF; padding:9px; display:flex; align-items:center;
  justify-content:center; box-shadow:0 2px 8px rgba(26,26,36,0.10);
  animation: ey-pulse 1.4s ease-in-out infinite; }}
.ey-load .spin .mark img {{ width:100%; display:block; }}
.ey-load .msg {{ font-size:14px; font-weight:700; color:{EY_DARK}; }}
.ey-load .msg small {{ display:block; text-align:center; font-weight:500;
  color:{INK_MUTED}; font-size:12px; margin-top:2px; }}
@keyframes ey-spin {{ to {{ transform: rotate(360deg); }} }}
@keyframes ey-pulse {{ 0%,100% {{ transform: scale(1); opacity:1; }}
  50% {{ transform: scale(0.93); opacity:0.85; }} }}
</style>
"""


def _inject_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def _initials(name: str) -> str:
    parts = [p for p in name.replace("#", " ").split() if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def user_chip(sidebar: bool = True) -> None:
    """Render the connected authorized user as an avatar + name chip."""
    user = st.session_state.get("auth_user") or {}
    name = user.get("name", "Analyste")
    target = st.sidebar if sidebar else st
    target.markdown(
        f'<div class="user-chip"><div class="ava">{_initials(name)}</div>'
        f'<div><div class="u-name">{name}</div>'
        f'<div class="u-role">Analyste fraude · connecté</div></div></div>',
        unsafe_allow_html=True,
    )


@contextmanager
def logo_loading(message: str, sub: str = ""):
    """Full-width overlay showing the animated OpenG2P logo while a task runs.

    Replaces st.spinner for the heavy operations the user watches (scoring,
    heatmap, database scan) so every wait is branded and obviously "working".
    """
    ph = st.empty()
    sub_html = f"<small>{sub}</small>" if sub else ""
    ph.markdown(
        f'<div class="ey-load"><div class="spin">'
        f'<div class="track"></div>'
        f'<div class="mark"><img src="{OPENG2P_LOGO_URI}" alt="OpenG2P"/></div>'
        f'</div><div class="msg">{message}{sub_html}</div></div>',
        unsafe_allow_html=True,
    )
    try:
        yield
    finally:
        ph.empty()


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
        f'<div class="kpi" style="--accent:{it.get("accent", BRAND_YELLOW)}">'
        f'<div class="k-label">{it["label"]}</div>'
        f'<div class="k-value">{it["value"]}</div>'
        f'<div class="k-sub">{it.get("sub", "")}</div></div>'
        for it in items
    )
    st.markdown(f'<div class="kpi-row">{tiles}</div>', unsafe_allow_html=True)


def risk_pill(level: str) -> str:
    """Colored pill with icon + French label — never the raw risk code alone."""
    s = RISK_STATUS.get(level, RISK_STATUS["LOW"])
    label = RISK_BADGES_FR.get(level, level)
    return (
        f'<span class="pill" style="background:{s["color"]}1a;color:{s["color"]};">'
        f'<span class="dot" style="background:{s["color"]}"></span>{label}</span>'
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
        raw_name = f.get("feature", "?")
        rows.append({"feature": FEATURE_LABELS.get(raw_name, raw_name), "value": value,
                     "shap": _shap_value(f)})
    if not rows:
        return

    st.subheader(title)
    rows.sort(key=lambda r: abs(r["shap"]), reverse=True)
    chart_df = pd.DataFrame(rows)
    chart_df["direction"] = chart_df["shap"].map(
        lambda v: "Augmente le risque" if v > 0 else "Diminue le risque")

    # Diverging encoding: red = increases risk, blue = decreases risk.
    base = alt.Chart(chart_df).encode(
        y=alt.Y("feature:N", sort=None, axis=_axis(""),
                scale=alt.Scale(paddingInner=0.45)),
        x=alt.X("shap:Q", axis=_axis("Impact sur le score de risque")),
        color=alt.Color(
            "direction:N",
            scale=alt.Scale(domain=["Augmente le risque", "Diminue le risque"],
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
        "Direction":   DIRECTION_LABELS_FR["increases_risk"] if r["shap"] > 0 else DIRECTION_LABELS_FR["decreases_risk"],
    } for r in rows]), use_container_width=True, hide_index=True)


# ── API helpers ───────────────────────────────────────────────────────────────

def _friendly_api_error(exc: Exception, doing: str = "cette opération") -> str:
    """Translate a raw requests exception into a message a fraud officer can
    act on, instead of leaking connection internals / full URLs.

    doing: a short French noun phrase describing what failed, e.g.
    "le chargement des dossiers" — inserted into "Une erreur est survenue
    pendant {doing}."
    """
    if isinstance(exc, requests.Timeout):
        return (f"Le moteur anti-fraude a mis trop de temps à répondre pendant {doing}. "
                f"Il est peut-être occupé — réessayez dans un instant.")
    if isinstance(exc, requests.ConnectionError):
        return ("Impossible de joindre le moteur anti-fraude actuellement. "
                "Vérifiez que le service est démarré, ou réessayez sous peu.")
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        code = exc.response.status_code
        if code in (401, 403):
            return ("Votre session a expiré. Rouvrez ce tableau de bord "
                     "depuis OpenG2P (Odoo) pour obtenir un lien valide.")
        if code == 404:
            return "Cet enregistrement est introuvable — il a peut-être été supprimé."
        if code >= 500:
            return (f"Le moteur anti-fraude a rencontré une erreur interne pendant {doing}. "
                     f"Si cela persiste, contactez un administrateur.")
    return f"Une erreur est survenue pendant {doing}. Veuillez réessayer."


def _get(path, params=None):
    try:
        r = requests.get(f"{API_BASE}{path}", headers=HEADERS, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(_friendly_api_error(exc, f"le chargement de {path}"))
        return None


def _post(path, json=None, params=None):
    try:
        r = requests.post(f"{API_BASE}{path}", headers=HEADERS, json=json, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(_friendly_api_error(exc, f"l'envoi vers {path}"))
        return None


@st.cache_data(ttl=300, show_spinner=False)
def _beneficiary_info_map() -> dict:
    """partner_id (str) -> {name, age, phone, address}, for display only.

    Cases/heatmap data only carry beneficiary_id/partner_id; /beneficiaries
    already returns real name/age/phone/address (wired in earlier tasks) so
    this avoids ever showing a raw ID or 'BEN-XXXXX' code in the UI, without
    touching any backend/scoring endpoint.
    """
    bene_list = _get("/beneficiaries", params={"limit": 5000}) or []
    return {
        str(b.get("partner_id")): {
            "name": (b.get("name") or "").strip(),
            "age": b.get("age"),
            "phone": (b.get("phone") or "").strip(),
            "address": (b.get("address") or "").strip(),
        }
        for b in bene_list if b
    }


def beneficiary_display_name(beneficiary_id) -> str:
    """Real name if known, else 'Beneficiary #N' — never a raw DB integer
    or a placeholder code shown as if it were meaningful."""
    name = _beneficiary_info_map().get(str(beneficiary_id), {}).get("name", "")
    return name if name else f"Beneficiary #{beneficiary_id}"


def beneficiary_age(beneficiary_id):
    """Real age if known, else None (rendered as an empty cell, not 0)."""
    return _beneficiary_info_map().get(str(beneficiary_id), {}).get("age")


def beneficiary_phone(beneficiary_id) -> str:
    """Real phone if known, else '—' (never a raw ID standing in for it)."""
    phone = _beneficiary_info_map().get(str(beneficiary_id), {}).get("phone", "")
    return phone if phone else "—"


def beneficiary_address(beneficiary_id) -> str:
    """Real street address if known, else '—'."""
    address = _beneficiary_info_map().get(str(beneficiary_id), {}).get("address", "")
    return address if address else "—"


def _patch(path, json):
    try:
        r = requests.patch(f"{API_BASE}{path}", headers=HEADERS, json=json, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(_friendly_api_error(exc, f"la mise à jour de {path}"))
        return None


def _delete(path):
    try:
        r = requests.delete(f"{API_BASE}{path}", headers=HEADERS, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(_friendly_api_error(exc, f"la suppression de {path}"))
        return None


# ── Country selector (deployment-country calibration for scans) ───────────────

# Small hardcoded fallback if the World Bank country-list endpoint is
# unreachable — enough to keep the selector usable offline/in a demo.
_FALLBACK_COUNTRIES = [
    ("TN", "Tunisia"), ("SN", "Senegal"), ("GN", "Guinea"),
    ("ML", "Mali"), ("CI", "Côte d'Ivoire"), ("MA", "Morocco"),
    ("NG", "Nigeria"), ("KE", "Kenya"),
]

# International calling codes by ISO2. Not exhaustive — covers the
# countries relevant to this deployment plus common demo/report targets.
# Unlisted countries just show the flag + name with no dial code.
_CALLING_CODES = {
    "TN": "+216", "SN": "+221", "GN": "+224", "ML": "+223", "CI": "+225",
    "MA": "+212", "NG": "+234", "KE": "+254", "DZ": "+213", "EG": "+20",
    "GH": "+233", "CM": "+237", "BF": "+226", "NE": "+227", "TD": "+235",
    "TG": "+228", "BJ": "+229", "MR": "+222", "LY": "+218", "SD": "+249",
    "ET": "+251", "TZ": "+255", "UG": "+256", "ZA": "+27", "FR": "+33",
    "US": "+1", "GB": "+44", "DE": "+49", "ES": "+34", "IT": "+39",
    "CA": "+1", "BE": "+32", "CH": "+41", "PT": "+351",
}


def _flag_emoji(iso2_code: str) -> str:
    """Render a flag from an ISO2 code via Unicode regional-indicator symbols.

    No lookup table needed: each letter A-Z maps to U+1F1E6.._1F1FF in
    order, so e.g. "TN" -> 🇹 + 🇳 -> 🇹🇳. Falls back to the bare code if
    it isn't two ASCII letters.
    """
    code = (iso2_code or "").strip().upper()
    if len(code) != 2 or not code.isalpha():
        return "🏳️"
    return "".join(chr(0x1F1E6 + ord(ch) - ord("A")) for ch in code)


def _country_label(code: str, name: str) -> str:
    dial = _CALLING_CODES.get(code)
    dial_part = f" {dial}" if dial else ""
    return f"{_flag_emoji(code)} {name} ({code}){dial_part}"


@st.cache_data(ttl=24 * 3600, show_spinner=False)
def _fetch_country_list() -> tuple[list[tuple[str, str]], bool]:
    """Return [(iso2_code, name), ...] from the World Bank country list.

    Cached for 24h in the Streamlit session. Falls back to a small
    hardcoded list (use_fallback=True) if the World Bank API is
    unreachable, so the selector never breaks the dashboard.
    """
    try:
        r = requests.get(
            "https://api.worldbank.org/v2/country",
            params={"format": "json", "per_page": 400},
            timeout=8,
        )
        r.raise_for_status()
        payload = r.json()
        rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
        countries = [
            # NB: row["id"] is the ISO3 code (e.g. "TUN") — the country-
            # profile backend and default_country_code ("TN") use ISO2, so
            # we must read row["iso2Code"] here, not row["id"].
            (row["iso2Code"], row["name"])
            for row in rows
            # World Bank's country list includes aggregate regions (e.g.
            # "Arab World") whose region.value is "Aggregates" — skip those,
            # keep only real countries.
            if row.get("region", {}).get("value") not in (None, "Aggregates")
            and row.get("iso2Code")
        ]
        countries.sort(key=lambda c: c[1])
        if countries:
            return countries, False
        return _FALLBACK_COUNTRIES, True
    except Exception:
        return _FALLBACK_COUNTRIES, True


@st.cache_data(ttl=24 * 3600, show_spinner=False)
def _fetch_country_profile_preview(country_code: str) -> dict | None:
    """Preview a country's economic profile via the fraud-engine API."""
    try:
        r = requests.get(
            f"{API_BASE}/country-profile/{country_code}", headers=HEADERS, timeout=10
        )
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return None


def render_country_selector() -> str:
    """Render the deployment-country selector shown before a scan.

    Returns the selected ISO-2 country_code (stored in session_state so it
    persists across the scoring page and the sidebar 'Scan Now' button).
    """
    countries, list_is_fallback = _fetch_country_list()
    labels = [_country_label(code, name) for code, name in countries]
    codes = [code for code, _ in countries]

    default_code = st.session_state.get("scan_country_code", "TN")
    default_idx = codes.index(default_code) if default_code in codes else 0

    st.markdown("**Pays de déploiement** — calibre les règles revenu/pauvreté sur les données économiques locales")
    selected_label = st.selectbox(
        "Pays de déploiement", labels, index=default_idx,
        label_visibility="collapsed", key="country_selectbox",
    )
    country_code = codes[labels.index(selected_label)]
    st.session_state["scan_country_code"] = country_code

    if list_is_fallback:
        st.caption("⚠️ Liste des pays : repli intégré (liste Banque mondiale injoignable)")

    profile = _fetch_country_profile_preview(country_code)
    if profile:
        if profile.get("use_fallback"):
            st.warning(
                f"⚠️ Données de repli — données Banque mondiale indisponibles pour {country_code}. "
                f"Valeurs de référence neutres (seuil de pauvreté ≈ ${profile.get('poverty_line', 0):.0f}/mois)."
            )
        else:
            st.success(
                f"✅ Données Banque mondiale en direct — revenu médian ≈ ${profile.get('median_income', 0):.0f}/mois, "
                f"seuil de pauvreté ≈ ${profile.get('poverty_line', 0):.0f}/mois"
            )
    else:
        st.caption("Aperçu du profil pays indisponible (moteur injoignable).")

    return country_code


def _fetch_country_stats() -> list[dict]:
    return _get("/stats/by-country") or []


def render_country_stats_sidebar() -> None:
    """Sidebar panel: how many scans were calibrated under each country.

    NB: this is NOT "where beneficiaries live" — this OpenG2P registry has
    no populated country/region field on beneficiaries (confirmed: 0 of
    20,042 registrants have country_id or area_id set). It's how many
    scans were run under each deployment-country calibration (see
    app/core/country_reference.py) — a scan setting, not beneficiary
    demographics. Reads /v1/stats/by-country (grouped from
    fraud_cases.country_code).
    """
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Scans par pays de calibrage**")
    st.sidebar.caption("Le pays dont les données économiques ont calibré chaque scan (pas le pays de résidence des bénéficiaires — ce registre n'a pas cette donnée).")
    stats = _fetch_country_stats()
    if not stats:
        st.sidebar.caption("Aucun scan enregistré pour l'instant.")
        return

    countries, _ = _fetch_country_list()
    name_by_code = {code: name for code, name in countries}

    for row in stats:
        code = row.get("country_code", "UNKNOWN")
        name = name_by_code.get(code, code)
        label = _country_label(code, name) if code != "UNKNOWN" else "❔ Unknown / pre-feature scans"
        total = row.get("total", 0)
        critical = row.get("critical", 0) or 0
        high = row.get("high", 0) or 0
        with st.sidebar.container():
            st.markdown(f"**{label}**")
            st.caption(
                f"{total} scanned · 🔴 {critical} CRITICAL · 🟠 {high} HIGH"
            )


# ── Cases page ────────────────────────────────────────────────────────────────

def show_cases_page() -> None:
    page_header("Dossiers de fraude", "Examiner, trier et clôturer les cas détectés", "🗂️")

    col1, col2, col3 = st.columns(3)
    risk_filter   = col1.selectbox("Niveau de risque", ["Tous", "LOW", "MEDIUM", "HIGH", "CRITICAL"],
                                   format_func=lambda v: risk_badge(v) if v != "Tous" else v)
    status_filter = col2.selectbox("Statut", ["Tous"] + STATUS_OPTIONS,
                                    format_func=lambda v: STATUS_LABELS_FR.get(v, v))
    limit         = col3.slider("Nombre max de lignes", 10, 500, 50)

    params: dict = {"limit": limit}
    if risk_filter   != "Tous": params["risk_level"] = risk_filter
    if status_filter != "Tous": params["status"]     = status_filter

    with st.spinner("Chargement des dossiers…"):
        data = _get("/cases", params=params)
    if data is None:
        return

    cases: list[dict] = data.get("cases", [])

    n_crit = sum(1 for c in cases if c.get("risk_level") == "CRITICAL")
    n_high = sum(1 for c in cases if c.get("risk_level") == "HIGH")
    n_open = sum(1 for c in cases if c.get("status") == "OPEN")
    kpi_row([
        {"label": "Dossiers trouvés", "value": data.get("total", len(cases)),
         "sub": "selon les filtres actifs"},
        {"label": "Critiques", "value": n_crit,
         "accent": RISK_STATUS["CRITICAL"]["color"], "sub": "paiement bloqué"},
        {"label": "Élevés", "value": n_high,
         "accent": RISK_STATUS["HIGH"]["color"], "sub": "revue manuelle"},
        {"label": "Ouverts", "value": n_open, "sub": "en attente de tri"},
    ])

    if not cases:
        st.info("Aucun dossier ne correspond aux filtres sélectionnés.")
        return

    rows = []
    for c in cases:
        rl = c.get("risk_level", "LOW")
        bid = c.get("beneficiary_id")
        rows.append({
            "N° Dossier":     c.get("case_id", "")[:8] + "…",
            "Bénéficiaire":   beneficiary_display_name(bid),
            "Âge":            beneficiary_age(bid),
            "Téléphone":      beneficiary_phone(bid),
            "Adresse":        beneficiary_address(bid),
            "Score":          float(c.get("final_score", 0) or 0),
            "Risque":         risk_badge(rl),
            "Recommandation": c.get("recommendation"),
            "Statut":         STATUS_LABELS_FR.get(c.get("status"), c.get("status")),
            "Créé le":        c.get("created_at", "")[:10],
            "_case_id":       c.get("case_id"),
        })

    df = pd.DataFrame(rows)
    event = st.dataframe(
        df.drop(columns=["_case_id"]),
        use_container_width=True, hide_index=True,
        on_select="rerun", selection_mode="single-row",
        key="cases_table",  # stable key so the row selection survives a
                            # st.rerun() after an action instead of resetting
        column_config={
            "Score": st.column_config.ProgressColumn(
                "Score", format="percent", min_value=0.0, max_value=1.0),
        },
    )

    # ── case actions ─────────────────────────────────────────────────────────
    st.subheader("Actions sur le dossier")
    case_ids = [r["_case_id"] for r in rows]
    picked_rows = event.selection.rows if event and event.selection else []
    if picked_rows:
        # Row click drives the selection — no more matching two differently
        # truncated IDs by eye between the table and a separate dropdown.
        selected_id = case_ids[picked_rows[0]]
        selected_row = rows[picked_rows[0]]
        st.caption(f"Sélectionné : **{selected_row['Bénéficiaire']}** — {selected_row['N° Dossier']}")
    else:
        selected_id = None
        st.info("Cliquez sur une ligne ci-dessus pour sélectionner un dossier.")

    action_tab, feedback_tab = st.tabs(["Changer le statut", "Soumettre un verdict"])
    no_selection = selected_id is None

    with action_tab:
        new_status = st.selectbox("Nouveau statut", STATUS_OPTIONS,
                                   format_func=lambda v: STATUS_LABELS_FR.get(v, v))
        notes      = st.text_area("Notes de l'agent", key="status_notes")
        if st.button("Mettre à jour le statut", disabled=no_selection):
            result = _patch(f"/cases/{selected_id}/status",
                            {"status": new_status, "notes": notes})
            if result:
                st.success(f"Dossier mis à jour : {STATUS_LABELS_FR.get(new_status, new_status)}")
                st.rerun()

    with feedback_tab:
        st.markdown(
            "Soumettez un verdict d'enquêteur pour entraîner le modèle. "
            "Les verdicts confirmés alimentent le **réentraînement hebdomadaire XGBoost**."
        )
        verdict = st.radio(
            "Verdict",
            ["confirmed_fraud", "false_positive", "uncertain"],
            horizontal=True,
            format_func=lambda v: {
                "confirmed_fraud": "Confirmer la fraude",
                "false_positive":  "Faux positif",
                "uncertain":       "Incertain",
            }[v],
        )
        investigator = st.text_input("Nom / ID de l'enquêteur", value="investigator")
        fb_notes     = st.text_area("Notes (optionnel)", key="feedback_notes")
        if st.button("Soumettre le verdict", type="primary", disabled=no_selection):
            result = _post(f"/cases/{selected_id}/feedback", {
                "verdict": verdict, "notes": fb_notes, "investigator": investigator,
            })
            if result:
                st.success(f"Verdict « {verdict} » enregistré — feedback_id : {result.get('feedback_id')}")

    # ── PDF download ──────────────────────────────────────────────────────────
    # Fetched bytes are cached in session_state so the download button
    # persists across reruns instead of vanishing the moment any other widget
    # on the page triggers a rerun (previously it would silently disappear,
    # reading as "nothing happened" on the first click).
    st.subheader("Télécharger le rapport")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Préparer le rapport PDF", disabled=no_selection):
            with st.spinner("Génération du PDF…"):
                try:
                    r = requests.get(
                        f"{API_BASE}/cases/{selected_id}/report/pdf",
                        headers=HEADERS, timeout=30
                    )
                    r.raise_for_status()
                    st.session_state["_pdf_bytes"] = r.content
                    st.session_state["_pdf_case_id"] = selected_id
                except requests.RequestException as exc:
                    st.session_state.pop("_pdf_bytes", None)
                    st.error(_friendly_api_error(exc, "la génération du PDF"))
        if st.session_state.get("_pdf_bytes") and st.session_state.get("_pdf_case_id") == selected_id:
            st.download_button(
                label="⬇ Enregistrer le PDF",
                data=st.session_state["_pdf_bytes"],
                file_name=f"fraud_case_{selected_id[:8]}.pdf",
                mime="application/pdf",
            )
    with c2:
        if st.button("Préparer l'export CSV"):
            with st.spinner("Export de tous les dossiers…"):
                try:
                    r = requests.get(
                        f"{API_BASE}/cases/export/csv",
                        headers=HEADERS, params={"limit": 5000}, timeout=30
                    )
                    r.raise_for_status()
                    st.session_state["_csv_bytes"] = r.content
                except requests.RequestException as exc:
                    st.session_state.pop("_csv_bytes", None)
                    st.error(_friendly_api_error(exc, "l'export CSV"))
        if st.session_state.get("_csv_bytes"):
            st.download_button(
                label="⬇ Enregistrer le CSV",
                data=st.session_state["_csv_bytes"],
                file_name="fraud_cases_export.csv",
                mime="text/csv",
            )


# ── Score / Scan page ─────────────────────────────────────────────────────────

def _render_decision(result: dict) -> None:
    rl    = result.get("risk_level", "LOW")
    score = float(result.get("final_score", 0) or 0)
    s     = RISK_STATUS.get(rl, RISK_STATUS["LOW"])

    st.markdown(
        f'<div class="hero-score" style="--pct:{score * 100:.1f};--ring:{s["color"]}">'
        f'<div class="ring">'
        f"{score:.0%}</div>"
        f'<div><div class="h-title">Score de risque final</div>'
        f'<div style="margin:6px 0">{risk_pill(rl)}</div>'
        f'<div class="h-rec">Recommandation — '
        f'<b>{result.get("recommendation", "")}</b></div></div></div>',
        unsafe_allow_html=True,
    )

    st.info(result.get("explanation", "Aucune explication disponible."))

    if result.get("rules_triggered"):
        st.subheader("Règles déclenchées")
        st.dataframe(pd.DataFrame([{
            "Règle":       r.get("rule_id"),
            "Nom":         r.get("name"),
            "Poids":       r.get("weight"),
            "Explication": r.get("explanation"),
        } for r in result["rules_triggered"]]),
            use_container_width=True, hide_index=True)

    render_shap(result.get("top_features") or [])

    st.caption(
        f"Traité en {result.get('processing_ms', 0):.0f} ms | "
        f"N° Dossier : {result.get('case_id', 'N/A')}"
    )


def render_threshold_comparison(selected_code: str) -> None:
    """Show the SE002/SE003 income-underreporting thresholds side by side
    for a few reference countries, so the calibration effect from
    app.core.country_reference / socio_economic.yaml is visible BEFORE
    running a scan, not just discoverable after the fact.
    """
    countries, _ = _fetch_country_list()
    name_by_code = {code: name for code, name in countries}

    # Always compare the selected country against 2 fixed references, so
    # the effect is visible regardless of what's currently selected.
    compare_codes = list(dict.fromkeys([selected_code, "TN", "SN"]))[:3]

    st.caption("Seuil de sous-déclaration de revenu (SE002/SE003) par pays — "
               "le même revenu déclaré peut être « sous le seuil de pauvreté » dans un pays et normal dans un autre :")
    cols = st.columns(len(compare_codes))
    for col, code in zip(cols, compare_codes):
        profile = _fetch_country_profile_preview(code)
        with col:
            name = name_by_code.get(code, code)
            st.markdown(f"{_flag_emoji(code)} **{name}**" + (" (sélectionné)" if code == selected_code else ""))
            if profile:
                poverty_line = profile.get("poverty_line", 0)
                st.metric("Seuil SE002 (50%)", f"${poverty_line * 0.5:.0f}/mois")
                st.caption(f"Seuil SE003 (75%) : ${poverty_line * 0.75:.0f}/mois")
                if profile.get("use_fallback"):
                    st.caption("⚠️ données de repli")
            else:
                st.caption("indisponible")


def show_scoring_page() -> None:
    page_header("Scorer un bénéficiaire",
                "Exécuter le pipeline hybride — règles, ML, anomalie et graphe", "⚡")

    with st.container():
        country_code = render_country_selector()

    # Always-visible calibration note — which country's poverty line decides
    # what "underreported income" even means is the single biggest factor
    # in whether a score reads as CRITICAL or normal, so it must not be
    # something a user can miss by leaving a collapsed expander closed.
    active_profile = _fetch_country_profile_preview(country_code)
    if active_profile:
        poverty_line = active_profile.get("poverty_line", 0)
        st.info(
            f"📊 Calibré pour **{country_code}** — un revenu sous "
            f"**${poverty_line * 0.5:.0f}/mois** est signalé comme sous-déclaré (SE002), "
            f"sous **${poverty_line * 0.75:.0f}/mois** comme limite (SE003). "
            f"Changez de pays ci-dessus pour modifier ce calibrage."
        )
    with st.expander("Comparer les seuils avec d'autres pays", expanded=False):
        render_threshold_comparison(country_code)
    st.divider()

    tab_single, tab_batch, tab_csv = st.tabs(["Bénéficiaire unique", "Scanner tout OpenG2P", "Import CSV en lot"])

    with tab_single:
        bene_list = _get("/beneficiaries", params={"limit": 5000}) or []
        # name -> partner_id; disambiguate duplicate names with the ID suffix
        options = {
            f"{(b.get('name') or 'Sans nom').strip()} (#{b.get('partner_id')})": str(b.get("partner_id"))
            for b in bene_list if b
        }
        if options:
            choice = st.selectbox(
                "Nom du bénéficiaire", sorted(options.keys()),
                placeholder="Rechercher par nom…", index=None,
            )
            bid = options.get(choice, "") if choice else ""
        else:
            st.caption("Impossible de charger les noms — saisie manuelle de l'ID.")
            bid = st.text_input("ID bénéficiaire (partner_id)", placeholder="ex. 42")

        if st.button("Lancer le score de fraude", type="primary"):
            if not bid.strip():
                st.warning("Veuillez sélectionner un bénéficiaire.")
                return
            with logo_loading("Scoring du bénéficiaire…",
                              f"Pipeline hybride en cours · ID {bid}"):
                result = _post(f"/score/beneficiary/{bid.strip()}", params={"country_code": country_code})
            if result:
                _render_decision(result)

    with tab_batch:
        limit = st.number_input("Limite (0 = tous)", 0, 10000, 100, 10)
        st.caption(
            "Utilise le même point d'accès parallèle que l'onglet CSV "
            "(jusqu'à 8 workers côté serveur) — bien plus rapide qu'un "
            "score bénéficiaire par bénéficiaire pour de gros volumes."
        )
        if st.button("Scanner tous les bénéficiaires", type="primary"):
            with st.spinner("Récupération de la liste des bénéficiaires…"):
                params = {"limit": int(limit)} if limit > 0 else {}
                bene_list = _get("/beneficiaries", params=params or None)

            if not bene_list:
                st.warning("Aucun bénéficiaire retourné.")
                return

            ids = [str(b.get("partner_id") or b.get("beneficiary_id")) for b in bene_list if b]
            csv_bytes = ("beneficiary_id\n" + "\n".join(ids)).encode("utf-8")
            with logo_loading("Scan de la base OpenG2P…",
                              f"Scoring de {len(ids)} bénéficiaires en parallèle"):
                try:
                    r = requests.post(
                        f"{API_BASE}/score/batch",
                        headers=HEADERS,
                        files={"file": ("scan_all.csv", csv_bytes, "text/csv")},
                        params={"country_code": country_code},
                        timeout=300,
                    )
                    r.raise_for_status()
                    st.session_state["_scan_all_csv"] = r.content
                    st.success(f"{len(ids)} bénéficiaires scorés.")
                except requests.RequestException as exc:
                    st.session_state.pop("_scan_all_csv", None)
                    st.error(_friendly_api_error(exc, "le scan de tous les bénéficiaires"))

        if st.session_state.get("_scan_all_csv") is not None:
            import io
            preview_df = pd.read_csv(io.BytesIO(st.session_state["_scan_all_csv"]))
            st.dataframe(preview_df, use_container_width=True, hide_index=True)
            st.download_button(
                label="⬇ Télécharger les résultats (CSV)",
                data=st.session_state["_scan_all_csv"],
                file_name="scan_all_scores.csv",
                mime="text/csv",
            )

    with tab_csv:
        st.markdown(
            "Importez un fichier CSV avec une colonne **`beneficiary_id`**. "
            "Tous les bénéficiaires listés sont scorés en parallèle, résultats retournés en CSV."
        )
        uploaded = st.file_uploader("Choisir un fichier CSV", type=["csv"])
        if uploaded and st.button("Scorer le lot", type="primary"):
            with logo_loading("Scoring du lot…", f"Traitement de {uploaded.name}"):
                try:
                    r = requests.post(
                        f"{API_BASE}/score/batch",
                        headers=HEADERS,
                        files={"file": (uploaded.name, uploaded.getvalue(), "text/csv")},
                        params={"country_code": country_code},
                        timeout=300,
                    )
                    r.raise_for_status()
                    st.session_state["_batch_csv_bytes"] = r.content
                    st.success("Scoring en lot terminé !")
                except requests.RequestException as exc:
                    st.session_state.pop("_batch_csv_bytes", None)
                    st.error(_friendly_api_error(exc, "le scoring du fichier importé"))

        # Cached in session_state (not just the try-block's local var) so the
        # download button and preview survive the rerun that clicking
        # download_button itself triggers, instead of vanishing immediately.
        if st.session_state.get("_batch_csv_bytes") is not None:
            content = st.session_state["_batch_csv_bytes"]
            st.download_button(
                label="⬇ Télécharger les résultats (CSV)",
                data=content,
                file_name="batch_scores.csv",
                mime="text/csv",
            )
            import io
            preview_df = pd.read_csv(io.BytesIO(content))
            st.dataframe(preview_df.head(50), use_container_width=True)

        st.caption("Jusqu'à 10 000 bénéficiaires par import. Jusqu'à 8 workers en parallèle.")


# ── Explainability page ───────────────────────────────────────────────────────

def show_explainability_page() -> None:
    page_header("Explicabilité de la décision",
                "Facteurs SHAP, règles déclenchées et résumé généré par IA", "💡")
    bid = st.text_input("ID Bénéficiaire", placeholder="ex. 12345", key="explain_id")

    if st.button("Obtenir l'explication", type="primary"):
        if not bid:
            st.warning("Veuillez saisir un ID Bénéficiaire.")
            return
        with st.spinner("Récupération de l'explication…"):
            result = _get(f"/explain/{bid}")
        if result is None:
            return

        st.subheader("Résumé")
        st.write(result.get("summary", ""))

        if result.get("top_reasons"):
            st.subheader("Principales raisons")
            for reason in result["top_reasons"]:
                st.markdown(f"- {reason}")

        if result.get("rule_explanations"):
            st.subheader("Explications des règles")
            for expl in result["rule_explanations"]:
                st.markdown(f"- {expl}")

        render_shap(result.get("feature_contributions") or [],
                    title="Contributions des facteurs (SHAP)")

        raw = result.get("raw_scores", {})
        if raw:
            st.subheader("Scores bruts")
            kpi_row([
                {"label": "Final", "value": f"{raw.get('final_score', 0):.3f}"
                 if raw.get("final_score") else "N/A"},
                {"label": "Règles", "value": f"{raw.get('rule_score', 0):.3f}"
                 if raw.get("rule_score") else "N/A", "accent": "#34d399"},
                {"label": "ML", "value": f"{raw.get('ml_score', 0):.3f}"
                 if raw.get("ml_score") else "N/A", "accent": "#8b5cf6"},
                {"label": "Graphe", "value": f"{raw.get('graph_score', 0):.3f}"
                 if raw.get("graph_score") else "N/A", "accent": "#fb923c"},
            ])


# ── Geo Hotspots page ─────────────────────────────────────────────────────────

def show_geo_page() -> None:
    page_header("Zones de fraude géospatiales",
                "Clusters DBSCAN pondérés par le score de fraude", "🗺️")
    st.markdown(
        "Les bénéficiaires sont regroupés géographiquement via **DBSCAN**. "
        "Le poids de la carte de chaleur est le score de fraude. "
        "Zones plus sombres/hautes = densité de fraude plus élevée."
    )

    with logo_loading("Chargement de la carte de chaleur…",
                      "Clustering géospatial des bénéficiaires"):
        heatmap_data = _get("/geo/heatmap")
        hotspots     = _get("/geo/hotspots")

    if not heatmap_data:
        st.warning("Aucun bénéficiaire scoré avec coordonnées pour l'instant. Scorez d'abord des bénéficiaires.")
        return

    df_map = pd.DataFrame(heatmap_data)
    df_map["name"] = df_map["partner_id"].apply(beneficiary_display_name)

    # Deep-link from Odoo: ?beneficiary=ID centers + highlights that partner
    focus_bid = st.query_params.get("beneficiary", "")
    if focus_bid:
        focus = df_map[df_map["partner_id"].astype(str) == str(focus_bid)]
        if not focus.empty:
            st.info(f"Localisation du bénéficiaire #{focus_bid} (surligné en bleu).")
        else:
            st.warning(f"Le bénéficiaire #{focus_bid} n'a pas encore de données géographiques.")

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
            tooltip={"text": "{name}\nScore: {fraud_score}"},
        ))

    except Exception as exc:
        st.warning(f"Carte pydeck indisponible ({exc}) — affichage en tableau à la place")
        fallback_df = df_map.drop(columns=["partner_id"]).sort_values("fraud_score", ascending=False)
        st.dataframe(apply_column_labels(fallback_df), use_container_width=True)

    # ── hotspot table ─────────────────────────────────────────────────────────
    if hotspots:
        high_risk = [h for h in hotspots if h.get("risk_label") in ("HIGH", "CRITICAL")]
        kpi_row([
            {"label": "Total clusters", "value": len(hotspots)},
            {"label": "Clusters à haut risque", "value": len(high_risk),
             "accent": RISK_STATUS["HIGH"]["color"]},
            {"label": "Bénéficiaires sur la carte", "value": len(df_map)},
        ])

        st.subheader(f"Clusters détectés ({len(hotspots)})")
        hs_rows = []
        for h in hotspots:
            rl = h.get("risk_label", "LOW")
            hs_rows.append({
                "Cluster":         h.get("cluster_id"),
                "Centre":          f"{h['center_lat']:.4f}, {h['center_lon']:.4f}",
                "Rayon km":        h.get("radius_km"),
                "Bénéficiaires":   h.get("count"),
                "ÉLEVÉ+CRITIQUE":  h.get("fraud_count"),
                "Taux de fraude":  f"{h.get('fraud_rate', 0):.1%}",
                "Score moyen":     f"{h.get('avg_score', 0):.3f}",
                "Risque":          risk_badge(rl),
            })
        st.dataframe(pd.DataFrame(hs_rows), use_container_width=True, hide_index=True)
    else:
        st.info("Aucun cluster dense détecté pour l'instant. Plus de données sont nécessaires pour DBSCAN.")


# ── Monitoring page ───────────────────────────────────────────────────────────

def show_monitoring_page() -> None:
    page_header("Supervision du système",
                "État du service, distribution des risques et cycle de vie du modèle", "📈")

    with st.spinner("Vérification de l'état du service…"):
        health = _get("/health")
    if health:
        status_ok = health.get("status", "") == "healthy" or health.get("status", "") == "ok"
        kpi_row([
            {"label": "État du service", "value": health.get("status", "unknown").upper(),
             "accent": "#0ca30c" if status_ok else "#d03b3b",
             "sub": f"v{health.get('version', '')}"},
            {"label": "Modèles prêts", "value": "Oui" if health.get("models_ready") else "Non",
             "accent": "#0ca30c" if health.get("models_ready") else "#d03b3b"},
            {"label": "Règles chargées", "value": health.get("rules_loaded", 0)},
        ])
        st.caption(f"Dernière vérification : {health.get('timestamp', '')[:19]}")

    st.subheader("Distribution des risques")
    with st.spinner("Chargement de la distribution des risques…"):
        data = _get("/cases", params={"limit": 500})
    if data:
        cases = data.get("cases", [])
        if cases:
            df = pd.DataFrame(cases)
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Dossiers par niveau de risque**")
                risk_counts = df["risk_level"].value_counts().reset_index()
                risk_counts.columns = ["risk_level", "count"]
                st.altair_chart(risk_distribution_chart(risk_counts),
                                use_container_width=True)
            with c2:
                st.markdown("**Dossiers par recommandation**")
                rec_counts = df["recommendation"].value_counts().reset_index()
                rec_counts.columns = ["recommendation", "count"]
                st.altair_chart(
                    count_bar_chart(rec_counts, "recommendation", "count"),
                    use_container_width=True)

    # ── feedback & retraining ─────────────────────────────────────────────────
    st.subheader("Retours des enquêteurs & réentraînement du modèle")
    with st.spinner("Chargement des statistiques de retours…"):
        stats = _get("/feedback/stats")
    if stats:
        kpi_row([
            {"label": "Total des verdicts", "value": stats.get("total_feedback", 0)},
            {"label": "Fraudes confirmées", "value": stats.get("confirmed_fraud", 0),
             "accent": RISK_STATUS["CRITICAL"]["color"]},
            {"label": "Faux positifs", "value": stats.get("false_positive", 0),
             "accent": RISK_STATUS["MEDIUM"]["color"]},
            {"label": "Précision estimée", "value": f"{stats.get('estimated_precision', 0):.1%}",
             "accent": "#0ca30c"},
        ])
        if stats.get("last_retrain"):
            st.caption(f"Dernier réentraînement : {stats['last_retrain'][:19]}")

    col_btn, col_info = st.columns([1, 3])
    with col_btn:
        if st.button("Réentraîner le modèle maintenant", type="primary"):
            result = _post("/retrain")
            if result:
                st.success(result.get("message", "Réentraînement démarré"))
    with col_info:
        st.info(
            "Le réentraînement utilise les verdicts confirmés (fraude / faux positif) des enquêteurs "
            "comme vérité terrain. Il tourne automatiquement tous les 7 jours. "
            f"Minimum {10} verdicts requis."
        )

    # ── MLflow model versions ─────────────────────────────────────────────────
    st.subheader("Historique des versions du modèle (MLflow)")
    with st.spinner("Chargement de l'historique des versions…"):
        versions = _get("/models/versions")
    if versions:
        ver_rows = []
        for v in versions:
            ver_rows.append({
                "ID d'exécution": v.get("run_id", ""),
                "Statut":         v.get("status", ""),
                "Démarré":        v.get("started", ""),
                "Précision":      f"{v.get('accuracy', 0):.3f}",
                "Échantillons":   v.get("n_samples", 0),
                "Retours":        v.get("n_feedback", 0),
            })
        df_ver = pd.DataFrame(ver_rows)
        st.dataframe(df_ver, use_container_width=True, hide_index=True)

        st.markdown("**Revenir à une version antérieure :**")
        run_ids = [v.get("run_id") for v in versions]
        rollback_id = st.selectbox("Sélectionner une exécution", run_ids)
        target = next((v for v in versions if v.get("run_id") == rollback_id), {})
        st.caption(
            f"Ceci remplace **immédiatement** le modèle utilisé par tous les futurs scores — "
            f"précision de l'exécution cible {target.get('accuracy', 0):.3f}, "
            f"entraînée sur {target.get('n_samples', 0)} échantillons "
            f"({target.get('started', 'date inconnue')})."
        )
        confirm_rollback = st.checkbox(f"Confirmer le retour à l'exécution {rollback_id}")
        if st.button("Revenir à cette version", type="secondary", disabled=not confirm_rollback):
            result = _post(f"/models/rollback/{rollback_id}")
            if result:
                if result.get("status") == "success":
                    st.success(f"Retour effectué vers l'exécution {rollback_id}")
                else:
                    st.error(result.get("error", "Échec du retour à cette version"))
    else:
        st.info("Aucune exécution MLflow trouvée. Déclenchez un réentraînement pour peupler ce tableau.")


# ── Rules Management page ───────────────────────────────────────────────────────

ALERT_LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]


def show_rules_page() -> None:
    page_header("Moteur de règles — Gestion",
                "Créer, ajuster, activer ou retirer des règles de détection", "⚙️")
    st.caption(
        "Les règles sont stockées en YAML sur le fraud-engine et rechargées à chaud à chaque "
        "modification — aucun redémarrage nécessaire. Les conditions sont évaluées par un "
        "évaluateur AST restreint (expressions arithmétiques/booléennes/de comparaison uniquement), "
        "aucun code Python arbitraire ne peut donc y être injecté."
    )

    with st.spinner("Chargement des règles…"):
        data = _get("/rules/admin")
    rules = data.get("rules", []) if data else []

    # ── summary ──────────────────────────────────────────────────────────────
    kpi_row([
        {"label": "Total des règles", "value": len(rules)},
        {"label": "Activées", "value": sum(1 for r in rules if r.get("enabled", True)),
         "accent": "#0ca30c"},
        {"label": "Désactivées", "value": sum(1 for r in rules if not r.get("enabled", True)),
         "accent": "#898781"},
    ])

    if st.button("Recharger depuis le disque"):
        result = _post("/rules/reload")
        if result:
            st.success(f"Rechargé — {result.get('count', 0)} règles actives")
            st.rerun()

    tab_list, tab_edit, tab_new = st.tabs(["Règles actives", "Modifier / Supprimer", "Nouvelle règle"])

    # ── Active rules table ───────────────────────────────────────────────────
    with tab_list:
        if not rules:
            st.info("Aucune règle trouvée.")
        else:
            rows = [{
                "ID": r["id"],
                "Nom": r["name"],
                "Scénario": r.get("scenario"),
                "Poids": r.get("weight"),
                "Niveau d'alerte": r.get("alert_level"),
                "Condition": r.get("condition"),
                "Statut": "🟢 Activée" if r.get("enabled", True) else "⚪ Désactivée",
            } for r in rules]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ── Edit / enable-disable / delete ───────────────────────────────────────
    with tab_edit:
        if not rules:
            st.info("Aucune règle à modifier — créez-en une dans l'onglet « Nouvelle règle ».")
        else:
            rule_ids = [r["id"] for r in rules]
            selected_id = st.selectbox("Sélectionner une règle", rule_ids, key="edit_select")
            rule = next(r for r in rules if r["id"] == selected_id)

            with st.form("edit_rule_form"):
                name = st.text_input("Nom", value=rule["name"])
                description = st.text_area("Description", value=rule.get("description", ""))
                weight = st.slider("Poids", 0.0, 1.0, float(rule.get("weight", 0.3)), 0.01)
                alert_level = st.selectbox(
                    "Niveau d'alerte", ALERT_LEVELS,
                    index=ALERT_LEVELS.index(rule.get("alert_level", "MEDIUM")),
                )
                condition = st.text_input(
                    "Condition", value=rule.get("condition", ""),
                    help="Expression booléenne sur les noms de facteurs, ex. "
                         "'shared_account_count >= 2 and income < 500'",
                )
                evidence_template = st.text_input(
                    "Modèle de preuve", value=rule.get("evidence_template", ""),
                    help="Explication affichée aux agents, ex. "
                         "'Partagé avec {shared_account_count} autres bénéficiaires'",
                )
                submitted = st.form_submit_button("Enregistrer les modifications", type="primary")

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
                    st.success(f"Règle « {selected_id} » mise à jour")
                    st.rerun()

            st.divider()
            col_toggle, col_delete = st.columns(2)
            with col_toggle:
                is_enabled = rule.get("enabled", True)
                label = "Désactiver la règle" if is_enabled else "Activer la règle"
                if st.button(label):
                    result = _patch(f"/rules/{selected_id}/enabled", {"enabled": not is_enabled})
                    if result:
                        st.success(f"Règle « {selected_id} » {'désactivée' if is_enabled else 'activée'}")
                        st.rerun()
            with col_delete:
                confirm = st.checkbox(f"Confirmer la suppression définitive de {selected_id}")
                if st.button("Supprimer la règle", type="secondary", disabled=not confirm):
                    result = _delete(f"/rules/{selected_id}")
                    if result:
                        st.success(f"Règle « {selected_id} » supprimée")
                        st.rerun()

    # ── New rule ─────────────────────────────────────────────────────────────
    with tab_new:
        scenarios_data = _get("/rules/scenarios")
        scenario_files = scenarios_data.get("files", []) if scenarios_data else []

        with st.form("new_rule_form", clear_on_submit=True):
            col_a, col_b = st.columns(2)
            with col_a:
                new_id = st.text_input("ID de la règle", placeholder="ex. NF007")
                new_name = st.text_input("Nom", placeholder="ex. Retrait important en espèces")
                new_weight = st.slider("Poids", 0.0, 1.0, 0.30, 0.01)
                new_alert_level = st.selectbox("Niveau d'alerte", ALERT_LEVELS, index=2)
            with col_b:
                scenario_choice = st.selectbox(
                    "Fichier de scénario",
                    scenario_files + ["+ Nouveau fichier…"],
                    help="Les règles sont groupées par fichier de scénario YAML sous app/rules/rules/",
                )
                if scenario_choice == "+ Nouveau fichier…":
                    scenario_file = st.text_input(
                        "Nom du nouveau fichier", placeholder="ex. custom_rules.yaml"
                    )
                else:
                    scenario_file = scenario_choice
                new_description = st.text_area("Description", placeholder="Que détecte cette règle ?")

            new_condition = st.text_input(
                "Condition",
                placeholder="ex. payment_gap_ratio > 0.5 and nb_programs >= 3",
                help="Seuls les opérateurs arithmétiques, booléens et de comparaison sont autorisés "
                     "(pas d'appels de fonction) — imposé par un évaluateur AST restreint.",
            )
            new_evidence = st.text_input(
                "Modèle de preuve",
                placeholder="ex. 'Écart de paiement {payment_gap_ratio:.2f} sur {nb_programs} programmes'",
            )
            create_submitted = st.form_submit_button("Créer la règle", type="primary")

        if create_submitted:
            if not new_id or not new_name or not new_condition or not scenario_file:
                st.error("L'ID, le nom, la condition et le fichier de scénario sont requis.")
            else:
                payload = {
                    "id": new_id,
                    "name": new_name,
                    "description": new_description,
                    "weight": new_weight,
                    "alert_level": new_alert_level,
                    "condition": new_condition,
                    "evidence_template": new_evidence or "Règle déclenchée",
                    "enabled": True,
                    "scenario_file": scenario_file if scenario_file.endswith(".yaml")
                                     else f"{scenario_file}.yaml",
                }
                result = _post("/rules", json=payload)
                if result:
                    st.success(f"Règle « {new_id} » créée dans {payload['scenario_file']}")
                    st.rerun()


# ── Sidebar ───────────────────────────────────────────────────────────────────

def _show_sidebar_scanner() -> None:
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Scan automatique**")
    status = _get("/scan/status")
    if status:
        st.sidebar.metric("Bénéficiaires OpenG2P", status.get("total_in_openg2p", 0))
        st.sidebar.metric("Scorés",                status.get("already_scored",  0))
        pending = status.get("pending", 0)
        if pending > 0:
            st.sidebar.warning(f"{pending} en attente")
        else:
            st.sidebar.success("Tous scorés")

    if st.sidebar.button("Scanner maintenant"):
        country_code = st.session_state.get("scan_country_code", "TN")
        with logo_loading("Scan de la base de données…",
                          f"Détection en cours · calibrage {country_code}"):
            result = _post("/scan/now", params={"country_code": country_code})
        if result:
            s = result.get("summary", {})
            st.sidebar.success(
                f"{s.get('scored', 0)} nouveaux scorés | "
                f"CRITIQUE : {s.get('CRITICAL', 0)}  ÉLEVÉ : {s.get('HIGH', 0)}"
            )

    render_country_stats_sidebar()


# ── Landing page ──────────────────────────────────────────────────────────────

# Interactive module cards → each navigates to its section via an on_click
# callback that sets the sidebar radio's session_state key (safe pattern:
# callbacks run before widgets instantiate, so no "already instantiated" error).
_MODULES = [
    ("Cases",                "🗂️", "Dossiers",             "Consulter et trier les bénéficiaires signalés par niveau de risque et statut."),
    ("Score / Scan / Batch", "🎯", "Scorer / Scanner",     "Scorer un bénéficiaire, lancer un scan complet ou importer un lot en CSV."),
    ("Explainability",       "🧠", "Explicabilité",        "Comprendre chaque décision via les facteurs SHAP et l'explication en langage naturel."),
    ("Geo Hotspots",         "🗺️", "Zones à risque",       "Visualiser la concentration géographique de la fraude sur une carte de chaleur."),
    ("Monitoring",           "📈", "Supervision",          "Suivre la santé du moteur, la distribution des risques et les versions du modèle."),
    ("Rules Management",     "⚙️", "Gestion des règles",   "Créer, modifier et activer les règles métier du moteur de détection."),
]


def _nav_to(target: str) -> None:
    st.session_state.nav = target


def show_landing_page() -> None:
    user = st.session_state.get("auth_user") or {}
    name = user.get("name", "Analyste")

    st.markdown(
        f'<div class="ey-hero">'
        f'<div class="hero-logo"><img src="{OPENG2P_LOGO_URI}" alt="OpenG2P"/></div>'
        f'<div><div class="h-eyebrow">Moteur intelligent de détection de fraude</div>'
        f'<h1>Live Alert Monitor</h1>'
        f'<p>Surveillance en temps réel des programmes de protection sociale OpenG2P — '
        f'scoring, analyse de réseau et explicabilité, réunis dans une interface unique.</p>'
        f'<div class="hero-user"><div class="ava">{_initials(name)}</div>'
        f'<div class="txt"><small>Connecté en tant que</small>{name}</div></div>'
        f'</div></div>',
        unsafe_allow_html=True,
    )

    # Live snapshot KPIs (branded loading on first fetch).
    with logo_loading("Chargement du tableau de bord…", "Récupération des dossiers actifs"):
        data = _get("/cases", {"limit": 500}) or {}
    cases = data.get("cases", data if isinstance(data, list) else [])
    total = len(cases)
    n_crit = sum(1 for c in cases if c.get("risk_level") == "CRITICAL")
    n_high = sum(1 for c in cases if c.get("risk_level") == "HIGH")
    n_open = sum(1 for c in cases if c.get("status") == "OPEN")
    kpi_row([
        {"label": "Dossiers actifs", "value": total},
        {"label": "Critiques", "value": n_crit, "accent": RISK_STATUS["CRITICAL"]["color"]},
        {"label": "Élevés",    "value": n_high, "accent": RISK_STATUS["HIGH"]["color"]},
        {"label": "En attente", "value": n_open},
    ])

    st.markdown("#### Accès rapide")
    cols = st.columns(3)
    for i, (target, icon, title, desc) in enumerate(_MODULES):
        with cols[i % 3]:
            st.markdown(
                f'<div class="mod-card"><div class="m-ico">{icon}</div>'
                f'<div class="m-title">{title}</div>'
                f'<div class="m-desc">{desc}</div></div>',
                unsafe_allow_html=True,
            )
            st.button("Ouvrir  →", key=f"go_{target}", use_container_width=True,
                      on_click=_nav_to, args=(target,))


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="OpenG2P — Détection de fraude",
        layout="wide",
        page_icon=OPENG2P_ICON_URI,
    )

    identity = _check_access_token()
    if not identity:
        st.error(
            "🔒 Accès refusé — ouvrez ce tableau de bord depuis OpenG2P (Odoo) en tant "
            "qu'agent ou superviseur anti-fraude."
        )
        st.stop()
    st.session_state["auth_user"] = identity

    _inject_css()

    st.sidebar.markdown(
        f'<div class="sb-brand"><div class="logo">'
        f'<img src="{OPENG2P_LOGO_URI}" alt="OpenG2P"/></div>'
        f'<div><div class="name">Détection de fraude</div>'
        f'<div class="sub">OpenG2P — Fraud Engine v2</div></div></div>',
        unsafe_allow_html=True,
    )
    user_chip(sidebar=True)
    st.sidebar.markdown(
        '<div class="sb-link"><a href="http://localhost:8069/odoo/'
        'action-g2p_fraud_detection.action_fraud_dashboard" target="_blank">'
        "↗ Ouvrir dans OpenG2P (Odoo)</a></div>",
        unsafe_allow_html=True,
    )

    pages = ["Accueil", "Cases", "Score / Scan / Batch", "Explainability",
             "Geo Hotspots", "Monitoring", "Rules Management"]
    # Respect ?page=… deep-linking from Odoo so cross-app navigation lands on
    # the right tab. Seed the radio's session_state once (before it is created)
    # so both deep-links and in-page module cards can drive navigation.
    _page_aliases = {
        "accueil": "Accueil", "home": "Accueil",
        "cases": "Cases", "score": "Score / Scan / Batch",
        "explain": "Explainability", "explainability": "Explainability",
        "geo": "Geo Hotspots", "heatmap": "Geo Hotspots", "hotspots": "Geo Hotspots",
        "monitor": "Monitoring", "monitoring": "Monitoring",
        "rules": "Rules Management", "rules_management": "Rules Management",
    }
    if "nav" not in st.session_state:
        _qp = st.query_params.get("page", "").lower()
        st.session_state.nav = _page_aliases.get(_qp, "Accueil")

    _nav_labels_fr = {
        "Accueil": "🏠 Accueil",
        "Cases": "Dossiers", "Score / Scan / Batch": "Scorer / Scanner / Lot",
        "Explainability": "Explicabilité", "Geo Hotspots": "Zones à risque",
        "Monitoring": "Supervision", "Rules Management": "Gestion des règles",
    }
    page = st.sidebar.radio(
        "Navigation", pages, key="nav",
        format_func=lambda p: _nav_labels_fr.get(p, p),
    )
    if page != "Accueil":
        _show_sidebar_scanner()

    if   page == "Accueil":              show_landing_page()
    elif page == "Cases":                show_cases_page()
    elif page == "Score / Scan / Batch": show_scoring_page()
    elif page == "Explainability":       show_explainability_page()
    elif page == "Geo Hotspots":         show_geo_page()
    elif page == "Monitoring":           show_monitoring_page()
    elif page == "Rules Management":     show_rules_page()


if __name__ == "__main__":
    main()
