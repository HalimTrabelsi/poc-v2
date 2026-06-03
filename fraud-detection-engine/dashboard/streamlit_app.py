"""Professional Streamlit dashboard for the Fraud Detection Engine."""
import os

import pandas as pd
import requests
import streamlit as st

_engine_url = os.getenv("FRAUD_ENGINE_URL", "http://localhost:8000")
API_BASE = f"{_engine_url.rstrip('/')}/api/v1"
API_KEY = os.getenv("FRAUD_API_KEY", "dev-secret-change-in-prod")
HEADERS = {"X-API-Key": API_KEY}

RISK_COLORS = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}
STATUS_OPTIONS = ["OPEN", "UNDER_REVIEW", "CLOSED", "FALSE_POSITIVE"]


# ── SHAP rendering ──────────────────────────────────────────────────────────────

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
    chart_df = pd.DataFrame(rows).set_index("feature")
    # Signed bars: positive = increases risk, negative = decreases risk.
    st.bar_chart(chart_df[["shap"]], horizontal=True, color="#d62728")
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


# ── Cases page ────────────────────────────────────────────────────────────────

def show_cases_page() -> None:
    st.header("Fraud Cases")

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
    st.metric("Total cases returned", data.get("total", len(cases)))

    if not cases:
        st.info("No cases match the selected filters.")
        return

    rows = []
    for c in cases:
        rl = c.get("risk_level", "LOW")
        rows.append({
            "Case ID":       c.get("case_id", "")[:8] + "…",
            "Beneficiary":   c.get("beneficiary_id"),
            "Score":         f"{c.get('final_score', 0):.2%}",
            "Risk":          f"{RISK_COLORS.get(rl, '')} {rl}",
            "Recommendation":c.get("recommendation"),
            "Status":        c.get("status"),
            "Created":       c.get("created_at", "")[:10],
            "_case_id":      c.get("case_id"),
        })

    df = pd.DataFrame(rows)
    st.dataframe(df.drop(columns=["_case_id"]), use_container_width=True)

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
    score = result.get("final_score", 0)

    col1, col2, col3 = st.columns(3)
    col1.metric("Final Score",    f"{score:.2%}")
    col2.metric("Risk Level",     f"{RISK_COLORS.get(rl, '')} {rl}")
    col3.metric("Recommendation", result.get("recommendation", ""))

    st.info(result.get("explanation", "No explanation available."))

    if result.get("rules_triggered"):
        st.subheader("Triggered Rules")
        st.dataframe(pd.DataFrame([{
            "Rule":        r.get("rule_id"),
            "Name":        r.get("name"),
            "Weight":      r.get("weight"),
            "Explanation": r.get("explanation"),
        } for r in result["rules_triggered"]]), use_container_width=True)

    render_shap(result.get("top_features") or [])

    st.caption(
        f"Processed in {result.get('processing_ms', 0):.0f} ms | "
        f"Case ID: {result.get('case_id', 'N/A')}"
    )


def show_scoring_page() -> None:
    st.header("Score a Beneficiary")
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
                        "Score":          f"{r.get('final_score', 0):.2%}",
                        "Risk":           f"{RISK_COLORS.get(r.get('risk_level','LOW'),'')} {r.get('risk_level','LOW')}",
                        "Recommendation": r.get("recommendation"),
                        "Case ID":        (r.get("case_id") or "")[:12],
                    })
                progress.progress((i + 1) / len(ids))
            if results:
                st.success(f"Scored {len(results)} beneficiaries.")
                st.dataframe(pd.DataFrame(results), use_container_width=True)

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
    st.header("Decision Explainability")
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
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Final",  f"{raw.get('final_score', 0):.3f}" if raw.get("final_score") else "N/A")
            c2.metric("Rules",  f"{raw.get('rule_score',  0):.3f}" if raw.get("rule_score")  else "N/A")
            c3.metric("ML",     f"{raw.get('ml_score',    0):.3f}" if raw.get("ml_score")    else "N/A")
            c4.metric("Graph",  f"{raw.get('graph_score', 0):.3f}" if raw.get("graph_score") else "N/A")


# ── Geo Hotspots page ─────────────────────────────────────────────────────────

def show_geo_page() -> None:
    st.header("Geospatial Fraud Hotspots")
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
                [0,   255, 0,   180],   # green  = low
                [255, 255, 0,   200],   # yellow = medium
                [255, 128, 0,   220],   # orange = high
                [255, 0,   0,   255],   # red    = critical
            ],
        )

        scatter_layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_map[df_map["fraud_score"] >= 0.60],
            get_position=["lon", "lat"],
            get_radius=5000,
            get_fill_color=[255, 0, 0, 160],
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
                get_fill_color=[30, 144, 255, 220],
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
        st.dataframe(pd.DataFrame(hs_rows), use_container_width=True)

        # Summary metrics
        high_risk = [h for h in hotspots if h.get("risk_label") in ("HIGH", "CRITICAL")]
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Clusters",         len(hotspots))
        c2.metric("High-Risk Clusters",     len(high_risk))
        c3.metric("Beneficiaries on Map",   len(df_map))
    else:
        st.info("No dense clusters detected yet. More data needed for DBSCAN to find hotspots.")


# ── Monitoring page ───────────────────────────────────────────────────────────

def show_monitoring_page() -> None:
    st.header("System Monitoring")

    health = _get("/health")
    if health:
        col1, col2, col3 = st.columns(3)
        col1.metric("Service Status", health.get("status", "unknown").upper())
        col2.metric("Models Ready",   "Yes" if health.get("models_ready") else "No")
        col3.metric("Rules Loaded",   health.get("rules_loaded", 0))
        st.caption(f"Version: {health.get('version')} | {health.get('timestamp', '')[:19]}")

    st.subheader("Risk Distribution")
    data = _get("/cases", params={"limit": 500})
    if data:
        cases = data.get("cases", [])
        if cases:
            df = pd.DataFrame(cases)
            c1, c2 = st.columns(2)
            with c1:
                risk_counts = df["risk_level"].value_counts().reset_index()
                risk_counts.columns = ["Risk Level", "Count"]
                st.bar_chart(risk_counts.set_index("Risk Level"))
            with c2:
                rec_counts = df["recommendation"].value_counts()
                st.bar_chart(rec_counts)

    # ── feedback & retraining ─────────────────────────────────────────────────
    st.subheader("Investigator Feedback & Model Retraining")
    stats = _get("/feedback/stats")
    if stats:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Verdicts",      stats.get("total_feedback", 0))
        c2.metric("Confirmed Fraud",     stats.get("confirmed_fraud", 0))
        c3.metric("False Positives",     stats.get("false_positive", 0))
        c4.metric("Est. Precision",      f"{stats.get('estimated_precision', 0):.1%}")
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
        st.dataframe(df_ver, use_container_width=True)

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
        page_icon="🔍",
    )

    st.sidebar.title("Fraud Detection")
    st.sidebar.markdown("**OpenG2P — Fraud Engine v2**")
    st.sidebar.markdown(
        '<a href="http://localhost:8069/odoo/action-g2p_fraud_detection.action_fraud_dashboard" '
        'target="_blank" style="text-decoration:none;">'
        '🔗 Open in OpenG2P (Odoo)</a>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown("---")
    _show_sidebar_scanner()

    pages = ["Cases", "Score / Scan / Batch", "Explainability", "Geo Hotspots", "Monitoring"]
    # Respect ?page=… deep-linking from Odoo so cross-app navigation lands on
    # the right tab. Falls back to Cases if the param is missing or invalid.
    _page_aliases = {
        "cases": "Cases", "score": "Score / Scan / Batch",
        "explain": "Explainability", "explainability": "Explainability",
        "geo": "Geo Hotspots", "heatmap": "Geo Hotspots", "hotspots": "Geo Hotspots",
        "monitor": "Monitoring", "monitoring": "Monitoring",
    }
    _qp = st.query_params.get("page", "").lower()
    default_idx = pages.index(_page_aliases[_qp]) if _qp in _page_aliases else 0

    page = st.sidebar.radio("Navigation", pages, index=default_idx)

    if   page == "Cases":                show_cases_page()
    elif page == "Score / Scan / Batch": show_scoring_page()
    elif page == "Explainability":       show_explainability_page()
    elif page == "Geo Hotspots":         show_geo_page()
    elif page == "Monitoring":           show_monitoring_page()


if __name__ == "__main__":
    main()
