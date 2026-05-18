"""Professional Streamlit dashboard for the Fraud Detection Engine."""
import pandas as pd
import requests
import streamlit as st

import os
_engine_url = os.getenv("FRAUD_ENGINE_URL", "http://localhost:8000")
API_BASE = f"{_engine_url.rstrip('/')}/api/v1"
API_KEY = "dev-secret-change-in-prod"
HEADERS = {"X-API-Key": API_KEY}

RISK_COLORS = {
    "LOW": "🟢",
    "MEDIUM": "🟡",
    "HIGH": "🟠",
    "CRITICAL": "🔴",
}

STATUS_OPTIONS = ["OPEN", "UNDER_REVIEW", "CLOSED", "FALSE_POSITIVE"]


def _get(path: str, params: dict | None = None) -> dict | list | None:
    try:
        r = requests.get(f"{API_BASE}{path}", headers=HEADERS, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(f"API error: {exc}")
        return None


def _post(path: str, json: dict | None = None) -> dict | None:
    try:
        r = requests.post(f"{API_BASE}{path}", headers=HEADERS, json=json, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(f"API error: {exc}")
        return None


def _patch(path: str, json: dict) -> dict | None:
    try:
        r = requests.patch(f"{API_BASE}{path}", headers=HEADERS, json=json, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        st.error(f"API error: {exc}")
        return None


def show_cases_page() -> None:
    st.header("Fraud Cases")

    col1, col2, col3 = st.columns(3)
    risk_filter = col1.selectbox("Risk Level", ["All", "LOW", "MEDIUM", "HIGH", "CRITICAL"])
    status_filter = col2.selectbox("Status", ["All"] + STATUS_OPTIONS)
    limit = col3.slider("Max rows", min_value=10, max_value=500, value=50)

    params: dict = {"limit": limit}
    if risk_filter != "All":
        params["risk_level"] = risk_filter
    if status_filter != "All":
        params["status"] = status_filter

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
        rows.append(
            {
                "Case ID": c.get("case_id", "")[:8] + "…",
                "Beneficiary": c.get("beneficiary_id"),
                "Score": f"{c.get('final_score', 0):.2%}",
                "Risk": f"{RISK_COLORS.get(rl, '')} {rl}",
                "Recommendation": c.get("recommendation"),
                "Status": c.get("status"),
                "Created": c.get("created_at", "")[:10],
                "_case_id": c.get("case_id"),
            }
        )

    df = pd.DataFrame(rows)
    st.dataframe(df.drop(columns=["_case_id"]), use_container_width=True)

    st.subheader("Update Case Status")
    case_ids = [r["_case_id"] for r in rows]
    selected_id = st.selectbox("Select Case", case_ids, format_func=lambda x: x[:12] + "…")
    new_status = st.selectbox("New Status", STATUS_OPTIONS)
    notes = st.text_area("Agent Notes")

    if st.button("Update Status"):
        result = _patch(f"/cases/{selected_id}/status", {"status": new_status, "notes": notes})
        if result:
            st.success(f"Case updated to {new_status}")
            st.rerun()


def _render_decision(result: dict) -> None:
    """Render a fraud decision result (shared between single score and batch)."""
    rl = result.get("risk_level", "LOW")
    score = result.get("final_score", 0)

    col1, col2, col3 = st.columns(3)
    col1.metric("Final Score", f"{score:.2%}")
    col2.metric("Risk Level", f"{RISK_COLORS.get(rl, '')} {rl}")
    col3.metric("Recommendation", result.get("recommendation", ""))

    st.info(result.get("explanation", "No explanation available."))

    if result.get("rules_triggered"):
        st.subheader("Triggered Rules")
        rule_rows = [
            {
                "Rule": r.get("rule_id"),
                "Name": r.get("name"),
                "Weight": r.get("weight"),
                "Explanation": r.get("explanation"),
            }
            for r in result["rules_triggered"]
        ]
        st.dataframe(pd.DataFrame(rule_rows), use_container_width=True)

    if result.get("top_features"):
        st.subheader("Top Feature Contributions (SHAP)")
        feat_rows = [
            {
                "Feature": f.get("feature"),
                "Value": f"{f.get('value', 0):.3f}",
                "SHAP Impact": f"{f.get('shap_value', 0):.4f}",
                "Direction": f.get("direction"),
            }
            for f in result["top_features"]
        ]
        st.dataframe(pd.DataFrame(feat_rows), use_container_width=True)

    st.caption(f"Processed in {result.get('processing_ms', 0):.0f} ms | Case ID: {result.get('case_id', 'N/A')}")


def show_scoring_page() -> None:
    st.header("Score a Beneficiary")

    tab_single, tab_batch = st.tabs(["🔎 Single Beneficiary", "📦 Scan All from OpenG2P"])

    with tab_single:
        st.markdown(
            "Enter the **partner_id** from OpenG2P. "
            "Find it by clicking a beneficiary at "
            "[localhost:8069/web#action=345](http://localhost:8069/web#action=345) "
            "— the ID appears in the URL as `id=<number>`."
        )
        beneficiary_id = st.text_input("Beneficiary ID (partner_id)", placeholder="e.g. 42")

        if st.button("Run Fraud Score", type="primary"):
            if not beneficiary_id.strip():
                st.warning("Please enter a Beneficiary ID.")
                return

            with st.spinner(f"Scoring beneficiary {beneficiary_id}…"):
                result = _post(f"/score/beneficiary/{beneficiary_id.strip()}")

            if result:
                _render_decision(result)

    with tab_batch:
        st.markdown(
            "Score **all beneficiaries** currently in OpenG2P at once. "
            "Results appear in the **Cases** tab. This may take a few minutes for large datasets."
        )
        limit = st.number_input("Limit (0 = all)", min_value=0, max_value=10000, value=100, step=10)

        if st.button("🚀 Scan All Beneficiaries", type="primary"):
            with st.spinner("Fetching beneficiary list from OpenG2P…"):
                params = {}
                if limit > 0:
                    params["limit"] = int(limit)
                bene_list = _get("/beneficiaries", params=params or None)

            if not bene_list:
                st.warning("No beneficiaries returned. Check that the fraud API can reach OpenG2P DB.")
                return

            ids = [str(b.get("partner_id") or b.get("beneficiary_id")) for b in bene_list if b]
            st.info(f"Found {len(ids)} beneficiaries. Scoring now…")

            progress = st.progress(0)
            results = []
            for i, bid in enumerate(ids):
                r = _post(f"/score/beneficiary/{bid}")
                if r:
                    results.append({
                        "Beneficiary ID": bid,
                        "Score": f"{r.get('final_score', 0):.2%}",
                        "Risk": f"{RISK_COLORS.get(r.get('risk_level','LOW'),'')} {r.get('risk_level','LOW')}",
                        "Recommendation": r.get("recommendation"),
                        "Case ID": (r.get("case_id") or "")[:12],
                    })
                progress.progress((i + 1) / len(ids))

            if results:
                st.success(f"Scored {len(results)} beneficiaries. See **Cases** tab for full details.")
                st.dataframe(pd.DataFrame(results), use_container_width=True)


def show_explainability_page() -> None:
    st.header("Decision Explainability")

    beneficiary_id = st.text_input("Beneficiary ID", placeholder="e.g. 12345", key="explain_id")

    if st.button("Get Explanation", type="primary"):
        if not beneficiary_id:
            st.warning("Please enter a Beneficiary ID.")
            return

        with st.spinner("Fetching explanation…"):
            result = _get(f"/explain/{beneficiary_id}")

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

        if result.get("feature_contributions"):
            st.subheader("Feature Contributions")
            feat_rows = [
                {
                    "Feature": f.get("feature"),
                    "Value": f"{f.get('value', 0):.3f}",
                    "SHAP": f"{f.get('shap_value', 0):.4f}",
                    "Direction": f.get("direction"),
                }
                for f in result["feature_contributions"]
            ]
            st.dataframe(pd.DataFrame(feat_rows), use_container_width=True)

        raw = result.get("raw_scores", {})
        if raw:
            st.subheader("Raw Scores")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Final", f"{raw.get('final_score', 0):.3f}" if raw.get("final_score") else "N/A")
            c2.metric("Rules", f"{raw.get('rule_score', 0):.3f}" if raw.get("rule_score") else "N/A")
            c3.metric("ML", f"{raw.get('ml_score', 0):.3f}" if raw.get("ml_score") else "N/A")
            c4.metric("Graph", f"{raw.get('graph_score', 0):.3f}" if raw.get("graph_score") else "N/A")


def show_monitoring_page() -> None:
    st.header("System Monitoring")

    health = _get("/health")
    if health:
        col1, col2, col3 = st.columns(3)
        col1.metric("Service Status", health.get("status", "unknown").upper())
        col2.metric("Models Ready", "Yes" if health.get("models_ready") else "No")
        col3.metric("Rules Loaded", health.get("rules_loaded", 0))
        st.caption(f"Version: {health.get('version')} | Checked: {health.get('timestamp', '')[:19]}")

    st.subheader("Recent Cases by Risk Level")
    data = _get("/cases", params={"limit": 200})
    if data:
        cases = data.get("cases", [])
        if cases:
            df = pd.DataFrame(cases)
            risk_counts = df["risk_level"].value_counts().reset_index()
            risk_counts.columns = ["Risk Level", "Count"]
            st.bar_chart(risk_counts.set_index("Risk Level"))

            rec_counts = df["recommendation"].value_counts()
            st.subheader("Recommendations Distribution")
            st.bar_chart(rec_counts)
        else:
            st.info("No cases found in the system.")

    st.subheader("Prometheus Metrics")
    st.markdown(
        "View raw metrics at [localhost:8000/metrics](http://localhost:8000/metrics) "
        "or connect Grafana to the Prometheus endpoint."
    )


def _show_sidebar_scanner() -> None:
    """Sidebar widget: scanner status + manual scan trigger."""
    st.sidebar.markdown("---")
    st.sidebar.markdown("**🔄 Auto-Scanner**")

    status = _get("/scan/status")
    if status:
        total = status.get("total_in_openg2p", 0)
        scored = status.get("already_scored", 0)
        pending = status.get("pending", 0)
        st.sidebar.metric("OpenG2P Beneficiaries", total)
        st.sidebar.metric("Scored", scored)
        if pending > 0:
            st.sidebar.warning(f"⏳ {pending} pending")
        else:
            st.sidebar.success("✅ All scored")

    if st.sidebar.button("🚀 Scan Now", help="Score all unscored beneficiaries immediately"):
        with st.sidebar:
            with st.spinner("Scanning…"):
                result = _post("/scan/now")
            if result:
                summary = result.get("summary", {})
                st.sidebar.success(
                    f"Scored {summary.get('scored', 0)} new | "
                    f"🔴 {summary.get('CRITICAL', 0)} CRITICAL  "
                    f"🟠 {summary.get('HIGH', 0)} HIGH"
                )


def main() -> None:
    st.set_page_config(
        page_title="Fraud Detection System",
        layout="wide",
        page_icon="🔍",
    )

    st.sidebar.title("🔍 Fraud Detection")
    st.sidebar.markdown("**OpenG2P — Fraud Engine v2**")
    _show_sidebar_scanner()

    page = st.sidebar.radio(
        "Navigation",
        ["📋 Cases", "🔎 Score / Scan", "💡 Explainability", "📊 Monitoring"],
    )

    if page == "📋 Cases":
        show_cases_page()
    elif page == "🔎 Score / Scan":
        show_scoring_page()
    elif page == "💡 Explainability":
        show_explainability_page()
    elif page == "📊 Monitoring":
        show_monitoring_page()


if __name__ == "__main__":
    main()
