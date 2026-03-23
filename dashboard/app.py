"""Streamlit Dashboard — Fraud Detection OpenG2P"""
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os

API_URL = os.getenv("FRAUD_ENGINE_URL", "http://localhost:8002")

st.set_page_config(
    page_title="Fraud Detection — OpenG2P",
    layout="wide",
    page_icon="🔍",
)

# ── Sidebar ──────────────────────────────────────────────────
st.sidebar.title("🔍 Fraud Detection")
st.sidebar.markdown(f"**API :** `{API_URL}`")

try:
    r = requests.get(f"{API_URL}/health", timeout=3)
    if r.status_code == 200:
        st.sidebar.success("✅ API connectée")
    else:
        st.sidebar.error("❌ API erreur")
except Exception:
    st.sidebar.warning("⚠️ API non disponible")

page = st.sidebar.radio("Navigation", [
    "🏠 Vue globale",
    "🔍 Analyser un bénéficiaire",
    "📋 Cas de fraude",
    "⚙️ Règles métier",
])

# ── Page 1 : Vue globale ─────────────────────────────────────
if page == "🏠 Vue globale":
    st.title("🏠 Tableau de bord — Détection de fraudes OpenG2P")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🔴 Critiques",   "0",    "aujourd'hui")
    c2.metric("🟠 Élevés",      "0")
    c3.metric("🟡 Moyens",      "0")
    c4.metric("🎯 Modèle ML",   "Non entraîné")

    st.info(
        "**Comment démarrer :**\n"
        "1. Entraîner le modèle : `python ml/train.py`\n"
        "2. Analyser un bénéficiaire dans l'onglet **Analyser**\n"
        "3. Consulter les alertes dans **Cas de fraude**"
    )

# ── Page 2 : Analyser ────────────────────────────────────────
elif page == "🔍 Analyser un bénéficiaire":
    st.title("🔍 Analyse d'un bénéficiaire")

    with st.form("score_form"):
        st.subheader("Données du bénéficiaire")
        col1, col2, col3 = st.columns(3)

        bid  = col1.text_input("ID Bénéficiaire", "BEN-001")
        nb_p = col1.number_input("Nb programmes inscrits", 0, 20, 1)
        amt  = col2.number_input("Montant total reçu (FCFA)", 0, 500000, 500)
        rat  = col2.number_input("Ratio montant/moyenne zone", 0.0, 10.0, 1.0)
        chg  = col3.number_input("Changements compte (30j)", 0, 20, 0)
        hh   = col3.number_input("Taille du ménage", 1, 25, 4)
        fail = col1.number_input("Échecs de paiement", 0, 20, 0)
        loc  = col2.slider("Score risque localisation", 0.0, 1.0, 0.3)

        submitted = st.form_submit_button(
            "🔍 Analyser", use_container_width=True
        )

    if submitted:
        payload = {
            "beneficiary_id":        bid,
            "nb_programs":           int(nb_p),
            "total_amount":          float(amt),
            "amount_ratio":          float(rat),
            "nb_cycles":             3,
            "days_since_enrollment": 180,
            "account_changes_30d":   int(chg),
            "household_size":        int(hh),
            "nb_payment_failures":   int(fail),
            "location_risk_score":   float(loc),
        }

        with st.spinner("Analyse en cours..."):
            try:
                resp   = requests.post(
                    f"{API_URL}/api/v1/score",
                    json=payload, timeout=60
                )
                result = resp.json()

                level = result.get("risk_level", "UNKNOWN")
                score = result.get("final_score", 0)
                action = result.get("action", "N/A")

                color = {
                    "LOW":      "🟢",
                    "MEDIUM":   "🟡",
                    "HIGH":     "🟠",
                    "CRITICAL": "🔴"
                }.get(level, "⚪")

                # Metrics
                m1, m2, m3 = st.columns(3)
                m1.metric(f"{color} Niveau", level)
                m2.metric("Score final", f"{score:.0%}")
                m3.metric("Action", action)

                # Score breakdown
                st.subheader("Décomposition du score")
                scores_df = pd.DataFrame({
                    "Composant": ["Règles", "ML", "Graph"],
                    "Score":     [
                        result.get("rule_score", 0),
                        result.get("ml_score",   0),
                        result.get("graph_score",0),
                    ]
                })
                fig = px.bar(
                    scores_df, x="Composant", y="Score",
                    color="Score",
                    color_continuous_scale=["green", "yellow", "red"],
                    range_y=[0, 1],
                    title="Score par composant"
                )
                st.plotly_chart(fig, use_container_width=True)

                # Explanation
                st.subheader("💡 Explication IA")
                st.info(result.get("explanation", "N/A"))

                # Flags
                if result.get("rule_flags"):
                    st.subheader("🚩 Alertes règles métier")
                    for flag in result["rule_flags"]:
                        st.warning(f"⚠️ {flag}")

                # SHAP factors
                if result.get("shap_factors"):
                    st.subheader("📊 Facteurs SHAP (top 3)")
                    shap_df = pd.DataFrame(result["shap_factors"])
                    st.dataframe(shap_df, use_container_width=True)

                # Processing time
                st.caption(
                    f"⚡ Traitement : {result.get('processing_ms', 0)} ms"
                )

            except Exception as e:
                st.error(f"Erreur API : {e}")
                st.warning(
                    "Vérifiez que le fraud-engine est démarré : "
                    "`docker compose up fraud-engine`"
                )

# ── Page 3 : Cas de fraude ───────────────────────────────────
elif page == "📋 Cas de fraude":
    st.title("📋 Manager de cas de fraude")

    try:
        resp  = requests.get(
            f"{API_URL}/api/v1/cases?status=pending", timeout=5
        )
        data  = resp.json()
        cases = data.get("cases", [])

        if cases:
            st.dataframe(pd.DataFrame(cases), use_container_width=True)
        else:
            st.info("Aucun cas en attente de révision.")
    except Exception:
        st.warning("API non disponible — démarrez le fraud-engine.")

# ── Page 4 : Règles ──────────────────────────────────────────
elif page == "⚙️ Règles métier":
    st.title("⚙️ Règles de détection actives")

    try:
        resp  = requests.get(f"{API_URL}/api/v1/rules", timeout=5)
        rules = resp.json()

        st.success(
            f"Version **{rules['version']}** — "
            f"**{rules['count']}** règles actives"
        )
        df = pd.DataFrame(rules["rules"])
        st.dataframe(df, use_container_width=True)

        # Score simulation
        st.subheader("Simulation poids total")
        total = df["weight"].sum()
        st.metric("Poids max possible", f"{total:.2f}")

    except Exception:
        st.warning("API non disponible — démarrez le fraud-engine.")
