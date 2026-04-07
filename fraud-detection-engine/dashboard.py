"""
dashboard.py — Interface de démonstration PFE
Détection de fraude OpenG2P · Halim Trabelsi
=============================================
Lancement : streamlit run dashboard.py
"""

import io
import os

import pandas as pd
import requests
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000/api/v1")

st.set_page_config(
    page_title="Fraud Detection — OpenG2P",
    page_icon="🛡️",
    layout="wide",
)

# ── Helpers ───────────────────────────────────────────────────────────────────

RISK_COLOR = {
    "LOW":      "🟢",
    "MEDIUM":   "🟡",
    "HIGH":     "🟠",
    "CRITICAL": "🔴",
}

def risk_badge(level: str) -> str:
    icon = RISK_COLOR.get(str(level).upper(), "⚪")
    return f"{icon} **{level}**"


def score_bar(value: float, label: str):
    pct = int(value * 100)
    color = (
        "#2ecc71" if pct < 35
        else "#f39c12" if pct < 65
        else "#e74c3c"
    )
    st.markdown(
        f"**{label}** — `{pct}%`\n"
        f"<div style='background:{color};width:{pct}%;height:10px;"
        f"border-radius:5px;margin-bottom:6px'></div>",
        unsafe_allow_html=True,
    )


def show_result_card(result: dict):
    col1, col2 = st.columns([1, 2])
    with col1:
        st.markdown(f"### {risk_badge(result.get('risk_level', 'LOW'))}")
        st.metric("Final score",   f"{result.get('final_score', 0):.3f}")
        st.metric("ML score",      f"{result.get('ml_score', 0):.3f}")
        st.metric("Rule score",    f"{result.get('rule_score', 0):.3f}")
        st.metric("Graph score",   f"{result.get('graph_score', 0):.3f}")
    with col2:
        score_bar(result.get("ml_score",    0), "ML score")
        score_bar(result.get("rule_score",  0), "Rule score")
        score_bar(result.get("graph_score", 0), "Graph score")
        score_bar(result.get("final_score", 0), "Score final")
        st.info(f"**Action recommandée :** {result.get('recommended_action', '—')}")
        if result.get("explanation"):
            with st.expander("Explication détaillée"):
                st.write(result["explanation"])


def api_post(endpoint: str, **kwargs) -> dict | None:
    try:
        r = requests.post(f"{API_BASE}{endpoint}", timeout=30, **kwargs)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        st.error("❌ API inaccessible. Vérifiez que le serveur FastAPI tourne sur `localhost:8000`.")
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ Erreur API ({e.response.status_code}) : {e.response.text}")
    except Exception as e:
        st.error(f"❌ Erreur inattendue : {e}")
    return None


def api_get(endpoint: str) -> dict | None:
    try:
        r = requests.get(f"{API_BASE}{endpoint}", timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        st.error("❌ API inaccessible. Vérifiez que le serveur FastAPI tourne sur `localhost:8000`.")
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ Erreur API ({e.response.status_code}) : {e.response.text}")
    except Exception as e:
        st.error(f"❌ Erreur inattendue : {e}")
    return None


# ── Header ────────────────────────────────────────────────────────────────────

st.title("🛡️ Fraud Detection Engine — OpenG2P")
st.caption("Interface de démonstration PFE · Halim Trabelsi")
st.divider()

tab1, tab2, tab3 = st.tabs([
    "📝  Score manuel",
    "🔍  Scan bénéficiaire",
    "📂  Upload CSV batch",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Score manuel
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Scoring manuel d'un profil")
    st.caption("Saisissez les valeurs du bénéficiaire pour obtenir un score en temps réel.")

    with st.form("manual_score_form"):
        c1, c2, c3 = st.columns(3)

        with c1:
            st.markdown("**Démographie**")
            age            = st.number_input("Âge",            min_value=1,   max_value=100, value=35)
            income         = st.number_input("Revenu (USD)",   min_value=0.0, value=600.0,   step=50.0)
            household_size = st.number_input("Taille ménage",  min_value=1,   max_value=20,  value=4)
            nb_children    = st.number_input("Nb enfants",     min_value=0,   max_value=15,  value=2)
            nb_elderly     = st.number_input("Nb âgés",        min_value=0,   max_value=10,  value=0)
            has_disabled   = st.selectbox("Personne handicapée", [0, 1])
            single_head    = st.selectbox("Chef seul",         [0, 1])

        with c2:
            st.markdown("**Programmes & Paiements**")
            nb_programs         = st.number_input("Nb programmes",    min_value=0, max_value=5,   value=1)
            nb_active_programs  = st.number_input("Programmes actifs",min_value=0, max_value=5,   value=1)
            pmt_score           = st.slider("PMT score",              0.0, 1.0, 0.6, 0.01)
            pmt_score_min       = st.slider("PMT score min",          0.0, 1.0, 0.5, 0.01)
            avg_enrollment_days = st.number_input("Jours inscription", min_value=0, value=365)
            payment_count       = st.number_input("Nb paiements",     min_value=0, value=3)
            payment_gap_ratio   = st.slider("Gap ratio paiements",    0.0, 1.0, 0.1, 0.01)
            payment_success_rate= st.slider("Taux succès paiements",  0.0, 1.0, 0.9, 0.01)

        with c3:
            st.markdown("**Réseau & Flags**")
            shared_phone_count   = st.number_input("Téléphones partagés",  min_value=0, max_value=10, value=0)
            shared_account_count = st.number_input("Comptes partagés",     min_value=0, max_value=10, value=0)
            network_risk         = st.slider("Score réseau",               0.0, 1.0, 0.0, 0.01)
            group_membership_count = st.number_input("Appartenance groupe",min_value=0, max_value=5,  value=0)
            high_amount_flag     = st.selectbox("Montant élevé (flag)",    [0, 1])
            income_program_inconsistency = st.selectbox("Incohérence revenu/prog", [0, 1])
            cycle_count          = st.number_input("Cycles",               min_value=0, max_value=20, value=3)
            amount_variance      = st.number_input("Variance montants",    min_value=0.0, value=0.0,  step=100.0)

        submitted = st.form_submit_button("🚀 Scorer", use_container_width=True, type="primary")

    if submitted:
        income_per_person   = income / max(household_size, 1)
        adults              = max(household_size - nb_children - nb_elderly, 1)
        dependency_ratio    = (nb_children + nb_elderly) / adults

        payload = {
            "age": age, "income": income,
            "income_per_person": round(income_per_person, 2),
            "household_size": household_size,
            "nb_children": nb_children, "nb_elderly": nb_elderly,
            "dependency_ratio": round(dependency_ratio, 3),
            "has_disabled": has_disabled, "single_head": single_head,
            "nb_programs": nb_programs, "nb_active_programs": nb_active_programs,
            "pmt_score": pmt_score, "pmt_score_min": pmt_score_min,
            "avg_enrollment_days": avg_enrollment_days,
            "payment_count": payment_count,
            "payment_gap_ratio": payment_gap_ratio,
            "payment_success_rate": payment_success_rate,
            "amount_variance": amount_variance, "cycle_count": cycle_count,
            "shared_phone_count": shared_phone_count,
            "shared_account_count": shared_account_count,
            "network_risk": network_risk,
            "group_membership_count": group_membership_count,
            "high_amount_flag": high_amount_flag,
            "income_program_inconsistency": income_program_inconsistency,
        }

        with st.spinner("Scoring en cours..."):
            result = api_post("/score", json=payload)

        if result:
            st.success("Résultat du scoring")
            show_result_card(result)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Scan bénéficiaire
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Scan d'un bénéficiaire depuis OpenG2P")
    st.caption("Entrez l'identifiant d'un bénéficiaire enregistré dans OpenG2P.")

    with st.form("scan_form"):
        beneficiary_id = st.number_input(
            "ID bénéficiaire (partner_id OpenG2P)",
            min_value=1, step=1, value=1,
        )
        scan_submitted = st.form_submit_button("🔍 Scanner", use_container_width=True, type="primary")

    if scan_submitted:
        with st.spinner(f"Extraction des features et scoring du bénéficiaire #{beneficiary_id}..."):
            result = api_get(f"/scan/{beneficiary_id}")

        if result:
            if not result.get("ready"):
                st.warning(f"Modèle non prêt : {result.get('error')}")
            else:
                st.success(f"Bénéficiaire #{beneficiary_id} — résultat")
                show_result_card(result)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Upload CSV batch
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Scoring batch par upload CSV")
    st.caption(
        "Uploadez un fichier CSV contenant les features des bénéficiaires. "
        "Les colonnes manquantes sont complétées automatiquement."
    )

    with st.expander("ℹ️ Colonnes requises dans le CSV"):
        st.markdown("""
| Colonne | Obligatoire | Description |
|---|:---:|---|
| `income` | ✅ | Revenu déclaré |
| `nb_programs` | ✅ | Nombre de programmes |
| `pmt_score` | ✅ | Score PMT |
| `payment_gap_ratio` | ✅ | Ratio de paiements manquants |
| `partner_id` / `beneficiary_id` | — | Identifiant (conservé dans la réponse) |
| `scenario`, `is_fraud` | — | Labels synthétiques (conservés pour analyse) |
| *Autres features* | — | Complétées avec valeurs par défaut si absentes |
        """)

    uploaded_file = st.file_uploader(
        "Choisir un fichier CSV",
        type=["csv"],
        help="Maximum 5 000 lignes.",
    )

    col_fmt, col_btn = st.columns([1, 2])
    with col_fmt:
        response_format = st.radio("Format de réponse", ["JSON (tableau)", "CSV (téléchargement)"],
                                   horizontal=True)

    if uploaded_file is not None:
        # Aperçu
        try:
            preview_df = pd.read_csv(uploaded_file)
            uploaded_file.seek(0)
            st.markdown(f"**Aperçu** — {len(preview_df)} lignes · {preview_df.shape[1]} colonnes")
            st.dataframe(preview_df.head(5), use_container_width=True)
        except Exception as e:
            st.error(f"Impossible de lire le CSV : {e}")
            st.stop()

        fmt_param = "csv" if "CSV" in response_format else "json"

        if st.button("🚀 Lancer le scoring batch", type="primary", use_container_width=True):
            with st.spinner(f"Scoring de {len(preview_df)} lignes en cours..."):
                try:
                    uploaded_file.seek(0)
                    r = requests.post(
                        f"{API_BASE}/score/upload",
                        params={"format": fmt_param},
                        files={"file": (uploaded_file.name, uploaded_file, "text/csv")},
                        timeout=120,
                    )
                    r.raise_for_status()
                except requests.exceptions.ConnectionError:
                    st.error("❌ API inaccessible.")
                    st.stop()
                except requests.exceptions.HTTPError as e:
                    st.error(f"❌ Erreur API ({e.response.status_code}) : {e.response.text}")
                    st.stop()

            if fmt_param == "csv":
                st.success("Scoring terminé — téléchargez le fichier enrichi.")
                st.download_button(
                    label="⬇️ Télécharger les résultats (.csv)",
                    data=r.content,
                    file_name=uploaded_file.name.replace(".csv", "_scored.csv"),
                    mime="text/csv",
                    use_container_width=True,
                )
            else:
                data = r.json()
                results = data.get("results", [])
                st.success(f"Scoring terminé — **{data.get('count', 0)}** lignes traitées.")

                if results:
                    df_results = pd.DataFrame(results)

                    # ── Statistiques rapides ─────────────────────────────
                    st.markdown("### Résumé")
                    s1, s2, s3, s4 = st.columns(4)
                    s1.metric("Total scorés",  len(df_results))
                    s2.metric("HIGH / CRITICAL",
                              int((df_results["risk_level"].isin(["HIGH","CRITICAL"])).sum()))
                    s3.metric("Score final moyen",
                              f"{df_results['final_score'].mean():.3f}")
                    if "is_fraud" in df_results.columns and df_results["is_fraud"].notna().any():
                        s4.metric("Vrais fraudeurs (label)",
                                  int(df_results["is_fraud"].sum()))

                    # ── Distribution des niveaux de risque ───────────────
                    st.markdown("### Distribution des niveaux de risque")
                    risk_counts = (
                        df_results["risk_level"]
                        .value_counts()
                        .reindex(["LOW", "MEDIUM", "HIGH", "CRITICAL"], fill_value=0)
                    )
                    st.bar_chart(risk_counts)

                    # ── Tableau complet ───────────────────────────────────
                    st.markdown("### Résultats détaillés")
                    display_cols = [c for c in [
                        "beneficiary_id", "scenario", "is_fraud",
                        "final_score", "ml_score", "rule_score", "graph_score",
                        "risk_level", "ml_prediction", "recommended_action",
                    ] if c in df_results.columns]

                    st.dataframe(
                        df_results[display_cols].style.background_gradient(
                            subset=["final_score"], cmap="RdYlGn_r"
                        ),
                        use_container_width=True,
                        height=420,
                    )

                    # ── Téléchargement JSON ───────────────────────────────
                    csv_bytes = df_results.to_csv(index=False).encode()
                    st.download_button(
                        label="⬇️ Télécharger les résultats (.csv)",
                        data=csv_bytes,
                        file_name=uploaded_file.name.replace(".csv", "_scored.csv"),
                        mime="text/csv",
                        use_container_width=True,
                    )

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    f"API : `{API_BASE}` · "
    "[Swagger UI](http://localhost:8000/docs) · "
    "PFE 2025 — Halim Trabelsi"
)
