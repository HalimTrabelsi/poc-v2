"""generate_report_figures.py — Produces the real figures used in the
Réalisation chapter of docs/Rapport/main.tex: class balance, ROC curves,
Isolation Forest anomaly distribution, and SHAP beeswarm/waterfall.

Rebuilds the exact same holdout split as train_openg2p.py (same SEED) so
every figure matches the metrics reported in the text and tables.

Usage:
    python ml/scripts/generate_report_figures.py
"""
import sys
from pathlib import Path

import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
from sklearn.metrics import roc_curve, roc_auc_score
from sklearn.model_selection import train_test_split

sys.path.insert(0, str(Path(__file__).resolve().parent))
from train_openg2p import load_data, prepare_features, TARGET, SEED, MODELS_DIR

OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "generated" / "figures"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Palette aligned with the report's EY colors
EY_DARK = "#2E2E38"
EY_YELLOW = "#FFE600"
RED = "#C62828"
GREEN = "#2E7D32"
ORANGE = "#E08600"


def rebuild_holdout():
    df = load_data()
    df, feature_cols = prepare_features(df)
    X, y = df[feature_cols], df[TARGET]
    X_trainval, X_holdout, y_trainval, y_holdout = train_test_split(
        X, y, test_size=0.15, stratify=y, random_state=SEED
    )
    return X_trainval, X_holdout, y_trainval, y_holdout, feature_cols


def fig_class_balance(y_trainval):
    counts = y_trainval.value_counts().sort_index()
    labels = ["Légitime", "Fraude"]
    values = [counts.get(0, 0), counts.get(1, 0)]
    pct = [v / sum(values) * 100 for v in values]

    fig, ax = plt.subplots(figsize=(5, 4))
    bars = ax.bar(labels, values, color=[EY_DARK, RED])
    for bar, v, p in zip(bars, values, pct):
        ax.text(bar.get_x() + bar.get_width() / 2, v, f"{v}\n({p:.1f}%)",
                ha="center", va="bottom", fontsize=11)
    ax.set_ylabel("Nombre de bénéficiaires")
    ax.set_title("Répartition des classes (données d'entraînement)")
    ax.set_ylim(0, max(values) * 1.2)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "class_balance.png", dpi=150)
    plt.close(fig)


def fig_roc_comparison(models, model_names, meta_model, meta_names, X_holdout, y_holdout):
    fig, ax = plt.subplots(figsize=(6, 5.5))
    colors = {"random_forest": EY_DARK, "xgboost": RED, "logreg": ORANGE}
    display_names = {"random_forest": "Random Forest", "xgboost": "XGBoost", "logreg": "Régression Logistique"}

    base_holdout_proba = {}
    for name in model_names:
        proba = models[name].predict_proba(X_holdout)[:, 1]
        base_holdout_proba[name] = proba
        fpr, tpr, _ = roc_curve(y_holdout, proba)
        auc = roc_auc_score(y_holdout, proba)
        ax.plot(fpr, tpr, label=f"{display_names[name]} (AUC={auc:.3f})",
                color=colors.get(name), linewidth=1.8)

    S_holdout = np.column_stack([base_holdout_proba[n] for n in meta_names])
    ensemble_proba = meta_model.predict_proba(S_holdout)[:, 1]
    fpr, tpr, _ = roc_curve(y_holdout, ensemble_proba)
    auc = roc_auc_score(y_holdout, ensemble_proba)
    ax.plot(fpr, tpr, label=f"Ensemble stacking (AUC={auc:.3f})",
            color=GREEN, linewidth=2.6, linestyle="--")

    ax.plot([0, 1], [0, 1], color="gray", linestyle=":", linewidth=1)
    ax.set_xlabel("Taux de faux positifs")
    ax.set_ylabel("Taux de vrais positifs")
    ax.set_title("Courbes ROC — modèles de base et ensemble")
    ax.legend(loc="lower right", fontsize=9)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "roc_comparaison_modeles.png", dpi=150)
    plt.close(fig)
    return ensemble_proba


def fig_isolation_forest(X_holdout, y_holdout):
    iso_path = MODELS_DIR / "isolation_forest.joblib"
    if not iso_path.exists():
        print("⚠️  isolation_forest.joblib introuvable — figure ignorée")
        return
    iso = joblib.load(iso_path)
    raw_score = iso.score_samples(X_holdout)
    anomaly_score = np.clip((-raw_score - 0.1) / 0.4, 0, 1)

    fig, ax = plt.subplots(figsize=(6, 4.5))
    ax.hist(anomaly_score[y_holdout == 0], bins=25, alpha=0.6, label="Légitime", color=EY_DARK, density=True)
    ax.hist(anomaly_score[y_holdout == 1], bins=25, alpha=0.6, label="Fraude", color=RED, density=True)
    ax.set_xlabel("Score d'anomalie (Isolation Forest)")
    ax.set_ylabel("Densité")
    ax.set_title("Distribution du score d'anomalie par classe")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "isolation_forest_distribution.png", dpi=150)
    plt.close(fig)


def fig_shap(models, X_holdout, y_holdout, feature_cols):
    xgb_pipeline = models["xgboost"]
    # Unwrap the raw booster the same way explainability_service.py does,
    # and transform features through the pipeline's own preprocessor so
    # SHAP sees the exact columns the booster was trained on.
    prep = xgb_pipeline.named_steps["prep"]
    clf = xgb_pipeline.named_steps["clf"]
    X_transformed = prep.transform(X_holdout)

    explainer = shap.TreeExplainer(clf)
    shap_values = explainer.shap_values(X_transformed)
    if isinstance(shap_values, list):
        shap_values = shap_values[1]

    X_display = pd.DataFrame(X_transformed, columns=feature_cols)

    plt.figure(figsize=(8, 7))
    shap.summary_plot(shap_values, X_display, show=False, max_display=15)
    plt.title("Importance des features (SHAP beeswarm) — XGBoost")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "shap_beeswarm.png", dpi=150)
    plt.close()

    # Waterfall for one real fraud case with a high predicted probability
    fraud_idx = np.where(y_holdout.to_numpy() == 1)[0]
    proba = clf.predict_proba(X_transformed)[:, 1]
    target_idx = fraud_idx[np.argmax(proba[fraud_idx])]

    # Display the ORIGINAL (untransformed) feature values in the plot labels
    # — shap_values were computed on the scaled data, but scaling is linear
    # so each SHAP value still lines up with its raw feature 1:1; showing
    # raw values (e.g. network_risk=0.42) instead of z-scores (=4.2) is what
    # makes the figure readable in the report.
    raw_row = X_holdout.iloc[target_idx][feature_cols].to_numpy()
    expl = shap.Explanation(
        values=shap_values[target_idx],
        base_values=explainer.expected_value,
        data=raw_row,
        feature_names=feature_cols,
    )
    plt.figure(figsize=(8, 6))
    shap.plots.waterfall(expl, max_display=12, show=False)
    plt.title("Facteurs explicatifs (SHAP) — cas de fraude individuel")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "shap_waterfall.png", dpi=150)
    plt.close()


def main():
    X_trainval, X_holdout, y_trainval, y_holdout, feature_cols = rebuild_holdout()

    model_names = ["random_forest", "xgboost", "logreg"]
    models = {name: joblib.load(MODELS_DIR / f"{name}.joblib") for name in model_names}
    meta_model = joblib.load(MODELS_DIR / "meta_ensemble.joblib")
    meta_names = model_names  # metadata.json's meta_model_input_order

    import json
    meta_names = json.loads((MODELS_DIR / "metadata.json").read_text())["meta_model_input_order"]

    print("Génération : répartition des classes...")
    fig_class_balance(y_trainval)

    print("Génération : courbes ROC...")
    fig_roc_comparison(models, model_names, meta_model, meta_names, X_holdout, y_holdout)

    print("Génération : distribution Isolation Forest...")
    fig_isolation_forest(X_holdout, y_holdout)

    print("Génération : SHAP (beeswarm + waterfall)...")
    fig_shap(models, X_holdout, y_holdout, feature_cols)

    print(f"\n✅ Figures sauvegardées dans {OUT_DIR}")


if __name__ == "__main__":
    main()
