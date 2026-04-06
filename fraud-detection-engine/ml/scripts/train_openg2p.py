"""
Train Script — Modèles hybrides fraude OpenG2P (FINAL ROBUST)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
from joblib import dump

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score


# ─────────────────────────────
SEED = 42
np.random.seed(SEED)

MODELS_DIR = Path("ml/models")
OUTPUT_SCORED = Path("ml/data/openg2p_scored.csv")


# ─────────────────────────────
def load_data():
    return pd.read_csv("../data/openg2p_features.csv")


# ─────────────────────────────
def with_fallback_columns(df: pd.DataFrame):
    out = df.copy()

    # ID
    if "partner_id" not in out.columns:
        out["partner_id"] = np.arange(len(out))

    # LABEL
    if "is_fraud" not in out.columns:
        if "synthetic_label" in out.columns:
            out["is_fraud"] = out["synthetic_label"]
        else:
            out["is_fraud"] = 0

    # FALLBACK FEATURES
    defaults = {
        "income": 0.0,
        "income_per_person": 0.0,
        "pmt_score": 0.5,
        "nb_programs": 0,
        "payment_count": 1,
        "household_size": 1,
        "nb_children": 0,
        "nb_elderly": 0,
    }

    for col, val in defaults.items():
        if col not in out.columns:
            out[col] = val

    # RULE FEATURES fallback
    if "payment_gap_ratio" not in out.columns:
        if "gap_ratio" in out.columns:
            out["payment_gap_ratio"] = out["gap_ratio"]
        else:
            out["payment_gap_ratio"] = 0.0

    for col in ["shared_phone_count", "shared_account_count"]:
        if col not in out.columns:
            out[col] = 0

    # FEATURES DERIVEES
    out["dependency_ratio"] = (out["nb_children"] + out["nb_elderly"]) / (out["household_size"] + 1)
    out["risk_density"] = out["nb_programs"] / (out["household_size"] + 1)
    out["financial_stress"] = out["income_per_person"] / (out["pmt_score"] + 0.01)
    out["behavior_score"] = out["payment_count"] / (out["nb_programs"] + 1)

    return out


# ─────────────────────────────
def make_rule_graph_scores(df: pd.DataFrame):
    out = df.copy()

    def safe(col, default=0):
        return out[col] if col in out.columns else default

    out["rule_score"] = (
        (safe("shared_account_count") > 0) * 0.30
        + (safe("shared_phone_count") > 0) * 0.25
        + (safe("payment_gap_ratio") > 0.5) * 0.20
        + (safe("nb_programs") > 3) * 0.15
    ).clip(0, 1)

    out["graph_score"] = (
        np.tanh(safe("shared_phone_count") / 3.0) * 0.5
        + np.tanh(safe("shared_account_count") / 3.0) * 0.5
    ).clip(0, 1)

    return out


# ─────────────────────────────
def build_pipeline(numeric_cols):
    return ColumnTransformer([
        ("num", Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler())
        ]), numeric_cols)
    ])


# ─────────────────────────────
def train_models(df):

    target = "is_fraud"

    feature_cols = [
        "age", "income", "household_size",
        "dependency_ratio", "income_per_person",
        "pmt_score",
        "risk_density", "financial_stress", "behavior_score"
    ]

    existing_features = [f for f in feature_cols if f in df.columns]

    print("\n🧪 Features utilisées:")
    print(existing_features)

    X = df[existing_features]
    y = df[target]

    preprocessor = build_pipeline(existing_features)

    x_train, x_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=SEED
    )

    logreg = Pipeline([
        ("prep", preprocessor),
        # C=0.1 → regularisation L2 forte pour eviter l'overfitting
        ("clf", LogisticRegression(max_iter=800, C=0.1, class_weight="balanced"))
    ])

    rf = Pipeline([
        ("prep", preprocessor),
        ("clf", RandomForestClassifier(
            n_estimators=200,       # reduit de 400
            max_depth=8,            # limite la profondeur (etait illimite)
            min_samples_leaf=10,    # au moins 10 samples par feuille
            max_features="sqrt",    # sous-echantillonnage features
            class_weight="balanced",
            random_state=SEED,
        ))
    ])

    logreg.fit(x_train, y_train)
    rf.fit(x_train, y_train)

    # Evaluation
    log_proba = logreg.predict_proba(x_test)[:, 1]
    rf_proba = rf.predict_proba(x_test)[:, 1]

    print("\n📊 Logistic Regression")
    print(classification_report(y_test, (log_proba >= 0.5).astype(int)))
    print("AUC:", roc_auc_score(y_test, log_proba))

    print("\n📊 Random Forest")
    print(classification_report(y_test, (rf_proba >= 0.5).astype(int)))
    print("AUC:", roc_auc_score(y_test, rf_proba))

    # Importance
    print("\n🧠 Feature importance:")
    importances = rf.named_steps["clf"].feature_importances_
    for f, imp in sorted(zip(existing_features, importances), key=lambda x: -x[1]):
        print(f"{f}: {round(imp,3)}")

    # contamination=0.12 correspond au vrai taux de fraude dans le dataset
    iso = IsolationForest(n_estimators=200, contamination=0.12, random_state=SEED)
    iso.fit(df[existing_features])

    return {
        "logreg": logreg,
        "rf": rf,
        "iso": iso,
        "feature_cols": existing_features
    }


# ─────────────────────────────
def score_dataset(df, model_pack):

    out = make_rule_graph_scores(df.copy())

    X = out[model_pack["feature_cols"]]

    out["ml_score"] = model_pack["rf"].predict_proba(X)[:, 1]

    decision = model_pack["iso"].decision_function(X)
    out["anomaly_score"] = 1 - (decision - decision.min()) / (decision.max() - decision.min())

    out["risk_score"] = (
        0.35 * out["rule_score"]
        + 0.35 * out["ml_score"]
        + 0.20 * out["anomaly_score"]
        + 0.10 * out["graph_score"]
    )

    out["risk_level"] = pd.cut(
        out["risk_score"],
        bins=[-1, 0.35, 0.65, 1],
        labels=["low", "medium", "high"]
    )

    return out


# ─────────────────────────────
def save_artifacts(model_pack, df):

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_SCORED.parent.mkdir(parents=True, exist_ok=True)

    dump(model_pack["rf"], MODELS_DIR / "random_forest.joblib")
    dump(model_pack["logreg"], MODELS_DIR / "logreg.joblib")
    dump(model_pack["iso"], MODELS_DIR / "isolation_forest.joblib")

    df.to_csv(OUTPUT_SCORED, index=False)

    print("\n✅ Sauvegarde OK")


# ─────────────────────────────
def train():

    print("\n🚀 Chargement données...")
    df = load_data()

    df = with_fallback_columns(df)

    model_pack = train_models(df)

    scored = score_dataset(df, model_pack)

    print("\n📈 Distribution:")
    print(scored["risk_level"].value_counts())

    save_artifacts(model_pack, scored)

    print("\n🎯 DONE")


# ─────────────────────────────
if __name__ == "__main__":
    train()