"""
=============================================================
Train Script — Modèles hybrides fraude OpenG2P
=============================================================
Ce script :
1. Charge le dataset enrichi (issu du seed)
2. Vérifie / complète les features avec fallback
3. Entraîne 3 modèles (LogReg, RandomForest, IsolationForest)
4. Calcule les scores hybrides (rule/ml/anomaly/graph)
5. Sauvegarde modèles + dataset scoré

Usage :
    python ml/train_openg2p.py
    # ou
    python train_openg2p.py
=============================================================
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from joblib import dump

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score


# ── Configuration ─────────────────────────────────────────────
SEED = 42
np.random.seed(SEED)

DATA_CANDIDATES = [
    Path("ml/data/openg2p_features.csv"),
    Path("data/openg2p_features.csv"),
    Path("openg2p_features.csv"),
]
MODELS_DIR = Path("ml/models")
OUTPUT_SCORED = Path("ml/data/openg2p_scored.csv")

FEATURE_DEFAULTS = {
    "age": 35,
    "gender": "unknown",
    "income": 0.0,
    "household_size": 1.0,
    "dependency_ratio": 0.0,
    "income_per_person": 0.0,
    "shared_phone_count": 0.0,
    "shared_account_count": 0.0,
    "nb_programs": 0.0,
    "program_overlap_flag": 0.0,
    "pmt_score": 0.5,
    "total_amount_issued": 0.0,
    "total_amount_paid": 0.0,
    "payment_gap": 0.0,
    "payment_gap_ratio": 0.0,
    "payment_count": 0.0,
    "payment_count_in_cycle": 0.0,
    "active_group_memberships": 0.0,
}


def load_data() -> pd.DataFrame:
    data_path = os.getenv("OPENG2P_FEATURES_PATH")
    if data_path:
        path = Path(data_path)
        if not path.exists():
            raise FileNotFoundError(f"Fichier introuvable: {path}")
        return pd.read_csv(path)

    for p in DATA_CANDIDATES:
        if p.exists():
            return pd.read_csv(p)

    raise FileNotFoundError(
        "Aucune donnée trouvée. Lance d'abord : python ml/seed_openg2p_data.py"
    )


def with_fallback_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Compatibilité d'ID
    if "partner_id" in out.columns and "id" not in out.columns:
        out["id"] = out["partner_id"]
    elif "id" not in out.columns:
        out["id"] = np.arange(1, len(out) + 1)

    for col, default in FEATURE_DEFAULTS.items():
        if col not in out.columns:
            out[col] = default

    # Label fallback si absent
    if "is_fraud" not in out.columns and "is_suspicious" not in out.columns:
        rule = (
            (out["shared_account_count"] > 0).astype(int) * 0.5
            + (out["shared_phone_count"] > 0).astype(int) * 0.4
            + (out["payment_gap_ratio"] > 0.5).astype(int) * 0.3
            + (out["nb_programs"] > 3).astype(int) * 0.3
            + (out["payment_count_in_cycle"] > out["payment_count_in_cycle"].quantile(0.9)).astype(int) * 0.2
        )
        out["is_fraud"] = (rule >= 0.5).astype(int)
    elif "is_suspicious" in out.columns and "is_fraud" not in out.columns:
        out["is_fraud"] = out["is_suspicious"].astype(int)

    return out


def make_rule_graph_scores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["rule_score"] = (
        (out["shared_account_count"] > 0).astype(float) * 0.30
        + (out["shared_phone_count"] > 0).astype(float) * 0.25
        + (out["payment_gap_ratio"] > 0.5).astype(float) * 0.20
        + (out["nb_programs"] > 3).astype(float) * 0.15
        + (out["payment_count_in_cycle"] > out["payment_count_in_cycle"].quantile(0.9)).astype(float) * 0.10
    ).clip(0, 1)

    out["graph_score"] = (
        np.tanh(out["shared_phone_count"] / 3.0) * 0.45
        + np.tanh(out["shared_account_count"] / 3.0) * 0.45
        + (out["nb_programs"] > 3).astype(float) * 0.10
    ).clip(0, 1)

    return out


def build_pipeline(numeric_cols, categorical_cols):
    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
        ]
    )

    return preprocessor


def train_models(df: pd.DataFrame) -> Tuple[Dict, pd.DataFrame]:
    target = "is_fraud"

    feature_cols = [
        "age", "gender", "income", "household_size", "dependency_ratio", "income_per_person",
        "shared_phone_count", "shared_account_count", "nb_programs", "program_overlap_flag",
        "pmt_score", "total_amount_issued", "total_amount_paid", "payment_gap", "payment_gap_ratio",
        "payment_count", "payment_count_in_cycle", "active_group_memberships",
    ]

    X = df[feature_cols].copy()
    y = df[target].astype(int)

    numeric_cols = [c for c in feature_cols if c != "gender"]
    categorical_cols = ["gender"]

    preprocessor = build_pipeline(numeric_cols, categorical_cols)

    x_train, x_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=SEED,
        stratify=y if y.nunique() > 1 else None,
    )

    logreg = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("clf", LogisticRegression(max_iter=800, class_weight="balanced", random_state=SEED)),
        ]
    )
    rf = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("clf", RandomForestClassifier(
                n_estimators=300,
                max_depth=10,
                min_samples_leaf=4,
                class_weight="balanced",
                random_state=SEED,
                n_jobs=-1,
            )),
        ]
    )

    logreg.fit(x_train, y_train)
    rf.fit(x_train, y_train)

    # Isolation Forest sur features numériques
    iso = IsolationForest(
        n_estimators=300,
        contamination=0.08,
        random_state=SEED,
        n_jobs=-1,
    )
    iso.fit(df[numeric_cols].fillna(0.0))

    # Évaluation rapide
    log_proba = logreg.predict_proba(x_test)[:, 1]
    rf_proba = rf.predict_proba(x_test)[:, 1]

    try:
        auc_log = roc_auc_score(y_test, log_proba)
        auc_rf = roc_auc_score(y_test, rf_proba)
    except Exception:
        auc_log, auc_rf = float("nan"), float("nan")

    print("\n📊 Rapport Logistic Regression")
    print(classification_report(y_test, (log_proba >= 0.5).astype(int), digits=3))
    print(f"AUC: {auc_log:.4f}")

    print("\n📊 Rapport Random Forest")
    print(classification_report(y_test, (rf_proba >= 0.5).astype(int), digits=3))
    print(f"AUC: {auc_rf:.4f}")

    model_pack = {
        "logreg": logreg,
        "rf": rf,
        "iso": iso,
        "feature_cols": feature_cols,
        "numeric_cols": numeric_cols,
    }
    return model_pack, df


def score_dataset(df: pd.DataFrame, model_pack: Dict) -> pd.DataFrame:
    out = make_rule_graph_scores(df.copy())

    X = out[model_pack["feature_cols"]]

    proba_log = model_pack["logreg"].predict_proba(X)[:, 1]
    proba_rf = model_pack["rf"].predict_proba(X)[:, 1]
    out["ml_score"] = (0.35 * proba_log + 0.65 * proba_rf).clip(0, 1)

    # IsolationForest: decision_function -> plus petit = plus anormal
    decision = model_pack["iso"].decision_function(out[model_pack["numeric_cols"]].fillna(0.0))
    # normalisation inverse en [0,1]
    d_min, d_max = float(np.min(decision)), float(np.max(decision))
    if d_max - d_min < 1e-9:
        out["anomaly_score"] = 0.0
    else:
        anomaly_norm = 1 - ((decision - d_min) / (d_max - d_min))
        out["anomaly_score"] = np.clip(anomaly_norm, 0, 1)

    out["risk_score_rule"] = out["rule_score"]
    out["risk_score"] = (
        0.30 * out["rule_score"]
        + 0.40 * out["ml_score"]
        + 0.20 * out["anomaly_score"]
        + 0.10 * out["graph_score"]
    ).clip(0, 1)

    out["is_suspicious"] = (out["risk_score"] >= 0.55).astype(int)
    out["risk_level"] = pd.cut(
        out["risk_score"],
        bins=[-0.001, 0.35, 0.65, 1.0],
        labels=["low", "medium", "high"],
    ).astype(str)

    # cycle_duration_days fallback
    if "cycle_duration_days" not in out.columns:
        out["cycle_duration_days"] = 90

    # Colonnes finales demandées
    final_cols = [
        "id",
        "age", "gender", "income",
        "household_size", "dependency_ratio",
        "shared_phone_count",
        "shared_account_count",
        "nb_programs",
        "total_amount_issued",
        "total_amount_paid",
        "payment_gap_ratio",
        "payment_count_in_cycle",
        "pmt_score",
        "cycle_duration_days",
        "risk_score_rule",
        "is_suspicious",
        "risk_level",
        "rule_score", "ml_score", "anomaly_score", "graph_score", "risk_score",
        "is_fraud",
    ]
    existing = [c for c in final_cols if c in out.columns]
    return out[existing].copy()


def save_artifacts(model_pack: Dict, scored_df: pd.DataFrame):
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_SCORED.parent.mkdir(parents=True, exist_ok=True)

    dump(model_pack["logreg"], MODELS_DIR / "logreg.joblib")
    dump(model_pack["rf"], MODELS_DIR / "random_forest.joblib")
    dump(model_pack["iso"], MODELS_DIR / "isolation_forest.joblib")

    metadata = {
        "feature_cols": model_pack["feature_cols"],
        "numeric_cols": model_pack["numeric_cols"],
        "rows": int(len(scored_df)),
        "fraud_rate_pred": float(scored_df["is_suspicious"].mean()) if "is_suspicious" in scored_df else None,
    }
    (MODELS_DIR / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    scored_df.to_csv(OUTPUT_SCORED, index=False)

    print("\n✅ Artefacts sauvegardés")
    print(f"   - {MODELS_DIR / 'logreg.joblib'}")
    print(f"   - {MODELS_DIR / 'random_forest.joblib'}")
    print(f"   - {MODELS_DIR / 'isolation_forest.joblib'}")
    print(f"   - {MODELS_DIR / 'metadata.json'}")
    print(f"   - {OUTPUT_SCORED}")


def train():
    print("\n🚀 Chargement des données OpenG2P...")
    df_raw = load_data()
    print(f"✅ Dataset chargé: {df_raw.shape}")

    df = with_fallback_columns(df_raw)
    print(f"✅ Dataset après fallback colonnes: {df.shape}")

    model_pack, df_train = train_models(df)
    scored_df = score_dataset(df_train, model_pack)

    print("\n📈 Distribution des risques")
    if "risk_level" in scored_df:
        print(scored_df["risk_level"].value_counts(dropna=False).to_string())

    save_artifacts(model_pack, scored_df)
    print("\n🎯 Entraînement terminé.")


if __name__ == "__main__":
    train()
