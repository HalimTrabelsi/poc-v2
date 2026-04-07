"""
Train Script — Modèles hybrides fraude OpenG2P
===============================================
Features alignées avec feature_extractor.py (schéma officiel).
Modèles : Logistic Regression, Random Forest, Isolation Forest.
Structure prête pour l'ajout d'un challenger XGBoost.
"""

from __future__ import annotations

import json
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


SEED = 42
np.random.seed(SEED)

SCRIPT_DIR  = Path(__file__).resolve().parent
REPO_ROOT   = SCRIPT_DIR.parents[2]          # fraud-detection-engine/
MODELS_DIR  = REPO_ROOT / "models_saved"     # read by the API service
DATA_DIR    = SCRIPT_DIR.parent / "data"

# Chemin du CSV synthétique généré par generate_dataset.py
DATASET_CSV = DATA_DIR / "synthetic" / "dataset_ml.csv"

TARGET = "is_fraud"

# ─────────────────────────────────────────────────────────────
# Features — miroir exact de ML_FEATURES dans feature_extractor.py
# ─────────────────────────────────────────────────────────────
ML_FEATURES = [
    # Démographie
    "age", "income", "income_per_person",
    "household_size", "nb_children", "nb_elderly",
    "dependency_ratio",
    "has_disabled", "single_head",
    # Programmes
    "nb_programs", "nb_active_programs",
    "pmt_score", "pmt_score_min",
    "avg_enrollment_days",
    # Paiements
    "payment_count", "payment_gap_ratio",
    "payment_success_rate", "amount_variance",
    "cycle_count",
    # Réseau
    "shared_phone_count", "shared_account_count",
    "network_risk",
    # Groupes
    "group_membership_count",
    # Flags dérivés
    "high_amount_flag", "income_program_inconsistency",
]

# Valeurs par défaut pour les colonnes éventuellement absentes du CSV
FEATURE_DEFAULTS: dict[str, float] = {
    "age": 35,
    "income": 0.0,
    "income_per_person": 0.0,
    "household_size": 1.0,
    "nb_children": 0.0,
    "nb_elderly": 0.0,
    "dependency_ratio": 0.0,
    "has_disabled": 0,
    "single_head": 0,
    "nb_programs": 1,
    "nb_active_programs": 1,
    "pmt_score": 0.5,
    "pmt_score_min": 0.5,
    "avg_enrollment_days": 365,
    "payment_count": 1,
    "payment_gap_ratio": 0.0,
    "payment_success_rate": 1.0,
    "amount_variance": 0.0,
    "cycle_count": 1,
    "shared_phone_count": 0,
    "shared_account_count": 0,
    "network_risk": 0.0,
    "group_membership_count": 0,
    "high_amount_flag": 0,
    "income_program_inconsistency": 0,
}


# ─────────────────────────────────────────────────────────────
def load_data(path: Path = DATASET_CSV) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset introuvable : {path}")
    df = pd.read_csv(path)
    print(f"📂 Chargé : {path}  ({len(df)} lignes, {df.shape[1]} colonnes)")
    return df


def prepare_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Applique les fallbacks sur les colonnes manquantes et retourne
    (df enrichi, liste effective des features disponibles).
    """
    out = df.copy()

    # Label
    if TARGET not in out.columns:
        if "synthetic_label" in out.columns:
            out[TARGET] = out["synthetic_label"]
        else:
            raise ValueError(f"Colonne cible '{TARGET}' introuvable.")

    # Alias rétrocompatible
    if "payment_gap_ratio" not in out.columns and "gap_ratio" in out.columns:
        out["payment_gap_ratio"] = out["gap_ratio"]

    # Colonnes manquantes → valeur par défaut
    missing = []
    for col in ML_FEATURES:
        if col not in out.columns:
            out[col] = FEATURE_DEFAULTS.get(col, 0.0)
            missing.append(col)

    if missing:
        print(f"⚠️  Colonnes absentes (fallback appliqué) : {missing}")

    effective_features = [f for f in ML_FEATURES if f in out.columns]
    return out, effective_features


# ─────────────────────────────────────────────────────────────
def build_preprocessor(numeric_cols: list[str]) -> ColumnTransformer:
    return ColumnTransformer([
        ("num", Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler",  StandardScaler()),
        ]), numeric_cols)
    ])


def train_supervised(X_train, y_train, X_test, y_test, feature_cols):
    preprocessor = build_preprocessor(feature_cols)

    logreg = Pipeline([
        ("prep", preprocessor),
        ("clf",  LogisticRegression(max_iter=800, C=0.1, class_weight="balanced")),
    ])

    rf = Pipeline([
        ("prep", build_preprocessor(feature_cols)),
        ("clf",  RandomForestClassifier(
            n_estimators=200,
            max_depth=8,
            min_samples_leaf=10,
            max_features="sqrt",
            class_weight="balanced",
            random_state=SEED,
        )),
    ])

    # === Futur challenger XGBoost ===
    # from xgboost import XGBClassifier
    # xgb = Pipeline([("prep", build_preprocessor(feature_cols)),
    #                 ("clf", XGBClassifier(n_estimators=200, max_depth=5,
    #                                       scale_pos_weight=..., eval_metric="auc",
    #                                       random_state=SEED))])
    # xgb.fit(X_train, y_train)

    logreg.fit(X_train, y_train)
    rf.fit(X_train, y_train)

    for name, model in [("Logistic Regression", logreg), ("Random Forest", rf)]:
        proba = model.predict_proba(X_test)[:, 1]
        pred  = (proba >= 0.5).astype(int)
        print(f"\n📊 {name}")
        print(classification_report(y_test, pred, zero_division=0))
        print(f"AUC : {roc_auc_score(y_test, proba):.4f}")

    # Feature importance RF
    importances = rf.named_steps["clf"].feature_importances_
    print("\n🧠 Feature importance (RF) :")
    for feat, imp in sorted(zip(feature_cols, importances), key=lambda x: -x[1])[:15]:
        print(f"  {feat:<35} {round(imp, 4)}")

    return logreg, rf


def train_anomaly(X_train, feature_cols):
    iso = IsolationForest(
        n_estimators=200,
        contamination=0.12,  # correspond au taux de fraude du dataset
        random_state=SEED,
    )
    iso.fit(X_train[feature_cols])
    return iso


# ─────────────────────────────────────────────────────────────
def save_artifacts(logreg, rf, iso, feature_cols: list[str]):
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    dump(rf,     MODELS_DIR / "random_forest.joblib")
    dump(logreg, MODELS_DIR / "logreg.joblib")
    dump(iso,    MODELS_DIR / "isolation_forest.joblib")

    metadata = {
        "feature_columns": feature_cols,
        "target_column": TARGET,
        "supervised_model_default": "random_forest",
        "available_models": {
            "random_forest": "random_forest.joblib",
            "logreg": "logreg.joblib",
            "isolation_forest": "isolation_forest.joblib",
        },
    }
    meta_path = MODELS_DIR / "metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))

    print(f"\n✅ Artefacts sauvegardés dans {MODELS_DIR}/")
    print(f"   metadata.json — {len(feature_cols)} features enregistrées")


# ─────────────────────────────────────────────────────────────
def train():
    print("\n🚀 Chargement données...")
    df = load_data()

    df, feature_cols = prepare_features(df)

    print(f"\n🧪 Features utilisées ({len(feature_cols)}) :")
    print(feature_cols)

    fraud_rate = df[TARGET].mean()
    print(f"\n📈 Taux de fraude : {fraud_rate:.2%}  ({df[TARGET].sum()} / {len(df)})")

    X = df[feature_cols]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=SEED
    )

    logreg, rf = train_supervised(X_train, y_train, X_test, y_test, feature_cols)

    # Isolation Forest entraîné uniquement sur le train set
    iso = train_anomaly(X_train, feature_cols)

    save_artifacts(logreg, rf, iso, feature_cols)

    print("\n🎯 DONE")


if __name__ == "__main__":
    train()
