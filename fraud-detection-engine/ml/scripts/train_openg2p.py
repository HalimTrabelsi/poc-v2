"""
train_openg2p.py — Entraînement modèles fraude OpenG2P
=======================================================
Modèles : Logistic Regression, Random Forest, XGBoost (challenger), Isolation Forest.
XGBoost activé automatiquement si xgboost est installé.

Usage :
    python ml/scripts/train_openg2p.py
    python ml/scripts/train_openg2p.py --model xgboost
    python ml/scripts/train_openg2p.py --model all
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import dump

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, average_precision_score

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

SEED = 42
np.random.seed(SEED)

SCRIPT_DIR = Path(__file__).resolve().parent
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/app/models_saved"))
DATA_DIR   = SCRIPT_DIR.parent / "data"
DATASET_CSV = Path(os.getenv("DATASET_PATH", str(DATA_DIR / "synthetic" / "dataset_ml.csv")))

TARGET = "is_fraud"

ML_FEATURES = [
    "age", "income", "income_per_person",
    "household_size", "nb_children", "nb_elderly",
    "dependency_ratio", "has_disabled", "single_head",
    "nb_programs", "nb_active_programs",
    "pmt_score", "pmt_score_min", "avg_enrollment_days",
    "payment_count", "payment_gap_ratio",
    "payment_success_rate", "amount_variance", "cycle_count",
    "shared_phone_count", "shared_account_count", "network_risk",
    "group_membership_count", "high_amount_flag", "income_program_inconsistency",
]

FEATURE_DEFAULTS = {
    "age": 35, "income": 0.0, "income_per_person": 0.0,
    "household_size": 1.0, "nb_children": 0.0, "nb_elderly": 0.0,
    "dependency_ratio": 0.0, "has_disabled": 0, "single_head": 0,
    "nb_programs": 1, "nb_active_programs": 1,
    "pmt_score": 0.5, "pmt_score_min": 0.5, "avg_enrollment_days": 365,
    "payment_count": 1, "payment_gap_ratio": 0.0,
    "payment_success_rate": 1.0, "amount_variance": 0.0, "cycle_count": 1,
    "shared_phone_count": 0, "shared_account_count": 0, "network_risk": 0.0,
    "group_membership_count": 0, "high_amount_flag": 0,
    "income_program_inconsistency": 0,
}


def load_data(path: Path = DATASET_CSV) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset introuvable : {path}")
    df = pd.read_csv(path)
    print(f"Chargé : {path}  ({len(df)} lignes, {df.shape[1]} colonnes)")
    return df


def prepare_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    out = df.copy()

    if TARGET not in out.columns:
        if "synthetic_label" in out.columns:
            out[TARGET] = out["synthetic_label"]
        else:
            raise ValueError(f"Colonne cible '{TARGET}' introuvable.")

    # Aliases rétrocompatibles
    alias_map = {
        "payment_gap_ratio": "gap_ratio",
        "group_membership_count": "group_count",
        "network_risk": "network_risk_score",
        "amount_variance": "amount_variannce",
    }
    for col, alt in alias_map.items():
        if col not in out.columns and alt in out.columns:
            out[col] = out[alt]

    for col in ML_FEATURES:
        if col not in out.columns:
            out[col] = FEATURE_DEFAULTS.get(col, 0.0)

    effective = [f for f in ML_FEATURES if f in out.columns]
    return out, effective


def build_preprocessor(numeric_cols: list[str]) -> ColumnTransformer:
    return ColumnTransformer([
        ("num", Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]), numeric_cols)
    ])


def evaluate(model, X_test, y_test, name: str) -> dict:
    proba = model.predict_proba(X_test)[:, 1]
    pred  = (proba >= 0.5).astype(int)
    auc   = roc_auc_score(y_test, proba)
    ap    = average_precision_score(y_test, proba)

    print(f"\n{'='*55}")
    print(f"  {name}")
    print(f"{'='*55}")
    print(classification_report(y_test, pred, zero_division=0))
    print(f"AUC-ROC          : {auc:.4f}")
    print(f"Average Precision: {ap:.4f}")

    return {"auc": round(auc, 4), "avg_precision": round(ap, 4)}


def train_all(X_train, y_train, X_test, y_test, feature_cols, model_filter="all"):
    results  = {}
    models   = {}

    # Ratio déséquilibre
    n_neg    = (y_train == 0).sum()
    n_pos    = (y_train == 1).sum()
    scale_pw = n_neg / max(n_pos, 1)

    # ── Logistic Regression ──────────────────────────────
    if model_filter in ("all", "logreg"):
        logreg = Pipeline([
            ("prep", build_preprocessor(feature_cols)),
            ("clf", LogisticRegression(max_iter=800, C=0.1, class_weight="balanced")),
        ])
        logreg.fit(X_train, y_train)
        results["logreg"] = evaluate(logreg, X_test, y_test, "Logistic Regression")
        models["logreg"]  = logreg

    # ── Random Forest ────────────────────────────────────
    if model_filter in ("all", "random_forest"):
        rf = Pipeline([
            ("prep", build_preprocessor(feature_cols)),
            ("clf", RandomForestClassifier(
                n_estimators=300,
                max_depth=10,
                min_samples_leaf=5,
                max_features="sqrt",
                class_weight="balanced",
                random_state=SEED,
                n_jobs=-1,
            )),
        ])
        rf.fit(X_train, y_train)
        results["random_forest"] = evaluate(rf, X_test, y_test, "Random Forest")
        models["random_forest"]  = rf

        # Feature importance RF
        importances = rf.named_steps["clf"].feature_importances_
        print("\nTop 10 features (Random Forest) :")
        for feat, imp in sorted(zip(feature_cols, importances), key=lambda x: -x[1])[:10]:
            print(f"  {feat:<35} {round(imp, 4)}")

    # ── XGBoost Challenger ───────────────────────────────
    if model_filter in ("all", "xgboost"):
        if XGBOOST_AVAILABLE:
            xgb_model = Pipeline([
                ("prep", build_preprocessor(feature_cols)),
                ("clf", xgb.XGBClassifier(
                    n_estimators=300,
                    max_depth=6,
                    learning_rate=0.05,
                    subsample=0.8,
                    colsample_bytree=0.8,
                    min_child_weight=5,
                    gamma=0.1,
                    scale_pos_weight=scale_pw,
                    eval_metric="auc",
                    random_state=SEED,
                    use_label_encoder=False,
                    n_jobs=-1,
                )),
            ])
            
            # XGBoost nécessite les feature names APRÈS le preprocessing
            # Créer une version avec les noms de colonnes
            X_train_prep = xgb_model.named_steps["prep"].fit_transform(X_train)
            X_test_prep = xgb_model.named_steps["prep"].transform(X_test)
            
            # Ajouter les noms de colonnes
            X_train_prep_df = pd.DataFrame(X_train_prep, columns=feature_cols)
            X_test_prep_df = pd.DataFrame(X_test_prep, columns=feature_cols)
            
            # Entraîner directement le classifier
            xgb_model.named_steps["clf"].fit(
                X_train_prep_df, y_train,
                eval_set=[(X_test_prep_df, y_test)],
                verbose=False,
            )
            
            results["xgboost"] = evaluate(xgb_model, X_test, y_test, "XGBoost Challenger")
            models["xgboost"]  = xgb_model

            # Feature importance XGBoost
            importances = xgb_model.named_steps["clf"].feature_importances_
            print("\nTop 10 features (XGBoost) :")
            for feat, imp in sorted(zip(feature_cols, importances), key=lambda x: -x[1])[:10]:
                print(f"  {feat:<35} {round(imp, 4)}")

            # Comparaison RF vs XGB
            if "random_forest" in results:
                rf_auc  = results["random_forest"]["auc"]
                xgb_auc = results["xgboost"]["auc"]
                print(f"\nComparaison : RF={rf_auc:.4f} | XGB={xgb_auc:.4f} | delta={xgb_auc-rf_auc:+.4f}")
        else:
            print("\nXGBoost non installé — pip install xgboost")
    return models, results


def train_anomaly(X_train, feature_cols) -> IsolationForest:
    iso = IsolationForest(n_estimators=200, contamination=0.12, random_state=SEED)
    iso.fit(X_train[feature_cols])
    return iso


def save_artifacts(models: dict, iso, feature_cols: list[str], metrics: dict):
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    file_map = {
        "random_forest": "random_forest.joblib",
        "logreg":        "logreg.joblib",
        "xgboost":       "xgboost.joblib",
    }

    for name, model in models.items():
        path = MODELS_DIR / file_map.get(name, f"{name}.joblib")
        dump(model, path)
        print(f"  Sauvegardé : {path}")

    dump(iso, MODELS_DIR / "isolation_forest.joblib")
    print(f"  Sauvegardé : {MODELS_DIR}/isolation_forest.joblib")

    # Choisir le meilleur modèle supervisé
    best_model = "random_forest"
    if "xgboost" in metrics and "random_forest" in metrics:
        if metrics["xgboost"]["auc"] > metrics["random_forest"]["auc"]:
            best_model = "xgboost"
            print(f"\nXGBoost sélectionné comme modèle par défaut (AUC supérieur)")

    available = {name: file_map[name] for name in models if name in file_map}
    available["isolation_forest"] = "isolation_forest.joblib"

    metadata = {
        "feature_columns":          feature_cols,
        "target_column":            TARGET,
        "supervised_model_default": best_model,
        "available_models":         available,
        "metrics":                  metrics,
    }

    meta_path = MODELS_DIR / "metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  metadata.json mis à jour — modèle par défaut : {best_model}")


def train(model_filter: str = "all"):
    print("\nChargement données...")
    df = load_data()
    df, feature_cols = prepare_features(df)

    print(f"\nFeatures ({len(feature_cols)}) : {feature_cols}")
    print(f"Taux de fraude : {df[TARGET].mean():.2%}  ({df[TARGET].sum()} / {len(df)})")

    X = df[feature_cols]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=SEED
    )

    models, metrics = train_all(X_train, y_train, X_test, y_test, feature_cols, model_filter)
    iso = train_anomaly(X_train, feature_cols)

    save_artifacts(models, iso, feature_cols, metrics)

    print("\nDONE")
    return metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="all",
                    choices=["all", "random_forest", "logreg", "xgboost"],
                    help="Modèle(s) à entraîner")
    args = ap.parse_args()

    if args.model == "xgboost" and not XGBOOST_AVAILABLE:
        print("XGBoost non installé — pip install xgboost")
        return

    train(model_filter=args.model)


if __name__ == "__main__":
    main()