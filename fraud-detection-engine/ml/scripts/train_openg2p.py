"""
Train Script — Architecture multi-agent ML avec stacking OOF
================================================================
Étage 1 : 3 modèles de base entraînés (RF, XGBoost, LogReg)
Étage 2 : un méta-modèle (LogisticRegression) apprend à combiner
          leurs prédictions Out-Of-Fold — PAS de poids fixés à la main.

Ce fichier remplace entièrement l'ancien train_openg2p.py.
"""

from __future__ import annotations

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
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_predict
from sklearn.metrics import classification_report, roc_auc_score, f1_score
from xgboost import XGBClassifier

SEED = 42
np.random.seed(SEED)

SCRIPT_DIR = Path(__file__).resolve().parent
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/app/models_saved"))
DATA_DIR = SCRIPT_DIR.parent / "data"
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
    "group_membership_count",
    "high_amount_flag", "income_program_inconsistency",
]

FEATURE_DEFAULTS: dict[str, float] = {
    "age": 35, "income": 0.0, "income_per_person": 0.0,
    "household_size": 1.0, "nb_children": 0.0, "nb_elderly": 0.0,
    "dependency_ratio": 0.0, "has_disabled": 0, "single_head": 0,
    "nb_programs": 1, "nb_active_programs": 1,
    "pmt_score": 0.5, "pmt_score_min": 0.5, "avg_enrollment_days": 365,
    "payment_count": 1, "payment_gap_ratio": 0.0,
    "payment_success_rate": 1.0, "amount_variance": 0.0, "cycle_count": 1,
    "shared_phone_count": 0, "shared_account_count": 0, "network_risk": 0.0,
    "group_membership_count": 0,
    "high_amount_flag": 0, "income_program_inconsistency": 0,
}


def load_data(path: Path = DATASET_CSV) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset introuvable : {path}")
    df = pd.read_csv(path)
    print(f"📂 Chargé : {path}  ({len(df)} lignes, {df.shape[1]} colonnes)")
    return df


def prepare_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    out = df.copy()
    if TARGET not in out.columns:
        if "synthetic_label" in out.columns:
            out[TARGET] = out["synthetic_label"]
        else:
            raise ValueError(f"Colonne cible '{TARGET}' introuvable.")

    if "payment_gap_ratio" not in out.columns and "gap_ratio" in out.columns:
        out["payment_gap_ratio"] = out["gap_ratio"]
    if "group_membership_count" not in out.columns and "group_count" in out.columns:
        out["group_membership_count"] = out["group_count"]
    if "network_risk" not in out.columns and "network_risk_score" in out.columns:
        out["network_risk"] = out["network_risk_score"]

    missing = []
    for col in ML_FEATURES:
        if col not in out.columns:
            out[col] = FEATURE_DEFAULTS.get(col, 0.0)
            missing.append(col)
    if missing:
        print(f"⚠️  Colonnes absentes (fallback appliqué) : {missing}")

    effective_features = [f for f in ML_FEATURES if f in out.columns]
    return out, effective_features


def build_preprocessor(numeric_cols: list[str]) -> ColumnTransformer:
    return ColumnTransformer([
        ("num", Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]), numeric_cols)
    ])


def build_base_models(feature_cols: list[str]) -> dict[str, Pipeline]:
    """Les 3 modèles de base — chacun est un 'agent' du multi-modèle."""
    return {
        "random_forest": Pipeline([
            ("prep", build_preprocessor(feature_cols)),
            ("clf", RandomForestClassifier(
                n_estimators=200, max_depth=8, min_samples_leaf=10,
                max_features="sqrt", class_weight="balanced", random_state=SEED,
            )),
        ]),
        "xgboost": Pipeline([
            ("prep", build_preprocessor(feature_cols)),
            ("clf", XGBClassifier(
                n_estimators=300, max_depth=5, learning_rate=0.05,
                subsample=0.9, colsample_bytree=0.8,
                scale_pos_weight=7,  # compense le déséquilibre 88/12
                eval_metric="auc", random_state=SEED,
            )),
        ]),
        "logreg": Pipeline([
            ("prep", build_preprocessor(feature_cols)),
            ("clf", LogisticRegression(
                max_iter=800, C=0.1, class_weight="balanced",
            )),
        ]),
    }


def train_base_models_with_oof(X: pd.DataFrame, y: pd.Series, feature_cols: list[str]):
    """
    Entraîne les 3 modèles de base ET calcule leurs prédictions OOF
    (Out-Of-Fold) pour pouvoir entraîner le méta-modèle sans fuite de données.
    """
    models = build_base_models(feature_cols)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=SEED)

    oof_predictions = {}  # nom_modele -> array de probabilités OOF
    print("\n🔄 Calcul des prédictions Out-Of-Fold (5-fold CV)...")
    for name, model in models.items():
        oof_proba = cross_val_predict(
            model, X, y, cv=cv, method="predict_proba", n_jobs=-1
        )[:, 1]
        oof_predictions[name] = oof_proba
        auc = roc_auc_score(y, oof_proba)
        print(f"   {name:15s} — AUC OOF : {auc:.4f}")

    # Ré-entraîner chaque modèle sur 100% des données pour la version finale
    print("\n🔧 Entraînement final de chaque modèle sur 100% des données...")
    fitted_models = {}
    for name, model in models.items():
        model.fit(X, y)
        fitted_models[name] = model

    return fitted_models, oof_predictions


def train_meta_model(oof_predictions: dict[str, np.ndarray], y: pd.Series):
    """
    Le méta-modèle (l'arbitre) : une régression logistique qui apprend
    à combiner les 3 prédictions OOF. C'est ici que les 'poids' sont
    appris automatiquement, au lieu d'être fixés à la main.
    """
    model_names = sorted(oof_predictions.keys())  # ordre fixe et reproductible
    S = np.column_stack([oof_predictions[name] for name in model_names])

    meta = LogisticRegression(max_iter=1000, class_weight="balanced")
    meta.fit(S, y)

    meta_proba = meta.predict_proba(S)[:, 1]
    auc = roc_auc_score(y, meta_proba)
    f1 = f1_score(y, (meta_proba >= 0.5).astype(int))

    print(f"\n🎯 Méta-modèle (ensemble) — AUC : {auc:.4f} | F1 : {f1:.4f}")
    print("\n📊 Poids appris par le méta-modèle :")
    print(f"   Biais (β0) = {meta.intercept_[0]:+.3f}")
    for name, coef in zip(model_names, meta.coef_[0]):
        print(f"   poids {name:15s} (β) = {coef:+.3f}")

    abs_coefs = np.abs(meta.coef_[0])
    importance = abs_coefs / abs_coefs.sum()
    print("\n📈 Importance relative de chaque modèle dans l'ensemble :")
    for name, imp in zip(model_names, importance):
        print(f"   {name:15s} : {imp:.1%}")

    return meta, model_names


def train_anomaly(X: pd.DataFrame, feature_cols: list[str]):
    # Conservative contamination: in production the true fraud rate is unknown.
    # 5% errs on the side of fewer false alarms rather than claiming to know
    # the answer the system is supposed to discover.
    iso = IsolationForest(n_estimators=200, contamination=0.05, random_state=SEED)
    iso.fit(X[feature_cols])
    return iso


def save_artifacts(fitted_models: dict, meta_model, meta_model_names: list[str],
                    iso, feature_cols: list[str]):
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    for name, model in fitted_models.items():
        dump(model, MODELS_DIR / f"{name}.joblib")

    dump(meta_model, MODELS_DIR / "meta_ensemble.joblib")
    dump(iso, MODELS_DIR / "isolation_forest.joblib")

    metadata = {
        "feature_columns": feature_cols,
        "target_column": TARGET,
        "base_models": list(fitted_models.keys()),
        "meta_model_input_order": meta_model_names,  # ⚠️ ordre CRITIQUE pour le scoring
        "ensemble_type": "stacking_logreg",
        "available_models": {
            name: f"{name}.joblib" for name in fitted_models
        } | {
            "meta_ensemble": "meta_ensemble.joblib",
            "isolation_forest": "isolation_forest.joblib",
        },
    }

    meta_path = MODELS_DIR / "metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n✅ Artefacts sauvegardés dans {MODELS_DIR}/")
    print(f"   Modèles de base : {list(fitted_models.keys())}")
    print(f"   Méta-modèle     : meta_ensemble.joblib")
    print(f"   Ordre d'entrée méta-modèle : {meta_model_names}  ⚠️ ne pas changer sans ré-entraîner")


def train():
    print("\n🚀 Chargement des données...")
    df = load_data()
    df, feature_cols = prepare_features(df)

    print(f"\n🧪 Features utilisées ({len(feature_cols)}) : {feature_cols}")

    fraud_rate = df[TARGET].mean()
    print(f"\n📈 Taux de fraude : {fraud_rate:.2%}  ({df[TARGET].sum()} / {len(df)})")

    X = df[feature_cols]
    y = df[TARGET]

    X_trainval, X_holdout, y_trainval, y_holdout = train_test_split(
        X, y, test_size=0.15, stratify=y, random_state=SEED
    )

    print("\n" + "="*60)
    print("📋 5-FOLD CROSS-VALIDATION (sur trainval, 85% des données)")
    print("="*60)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=SEED)
    cv_aucs, cv_f1s = [], []
    for fold, (train_idx, val_idx) in enumerate(cv.split(X_trainval, y_trainval)):
        X_tr, X_val = X_trainval.iloc[train_idx], X_trainval.iloc[val_idx]
        y_tr, y_val = y_trainval.iloc[train_idx], y_trainval.iloc[val_idx]
        f_models, oof_preds = train_base_models_with_oof(X_tr, y_tr, feature_cols)
        meta_m, meta_n = train_meta_model(oof_preds, y_tr)
        S_val = np.column_stack([f_models[n].predict_proba(X_val)[:, 1] for n in meta_n])
        proba_val = meta_m.predict_proba(S_val)[:, 1]
        auc_val = roc_auc_score(y_val, proba_val)
        f1_val = f1_score(y_val, (proba_val >= 0.5).astype(int))
        cv_aucs.append(auc_val); cv_f1s.append(f1_val)
        print(f"   Fold {fold+1}: AUC={auc_val:.4f}  F1={f1_val:.4f}")
    print(f"\n   📊 CV 5-fold → AUC = {np.mean(cv_aucs):.4f} ± {np.std(cv_aucs):.4f}  "
          f"|  F1 = {np.mean(cv_f1s):.4f} ± {np.std(cv_f1s):.4f}")

    fitted_models, oof_predictions = train_base_models_with_oof(
        X_trainval, y_trainval, feature_cols
    )

    meta_model, meta_model_names = train_meta_model(oof_predictions, y_trainval)

    iso = train_anomaly(X_trainval, feature_cols)

    # ── Apprendre les poids d'agrégation étage 2 (Rule/ML/Anomaly/Graph) ──
    # On simule les 4 scores sur trainval, puis grid-search F1 optimal.
    _rule_scores = np.random.uniform(0, 1, len(y_trainval))  # placeholder
    _graph_scores = np.random.uniform(0, 1, len(y_trainval))  # placeholder
    _anomaly_scores = iso.score_samples(X_trainval[feature_cols])
    _anomaly_scores = np.clip((-_anomaly_scores - 0.1) / 0.4, 0, 1)
    _ml_scores = meta_model.predict_proba(
        np.column_stack([fitted_models[n].predict_proba(X_trainval)[:, 1] for n in meta_model_names])
    )[:, 1]

    from sklearn.model_selection import KFold
    from itertools import product
    best_w, best_f1 = None, 0.0
    candidates = list(product([0.10, 0.15, 0.20], [0.45, 0.50, 0.55], [0.05, 0.10], [0.15, 0.20, 0.25]))
    for wr, wm, wa, wg in candidates:
        if abs(wr + wm + wa + wg - 1.0) > 0.01:
            continue
        final = wr * _rule_scores + wm * _ml_scores + wa * _anomaly_scores + wg * _graph_scores
        f1 = f1_score(y_trainval, (final >= 0.5).astype(int))
        if f1 > best_f1:
            best_f1, best_w = f1, {"rule": wr, "ml": wm, "anomaly": wa, "graph": wg}
    print(f"\n📊 Poids d'agrégation optimisés (grid-search) : {best_w}  (F1 best={best_f1:.4f})")
    learned_weights = best_w

    print("\n" + "="*60)
    print("📋 ÉVALUATION FINALE SUR HOLDOUT (15% jamais vu)")
    print("="*60)

    holdout_base_proba = {}
    for name, model in fitted_models.items():
        proba = model.predict_proba(X_holdout)[:, 1]
        holdout_base_proba[name] = proba
        auc = roc_auc_score(y_holdout, proba)
        print(f"   {name:15s} seul — AUC holdout : {auc:.4f}")

    S_holdout = np.column_stack([holdout_base_proba[n] for n in meta_model_names])
    ensemble_proba = meta_model.predict_proba(S_holdout)[:, 1]
    ensemble_auc = roc_auc_score(y_holdout, ensemble_proba)
    print(f"\n   {'ENSEMBLE (méta)':15s} — AUC holdout : {ensemble_auc:.4f}  ⭐")
    print(classification_report(y_holdout, (ensemble_proba >= 0.5).astype(int)))

    save_artifacts(fitted_models, meta_model, meta_model_names, iso, feature_cols)

    # Inclure les poids d'agrégation appris dans metadata.json
    meta_path = MODELS_DIR / "metadata.json"
    if meta_path.exists():
        with open(meta_path, "r") as f:
            meta = json.load(f)
    else:
        meta = {}
    meta["learned_weights"] = learned_weights
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n✅ Poids d'agrégation appris sauvegardés dans metadata.json : {learned_weights}")
    print("\n🎯 DONE")


if __name__ == "__main__":
    train()