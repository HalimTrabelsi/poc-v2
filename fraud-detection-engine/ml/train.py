"""
ML Training Script — XGBoost + IsolationForest
Usage: python ml/train.py
"""
import os
import numpy as np
import pandas as pd
import joblib
import mlflow
import mlflow.sklearn
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    auc, roc_auc_score, f1_score,
    classification_report, confusion_matrix
)
from xgboost import XGBClassifier
from imblearn.over_sampling import SMOTE

FEATURES = [
    "nb_programs",
    "total_amount",
    "amount_ratio",
    "nb_cycles",
    "days_since_enrollment",
    "account_changes_30d",
    "household_size",
    "nb_payment_failures",
    "location_risk_score",
]


def generate_synthetic_data(n: int = 2000, fraud_rate: float = 0.05):
    """Generate synthetic OpenG2P-like beneficiary data"""
    np.random.seed(42)
    n_fraud = int(n * fraud_rate)
    n_legit = n - n_fraud

    legit = pd.DataFrame({
        "nb_programs":           np.random.randint(1, 3, n_legit),
        "total_amount":          np.random.normal(500, 150, n_legit).clip(50),
        "amount_ratio":          np.random.normal(1.0, 0.3, n_legit).clip(0.1),
        "nb_cycles":             np.random.randint(1, 12, n_legit),
        "days_since_enrollment": np.random.randint(30, 730, n_legit),
        "account_changes_30d":   np.random.randint(0, 2, n_legit),
        "household_size":        np.random.randint(1, 8, n_legit),
        "nb_payment_failures":   np.random.randint(0, 3, n_legit),
        "location_risk_score":   np.random.uniform(0.0, 0.4, n_legit),
        "is_fraud": 0,
    })

    fraud = pd.DataFrame({
        "nb_programs":           np.random.randint(4, 8, n_fraud),
        "total_amount":          np.random.normal(2000, 500, n_fraud).clip(500),
        "amount_ratio":          np.random.normal(4.0, 1.0, n_fraud).clip(2.0),
        "nb_cycles":             np.random.randint(1, 6, n_fraud),
        "days_since_enrollment": np.random.randint(1, 90, n_fraud),
        "account_changes_30d":   np.random.randint(4, 10, n_fraud),
        "household_size":        np.random.randint(10, 18, n_fraud),
        "nb_payment_failures":   np.random.randint(5, 12, n_fraud),
        "location_risk_score":   np.random.uniform(0.6, 1.0, n_fraud),
        "is_fraud": 1,
    })

    df = pd.concat([legit, fraud]).sample(
        frac=1, random_state=42
    ).reset_index(drop=True)

    print(f"Dataset: {len(df)} rows | "
          f"Fraud: {df['is_fraud'].sum()} ({fraud_rate:.0%})")
    return df


def train():
    mlflow.set_tracking_uri(
        os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    )
    mlflow.set_experiment("fraud-detection-openg2p")

    # ── Data ─────────────────────────────────────────────────
    df = generate_synthetic_data(n=2000)
    X, y = df[FEATURES], df["is_fraud"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    with mlflow.start_run(run_name="xgboost_v1"):

        # ── SMOTE ────────────────────────────────────────────
        sm = SMOTE(random_state=42)
        X_bal, y_bal = sm.fit_resample(X_train, y_train)
        mlflow.log_param("smote", True)
        mlflow.log_param("train_size", len(X_bal))

        # ── XGBoost ──────────────────────────────────────────
        xgb = XGBClassifier(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            scale_pos_weight=15,
            eval_metric="auc",
            use_label_encoder=False,
            random_state=42,
        )
        xgb.fit(
            X_bal, y_bal,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        y_pred  = xgb.predict(X_test)
        y_proba = xgb.predict_proba(X_test)[:, 1]
        auc     = roc_auc_score(y_test, y_proba)
        f1      = f1_score(y_test, y_pred)
        
        mlflow.log_metric("auc_roc",  auc)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_params(xgb.get_params())
        mlflow.sklearn.log_model(
            xgb, "xgboost",
            registered_model_name="FraudXGBoost"
        )

        # ── IsolationForest ───────────────────────────────────
        iso = IsolationForest(
            contamination=0.05,
            n_estimators=100,
            random_state=42,
        )
        iso.fit(X)
        mlflow.sklearn.log_model(iso, "isolation_forest")

        # ── Save locally ──────────────────────────────────────
        os.makedirs("models_saved", exist_ok=True)
        joblib.dump(xgb, "models_saved/xgboost.pkl")
        joblib.dump(iso, "models_saved/isoforest.pkl")
        mlflow.log_artifact("models_saved/xgboost.pkl")
        mlflow.log_artifact("models_saved/isoforest.pkl")
        # ── Report ────────────────────────────────────────────
        print("\n" + "="*50)
        print(f"AUC-ROC : {auc:.4f}")
        print(f"F1-Score: {f1:.4f}")
        print("="*50)
        print(classification_report(
            y_test, y_pred,
            target_names=["Legitime", "Fraudeur"]
        ))
        print("Confusion Matrix:")
        print(confusion_matrix(y_test, y_pred))
        print("="*50)
        print("Models saved in models_saved/")
        print(f"MLflow run logged at: "
              f"{os.getenv('MLFLOW_TRACKING_URI','http://localhost:5000')}")


if __name__ == "__main__":
    train()