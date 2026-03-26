import os
import json
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    ConfusionMatrixDisplay,
)
from xgboost import XGBClassifier


# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = os.getenv("TRAIN_DATA_PATH", str(BASE_DIR / "data" / "dataset_v2_clean.csv"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", str(BASE_DIR / "models_saved")))
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "fraud-detection-training")

MODELS_DIR.mkdir(parents=True, exist_ok=True)

FEATURES = [
    "gender",
    "age",
    "income",
    "household_size",
    "nb_children",
    "vehicles_owned",
    "dependency_ratio",
    "income_per_person",
    "disability_flag",
    "immigration_flag",
    "own_home_flag",
    "shared_phone_count",
    "shared_account_count",
]

TARGET = "is_suspicious"


# =========================================================
# HELPERS
# =========================================================
def load_data() -> pd.DataFrame:
    if not Path(DATA_PATH).exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)

    missing_cols = [col for col in FEATURES + [TARGET] if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in dataset: {missing_cols}")

    return df


def prepare_xy(df: pd.DataFrame):
    X = df[FEATURES].copy()
    y = df[TARGET].copy()

    X = X.fillna(0)

    return X, y


def plot_and_save_confusion_matrix(y_true, y_pred, title: str, output_path: Path):
    cm = confusion_matrix(y_true, y_pred)
    disp = ConfusionMatrixDisplay(confusion_matrix=cm)
    disp.plot()
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def save_json(data: dict, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def train_and_log_model(
    model,
    model_name: str,
    X_train,
    X_test,
    y_train,
    y_test,
    extra_params: dict,
):
    with mlflow.start_run(run_name=model_name):
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        metrics = {
            "accuracy": accuracy_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred, zero_division=0),
            "recall": recall_score(y_test, y_pred, zero_division=0),
            "f1_score": f1_score(y_test, y_pred, zero_division=0),
        }

        # Log params
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("dataset_path", DATA_PATH)
        mlflow.log_param("n_features", len(FEATURES))
        for k, v in extra_params.items():
            mlflow.log_param(k, v)

        # Log metrics
        for k, v in metrics.items():
            mlflow.log_metric(k, v)

        # Save confusion matrix
        cm_path = MODELS_DIR / f"{model_name}_confusion_matrix.png"
        plot_and_save_confusion_matrix(
            y_test,
            y_pred,
            f"Confusion Matrix - {model_name}",
            cm_path,
        )
        mlflow.log_artifact(str(cm_path))

        # Save model locally
        model_path = MODELS_DIR / f"{model_name}.pkl"
        joblib.dump(model, model_path)

        # Log model to MLflow
        mlflow.sklearn.log_model(model, artifact_path=model_name)

        # Save metrics locally
        metrics_path = MODELS_DIR / f"{model_name}_metrics.json"
        save_json(metrics, metrics_path)

        print(f"\n===== {model_name.upper()} =====")
        for k, v in metrics.items():
            print(f"{k}: {v:.4f}")

        return metrics, model_path


# =========================================================
# MAIN
# =========================================================
def main():
    print(f"Loading dataset from: {DATA_PATH}")
    df = load_data()

    print("Dataset shape:", df.shape)
    print("Target distribution:")
    print(df[TARGET].value_counts(dropna=False))

    X, y = prepare_xy(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    # Save features once
    features_path = MODELS_DIR / "features.json"
    save_json({"features": FEATURES, "target": TARGET}, features_path)

    # 1) Random Forest
    rf = RandomForestClassifier(
        n_estimators=200,
        random_state=42,
    )

    rf_metrics, rf_model_path = train_and_log_model(
        model=rf,
        model_name="random_forest",
        X_train=X_train,
        X_test=X_test,
        y_train=y_train,
        y_test=y_test,
        extra_params={
            "n_estimators": 200,
            "random_state": 42,
        },
    )

    # 2) XGBoost
    xgb = XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=42,
        eval_metric="logloss",
    )

    xgb_metrics, xgb_model_path = train_and_log_model(
        model=xgb,
        model_name="xgboost",
        X_train=X_train,
        X_test=X_test,
        y_train=y_train,
        y_test=y_test,
        extra_params={
            "n_estimators": 200,
            "max_depth": 4,
            "learning_rate": 0.05,
            "subsample": 0.9,
            "colsample_bytree": 0.9,
            "random_state": 42,
        },
    )

    # Compare and save summary
    comparison = {
        "random_forest": rf_metrics,
        "xgboost": xgb_metrics,
        "selected_baseline_model": (
            "random_forest"
            if rf_metrics["f1_score"] >= xgb_metrics["f1_score"]
            else "xgboost"
        ),
        "artifacts": {
            "random_forest_model": str(rf_model_path),
            "xgboost_model": str(xgb_model_path),
            "features": str(features_path),
        },
    }

    comparison_path = MODELS_DIR / "training_summary.json"
    save_json(comparison, comparison_path)

    print("\nTraining completed.")
    print(json.dumps(comparison, indent=2))


if __name__ == "__main__":
    main()