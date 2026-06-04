"""
backfill_mlflow_history.py — Reconstruct historical experiments in MLflow
=========================================================================
One-time forensic backfill.

The MLflow tracking integration was only added on 2026-06-04, so the earlier
model-development stages were never logged as runs. Their metrics survive only
as static artifacts:

  - app/models_saved/metadata.json          (XGBoost final + 3-model comparison)
  - app/models_saved/paysim_metadata.json   (PaySim transaction model)
  - app/models_saved/openg2p_demo_metadata.json (identity-fraud demo model)
  - ml/outputs/performance_summary.csv      (Rule / ML / Hybrid)
  - ml/reports/evaluation_report.txt        (human-readable summary)

This script READS those real numbers (it does NOT retrain or invent values) and
recreates them as MLflow runs, each tagged with the git commit / date of the
stage it belongs to, so the dashboard shows the full project timeline.

Run ONCE:
    python ml/scripts/backfill_mlflow_history.py
    python ml/scripts/backfill_mlflow_history.py --dry-run   # preview only
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import mlflow

# --- paths -----------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent.parent  # fraud-detection-engine/
MODELS = ROOT / "app" / "models_saved"
OUTPUTS = ROOT / "ml" / "outputs"
REPORTS = ROOT / "ml" / "reports"

MLFLOW_URI = "http://localhost:5000"

# Stage -> git commit + date, recovered from `git log`. Used as run tags so each
# backfilled run is traceable to the exact code state of that development stage.
STAGES = {
    "phase2_first_models":   {"commit": "e8191b6", "date": "2026-03-26"},
    "phase3_hybrid":         {"commit": "5f4bfc3", "date": "2026-04-06"},
    "phase4_full_pipeline":  {"commit": "49473d6", "date": "2026-04-07"},
    "phase9_kaggle_fusion":  {"commit": "71e6582", "date": "2026-06-01"},
    "phase10_shap_retrain":  {"commit": "12bcf60", "date": "2026-06-03"},
}


def _ts(date_str: str) -> int:
    """Convert YYYY-MM-DD to epoch-ms so the run's start_time matches history."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _scalar_metrics(d: dict) -> dict:
    """Keep only numeric metric values (drop nested confusion matrices etc.)."""
    return {k: float(v) for k, v in d.items()
            if isinstance(v, (int, float)) and not isinstance(v, bool)}


def _log_run(experiment, run_name, stage_key, params, metrics,
             artifacts=None, extra_tags=None, dry_run=False):
    stage = STAGES[stage_key]
    tags = {
        "git_commit": stage["commit"],
        "stage_date": stage["date"],
        "phase": stage_key,
        "backfilled": "true",
        "source": "forensic_reconstruction",
    }
    if extra_tags:
        tags.update(extra_tags)

    if dry_run:
        print(f"\n[DRY-RUN] {experiment} / {run_name}")
        print(f"   tags    : {tags}")
        print(f"   params  : {params}")
        print(f"   metrics : {metrics}")
        print(f"   artifacts: {artifacts or []}")
        return

    mlflow.set_experiment(experiment)
    with mlflow.start_run(run_name=run_name, tags=tags):
        if params:
            mlflow.log_params(params)
        if metrics:
            mlflow.log_metrics(metrics)
        for art in (artifacts or []):
            if Path(art).exists():
                mlflow.log_artifact(str(art))
    print(f"  [OK] {experiment} / {run_name}")


def backfill(dry_run=False):
    if not dry_run:
        mlflow.set_tracking_uri(MLFLOW_URI)

    # --- Source 1: metadata.json (XGBoost final + 3-model comparison) -------
    meta_path = MODELS / "metadata.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    m = meta.get("metrics", {})

    # Run: each comparison model (Phase 2 — first models / benchmark)
    comp = m.get("model_comparison", {})
    name_map = {
        "Logistic Regression (baseline)": ("logreg_baseline", "LogisticRegression"),
        "Random Forest":                  ("random_forest_baseline", "RandomForest"),
        "XGBoost (calibré)":              ("xgboost_calibrated", "XGBoost"),
    }
    for raw_name, vals in comp.items():
        run_name, algo = name_map.get(
            raw_name, (raw_name.lower().replace(" ", "_"), raw_name))
        _log_run(
            "fraud_detection_beneficiary", run_name, "phase2_first_models",
            params={"algorithm": algo, "dataset": "synthetic_5k"},
            metrics=_scalar_metrics(vals),
            extra_tags={"model_stage": "comparison"},
            dry_run=dry_run,
        )

    # Run: final calibrated XGBoost (Phase 10 — SHAP retrain) w/ artifacts
    xgb = m.get("xgboost_calibrated", {})
    weights = meta.get("ensemble_weights", {})
    _log_run(
        "fraud_detection_beneficiary", "xgboost_calibrated_final",
        "phase10_shap_retrain",
        params={
            "algorithm": "XGBoost",
            "n_estimators": 400, "learning_rate": 0.05, "max_depth": 6,
            "calibration": meta.get("calibration_method", "isotonic"),
            "n_features": len(meta.get("feature_columns", [])),
            "n_features_removed": len(meta.get("removed_features", [])),
        },
        metrics=_scalar_metrics(xgb),
        artifacts=[meta_path, REPORTS / "evaluation_report.txt"],
        extra_tags={"model_stage": "production"},
        dry_run=dry_run,
    )

    # --- Source 2: performance_summary.csv (Rule / ML / Hybrid) -------------
    perf = OUTPUTS / "performance_summary.csv"
    if perf.exists():
        with perf.open(encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                model = row.get("Modèle") or row.get("Mod\udce8le") or "model"
                metrics = {}
                for col in ("Accuracy", "Precision", "Recall", "F1-Score", "AUC"):
                    if row.get(col):
                        key = col.lower().replace("-", "_")
                        metrics[key] = float(row[col])
                run_name = (model.lower().replace(" ", "_")
                            .replace("è", "e").replace("é", "e"))
                _log_run(
                    "hybrid_ensemble", f"{run_name}_scoring", "phase3_hybrid",
                    params={"weights": json.dumps(weights), "dataset": "scored_10k"},
                    metrics=metrics,
                    artifacts=[p for p in OUTPUTS.glob("*.png")],
                    extra_tags={"model_stage": "hybrid"},
                    dry_run=dry_run,
                )

    # --- Source 3: PaySim transaction model ---------------------------------
    paysim_path = MODELS / "paysim_metadata.json"
    if paysim_path.exists():
        ps = json.loads(paysim_path.read_text(encoding="utf-8"))
        _log_run(
            "fraud_detection_transaction_paysim", "xgboost_paysim_chrono",
            "phase9_kaggle_fusion",
            params={
                "algorithm": "XGBoost",
                "dataset": ps.get("source_dataset", "PaySim"),
                "split_method": ps.get("split_method", "chronological"),
            },
            metrics=_scalar_metrics(ps.get("metrics", {})),
            artifacts=[paysim_path],
            extra_tags={"model_stage": "transaction", "dataset": "PaySim"},
            dry_run=dry_run,
        )

    # --- Source 4: OpenG2P identity-fraud demo model ------------------------
    demo_path = MODELS / "openg2p_demo_metadata.json"
    if demo_path.exists():
        demo = json.loads(demo_path.read_text(encoding="utf-8"))
        _log_run(
            "fraud_detection_identity_demo", "xgboost_openg2p_demo",
            "phase9_kaggle_fusion",
            params={
                "algorithm": "XGBoost",
                "dataset": demo.get("training_set", "demo"),
                "n_train": demo.get("n_train"), "n_test": demo.get("n_test"),
                "fraud_rate": demo.get("fraud_rate"),
            },
            metrics=_scalar_metrics(demo.get("metrics", {})),
            artifacts=[demo_path],
            extra_tags={"model_stage": "demo", "dataset": "openg2p_demo"},
            dry_run=dry_run,
        )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="Preview the runs without writing to MLflow")
    args = ap.parse_args()

    print("=" * 60)
    print("  MLflow Historical Backfill — Forensic Reconstruction")
    print("=" * 60)
    if args.dry_run:
        print("  MODE: DRY-RUN (nothing will be written)\n")
    else:
        print(f"  Target: {MLFLOW_URI}\n")

    backfill(dry_run=args.dry_run)

    print("\n" + "=" * 60)
    print("  Done. Open http://localhost:5000 to view the timeline.")
    print("  Experiments created:")
    print("    - fraud_detection_beneficiary")
    print("    - hybrid_ensemble")
    print("    - fraud_detection_transaction_paysim")
    print("    - fraud_detection_identity_demo")
    print("=" * 60)


if __name__ == "__main__":
    main()
