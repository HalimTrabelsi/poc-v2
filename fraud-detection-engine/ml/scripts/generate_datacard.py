"""generate_datacard.py — Produces a "data card" describing the current
synthetic dataset: shape, column types, target balance, missing values,
and per-feature descriptive statistics grouped by category.

Re-run this any time dataset_ml.csv is regenerated so the datacard (JSON +
LaTeX table) always reflects the real, current data instead of going stale.

Usage:
    python ml/scripts/generate_datacard.py
    (optional) --csv <path>  --out-dir <dir>
"""
import argparse
import json
from pathlib import Path

import pandas as pd

DEFAULT_CSV = Path(__file__).resolve().parents[1] / "data" / "synthetic" / "dataset_ml.csv"
# Written under ml/data/ (bind-mounted into the container) rather than
# directly under docs/Rapport/ (which lives outside fraud-detection-engine
# and is NOT mounted into the container) — copy these two files to
# docs/Rapport/generated/ on the host after running this inside Docker.
DEFAULT_OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "generated"

TARGET_COLUMN = "is_fraud"

# Same 28 ML features as train_openg2p.py, grouped by category for a
# readable data card (kept as a local copy so this script has no import
# dependency on the training script or the app package).
FEATURE_CATEGORIES: dict[str, list[str]] = {
    "Démographie": ["age", "income", "income_per_person"],
    "Ménage": [
        "household_size", "nb_children", "nb_elderly",
        "dependency_ratio", "has_disabled", "single_head",
    ],
    "Programmes": ["nb_programs", "nb_active_programs", "avg_enrollment_days"],
    "Score PMT": ["pmt_score", "pmt_score_min"],
    "Paiements": [
        "payment_count", "payment_gap_ratio", "payment_success_rate",
        "amount_variance", "cycle_count",
    ],
    "Réseau": ["shared_phone_count", "shared_account_count", "network_risk"],
    "Groupe": ["group_membership_count"],
    "Signaux dérivés (flags)": ["high_amount_flag", "income_program_inconsistency"],
    "Calibration économique": ["income_ratio_to_national", "household_size_deviation"],
    "Identité": ["duplicate_national_id_count"],
}

ML_FEATURES = [f for cols in FEATURE_CATEGORIES.values() for f in cols]


def _tex_escape(s: str) -> str:
    return s.replace("_", r"\_").replace("%", r"\%")


def build_datacard(df: pd.DataFrame) -> dict:
    n_rows, n_cols = df.shape
    dtype_counts = df.dtypes.astype(str).value_counts().to_dict()

    fraud_count = int(df[TARGET_COLUMN].sum()) if TARGET_COLUMN in df.columns else None
    fraud_rate = float(df[TARGET_COLUMN].mean()) if TARGET_COLUMN in df.columns else None

    present_features = [f for f in ML_FEATURES if f in df.columns]
    missing_features = [f for f in ML_FEATURES if f not in df.columns]
    # Missing values are only meaningful on the 28 ML features — cosmetic
    # identity columns (name, email, address, company_name...) are sparse
    # by design (not every scenario populates them) and inflate a whole-
    # dataframe missing count into a misleading, unrepresentative number.
    missing_ml_features = int(df[present_features].isnull().sum().sum())

    feature_stats = {}
    for feat in present_features:
        col = df[feat]
        feature_stats[feat] = {
            "mean": round(float(col.mean()), 3),
            "std": round(float(col.std()), 3),
            "min": round(float(col.min()), 3),
            "max": round(float(col.max()), 3),
        }

    return {
        "n_rows": n_rows,
        "n_cols": n_cols,
        "n_ml_features": len(present_features),
        "dtype_counts": {k: int(v) for k, v in dtype_counts.items()},
        "missing_ml_features": missing_ml_features,
        "target_column": TARGET_COLUMN,
        "fraud_count": fraud_count,
        "legit_count": (n_rows - fraud_count) if fraud_count is not None else None,
        "fraud_rate": round(fraud_rate, 4) if fraud_rate is not None else None,
        "missing_features_from_expected_28": missing_features,
        "feature_stats": feature_stats,
    }


def render_latex_table(card: dict) -> str:
    lines = []
    lines.append(r"% Auto-généré par ml/scripts/generate_datacard.py — ne pas éditer à la main.")
    lines.append(r"\begin{table}[H]")
    lines.append(r"\centering")
    lines.append(r"\caption{Fiche technique (datacard) du dataset synthétique}")
    lines.append(r"\label{tab:datacard-summary}")
    lines.append(r"\renewcommand{\arraystretch}{1.4}")
    lines.append(r"\begin{tabular}{>{\bfseries}p{5cm} p{9cm}}")
    lines.append(r"\hline")
    lines.append(r"\thead{Caractéristique} & \thead{Valeur} \\")
    lines.append(r"\hline")
    lines.append(r"\rowcolor{EYgray}")
    lines.append(rf"Nombre de lignes (bénéficiaires) & {card['n_rows']:,} \\".replace(",", "\\,"))
    lines.append(rf"Nombre total de colonnes & {card['n_cols']} \\")
    lines.append(r"\rowcolor{EYgray}")
    lines.append(rf"Nombre de features ML & {card['n_ml_features']} \\")
    dtype_str = ", ".join(f"{k}: {v}" for k, v in card["dtype_counts"].items())
    lines.append(rf"Types de colonnes & {_tex_escape(dtype_str)} \\")
    lines.append(r"\rowcolor{EYgray}")
    lines.append(rf"Valeurs manquantes (sur les {card['n_ml_features']} features ML) & {card['missing_ml_features']} \\")
    lines.append(rf"Bénéficiaires légitimes & {card['legit_count']:,} \\".replace(",", "\\,"))
    lines.append(r"\rowcolor{EYgray}")
    lines.append(rf"Bénéficiaires frauduleux & {card['fraud_count']:,} \\".replace(",", "\\,"))
    lines.append(rf"Taux de fraude & {card['fraud_rate']*100:.2f}~\% \\")
    lines.append(r"\hline")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    lines.append("")

    lines.append(r"\begin{table}[H]")
    lines.append(r"\centering")
    lines.append(r"\caption{Statistiques descriptives des features par catégorie}")
    lines.append(r"\label{tab:datacard-features}")
    lines.append(r"\renewcommand{\arraystretch}{1.3}")
    lines.append(r"\begin{tabular}{>{\bfseries}p{3.2cm} p{3.3cm} p{2cm} p{2cm} p{2cm} p{2cm}}")
    lines.append(r"\hline")
    lines.append(r"\thead{Catégorie} & \thead{Feature} & \thead{Moy.} & \thead{Écart-t.} & \thead{Min} & \thead{Max} \\")
    lines.append(r"\hline")
    row_i = 0
    for category, feats in FEATURE_CATEGORIES.items():
        present = [f for f in feats if f in card["feature_stats"]]
        for i, feat in enumerate(present):
            stats = card["feature_stats"][feat]
            shade = r"\rowcolor{EYgray}" if row_i % 2 == 0 else ""
            if shade:
                lines.append(shade)
            cat_cell = category if i == 0 else ""
            lines.append(
                rf"{_tex_escape(cat_cell)} & \texttt{{{_tex_escape(feat)}}} & "
                rf"{stats['mean']} & {stats['std']} & {stats['min']} & {stats['max']} \\"
            )
            row_i += 1
    lines.append(r"\hline")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args()

    if not args.csv.exists():
        raise FileNotFoundError(f"Dataset introuvable : {args.csv}")

    df = pd.read_csv(args.csv)
    card = build_datacard(df)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.out_dir / "datacard.json"
    tex_path = args.out_dir / "datacard_tables.tex"

    json_path.write_text(json.dumps(card, indent=2, ensure_ascii=False), encoding="utf-8")
    tex_path.write_text(render_latex_table(card), encoding="utf-8")

    print(f"Dataset : {args.csv}  ({card['n_rows']} lignes, {card['n_cols']} colonnes)")
    print(f"Taux de fraude : {card['fraud_rate']*100:.2f}% ({card['fraud_count']}/{card['n_rows']})")
    if card["missing_features_from_expected_28"]:
        print(f"⚠️  Features ML attendues mais absentes : {card['missing_features_from_expected_28']}")
    print(f"\n✅ JSON  -> {json_path}")
    print(f"✅ LaTeX -> {tex_path}")


if __name__ == "__main__":
    main()
