"""generate_dataset_histograms.py — Two comparative histograms for the report:

1. OpenG2P synthetic dataset: pmt_score distribution, légitime vs fraude —
   demonstrates the two classes overlap (not perfectly separable), which is
   the realism property the training pipeline deliberately targets.
2. PaySim (real Kaggle dataset): transaction amount distribution, légitime
   vs fraude — same comparative logic on real-world data.

Runs directly on the host (plain pandas/matplotlib, no Docker needed) since
both source CSVs already live under this repo.

Usage:
    python ml/scripts/generate_dataset_histograms.py
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # fraud-detection-engine
OPENG2P_CSV = ROOT / "ml" / "data" / "synthetic" / "dataset_ml.csv"
PAYSIM_CSV = ROOT / "data" / "paysim_clean_balanced.csv"
OUT_DIR = ROOT.parent / "docs" / "Rapport" / "architecture"

EY_DARK = "#2E2E38"
RED = "#C62828"


def fig_openg2p_feature(feature: str = "pmt_score"):
    df = pd.read_csv(OPENG2P_CSV)
    legit = df.loc[df["is_fraud"] == 0, feature].dropna()
    fraud = df.loc[df["is_fraud"] == 1, feature].dropna()

    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    bins = np.linspace(
        min(legit.min(), fraud.min()), max(legit.max(), fraud.max()), 30
    )
    ax.hist(legit, bins=bins, alpha=0.6, label="Légitime", color=EY_DARK, density=True)
    ax.hist(fraud, bins=bins, alpha=0.6, label="Fraude", color=RED, density=True)
    ax.set_xlabel(feature)
    ax.set_ylabel("Densité")
    ax.set_title(f"Distribution de {feature} — dataset OpenG2P synthétique")
    ax.legend()
    fig.tight_layout()
    out = OUT_DIR / f"openg2p_{feature}_distribution.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"✅ {out}")
    print(f"   Légitime : moyenne={legit.mean():.3f}  écart-type={legit.std():.3f}")
    print(f"   Fraude   : moyenne={fraud.mean():.3f}  écart-type={fraud.std():.3f}")


def fig_paysim_amount():
    if not PAYSIM_CSV.exists():
        print(f"⚠️  {PAYSIM_CSV} introuvable — figure PaySim ignorée")
        return
    df = pd.read_csv(PAYSIM_CSV, usecols=["log_amount", "is_fraud"])
    legit = df.loc[df["is_fraud"] == 0, "log_amount"].dropna()
    fraud = df.loc[df["is_fraud"] == 1, "log_amount"].dropna()

    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    bins = np.linspace(
        min(legit.min(), fraud.min()), max(legit.max(), fraud.max()), 40
    )
    ax.hist(legit, bins=bins, alpha=0.6, label="Légitime", color=EY_DARK, density=True)
    ax.hist(fraud, bins=bins, alpha=0.6, label="Fraude", color=RED, density=True)
    ax.set_xlabel("log(montant de la transaction)")
    ax.set_ylabel("Densité")
    ax.set_title("Distribution du montant — dataset PaySim (Kaggle, réel)")
    ax.legend()
    fig.tight_layout()
    out = OUT_DIR / "paysim_amount_distribution.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"✅ {out}  (n={len(df)}, fraude={df['is_fraud'].mean()*100:.2f}%)")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Génération : distribution pmt_score (OpenG2P)...")
    fig_openg2p_feature("pmt_score")
    print("\nGénération : distribution des montants (PaySim, données réelles)...")
    fig_paysim_amount()


if __name__ == "__main__":
    main()
