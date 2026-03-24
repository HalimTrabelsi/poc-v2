"""
ML Training Script — Entraînement sur les vraies données OpenG2P
"""
import os
import sys
import numpy as np
import pandas as pd
import joblib
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import (
    roc_auc_score, f1_score,
    classification_report, confusion_matrix,
    precision_score, recall_score
)
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier
from imblearn.over_sampling import SMOTE
import warnings
warnings.filterwarnings("ignore")
os.environ["GIT_PYTHON_REFRESH"] = "quiet"

# ── Features à utiliser depuis OpenG2P ───────────────────────
FEATURES = [
    "age",
    "income",
    "household_size",
    "nb_programs",
    "payment_count",
    "income_per_person",      # feature dérivée
    "region_encoded",         # feature dérivée
    "gender_encoded",         # feature dérivée
    "high_income_flag",       # feature dérivée
    "multi_program_flag",     # feature dérivée
]

OPENG2P_DB_URL = os.getenv(
    "OPENG2P_DB_URL",
    "postgresql://odoo:openg2p@postgresql:5432/openg2p"
)


def load_from_database() -> pd.DataFrame:
    """Charge les données depuis la base OpenG2P"""
    print("🔌 Connexion à la base OpenG2P...")
    try:
        engine = create_engine(OPENG2P_DB_URL, connect_args={"connect_timeout": 10})

        # Adapter cette requête à ta vraie structure de tables
        query = text("""
            SELECT
                r.id,
                r.name,
                EXTRACT(YEAR FROM AGE(r.birthdate))::int   AS age,
                COALESCE(SUM(p.amount_paid), 0)            AS income,
                COUNT(DISTINCT gm.individual_id)           AS household_size,
                COUNT(DISTINCT pm.program_id)              AS nb_programs,
                COUNT(DISTINCT p.id)                       AS payment_count,
                r.gender,
                'Centre'                                   AS region
            FROM res_partner r
            LEFT JOIN g2p_program_membership pm ON r.id = pm.partner_id
            LEFT JOIN g2p_payment p             ON r.id = p.partner_id
            LEFT JOIN g2p_group_membership gm   ON r.id = gm.group_id
            WHERE r.active = true
            GROUP BY r.id, r.name, r.birthdate, r.gender
            HAVING COUNT(DISTINCT pm.program_id) > 0
        """)

        df = pd.read_sql(query, engine)
        print(f"✅ {len(df)} bénéficiaires chargés depuis OpenG2P")
        return df

    except Exception as e:
        print(f"⚠️  DB non accessible ({e})")
        print("📂 Chargement depuis CSV local...")
        return None


def load_from_csv(csv_path: str = "ml/data/openg2p_data.csv") -> pd.DataFrame:
    """Charge depuis le CSV exporté d'OpenG2P"""
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        print(f"✅ {len(df)} bénéficiaires chargés depuis {csv_path}")
        return df
    raise FileNotFoundError(f"CSV non trouvé : {csv_path}")


def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée les features ML depuis les données brutes OpenG2P.
    C'est l'étape la plus importante du pipeline.
    """
    df = df.copy()

    # 1. Revenu par personne du ménage
    df["income_per_person"] = df["income"] / df["household_size"].clip(lower=1)

    # 2. Encodage région
    region_map = {"Nord": 0, "Sud": 1, "Est": 2, "Ouest": 3, "Centre": 4}
    df["region_encoded"] = df["region"].map(region_map).fillna(2)

    # 3. Encodage genre
    df["gender_encoded"] = (df["gender"] == "M").astype(int)

    # 4. Flag revenu anormalement élevé
    income_q90 = df["income"].quantile(0.90)
    df["high_income_flag"] = (df["income"] > income_q90).astype(int)

    # 5. Flag multi-inscription programmes
    df["multi_program_flag"] = (df["nb_programs"] > 3).astype(int)

    # 6. Normaliser le revenu (éviter qu'il domine tout)
    df["income_log"] = np.log1p(df["income"])

    return df


def analyze_data(df: pd.DataFrame):
    """Analyse les patterns de fraude dans les données"""
    print("\n" + "="*55)
    print("  ANALYSE DES DONNÉES OPENG2P")
    print("="*55)
    print(f"  Total bénéficiaires : {len(df)}")
    print(f"  Fraudeurs           : {df['is_fraud'].sum()} ({df['is_fraud'].mean():.1%})")
    print(f"  Légitimes           : {(df['is_fraud']==0).sum()}")

    print("\n  Comparaison Fraudeurs vs Légitimes :")
    cols = ["age", "income", "household_size", "nb_programs", "payment_count"]
    comparison = df.groupby("is_fraud")[cols].mean().round(1)
    comparison.index = ["Légitime", "Fraudeur"]
    print(comparison.to_string())

    print("\n  ⚠️  Attention aux features trop discriminantes :")
    for col in cols:
        fraud_mean  = df[df["is_fraud"]==1][col].mean()
        legit_mean  = df[df["is_fraud"]==0][col].mean()
        ratio       = fraud_mean / max(legit_mean, 0.001)
        flag        = "🚨 TROP discriminante" if ratio > 10 else "✅ OK"
        print(f"    {col:<20} ratio={ratio:.1f}x  {flag}")
    print("="*55)


def train():
    mlflow.set_tracking_uri(
        os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    )
    mlflow.set_experiment("fraud-openg2p-realdata")

    # ── 1. Charger les données réelles ───────────────────────
    df = load_from_database()
    if df is None:
        df = load_from_csv("ml/data/openg2p_data.csv")

    # ── 2. Feature Engineering ───────────────────────────────
    print("\n🔧 Feature Engineering...")
    df = feature_engineering(df)

    # ── 3. Analyser les données ──────────────────────────────
    analyze_data(df)

    # ── 4. Préparer X et y ───────────────────────────────────
    # Utiliser income_log au lieu de income brut
    features_used = [
        "age", "income_log", "household_size",
        "nb_programs", "payment_count",
        "income_per_person", "region_encoded",
        "gender_encoded", "high_income_flag", "multi_program_flag"
    ]

    X = df[features_used]
    y = df["is_fraud"]

    print(f"\n📊 Features utilisées : {features_used}")

    # ── 5. Split train/validation/test ───────────────────────
    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )
    X_train, X_val, y_train, y_val = train_test_split(
        X_temp, y_temp, test_size=0.20, random_state=42, stratify=y_temp
    )

    print(f"\nSplit:")
    print(f"  Train : {len(X_train)} ({y_train.sum()} fraudes)")
    print(f"  Val   : {len(X_val)} ({y_val.sum()} fraudes)")
    print(f"  Test  : {len(X_test)} ({y_test.sum()} fraudes)")

    with mlflow.start_run(run_name="xgboost_realdata"):

        # ── 6. SMOTE ─────────────────────────────────────────
        sm = SMOTE(random_state=42, k_neighbors=3)
        X_bal, y_bal = sm.fit_resample(X_train, y_train)
        print(f"\nAprès SMOTE: {len(X_bal)} lignes")

        # ── 7. XGBoost ───────────────────────────────────────
        xgb = XGBClassifier(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=10,
            min_child_weight=5,
            gamma=0.2,
            reg_alpha=0.1,
            reg_lambda=1.0,
            eval_metric="auc",
            use_label_encoder=False,
            random_state=42,
        )

        xgb.fit(X_bal, y_bal,
                eval_set=[(X_val, y_val)],
                verbose=False)

        # ── 8. Évaluation ────────────────────────────────────
        y_pred  = xgb.predict(X_test)
        y_proba = xgb.predict_proba(X_test)[:, 1]

        auc       = roc_auc_score(y_test, y_proba)
        f1        = f1_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall    = recall_score(y_test, y_pred, zero_division=0)

        print(f"\n{'='*55}")
        print(f"  RÉSULTATS SUR DONNÉES TEST (jamais vues)")
        print(f"{'='*55}")
        print(f"  AUC-ROC   : {auc:.4f}")
        print(f"  F1-Score  : {f1:.4f}")
        print(f"  Précision : {precision:.4f}")
        print(f"  Rappel    : {recall:.4f}")

        if auc >= 0.99:
            print(f"\n  ⚠️  AUC très élevé → une feature est trop discriminante")
            print(f"      Probablement 'income' — les fraudeurs ont des revenus")
            print(f"      très différents dans tes données")
        elif auc >= 0.80:
            print(f"\n  ✅ Bon modèle réaliste !")

        print(f"\n{classification_report(y_test, y_pred, target_names=['Légitime','Fraudeur'])}")

        cm = confusion_matrix(y_test, y_pred)
        print(f"  Vrais Négatifs  (légitimes OK)     : {cm[0][0]}")
        print(f"  Faux Positifs   (fausses alertes)  : {cm[0][1]}")
        print(f"  Faux Négatifs   (fraudes ratées)   : {cm[1][0]}")
        print(f"  Vrais Positifs  (fraudes détectées): {cm[1][1]}")

        # ── 9. Feature importance ────────────────────────────
        print(f"\n📈 Importance des features :")
        importance = pd.Series(
            xgb.feature_importances_,
            index=features_used
        ).sort_values(ascending=False)
        for feat, imp in importance.items():
            bar = "█" * int(imp * 40)
            print(f"  {feat:<25} {bar} {imp:.3f}")

        # ── 10. Validation croisée ───────────────────────────
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        cv_scores = cross_val_score(xgb, X, y, cv=cv,
                                    scoring="roc_auc", n_jobs=-1)
        print(f"\n🔄 Validation croisée (5-fold) :")
        print(f"  AUC moyen : {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
        for i, s in enumerate(cv_scores):
            print(f"  Fold {i+1}    : {s:.4f}")

        # ── 11. Log MLflow ───────────────────────────────────
        mlflow.log_params({
            "features": str(features_used),
            "n_train": len(X_train),
            "n_test": len(X_test),
            "fraud_rate": float(y.mean()),
        })
        mlflow.log_metrics({
            "auc_roc":      auc,
            "f1_score":     f1,
            "precision":    precision,
            "recall":       recall,
            "cv_auc_mean":  cv_scores.mean(),
            "cv_auc_std":   cv_scores.std(),
        })
        mlflow.sklearn.log_model(
            xgb, "xgboost",
            registered_model_name="FraudXGBoost"
        )

        # ── 12. IsolationForest ──────────────────────────────
        iso = IsolationForest(
            contamination=0.05, n_estimators=100,
            max_samples=0.8, random_state=42
        )
        iso.fit(X_train)
        mlflow.sklearn.log_model(iso, "isolation_forest")

        # ── 13. Sauvegarder ─────────────────────────────────
        os.makedirs("models_saved", exist_ok=True)
        joblib.dump(xgb, "models_saved/xgboost.pkl")
        joblib.dump(iso, "models_saved/isoforest.pkl")
        joblib.dump(features_used, "models_saved/features.pkl")

        print(f"\n✅ Modèles sauvegardés !")
        print(f"✅ Features sauvegardées : {len(features_used)} variables")


if __name__ == "__main__":
    train()