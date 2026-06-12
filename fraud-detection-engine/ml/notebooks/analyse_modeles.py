#!/usr/bin/env python
"""
Analyse des modèles de détection de fraude — version autonome (sans Jupyter).
Génère des graphiques en PNG dans ml/outputs/

Usage:
    cd fraud-detection-engine
    python ml/notebooks/analyse_modeles.py
"""

import warnings
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import (
    roc_auc_score, average_precision_score, f1_score, precision_score, recall_score,
    confusion_matrix, roc_curve, classification_report,
)
import xgboost as xgb

warnings.filterwarnings('ignore')
plt.style.use('seaborn-v0_8-whitegrid') if 'seaborn-v0_8-whitegrid' in plt.style.available else plt.style.use('default')

RANDOM_STATE = 42
OUTPUT_DIR = Path(__file__).parent.parent / 'outputs'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 70)
print("  ANALYSE DES MODÈLES DE DÉTECTION DE FRAUDE")
print("=" * 70)
print(f"\nGraphiques sauvegardés dans : {OUTPUT_DIR}\n")

# ========== Chargement données ==========
DATA_PATH = Path(__file__).parent.parent / 'data' / 'synthetic' / 'dataset_ml.csv'
df = pd.read_csv(DATA_PATH)
print(f"[OK] Dataset charge : {df.shape[0]} lignes, taux fraude = {df['is_fraud'].mean():.1%}")

FEATURES = [
    'age', 'income', 'income_per_person', 'dependency_ratio',
    'nb_programs', 'nb_active_programs', 'avg_enrollment_days',
    'payment_count', 'payment_gap_ratio', 'payment_success_rate',
    'amount_variance', 'cycle_count', 'shared_phone_count',
    'shared_account_count', 'network_risk', 'group_membership_count',
    'high_amount_flag', 'income_program_inconsistency',
]
FEATURES = [c for c in FEATURES if c in df.columns]
X = df[FEATURES].fillna(0)
y = df['is_fraud'].astype(int)
print(f"[OK] {len(FEATURES)} features selectionnees")

# ========== Train/test ==========
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=RANDOM_STATE
)

# ========== 1. Distributions ==========
key_feats = ['network_risk', 'shared_phone_count', 'payment_gap_ratio', 'nb_programs']
fig, axes = plt.subplots(2, 2, figsize=(14, 9))
for ax, feat in zip(axes.ravel(), key_feats):
    for label, name, color in [(0, 'Légitime', 'tab:blue'), (1, 'Fraude', 'tab:red')]:
        ax.hist(df[df.is_fraud == label][feat], bins=30, alpha=0.5, label=name, color=color, density=True)
    ax.set_title(feat, fontsize=11); ax.legend()
fig.suptitle('Distributions Légitime vs Fraude (chevauchement = problème réaliste)', fontsize=13)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '01_feature_distributions.png', dpi=100, bbox_inches='tight')
print("[OK] 01_feature_distributions.png")
plt.close()

# ========== Entraînement ==========
scale = float((y_train == 0).sum() / max((y_train == 1).sum(), 1))
models = {}

models['Logistic Regression'] = LogisticRegression(
    class_weight='balanced', max_iter=1000, random_state=RANDOM_STATE
).fit(X_train, y_train)

models['Random Forest'] = RandomForestClassifier(
    n_estimators=300, max_depth=8, class_weight='balanced',
    random_state=RANDOM_STATE, n_jobs=-1
).fit(X_train, y_train)

xgb_base = xgb.XGBClassifier(
    n_estimators=400, learning_rate=0.05, max_depth=6,
    min_child_weight=3, subsample=0.8, colsample_bytree=0.8,
    scale_pos_weight=scale, eval_metric='auc',
    random_state=RANDOM_STATE, n_jobs=-1,
)
models['XGBoost calibré'] = CalibratedClassifierCV(
    xgb_base, cv=3, method='isotonic'
).fit(X_train, y_train)

iso = IsolationForest(
    n_estimators=200, contamination=0.12, random_state=RANDOM_STATE, n_jobs=-1
).fit(X_train)
iso_scores = -iso.score_samples(X_test)
iso_scores = (iso_scores - iso_scores.min()) / (iso_scores.max() - iso_scores.min())

print("[OK] 4 modeles entraines")

# ========== Métriques ==========
rows = []
proba_cache = {}
for name, model in models.items():
    proba = model.predict_proba(X_test)[:, 1]
    pred = (proba >= 0.5).astype(int)
    proba_cache[name] = proba
    rows.append({
        'Modèle': name, 'AUC-ROC': roc_auc_score(y_test, proba),
        'PR-AUC': average_precision_score(y_test, proba),
        'F1': f1_score(y_test, pred), 'Précision': precision_score(y_test, pred),
        'Rappel': recall_score(y_test, pred),
    })

rows.append({
    'Modèle': 'Isolation Forest', 'AUC-ROC': roc_auc_score(y_test, iso_scores),
    'PR-AUC': average_precision_score(y_test, iso_scores),
    'F1': np.nan, 'Précision': np.nan, 'Rappel': np.nan,
})

results = pd.DataFrame(rows).set_index('Modèle').round(4)
print("\n" + results.to_string())

# ========== 2. ROC Curves ==========
plt.figure(figsize=(9, 7))
for name, proba in proba_cache.items():
    fpr, tpr, _ = roc_curve(y_test, proba)
    plt.plot(fpr, tpr, label=f'{name} (AUC={roc_auc_score(y_test, proba):.3f})')
fpr, tpr, _ = roc_curve(y_test, iso_scores)
plt.plot(fpr, tpr, '--', label=f'Isolation Forest (AUC={roc_auc_score(y_test, iso_scores):.3f})')
plt.plot([0, 1], [0, 1], 'k:', alpha=0.5, label='Random')
plt.xlabel('Taux de faux positifs'); plt.ylabel('Taux de vrais positifs')
plt.title('Courbes ROC — Comparaison des modèles')
plt.legend(loc='lower right'); plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '02_roc_curves.png', dpi=100, bbox_inches='tight')
print("[OK] 02_roc_curves.png")
plt.close()

# ========== 3. Matrice confusion ==========
best = 'XGBoost calibré'
pred = (proba_cache[best] >= 0.5).astype(int)
cm = confusion_matrix(y_test, pred)

fig, ax = plt.subplots(figsize=(6, 5))
im = ax.imshow(cm, cmap='Blues')
labels = ['Légitime', 'Fraude']
ax.set_xticks([0, 1]); ax.set_xticklabels(labels)
ax.set_yticks([0, 1]); ax.set_yticklabels(labels)
ax.set_xlabel('Prédiction'); ax.set_ylabel('Réalité')
ax.set_title(f'Matrice de confusion — {best}')
thresh = cm.max() / 2
for i in range(2):
    for j in range(2):
        ax.text(j, i, format(cm[i, j], 'd'), ha='center', va='center',
                color='white' if cm[i, j] > thresh else 'black', fontsize=14)
fig.colorbar(im, ax=ax)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '03_confusion_matrix.png', dpi=100, bbox_inches='tight')
print("[OK] 03_confusion_matrix.png")
plt.close()
print("\nClassification report (XGBoost calibré):")
print(classification_report(y_test, pred, target_names=['Légitime', 'Fraude']))

# ========== 4. Feature importance ==========
rf = models['Random Forest']
imp = pd.Series(rf.feature_importances_, index=FEATURES).sort_values()
fig, ax = plt.subplots(figsize=(9, 7))
imp.plot(kind='barh', ax=ax, color='tab:green')
ax.set_title('Importance des features — Random Forest')
ax.set_xlabel('Importance')
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '04_feature_importance.png', dpi=100, bbox_inches='tight')
print("[OK] 04_feature_importance.png")
plt.close()

# ========== 5. Score distribution ==========
fig, ax = plt.subplots(figsize=(9, 5))
proba = proba_cache[best]
ax.hist(proba[y_test == 0], bins=40, alpha=0.6, label='Légitime', color='tab:blue', density=True)
ax.hist(proba[y_test == 1], bins=40, alpha=0.6, label='Fraude', color='tab:red', density=True)
ax.set_xlabel('Score de fraude prédit'); ax.set_ylabel('Densité')
ax.set_title(f'Distribution des scores — {best}')
ax.legend(); ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '05_score_distribution.png', dpi=100, bbox_inches='tight')
print("[OK] 05_score_distribution.png")
plt.close()

print("\n" + "=" * 70)
print("[DONE] Analyse terminée avec succès!")
print(f"Tous les graphiques dans : {OUTPUT_DIR}")
print("=" * 70)
