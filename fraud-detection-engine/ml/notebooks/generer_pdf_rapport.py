#!/usr/bin/env python
"""
Assemble toutes les visualisations + syntheses en un seul PDF pour le rapport PFE.
Utilise matplotlib PdfPages (aucune dependance externe).

Usage:
    cd fraud-detection-engine
    python ml/notebooks/generer_pdf_rapport.py
"""

from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
import textwrap
import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = BASE_DIR / 'ml' / 'outputs'
PDF_PATH = OUTPUT_DIR / 'RAPPORT_PFE_RESULTATS_MODELES.pdf'

print("=" * 80)
print("  GENERATION DU PDF - RAPPORT PFE")
print("=" * 80)

# Structure du rapport : (titre de section, liste de figures, description)
SECTIONS = [
    {
        'title': "1. Analyse du Dataset Synthetique (Production)",
        'subtitle': "Modele principal beneficiaire - 5000 echantillons, 14% fraude",
        'figures': [
            ('01_feature_distributions.png', "Distributions Legitime vs Fraude : chevauchement realiste des classes"),
            ('02_roc_curves.png', "Courbes ROC comparatives des 4 modeles"),
            ('03_confusion_matrix.png', "Matrice de confusion du meilleur modele (XGBoost calibre)"),
            ('04_feature_importance.png', "Importance des features (Random Forest)"),
            ('05_score_distribution.png', "Distribution des scores de fraude predits"),
        ],
    },
    {
        'title': "2. Comparaison Multi-Datasets",
        'subtitle': "Synthetic (5K) vs PaySim (172K) - tous les modeles",
        'figures': [
            ('comparison_auc_all.png', "AUC-ROC : comparaison de tous les modeles et datasets"),
            ('heatmap_metrics_all.png', "Matrice des metriques (AUC, F1, Precision, Recall)"),
            ('correlation_matrices.png', "Matrices de correlation des features par dataset"),
        ],
    },
    {
        'title': "3. Evolution et Amelioration des Modeles",
        'subtitle': "De la fuite de donnees (0.99) au modele production (0.86)",
        'figures': [
            ('16_evolution_auc.png', "Evolution AUC-ROC : 4 phases d'amelioration"),
            ('17_evolution_precision_recall.png', "Evolution du trade-off Precision-Recall"),
            ('18_feature_engineering_impact.png', "Impact du feature engineering et de la calibration"),
            ('19_issues_timeline.png', "Timeline de resolution des problemes (data leakage, etc.)"),
        ],
    },
]

# Sections optionnelles (figures de sessions precedentes si elles existent)
OPTIONAL_FIGURES = [
    ('20_auc_comparison_all.png', "Comparaison AUC etendue"),
    ('21_performance_matrix.png', "Matrice de performance detaillee"),
    ('22_correlations_all.png', "Correlations completes tous datasets"),
    ('scenarios_analysis.png', "Analyse par scenario de fraude"),
]


def add_text_page(pdf, title, body, fontsize=10):
    """Ajoute une page de texte au PDF."""
    fig = plt.figure(figsize=(8.27, 11.69))  # A4 portrait
    fig.text(0.1, 0.92, title, fontsize=16, fontweight='bold', va='top')
    wrapped = "\n".join(textwrap.fill(line, width=95) if len(line) > 95 else line
                        for line in body.split("\n"))
    fig.text(0.1, 0.86, wrapped, fontsize=fontsize, va='top', family='monospace')
    plt.axis('off')
    pdf.savefig(fig)
    plt.close(fig)


def add_cover_page(pdf):
    """Page de garde."""
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.text(0.5, 0.72, "RAPPORT PFE", fontsize=32, fontweight='bold', ha='center')
    fig.text(0.5, 0.66, "Resultats des Modeles de Detection de Fraude", fontsize=16, ha='center')
    fig.text(0.5, 0.60, "Moteur Hybride : Rules + ML + Graph Analytics", fontsize=13, ha='center', style='italic')

    fig.text(0.5, 0.48, "Datasets analyses :", fontsize=12, fontweight='bold', ha='center')
    fig.text(0.5, 0.44, "Synthetic (5000) | PaySim (172K) | OpenG2P | Nigeria Registry", fontsize=11, ha='center')

    fig.text(0.5, 0.36, "Modeles compares :", fontsize=12, fontweight='bold', ha='center')
    fig.text(0.5, 0.32, "Logistic Regression | Random Forest | XGBoost calibre | Isolation Forest", fontsize=11, ha='center')

    date_str = datetime.date.today().strftime("%d %B %Y")
    fig.text(0.5, 0.12, f"Genere le {date_str}", fontsize=10, ha='center', style='italic')

    # Cadre decoratif
    fig.patches.append(plt.Rectangle((0.08, 0.08), 0.84, 0.84, fill=False,
                                     edgecolor='#1f77b4', linewidth=2,
                                     transform=fig.transFigure))
    plt.axis('off')
    pdf.savefig(fig)
    plt.close(fig)


def add_image_page(pdf, img_path, caption, section_title=None):
    """Ajoute une page avec une image."""
    if not img_path.exists():
        print(f"      [SKIP] {img_path.name} introuvable")
        return False
    fig = plt.figure(figsize=(8.27, 11.69))
    if section_title:
        fig.text(0.1, 0.95, section_title, fontsize=11, color='#1f77b4', fontweight='bold', va='top')
    img = mpimg.imread(str(img_path))
    ax = fig.add_axes([0.05, 0.15, 0.9, 0.75])
    ax.imshow(img)
    ax.axis('off')
    fig.text(0.5, 0.10, caption, fontsize=10, ha='center', style='italic', wrap=True)
    fig.text(0.5, 0.05, img_path.name, fontsize=7, ha='center', color='gray')
    pdf.savefig(fig)
    plt.close(fig)
    print(f"      [OK] {img_path.name}")
    return True


def add_table_page(pdf, csv_path, title):
    """Ajoute une page avec un tableau a partir d'un CSV."""
    if not csv_path.exists():
        return
    df = pd.read_csv(csv_path)
    # Arrondir les floats
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].round(4)

    fig = plt.figure(figsize=(11.69, 8.27))  # A4 paysage
    fig.text(0.5, 0.93, title, fontsize=14, fontweight='bold', ha='center')
    ax = fig.add_axes([0.05, 0.1, 0.9, 0.75])
    ax.axis('off')
    table = ax.table(cellText=df.values, colLabels=df.columns,
                     cellLoc='center', loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.5)
    # Style header
    for j in range(len(df.columns)):
        table[(0, j)].set_facecolor('#1f77b4')
        table[(0, j)].set_text_props(color='white', fontweight='bold')
    pdf.savefig(fig)
    plt.close(fig)
    print(f"      [OK] Tableau: {csv_path.name}")


# ========== Generation du PDF ==========
with PdfPages(PDF_PATH) as pdf:
    print("\nGeneration des pages...")

    # Page de garde
    add_cover_page(pdf)
    print("      [OK] Page de garde")

    # Tableau de resultats principal
    add_table_page(pdf, OUTPUT_DIR / 'resultats_pfe.csv', "Tableau des Resultats - Tous Datasets")

    # Sections avec figures
    for section in SECTIONS:
        # Page titre de section
        add_text_page(pdf, section['title'],
                     f"\n{section['subtitle']}\n\n" + "-" * 80,
                     fontsize=11)
        print(f"\n   Section: {section['title']}")
        # Figures
        for fig_name, caption in section['figures']:
            add_image_page(pdf, OUTPUT_DIR / fig_name, caption, section['title'])

    # Section optionnelle
    print(f"\n   Section: Visualisations complementaires")
    has_optional = False
    for fig_name, caption in OPTIONAL_FIGURES:
        if (OUTPUT_DIR / fig_name).exists():
            if not has_optional:
                add_text_page(pdf, "4. Visualisations Complementaires",
                             "\nAnalyses additionnelles generees lors des experiences.\n\n" + "-" * 80)
                has_optional = True
            add_image_page(pdf, OUTPUT_DIR / fig_name, caption, "4. Visualisations Complementaires")

    # Synthese finale (depuis les fichiers txt)
    synthese_files = [
        ('SYNTHESE_EVOLUTION_MODELES.txt', "Synthese : Evolution des Modeles"),
        ('RAPPORT_PFE_FINAL_SYNTHESE.txt', "Synthese Finale"),
    ]
    for txt_name, title in synthese_files:
        txt_path = OUTPUT_DIR / txt_name
        if txt_path.exists():
            content = txt_path.read_text(encoding='utf-8', errors='replace')
            # Decouper en pages de ~50 lignes
            lines = content.split("\n")
            chunk_size = 50
            for i in range(0, len(lines), chunk_size):
                chunk = "\n".join(lines[i:i+chunk_size])
                page_title = title if i == 0 else f"{title} (suite)"
                add_text_page(pdf, page_title, chunk, fontsize=7)
            print(f"      [OK] Synthese: {txt_name}")

print("\n" + "=" * 80)
print(f"[DONE] PDF genere : {PDF_PATH}")
print(f"       Taille : {PDF_PATH.stat().st_size / 1024:.0f} KB")
print("=" * 80)
