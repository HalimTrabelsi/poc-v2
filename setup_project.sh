#!/bin/bash
# ══════════════════════════════════════════════════════════════
#  FRAUD DETECTION ENGINE — Setup Project Structure
#  PFE OpenG2P — Halim Trabelsi
#  Usage : bash setup_project.sh
# ══════════════════════════════════════════════════════════════
set -e

echo "======================================"
echo " Setup Fraud Detection Engine"
echo "======================================"

# ── Adapter ce nom selon ton repo ──
PROJECT="fraud-detection-project"

# ══════════════════════════════════════════════════════════════
# ETAPE 1 — CREATION DE TOUS LES DOSSIERS
# ══════════════════════════════════════════════════════════════

# Fraud Detection Engine (FastAPI + ML)
mkdir -p $PROJECT/fraud-detection-engine/app/api
mkdir -p $PROJECT/fraud-detection-engine/app/core
mkdir -p $PROJECT/fraud-detection-engine/app/db
mkdir -p $PROJECT/fraud-detection-engine/app/schemas
mkdir -p $PROJECT/fraud-detection-engine/ml/rules
mkdir -p $PROJECT/fraud-detection-engine/ml/notebooks
mkdir -p $PROJECT/fraud-detection-engine/models_saved
mkdir -p $PROJECT/fraud-detection-engine/tests

# Module Odoo custom
mkdir -p $PROJECT/odoo-fraud-module/g2p_fraud_detection/models
mkdir -p $PROJECT/odoo-fraud-module/g2p_fraud_detection/views
mkdir -p $PROJECT/odoo-fraud-module/g2p_fraud_detection/security
mkdir -p $PROJECT/odoo-fraud-module/g2p_fraud_detection/controllers

# Monitoring (Grafana + Prometheus)
mkdir -p $PROJECT/monitoring/grafana/datasources
mkdir -p $PROJECT/monitoring/grafana/dashboards

# Dashboard Streamlit
mkdir -p $PROJECT/dashboard

# Scripts SQL
mkdir -p $PROJECT/scripts

# GitHub Actions CI/CD
mkdir -p $PROJECT/.github/workflows

echo "✅ [1/4] Dossiers crees"

# ══════════════════════════════════════════════════════════════
# ETAPE 2 — FICHIERS __init__.py (Python packages)
# ══════════════════════════════════════════════════════════════
touch $PROJECT/fraud-detection-engine/app/__init__.py
touch $PROJECT/fraud-detection-engine/app/api/__init__.py
touch $PROJECT/fraud-detection-engine/app/core/__init__.py
touch $PROJECT/fraud-detection-engine/app/db/__init__.py
touch $PROJECT/fraud-detection-engine/app/schemas/__init__.py
touch $PROJECT/fraud-detection-engine/tests/__init__.py
touch $PROJECT/odoo-fraud-module/g2p_fraud_detection/__init__.py
touch $PROJECT/odoo-fraud-module/g2p_fraud_detection/models/__init__.py
touch $PROJECT/odoo-fraud-module/g2p_fraud_detection/controllers/__init__.py

echo "✅ [2/4] Fichiers __init__.py crees"

# ══════════════════════════════════════════════════════════════
# ETAPE 3 — FICHIERS VIDES (placeholders)
# ══════════════════════════════════════════════════════════════

# FastAPI
touch $PROJECT/fraud-detection-engine/app/main.py
touch $PROJECT/fraud-detection-engine/app/api/routes_scoring.py
touch $PROJECT/fraud-detection-engine/app/api/routes_graph.py
touch $PROJECT/fraud-detection-engine/app/api/routes_cases.py
touch $PROJECT/fraud-detection-engine/app/api/routes_rules.py
touch $PROJECT/fraud-detection-engine/app/core/rule_engine.py
touch $PROJECT/fraud-detection-engine/app/core/ml_scorer.py
touch $PROJECT/fraud-detection-engine/app/core/graph_analyzer.py
touch $PROJECT/fraud-detection-engine/app/core/shap_explainer.py
touch $PROJECT/fraud-detection-engine/app/core/llm_explainer.py
touch $PROJECT/fraud-detection-engine/app/core/pipeline.py
touch $PROJECT/fraud-detection-engine/app/db/postgres.py
touch $PROJECT/fraud-detection-engine/app/schemas/fraud.py

# ML
touch $PROJECT/fraud-detection-engine/ml/train.py
touch $PROJECT/fraud-detection-engine/ml/evaluate.py
touch $PROJECT/fraud-detection-engine/ml/feature_engineering.py
touch $PROJECT/fraud-detection-engine/ml/rules/fraud_rules.json

# Config
touch $PROJECT/fraud-detection-engine/Dockerfile
touch $PROJECT/fraud-detection-engine/requirements.txt
touch $PROJECT/fraud-detection-engine/.env.example
touch $PROJECT/fraud-detection-engine/tests/test_rule_engine.py
touch $PROJECT/fraud-detection-engine/tests/test_pipeline.py

# Odoo module
touch $PROJECT/odoo-fraud-module/g2p_fraud_detection/__manifest__.py
touch $PROJECT/odoo-fraud-module/g2p_fraud_detection/models/fraud_alert.py
touch $PROJECT/odoo-fraud-module/g2p_fraud_detection/views/fraud_alert_views.xml
touch $PROJECT/odoo-fraud-module/g2p_fraud_detection/views/fraud_menu.xml
touch $PROJECT/odoo-fraud-module/g2p_fraud_detection/security/ir.model.access.csv

# Monitoring
touch $PROJECT/monitoring/prometheus.yml
touch $PROJECT/monitoring/grafana/datasources/prometheus.yml
touch $PROJECT/monitoring/grafana/dashboards/dashboard.yml
touch $PROJECT/monitoring/grafana/dashboards/fraud_dashboard.json

# Dashboard
touch $PROJECT/dashboard/app.py
touch $PROJECT/dashboard/Dockerfile
touch $PROJECT/dashboard/requirements.txt

# Scripts
touch $PROJECT/scripts/init_db.sql
touch $PROJECT/scripts/seed_data.py

# CI/CD
touch $PROJECT/.github/workflows/ci.yml

# Root
touch $PROJECT/docker-compose.full.yml
touch $PROJECT/README.md
touch $PROJECT/.gitignore
touch $PROJECT/.env.example

echo "✅ [3/4] Fichiers crees"

# ══════════════════════════════════════════════════════════════
# ETAPE 4 — GIT INIT
# ══════════════════════════════════════════════════════════════
cd $PROJECT
git init
git add .
git commit -m "feat: initial project structure — Fraud Detection Engine OpenG2P"
cd ..

echo "✅ [4/4] Git initialise"

# ══════════════════════════════════════════════════════════════
# VERIFICATION FINALE
# ══════════════════════════════════════════════════════════════
echo ""
echo "======================================"
echo " Structure complete :"
echo "======================================"
find $PROJECT -not -path '*/.git/*' | sort
echo ""
echo "======================================"
echo " DONE ! Prochaine etape :"
echo " cd $PROJECT"
echo " Coller les fichiers de code (main.py, etc.)"
echo " docker compose -f docker-compose.full.yml up -d"
echo "======================================"
