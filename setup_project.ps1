# ══════════════════════════════════════════════════════════════
#  FRAUD DETECTION ENGINE — Setup Project Structure
#  PFE OpenG2P — Halim Trabelsi
#  Usage : .\setup_project.ps1
#  Depuis : C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\
# ══════════════════════════════════════════════════════════════

Write-Host "======================================" -ForegroundColor Cyan
Write-Host " Setup Fraud Detection Engine" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan

# ──────────────────────────────────────────────────────────────
# ETAPE 1 — CREATION DE TOUS LES DOSSIERS
# ──────────────────────────────────────────────────────────────

$dirs = @(
    # Fraud Detection Engine (FastAPI + ML)
    "fraud-detection-engine\app\api",
    "fraud-detection-engine\app\core",
    "fraud-detection-engine\app\db",
    "fraud-detection-engine\app\schemas",
    "fraud-detection-engine\ml\rules",
    "fraud-detection-engine\ml\notebooks",
    "fraud-detection-engine\models_saved",
    "fraud-detection-engine\tests",

    # Module Odoo custom
    "odoo-fraud-module\g2p_fraud_detection\models",
    "odoo-fraud-module\g2p_fraud_detection\views",
    "odoo-fraud-module\g2p_fraud_detection\security",
    "odoo-fraud-module\g2p_fraud_detection\controllers",

    # Monitoring
    "monitoring\grafana\datasources",
    "monitoring\grafana\dashboards",

    # Dashboard Streamlit
    "dashboard",

    # Scripts SQL
    "scripts",

    # GitHub Actions CI/CD
    ".github\workflows"
)

foreach ($dir in $dirs) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
}

Write-Host "✅ [1/4] Dossiers crees" -ForegroundColor Green

# ──────────────────────────────────────────────────────────────
# ETAPE 2 — FICHIERS __init__.py (Python packages)
# ──────────────────────────────────────────────────────────────

$initFiles = @(
    "fraud-detection-engine\app\__init__.py",
    "fraud-detection-engine\app\api\__init__.py",
    "fraud-detection-engine\app\core\__init__.py",
    "fraud-detection-engine\app\db\__init__.py",
    "fraud-detection-engine\app\schemas\__init__.py",
    "fraud-detection-engine\tests\__init__.py",
    "odoo-fraud-module\g2p_fraud_detection\__init__.py",
    "odoo-fraud-module\g2p_fraud_detection\models\__init__.py",
    "odoo-fraud-module\g2p_fraud_detection\controllers\__init__.py"
)

foreach ($f in $initFiles) {
    New-Item -ItemType File -Force -Path $f | Out-Null
}

Write-Host "✅ [2/4] Fichiers __init__.py crees" -ForegroundColor Green

# ──────────────────────────────────────────────────────────────
# ETAPE 3 — FICHIERS VIDES (placeholders)
# ──────────────────────────────────────────────────────────────

$files = @(
    # FastAPI app
    "fraud-detection-engine\app\main.py",
    "fraud-detection-engine\app\api\routes_scoring.py",
    "fraud-detection-engine\app\api\routes_graph.py",
    "fraud-detection-engine\app\api\routes_cases.py",
    "fraud-detection-engine\app\api\routes_rules.py",
    "fraud-detection-engine\app\core\rule_engine.py",
    "fraud-detection-engine\app\core\ml_scorer.py",
    "fraud-detection-engine\app\core\graph_analyzer.py",
    "fraud-detection-engine\app\core\shap_explainer.py",
    "fraud-detection-engine\app\core\llm_explainer.py",
    "fraud-detection-engine\app\core\pipeline.py",
    "fraud-detection-engine\app\db\postgres.py",
    "fraud-detection-engine\app\schemas\fraud.py",

    # ML
    "fraud-detection-engine\ml\train.py",
    "fraud-detection-engine\ml\evaluate.py",
    "fraud-detection-engine\ml\feature_engineering.py",
    "fraud-detection-engine\ml\rules\fraud_rules.json",

    # Config
    "fraud-detection-engine\Dockerfile",
    "fraud-detection-engine\requirements.txt",
    "fraud-detection-engine\.env.example",
    "fraud-detection-engine\tests\test_rule_engine.py",
    "fraud-detection-engine\tests\test_pipeline.py",

    # Odoo module
    "odoo-fraud-module\g2p_fraud_detection\__manifest__.py",
    "odoo-fraud-module\g2p_fraud_detection\models\fraud_alert.py",
    "odoo-fraud-module\g2p_fraud_detection\views\fraud_alert_views.xml",
    "odoo-fraud-module\g2p_fraud_detection\views\fraud_menu.xml",
    "odoo-fraud-module\g2p_fraud_detection\security\ir.model.access.csv",

    # Monitoring
    "monitoring\prometheus.yml",
    "monitoring\grafana\datasources\prometheus.yml",
    "monitoring\grafana\dashboards\dashboard.yml",
    "monitoring\grafana\dashboards\fraud_dashboard.json",

    # Dashboard Streamlit
    "dashboard\app.py",
    "dashboard\Dockerfile",
    "dashboard\requirements.txt",

    # Scripts
    "scripts\init_db.sql",
    "scripts\seed_data.py",

    # CI/CD
    ".github\workflows\ci.yml",

    # Root
    "docker-compose.full.yml",
    "README.md",
    ".env.example"
)

foreach ($f in $files) {
    New-Item -ItemType File -Force -Path $f | Out-Null
}

Write-Host "✅ [3/4] Fichiers crees" -ForegroundColor Green

# ──────────────────────────────────────────────────────────────
# ETAPE 4 — VERIFICATION FINALE
# ──────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host " Structure creee :" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan

Get-ChildItem -Recurse -Name | Where-Object { $_ -notmatch "\.git" } | Sort-Object

Write-Host ""
Write-Host "✅ [4/4] Structure complete !" -ForegroundColor Green
Write-Host ""
Write-Host "======================================" -ForegroundColor Yellow
Write-Host " PROCHAINES ETAPES :" -ForegroundColor Yellow
Write-Host "======================================" -ForegroundColor Yellow
Write-Host " 1. git add ." -ForegroundColor White
Write-Host " 2. git commit -m 'feat: initial project structure'" -ForegroundColor White
Write-Host " 3. git push" -ForegroundColor White
Write-Host " 4. Remplir les fichiers avec le code" -ForegroundColor White
Write-Host " 5. docker compose -f docker-compose.full.yml up -d" -ForegroundColor White
Write-Host "======================================" -ForegroundColor Yellow
