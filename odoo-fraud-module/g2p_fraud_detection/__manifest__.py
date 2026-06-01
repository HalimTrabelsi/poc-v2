{
    "name": "G2P Fraud Detection",
    "version": "17.0.1.0.0",
    "category": "G2P/Fraud",
    "summary": "Real-time fraud alert monitor + case management inside Odoo",
    "description": """
Native Odoo integration for the fraud-detection-engine.

* Live alert kanban with auto-refresh via Odoo bus.bus
* Beneficiary-linked cases with full registry/payment context
* Role-based access (officer / supervisor / admin)
* Periodic sync with the fraud-engine REST API
* Mail thread + activities for case investigation
""",
    "author": "PFE OpenG2P",
    "depends": ["base", "mail", "bus", "g2p_registry_base"],
    "data": [
        "security/fraud_security.xml",
        "security/ir.model.access.csv",
        "views/fraud_case_views.xml",
        "views/fraud_dashboard_views.xml",
        "views/fraud_menu.xml",
        "data/cron.xml",
        "data/config_parameters.xml",
    ],
    "assets": {
        "web.assets_backend": [
            "g2p_fraud_detection/static/src/js/live_monitor.js",
            "g2p_fraud_detection/static/src/css/fraud_dashboard.css",
        ],
    },
    "installable": True,
    "application": True,
    "license": "LGPL-3",
}
