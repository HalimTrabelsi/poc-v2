"""HTTP controller for the fraud detection module."""
from odoo import http
from odoo.http import request


class FraudController(http.Controller):

    @http.route("/fraud/sync_now", type="json", auth="user")
    def sync_now(self):
        """Trigger an immediate sync with the fraud-engine API."""
        if not request.env.user.has_group("g2p_fraud_detection.group_fraud_officer"):
            return {"error": "unauthorized"}
        ok = request.env["fraud.sync"].sudo().cron_sync_cases()
        return {"ok": bool(ok)}

    @http.route("/fraud/stats", type="json", auth="user")
    def stats(self):
        """Return live counts by risk level for the dashboard."""
        Case = request.env["fraud.case"]
        return {
            "critical": Case.search_count([("risk_level", "=", "CRITICAL"),
                                            ("state", "in", ("open", "investigating"))]),
            "high": Case.search_count([("risk_level", "=", "HIGH"),
                                        ("state", "in", ("open", "investigating"))]),
            "medium": Case.search_count([("risk_level", "=", "MEDIUM"),
                                          ("state", "in", ("open", "investigating"))]),
            "total_open": Case.search_count([("state", "in", ("open", "investigating"))]),
        }
