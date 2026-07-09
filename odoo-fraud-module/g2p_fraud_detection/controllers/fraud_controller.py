"""HTTP controller for the fraud detection module."""
import hashlib
import hmac
import os
import time

from werkzeug.urls import url_encode

from odoo import http
from odoo.http import request


def _is_fraud_user(user):
    return user.has_group("g2p_fraud_detection.group_fraud_officer") or user.has_group(
        "g2p_fraud_detection.group_fraud_supervisor"
    )


class FraudController(http.Controller):

    @http.route("/fraud/sync_now", type="json", auth="user")
    def sync_now(self):
        """Trigger an immediate sync with the fraud-engine API."""
        if not request.env.user.has_group("g2p_fraud_detection.group_fraud_officer"):
            return {"error": "unauthorized"}
        ok = request.env["fraud.sync"].sudo().cron_sync_cases()
        return {"ok": bool(ok)}

    @http.route("/fraud/open_dashboard", type="http", auth="user")
    def open_dashboard(self, page=None, **kwargs):
        """Mint a short-lived signed token and redirect to the Streamlit dashboard.

        The dashboard has no shared session with Odoo, so identity is
        delegated here: only Fraud Officers/Supervisors get a valid token,
        closing the "anyone who can reach port 8501 is a fraud analyst" gap.
        """
        user = request.env.user
        if not _is_fraud_user(user):
            return request.not_found()

        ICP = request.env["ir.config_parameter"].sudo()
        base = ICP.get_param("fraud_detection.dashboard_url", "http://localhost:8501").rstrip("/")
        secret = os.environ.get("DASHBOARD_TOKEN_SECRET", "")

        expiry = int(time.time()) + 300
        payload = f"{user.id}:{expiry}"
        signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        token = f"{payload}:{signature}"

        params = {"token": token}
        if page:
            params["page"] = page
        for key, value in kwargs.items():
            params[key] = value

        return request.redirect(f"{base}/?{url_encode(params)}", local=False)

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
