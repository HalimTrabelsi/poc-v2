"""HTTP controller for the fraud detection module."""
import base64
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
        # Carry the display name (URL-safe base64, no padding — keeps the ':'
        # token separator unambiguous) so the dashboard can greet the analyst.
        name_b64 = base64.urlsafe_b64encode(
            (user.name or "").encode("utf-8")
        ).decode("ascii").rstrip("=")
        payload = f"{user.id}:{expiry}:{name_b64}"
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

    @http.route("/fraud/cron_status", type="json", auth="user")
    def cron_status(self):
        """Expose the sync cron's real timing so the Live Alert Monitor can
        show a live "last scan / next scan" indicator — visible proof the
        1-minute cron is actually running, not just a static claim."""
        ICP = request.env["ir.config_parameter"].sudo()
        cron = request.env.ref("g2p_fraud_detection.ir_cron_fraud_sync", raise_if_not_found=False)
        return {
            "last_sync_at": ICP.get_param("fraud_detection.last_sync_at", ""),
            "last_sync_count": int(ICP.get_param("fraud_detection.last_sync_count", "0") or 0),
            "next_run_at": str(cron.sudo().nextcall) if cron else "",
            "interval_seconds": (cron.sudo().interval_number * 60) if cron and cron.sudo().interval_type == "minutes" else 60,
        }
