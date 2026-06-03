"""Fraud case model — stores cases synced from the fraud-detection-engine."""
from odoo import fields, models, api, _
from odoo.exceptions import UserError


RISK_LEVELS = [
    ("CRITICAL", "Critical"),
    ("HIGH", "High"),
    ("MEDIUM", "Medium"),
    ("LOW", "Low"),
]

CASE_STATES = [
    ("open", "Open"),
    ("investigating", "Investigating"),
    ("confirmed", "Confirmed Fraud"),
    ("dismissed", "Dismissed (False Positive)"),
    ("closed", "Closed"),
]

RECOMMENDATIONS = [
    ("CLEAR", "Clear"),
    ("MONITOR", "Monitor"),
    ("MANUAL_REVIEW", "Manual Review"),
    ("BLOCK_PAYMENT", "Block Payment"),
]


class FraudCase(models.Model):
    _name = "fraud.case"
    _description = "Fraud Detection Case"
    _inherit = ["mail.thread", "mail.activity.mixin"]
    _order = "create_date desc"
    _rec_name = "case_id"

    case_id = fields.Char(
        string="Case ID", required=True, copy=False, index=True, tracking=True,
    )
    beneficiary_id = fields.Char(
        string="Beneficiary ID", required=True, index=True, tracking=True,
    )
    partner_id = fields.Many2one(
        "res.partner", string="Beneficiary", compute="_compute_partner_id",
        store=True, index=True,
    )
    final_score = fields.Float(string="Risk Score", digits=(5, 4), tracking=True)
    risk_level = fields.Selection(
        RISK_LEVELS, string="Risk Level", required=True, index=True, tracking=True,
    )
    recommendation = fields.Selection(
        RECOMMENDATIONS, string="Recommendation", tracking=True,
    )
    state = fields.Selection(
        CASE_STATES, string="Status", default="open", required=True, tracking=True,
    )
    rules_triggered = fields.Text(string="Rules Triggered")
    rules_count = fields.Integer(
        string="# Rules", compute="_compute_rules_count", store=True,
    )
    explanation = fields.Text(string="Explanation")
    llm_explanation = fields.Text(string="AI Explanation")
    top_features = fields.Text(
        string="Key Factors (SHAP)",
        help="ML feature contributions that drove the risk score, "
             "most influential first.",
    )
    detected_at = fields.Datetime(string="Detected At", tracking=True)
    assigned_to = fields.Many2one("res.users", string="Assigned To", tracking=True)
    notes = fields.Text(string="Investigation Notes")
    score_pct = fields.Char(
        string="Score %", compute="_compute_score_pct",
    )
    color = fields.Integer(string="Kanban Color", compute="_compute_color")

    _sql_constraints = [
        ("case_id_unique", "UNIQUE(case_id)", "Case ID must be unique."),
    ]

    @api.depends("beneficiary_id")
    def _compute_partner_id(self):
        for rec in self:
            if not rec.beneficiary_id:
                rec.partner_id = False
                continue
            try:
                pid = int(rec.beneficiary_id)
            except (TypeError, ValueError):
                rec.partner_id = False
                continue
            rec.partner_id = self.env["res.partner"].browse(pid).exists()

    @api.depends("rules_triggered")
    def _compute_rules_count(self):
        for rec in self:
            if not rec.rules_triggered:
                rec.rules_count = 0
                continue
            rec.rules_count = len([r for r in rec.rules_triggered.split(",") if r.strip()])

    @api.depends("final_score")
    def _compute_score_pct(self):
        for rec in self:
            rec.score_pct = f"{(rec.final_score or 0) * 100:.1f}%"

    @api.depends("risk_level")
    def _compute_color(self):
        color_map = {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 10}
        for rec in self:
            rec.color = color_map.get(rec.risk_level, 0)

    def action_mark_investigating(self):
        self.write({"state": "investigating"})
        for rec in self:
            rec.message_post(body=_("Case moved to investigating."))

    def action_mark_confirmed(self):
        self.write({"state": "confirmed"})
        for rec in self:
            rec.message_post(body=_("Confirmed as fraud."))

    def action_mark_dismissed(self):
        self.write({"state": "dismissed"})
        for rec in self:
            rec.message_post(body=_("Dismissed as false positive."))

    def action_close(self):
        self.write({"state": "closed"})

    def action_generate_llm_explanation(self):
        """Call the fraud-engine /llm_explain endpoint and store the result."""
        import json
        import urllib.request
        import urllib.error
        import logging

        _logger = logging.getLogger(__name__)
        self.ensure_one()

        ICP = self.env["ir.config_parameter"].sudo()
        base = ICP.get_param("fraud_detection.api_url", "http://fraud-engine:8000/api/v1")
        key = ICP.get_param("fraud_detection.api_key", "dev-secret-change-in-prod")

        url = f"{base}/cases/{self.case_id}/llm_explain"
        req = urllib.request.Request(
            url, method="POST", headers={"X-API-Key": key, "Content-Type": "application/json"},
            data=b"{}",
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            text = payload.get("llm_explanation") or ""
            if text:
                self.llm_explanation = text
                self.message_post(body="AI explanation generated.")
            else:
                self.message_post(body="LLM returned an empty response — model may still be loading.")
        except urllib.error.URLError as e:
            _logger.warning("LLM explain call failed: %s", e)
            self.message_post(body=f"Failed to reach LLM service: {e}")
        return True

    def action_view_on_heatmap(self):
        """Open the Streamlit heatmap centered on this beneficiary in a new tab."""
        self.ensure_one()
        ICP = self.env["ir.config_parameter"].sudo()
        base = ICP.get_param(
            "fraud_detection.dashboard_url", "http://localhost:8501"
        ).rstrip("/")
        return {
            "type": "ir.actions.act_url",
            "url": f"{base}/?page=geo&beneficiary={self.beneficiary_id}",
            "target": "new",
        }

    def action_open_beneficiary(self):
        self.ensure_one()
        if not self.partner_id:
            raise UserError(_("No matching beneficiary record found in the registry."))
        return {
            "type": "ir.actions.act_window",
            "res_model": "res.partner",
            "res_id": self.partner_id.id,
            "view_mode": "form",
            "target": "current",
        }

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        for rec in records:
            if rec.risk_level in ("CRITICAL", "HIGH"):
                fraud_users = self.env["res.users"].search([
                    "|",
                    ("groups_id", "=", self.env.ref("g2p_fraud_detection.group_fraud_officer").id),
                    ("groups_id", "=", self.env.ref("g2p_fraud_detection.group_fraud_supervisor").id),
                ])
                for user in fraud_users:
                    # Skip users with no linked partner — the channel would be
                    # res.partner-False and _sendone would target nothing.
                    if not user.partner_id:
                        continue
                    # Odoo 17 signature: _sendone(target, notification_type, message)
                    self.env["bus.bus"]._sendone(
                        user.partner_id,
                        "new_alert",
                        {
                            "case_id": rec.case_id,
                            "beneficiary_id": rec.beneficiary_id,
                            "risk_level": rec.risk_level,
                            "final_score": rec.final_score,
                            "recommendation": rec.recommendation,
                        },
                    )
        return records
