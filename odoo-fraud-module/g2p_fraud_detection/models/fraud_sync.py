"""Periodic sync between the fraud-engine REST API and fraud.case records."""
import logging
import urllib.request
import urllib.error
import json
from datetime import datetime

from odoo import api, models

_logger = logging.getLogger(__name__)


class FraudSync(models.AbstractModel):
    _name = "fraud.sync"
    _description = "Fraud Engine Sync Service"

    @api.model
    def _get_config(self):
        ICP = self.env["ir.config_parameter"].sudo()
        return {
            "url": ICP.get_param("fraud_detection.api_url",
                                  "http://fraud-engine:8000/api/v1"),
            "api_key": ICP.get_param("fraud_detection.api_key",
                                      "dev-secret-change-in-prod"),
            "limit": int(ICP.get_param("fraud_detection.sync_limit", "200")),
        }

    @api.model
    def cron_sync_cases(self):
        cfg = self._get_config()
        url = f"{cfg['url']}/cases?limit={cfg['limit']}"
        req = urllib.request.Request(url, headers={"X-API-Key": cfg["api_key"]})

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            _logger.warning("Fraud sync failed to reach %s: %s", url, e)
            return False
        except (json.JSONDecodeError, ValueError) as e:
            _logger.warning("Fraud sync got invalid JSON: %s", e)
            return False

        cases = payload.get("cases", [])
        if not cases:
            _logger.info("Fraud sync: no cases returned")
            return True

        Case = self.env["fraud.case"]
        existing_ids = set(
            Case.search([("case_id", "in", [c.get("case_id") for c in cases if c.get("case_id")])])
                .mapped("case_id")
        )
        valid_recommendations = {
            v for v, _ in Case._fields["recommendation"].selection
        }
        valid_risk_levels = {
            v for v, _ in Case._fields["risk_level"].selection
        }
        created = 0
        for c in cases:
            cid = c.get("case_id")
            if not cid or cid in existing_ids:
                continue
            created_at_raw = c.get("created_at") or ""
            try:
                detected_at = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                detected_at = detected_at.replace(tzinfo=None)
            except (ValueError, AttributeError):
                detected_at = fields_now()

            rules = c.get("rules_triggered") or []
            if isinstance(rules, list):
                rules_str = self._format_rules(rules)
            else:
                rules_str = str(rules)

            risk_level = (c.get("risk_level") or "LOW").upper()
            if risk_level not in valid_risk_levels:
                risk_level = "LOW"
            recommendation = (c.get("recommendation") or "").upper()
            if recommendation not in valid_recommendations:
                recommendation = False

            new_case = Case.create({
                "case_id": cid,
                "beneficiary_id": str(c.get("beneficiary_id") or ""),
                "final_score": float(c.get("final_score") or 0.0),
                "risk_level": risk_level,
                "recommendation": recommendation,
                "rules_triggered": rules_str,
                "explanation": c.get("explanation") or "",
                "llm_explanation": c.get("llm_explanation") or "",
                "detected_at": detected_at,
                "state": "open",
            })
            created += 1

            # Auto-generate LLM explanation for CRITICAL/HIGH so officers
            # don't have to click a button. Best-effort: failures are logged
            # and the officer can still trigger manually via the form button.
            if risk_level in ("CRITICAL", "HIGH"):
                self._fetch_llm_explanation(new_case, cfg)

        _logger.info("Fraud sync: created %d new cases", created)
        return True

    def _fetch_llm_explanation(self, case, cfg):
        """Call the engine's /llm_explain endpoint and store the result on the case."""
        url = f"{cfg['url']}/cases/{case.case_id}/llm_explain"
        req = urllib.request.Request(
            url,
            method="POST",
            data=b"",
            headers={"X-API-Key": cfg["api_key"], "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            text = payload.get("llm_explanation") or ""
            if text:
                case.llm_explanation = text
        except urllib.error.URLError as e:
            _logger.warning("LLM auto-gen failed for %s: %s", case.case_id, e)
        except Exception as e:
            _logger.warning("LLM auto-gen error for %s: %s", case.case_id, e)

    @staticmethod
    def _format_rules(rules: list) -> str:
        """Render a list of triggered-rule dicts as a human-readable block.

        Each entry becomes a bullet line with the rule's display name and its
        contextual explanation, e.g.:

            • Shared Bank Account — Bank account shared with 4 other beneficiaries
            • Shared Phone Number — Phone number shared with 6 other beneficiaries

        Falls back to the rule_id only when no name is available.
        """
        lines = []
        for r in rules:
            if not isinstance(r, dict):
                lines.append(f"• {r}")
                continue
            name = r.get("name") or r.get("rule_id") or "Unknown rule"
            explanation = r.get("explanation") or ""
            if explanation:
                lines.append(f"• {name} — {explanation}")
            else:
                lines.append(f"• {name}")
        return "\n".join(lines)


def fields_now():
    from odoo.fields import Datetime
    return Datetime.now()
