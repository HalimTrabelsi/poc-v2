"""Alert service — notify investigators the moment a CRITICAL/HIGH case is detected.

Supports three channels (all configurable, all optional):
  1. Generic HTTP webhook  → Slack / Microsoft Teams / PagerDuty / custom
  2. Odoo mail.message     → Appears in OpenG2P internal inbox (no module required)
  3. Stdout log            → Always active; shows up in docker logs

Configuration (app/config.py / .env):
  ALERT_WEBHOOK_URL       = https://hooks.slack.com/services/... (leave blank to disable)
  ALERT_MIN_RISK_LEVEL    = CRITICAL  (or HIGH to widen the net)
  ALERT_ODOO_ENABLED      = true
"""
import logging
import threading
from typing import Optional

logger = logging.getLogger(__name__)

_RISK_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


def _should_alert(risk_level: str, min_level: str) -> bool:
    return _RISK_ORDER.get(risk_level, 0) >= _RISK_ORDER.get(min_level, 3)


class AlertService:
    """Fire-and-forget alert dispatcher (runs in a daemon thread)."""

    def send(self, decision: dict) -> None:
        """Non-blocking: dispatch alert in a background thread."""
        from app.config import settings
        risk = decision.get("risk_level", "LOW")
        if not _should_alert(risk, settings.alert_min_risk_level):
            return
        threading.Thread(
            target=self._dispatch,
            args=(decision,),
            daemon=True,
            name="AlertDispatch",
        ).start()

    def _dispatch(self, decision: dict) -> None:
        from app.config import settings
        bid    = decision.get("beneficiary_id", "?")
        risk   = decision.get("risk_level", "?")
        score  = decision.get("final_score", 0)
        rec    = decision.get("recommendation", "?")
        cid    = decision.get("case_id", "?")
        rules  = [r.get("rule_id") for r in (decision.get("rules_triggered") or [])]

        logger.warning(
            "ALERT [%s] beneficiary=%s score=%.3f recommendation=%s case=%s rules=%s",
            risk, bid, score, rec, cid, rules,
        )

        if settings.alert_webhook_url:
            self._send_webhook(settings.alert_webhook_url, decision)

        if settings.alert_odoo_enabled:
            self._send_odoo_message(decision)

    # ── Webhook (Slack / Teams / generic) ────────────────────────────────────

    def _send_webhook(self, url: str, decision: dict) -> None:
        """POST a JSON payload to any HTTP webhook endpoint.

        The payload is compatible with Slack's incoming-webhook format.
        Teams and PagerDuty also accept similar JSON structures.
        """
        import requests as req

        bid   = decision.get("beneficiary_id", "?")
        risk  = decision.get("risk_level", "?")
        score = decision.get("final_score", 0)
        rec   = decision.get("recommendation", "?")
        cid   = (decision.get("case_id") or "")[:12]
        rules = ", ".join(r.get("rule_id","") for r in (decision.get("rules_triggered") or []))

        emoji = {"CRITICAL": ":red_circle:", "HIGH": ":large_orange_circle:"}.get(risk, ":warning:")

        # Slack-compatible block kit payload
        payload = {
            "text": f"{emoji} *Fraud Alert [{risk}]* — Beneficiary `{bid}`",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"{emoji} *Fraud Alert — {risk}*\n"
                            f"*Beneficiary:* `{bid}`\n"
                            f"*Score:* {score:.1%}   *Action:* {rec}\n"
                            f"*Case:* `{cid}…`   *Rules:* {rules or 'N/A'}"
                        ),
                    },
                }
            ],
        }
        try:
            r = req.post(url, json=payload, timeout=5)
            logger.info("Webhook alert sent: status=%d", r.status_code)
        except Exception as exc:
            logger.warning("Webhook alert failed: %s", exc)

    # ── Odoo mail.message (direct SQL insert) ─────────────────────────────────

    def _send_odoo_message(self, decision: dict) -> None:
        """Insert a mail.message into Odoo so it appears in the internal inbox.

        No XML-RPC needed — we write directly to the mail_message table.
        The message is linked to the res.partner record of the flagged beneficiary.
        """
        try:
            from sqlalchemy import create_engine, text
            from app.config import settings

            bid  = decision.get("beneficiary_id", "0")
            risk = decision.get("risk_level", "?")
            score = decision.get("final_score", 0)
            rec  = decision.get("recommendation", "?")
            cid  = (decision.get("case_id") or "")[:36]
            rules = ", ".join(
                r.get("rule_id", "") for r in (decision.get("rules_triggered") or [])
            )

            body = (
                f"<p><b>Fraud Alert [{risk}]</b></p>"
                f"<p>Score: <b>{score:.1%}</b> &mdash; Recommendation: <b>{rec}</b></p>"
                f"<p>Rules triggered: {rules or 'N/A'}</p>"
                f"<p>Case ID: <code>{cid}</code></p>"
                f"<p><em>Generated by Fraud Detection Engine v2</em></p>"
            )

            e = create_engine(settings.openg2p_db_url)
            with e.begin() as conn:
                # Find the model id for res.partner
                model_row = conn.execute(
                    text("SELECT id FROM ir_model WHERE model='res.partner' LIMIT 1")
                ).fetchone()
                if not model_row:
                    return
                model_id = model_row[0]

                # Get a valid author (first internal user)
                author_row = conn.execute(
                    text("SELECT id FROM res_partner WHERE id IN "
                         "(SELECT partner_id FROM res_users WHERE active=true AND share=false "
                         "ORDER BY id LIMIT 1)")
                ).fetchone()
                author_id = author_row[0] if author_row else None

                # Insert mail_message
                msg_row = conn.execute(text("""
                    INSERT INTO mail_message
                        (subject, body, model, res_id, message_type,
                         subtype_id, author_id, create_uid, write_uid,
                         create_date, write_date, date)
                    VALUES
                        (:subject, :body, 'res.partner', :res_id, 'notification',
                         1, :author_id, 1, 1,
                         NOW(), NOW(), NOW())
                    RETURNING id
                """), {
                    "subject": f"Fraud Alert [{risk}] — Beneficiary {bid}",
                    "body":    body,
                    "res_id":  int(bid),
                    "author_id": author_id,
                }).fetchone()

                msg_id = msg_row[0] if msg_row else None

                # Insert mail_notification for every internal user so it
                # appears in each investigator's Odoo inbox (is_read=false)
                if msg_id:
                    internal_partners = conn.execute(text("""
                        SELECT partner_id FROM res_users
                        WHERE active = true AND share = false
                        AND partner_id IS NOT NULL
                    """)).fetchall()

                    # Get the database name for the bus channel
                    db_row = conn.execute(text("SELECT current_database()")).fetchone()
                    db_name = db_row[0] if db_row else "openg2p"

                    for (partner_id,) in internal_partners:
                        # mail_notification makes it appear in the inbox list
                        conn.execute(text("""
                            INSERT INTO mail_notification
                                (mail_message_id, res_partner_id,
                                 notification_type, notification_status, is_read)
                            VALUES
                                (:msg_id, :partner_id, 'inbox', 'sent', false)
                        """), {"msg_id": msg_id, "partner_id": partner_id})

                        # bus_bus triggers the live push — browser updates instantly
                        import json
                        bus_channel = json.dumps([db_name, "res.partner", partner_id])
                        bus_message = json.dumps({
                            "type": "mail.message/inbox",
                            "payload": {
                                "id": msg_id,
                                "subject": f"Fraud Alert [{risk}] — Beneficiary {bid}",
                                "body": body,
                                "model": "res.partner",
                                "res_id": int(bid),
                                "author": {"id": author_id},
                                "needaction_partner_ids": [partner_id],
                            }
                        })
                        conn.execute(text("""
                            INSERT INTO bus_bus
                                (create_uid, write_uid, create_date, write_date, channel, message)
                            VALUES
                                (1, 1, NOW(), NOW(), :channel, :message)
                        """), {"channel": bus_channel, "message": bus_message})

            logger.info("Odoo inbox notification sent for beneficiary %s", bid)

        except Exception as exc:
            logger.warning("Odoo alert failed: %s", exc)


# Singleton
_alert_service: Optional[AlertService] = None


def get_alert_service() -> AlertService:
    global _alert_service
    if _alert_service is None:
        _alert_service = AlertService()
    return _alert_service
