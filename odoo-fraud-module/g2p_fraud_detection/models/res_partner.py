"""Auto-finalize beneficiary imports.

When a registrant is created via the OpenG2P dashboard CSV import, the
wizard only writes res_partner columns. The fraud engine, however, reads
its signals from satellite tables (g2p_phone_number for shared-phone
detection, res_partner_bank for shared-account detection) and needs the
beneficiary enrolled in a program for any rule to fire.

This override transparently creates those records right after the partner
is created, so the demo (and real imports) need no manual finalize step.

The bank account is read from the import 'comment' field in the form
'account=NNNNNN' — the same convention the demo CSV uses. If no account
token is present the bank step is skipped (phones + enrolment still run).
"""
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

# 'Social Aid Program' — the single program created at OpenG2P setup time.
# Configurable via ir.config_parameter so non-demo deployments can point
# at their own default program (or disable auto-enrolment with 0).
DEFAULT_PROGRAM_PARAM = "fraud_detection.default_program_id"
DEFAULT_PROGRAM_ID = 16


class ResPartner(models.Model):
    _inherit = "res.partner"

    @api.model_create_multi
    def create(self, vals_list):
        partners = super().create(vals_list)
        # Only act on registrants; skip companies, users, contacts, groups.
        registrants = partners.filtered(
            lambda p: p.is_registrant and not p.is_group
        )
        if registrants:
            try:
                registrants._fraud_auto_finalize()
            except Exception as exc:  # never block partner creation
                _logger.warning("Auto-finalize skipped: %s", exc)
        return partners

    def _fraud_auto_finalize(self):
        """Create phone / bank / program records for these registrants."""
        ICP = self.env["ir.config_parameter"].sudo()
        try:
            program_id = int(ICP.get_param(DEFAULT_PROGRAM_PARAM, DEFAULT_PROGRAM_ID))
        except (TypeError, ValueError):
            program_id = DEFAULT_PROGRAM_ID

        cr = self.env.cr
        for partner in self:
            pid = partner.id

            # 1) Phone record — needed for shared-phone detection
            phone = partner.phone or partner.mobile
            if phone:
                cr.execute(
                    "SELECT 1 FROM g2p_phone_number WHERE partner_id=%s LIMIT 1",
                    (pid,),
                )
                if not cr.fetchone():
                    cr.execute(
                        """
                        INSERT INTO g2p_phone_number
                            (partner_id, phone_no, phone_sanitized,
                             date_collected, create_date)
                        VALUES (%s, %s, %s, CURRENT_DATE, NOW())
                        """,
                        (pid, phone, phone),
                    )

            # 2) Bank account — parsed from the 'account=...' token in comment
            account = self._extract_account(partner.comment or "")
            if account:
                cr.execute(
                    "SELECT 1 FROM res_partner_bank WHERE partner_id=%s LIMIT 1",
                    (pid,),
                )
                if not cr.fetchone():
                    cr.execute(
                        """
                        INSERT INTO res_partner_bank
                            (partner_id, acc_number, sanitized_acc_number,
                             active, create_date)
                        VALUES (%s, %s, %s, true, NOW())
                        """,
                        (pid, account, account),
                    )

            # 3) Program enrolment — without this, no rule has context to fire
            if program_id:
                cr.execute(
                    "SELECT 1 FROM g2p_program_membership "
                    "WHERE partner_id=%s AND program_id=%s LIMIT 1",
                    (pid, program_id),
                )
                if not cr.fetchone():
                    cr.execute(
                        """
                        INSERT INTO g2p_program_membership
                            (partner_id, program_id, state,
                             enrollment_date, create_date)
                        VALUES (%s, %s, 'enrolled', NOW(), NOW())
                        """,
                        (pid, program_id),
                    )

    @staticmethod
    def _extract_account(comment: str) -> str:
        """Pull 'account=NNNN' out of the free-text comment field."""
        for chunk in (comment or "").split(";"):
            chunk = chunk.strip()
            if chunk.startswith("account="):
                return chunk.split("=", 1)[1].strip()
        return ""
