from odoo import api, fields, models


class G2PDocument(models.Model):
    _inherit = "storage.file"

    program_membership_id = fields.Many2one("g2p.program_membership")

    entitlement_id = fields.Many2one("g2p.entitlement")

    @api.constrains("entitlement_id")
    def _constrains_entitlement_id(self):
        for rec in self:
            if not rec.program_membership_id:
                prog_mem = rec.entitlement_id.partner_id.program_membership_ids.filtered(
                    lambda x: x.program_id.id == rec.entitlement_id.program_id.id
                )
                if prog_mem:
                    rec.program_membership_id = prog_mem[0]

    @api.constrains("program_membership_id")
    def _constrains_program_membership_id(self):
        for rec in self:
            if not rec.registrant_id:
                rec.registrant_id = rec.program_membership_id.partner_id
