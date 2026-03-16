from odoo import api, fields, models


class G2PEntitlement(models.Model):
    _inherit = "g2p.entitlement"

    supporting_document_ids = fields.One2many("storage.file", "entitlement_id")
    document_count = fields.Integer(compute="_compute_document_count")

    @api.depends("supporting_document_ids")
    def _compute_document_count(self):
        for record in self:
            record.document_count = len(record.supporting_document_ids)
