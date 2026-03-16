from odoo import fields, models


class G2PProgram(models.Model):
    _inherit = "g2p.program"

    supporting_documents_store = fields.Many2one("storage.backend")

    def get_documents_store(self):
        self.ensure_one()
        if self.supporting_documents_store:
            return self.supporting_documents_store
        reg_doc_store = self.env["res.partner"].get_registry_documents_store()
        if reg_doc_store:
            return reg_doc_store
        else:
            return None
