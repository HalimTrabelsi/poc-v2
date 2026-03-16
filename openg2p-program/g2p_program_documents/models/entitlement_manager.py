from odoo import models


class DefaultEntitlementManagerForDocument(models.Model):
    _inherit = "g2p.program.entitlement.manager.default"

    def prepare_entitlements(self, cycle, beneficiaries):
        ents = super().prepare_entitlements(cycle, beneficiaries)

        if ents:
            self.copy_documents_from_beneficiary(ents)
        return ents

    def copy_documents_from_beneficiary(self, entitlements):
        for rec in entitlements:
            prog_mem = rec.partner_id.program_membership_ids.filtered(
                lambda x: x.program_id.id == rec.program_id.id
            )[0]
            old_entitlements = rec.partner_id.entitlement_ids.filtered(
                lambda x: x.program_id.id == rec.program_id.id and x.id != rec.id
            )
            old_entitlements = old_entitlements.sorted("create_date", reverse=True)
            old_entitlement = None
            if old_entitlements:
                old_entitlement = old_entitlements[0]
            for document in prog_mem.supporting_documents_ids:
                if not document.entitlement_id:
                    if (not old_entitlement) or (document.create_date > old_entitlement.create_date):
                        document.entitlement_id = rec
