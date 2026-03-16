# Part of OpenG2P. See LICENSE file for full copyright and licensing details.

from odoo import fields, models


class G2PCreateProgramWizard(models.TransientModel):
    _inherit = "g2p.program.create.wizard"

    portal_form_builder_id = fields.Many2one(
        "formio.builder", string="Program Form", domain="[('is_form_mapped_with_program', '=', False)]"
    )

    is_multiple_form_submission = fields.Boolean(default=False)

    def create_program(self):
        res = super().create_program()

        program = self.env["g2p.program"].browse(res["res_id"])
        portal_form = self.portal_form_builder_id

        if portal_form:
            program.portal_form_builder_id = portal_form

        program.is_multiple_form_submission = self.is_multiple_form_submission

        return res
