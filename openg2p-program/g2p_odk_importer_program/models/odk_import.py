from datetime import date

from odoo import api, fields, models


class OdkImport(models.Model):
    _inherit = "odk.import"

    target_program = fields.Many2one("g2p.program", domain="[('target_type', '=', target_registry)]")

    @api.onchange("target_registry")
    def onchange_target_registry(self):
        for rec in self:
            rec.target_program = None

    def process_records_handle_addl_data(self, mapped_json):
        if self.target_program:
            mapped_json["program_membership_ids"] = [
                (
                    0,
                    0,
                    {
                        "program_id": self.target_program.id,
                        "state": "draft",
                        "enrollment_date": date.today(),
                    },
                )
            ]

        if "program_registrant_info_ids" in mapped_json:
            prog_reg_info = mapped_json.get("program_registrant_info_ids", None)

            if not self.target_program:
                mapped_json.pop("program_registrant_info_ids")
                return mapped_json

            mapped_json["program_registrant_info_ids"] = [
                (
                    0,
                    0,
                    {
                        "program_id": self.target_program.id,
                        "state": "active",
                        "program_registrant_info": prog_reg_info if prog_reg_info else None,
                    },
                )
            ]
        return mapped_json
