import base64
from unittest.mock import patch

from odoo.addons.component.tests.common import TransactionComponentCase


class TestG2PPaymentFileConfig(TransactionComponentCase):
    def setUp(self):
        super().setUp()
        self.document_store = self.env["storage.backend"].create({"name": "Test Storage Backend"})
        self.payment_file_config = self.env["g2p.payment.file.config"].create(
            {
                "name": "Test Payment File Config",
                "type": "pdf",
                "body_string": "<p>Sample Body</p>",
            }
        )

    @patch("odoo.addons.mail.models.mail_render_mixin.MailRenderMixin._render_template")
    @patch("pdfkit.from_string")
    def test_render_and_store_pdf(self, mock_pdfkit, mock_render_template):
        mock_render_template.return_value = {1: "<p>Rendered HTML</p>"}
        mock_pdfkit.return_value = b"PDF Content"
        document_files = self.payment_file_config.render_and_store(
            "g2p.entitlement", [1], self.document_store
        )
        self.assertEqual(len(document_files), 1)
        self.assertIn(".pdf", document_files[0].name)
        self.assertIn(
            "PDF Content",
            base64.b64decode(document_files[0].data).decode("utf-8"),
        )
        mock_pdfkit.assert_called_once()

    @patch("odoo.addons.mail.models.mail_render_mixin.MailRenderMixin._render_template")
    def test_render_and_store_csv(self, mock_render_template):
        mock_render_template.return_value = {1: "Rendered CSV"}
        self.payment_file_config.type = "csv"
        document_files = self.payment_file_config.render_and_store(
            "g2p.entitlement", [1], self.document_store
        )
        self.assertEqual(len(document_files), 1)
        self.assertIn(".csv", document_files[0].name)
        self.assertIn(
            "Rendered CSV",
            base64.b64decode(document_files[0].data).decode("utf-8"),
        )

    @patch("odoo.addons.mail.models.mail_render_mixin.MailRenderMixin._render_template")
    def test_render_html(self, mock_render_template):
        mock_render_template.return_value = {1: "<p>Sample Body</p>"}
        result = self.payment_file_config.render_html("g2p.entitlement", 1)
        self.assertEqual(result, "<p>Sample Body</p>")
