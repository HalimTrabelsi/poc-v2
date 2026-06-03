#!/usr/bin/env bash
# Reset the demo environment: empty fraud cases + OpenG2P beneficiary data,
# leaving the schema intact. Safe to re-run.
#
# What this deletes:
#   • All fraud_cases in fraud-db
#   • All fraud.case records in Odoo
#   • All g2p_payment, g2p_entitlement, g2p_program_membership records
#   • All g2p_phone_number records
#   • All res_partner registrants (is_registrant=true)
#
# What this PRESERVES:
#   • The schema and all configuration
#   • Programs, cycles, users, groups, system parameters
#   • The 'Social Aid Program' (program_id=16) and its cycle
#
# Usage:  ./reset_demo.sh
set -e

echo "==> Wiping fraud cases (fraud-db)"
docker exec fraud-db psql -U fraud -d fraud_engine -c "
    DELETE FROM fraud_feedback;
    DELETE FROM fraud_cases;
" >/dev/null
echo "    fraud_cases: 0"

echo "==> Wiping fraud cases (Odoo mirror)"
docker exec openg2p-postgresql psql -U odoo -d openg2p -c "
    DELETE FROM fraud_case;
" >/dev/null
echo "    fraud.case: 0"

echo "==> Wiping OpenG2P beneficiary data"
docker exec openg2p-postgresql psql -U odoo -d openg2p -c "
    -- Payments first (depend on entitlements)
    DELETE FROM g2p_payment_g2p_payment_batch_rel;
    DELETE FROM g2p_payment;
    DELETE FROM g2p_entitlement;
    DELETE FROM g2p_program_membership;
    DELETE FROM g2p_phone_number;
    DELETE FROM g2p_reg_id;
    DELETE FROM g2p_reg_rel;
    DELETE FROM g2p_group_membership;
    -- Drop registrants but NOT the system users (id < 10 in Odoo convention)
    DELETE FROM res_partner WHERE is_registrant = true AND id > 10;
" >/dev/null

echo "==> Cleaning orphaned mail messages (deleted partners/cases)"
docker exec openg2p-postgresql psql -U odoo -d openg2p -c "
    -- Notifications + messages whose linked record was just deleted.
    -- Dangling refs here break the Discuss inbox fetch entirely.
    DELETE FROM mail_notification WHERE mail_message_id IN (
        SELECT id FROM mail_message
        WHERE model = 'res.partner' AND res_id NOT IN (SELECT id FROM res_partner)
    );
    DELETE FROM mail_message
        WHERE model = 'res.partner' AND res_id NOT IN (SELECT id FROM res_partner);
    DELETE FROM mail_notification WHERE mail_message_id IN (
        SELECT id FROM mail_message
        WHERE model = 'fraud.case' AND res_id NOT IN (SELECT id FROM fraud_case)
    );
    DELETE FROM mail_message
        WHERE model = 'fraud.case' AND res_id NOT IN (SELECT id FROM fraud_case);
" >/dev/null
echo "    mail orphans: cleaned"

echo "==> Verifying"
docker exec openg2p-postgresql psql -U odoo -d openg2p -c "
SELECT 'res_partner_registrants' AS table, COUNT(*) FROM res_partner WHERE is_registrant=true
UNION ALL SELECT 'g2p_program_membership', COUNT(*) FROM g2p_program_membership
UNION ALL SELECT 'g2p_phone_number', COUNT(*) FROM g2p_phone_number
UNION ALL SELECT 'g2p_payment', COUNT(*) FROM g2p_payment
UNION ALL SELECT 'fraud_case', COUNT(*) FROM fraud_case;
"

echo
echo "Demo reset complete. Ready to import the CSV."
