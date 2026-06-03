-- Clean up orphaned mail messages and notifications.
--
-- When DEMO partners or fraud cases are deleted (during demo resets), their
-- mail.message / mail.notification rows are left behind pointing to res_ids
-- that no longer exist. Odoo's Discuss inbox tries to resolve every linked
-- record when fetching messages; a single dangling reference makes the whole
-- inbox fetch fail with "An error occurred while fetching messages."
--
-- This script removes any message (and its notifications) whose target record
-- has been deleted, for the models the fraud demo touches.

-- 1) Notifications first (FK to mail_message), for deleted res.partner targets
DELETE FROM mail_notification
WHERE mail_message_id IN (
    SELECT id FROM mail_message
    WHERE model = 'res.partner'
      AND res_id NOT IN (SELECT id FROM res_partner)
);

-- 2) The orphaned res.partner messages themselves
DELETE FROM mail_message
WHERE model = 'res.partner'
  AND res_id NOT IN (SELECT id FROM res_partner);

-- 3) Notifications for deleted fraud.case targets
DELETE FROM mail_notification
WHERE mail_message_id IN (
    SELECT id FROM mail_message
    WHERE model = 'fraud.case'
      AND res_id NOT IN (SELECT id FROM fraud_case)
);

-- 4) The orphaned fraud.case messages themselves
DELETE FROM mail_message
WHERE model = 'fraud.case'
  AND res_id NOT IN (SELECT id FROM fraud_case);
