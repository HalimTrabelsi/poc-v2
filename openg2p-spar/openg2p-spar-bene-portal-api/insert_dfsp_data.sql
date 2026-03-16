-- Insert DFSP Provider Types
INSERT INTO dfsp_provider (code, name, provider_type, description, validation_regex, created_at, updated_at, active)
VALUES
  ('BANK', 'Bank', 'BANK', 'Bank provider type', NULL, NOW(), NOW(), true),
  ('EMAIL_WALLET', 'Email Wallet', 'EMAIL_WALLET', 'Email-based wallet provider', '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', NOW(), NOW(), true),
  ('MOBILE_WALLET', 'Mobile Wallet', 'MOBILE_WALLET', 'Mobile-based wallet provider', '^\+?[1-9]\d{1,14}$', NOW(), NOW(), true);

-- Insert Bank Values
INSERT INTO dfsp_provider_value (code, name, provider_type, parent_id, description, validation_regex, created_at, updated_at, active)
VALUES
  ('ICIC', 'ICICI Bank', 'BANK', NULL, 'ICICI Bank Limited', NULL, NOW(), NOW(), true),
  ('HDFC', 'HDFC Bank', 'BANK', NULL, 'HDFC Bank Limited', NULL, NOW(), NOW(), true),
  ('SBI', 'State Bank of India', 'BANK', NULL, 'State Bank of India', NULL, NOW(), NOW(), true);

-- Insert Bank Branches (ICICI)
INSERT INTO dfsp_provider_value (code, name, provider_type, parent_id, description, validation_regex, created_at, updated_at, active)
VALUES
  ('ICIC_DELHI', 'ICICI Delhi Branch', 'BANK', (SELECT id FROM dfsp_provider_value WHERE code = 'ICIC'), 'ICICI Bank Delhi Branch', NULL, NOW(), NOW(), true),
  ('ICIC_MUMBAI', 'ICICI Mumbai Branch', 'BANK', (SELECT id FROM dfsp_provider_value WHERE code = 'ICIC'), 'ICICI Bank Mumbai Branch', NULL, NOW(), NOW(), true);

-- Insert Bank Branches (HDFC)
INSERT INTO dfsp_provider_value (code, name, provider_type, parent_id, description, validation_regex, created_at, updated_at, active)
VALUES
  ('HDFC_BANGALORE', 'HDFC Bangalore Branch', 'BANK', (SELECT id FROM dfsp_provider_value WHERE code = 'HDFC'), 'HDFC Bank Bangalore Branch', NULL, NOW(), NOW(), true),
  ('HDFC_PUNE', 'HDFC Pune Branch', 'BANK', (SELECT id FROM dfsp_provider_value WHERE code = 'HDFC'), 'HDFC Bank Pune Branch', NULL, NOW(), NOW(), true);

-- Insert Email Wallet Providers
INSERT INTO dfsp_provider_value (code, name, provider_type, parent_id, description, validation_regex, created_at, updated_at, active)
VALUES
  ('GOOGLE', 'Google Wallet', 'EMAIL_WALLET', NULL, 'Google-based wallet', NULL, NOW(), NOW(), true),
  ('PAYPAL', 'PayPal Wallet', 'EMAIL_WALLET', NULL, 'PayPal-based wallet', NULL, NOW(), NOW(), true);

-- Insert Mobile Wallet Providers
INSERT INTO dfsp_provider_value (code, name, provider_type, parent_id, description, validation_regex, created_at, updated_at, active)
VALUES
  ('PAYTM', 'Paytm Wallet', 'MOBILE_WALLET', NULL, 'Paytm mobile wallet', NULL, NOW(), NOW(), true),
  ('PHONEPE', 'PhonePe Wallet', 'MOBILE_WALLET', NULL, 'PhonePe mobile wallet', NULL, NOW(), NOW(), true);
