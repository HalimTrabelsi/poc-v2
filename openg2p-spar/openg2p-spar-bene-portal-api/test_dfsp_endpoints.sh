#!/bin/bash

# DFSP API Testing Script
# Base URL
BASE_URL="http://localhost:8004"

echo "=========================================="
echo "DFSP API Endpoint Testing"
echo "=========================================="
echo ""

# Test 1: Get all provider types
echo "1️⃣  GET /dfsp/providers - Get all provider types"
echo "---"
curl -X GET "$BASE_URL/dfsp/providers" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

# Test 2: Get BANK provider type
echo "2️⃣  GET /dfsp/providers/BANK - Get BANK provider type"
echo "---"
curl -X GET "$BASE_URL/dfsp/providers/BANK" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

# Test 3: Get EMAIL_WALLET provider type
echo "3️⃣  GET /dfsp/providers/EMAIL_WALLET - Get EMAIL_WALLET provider type"
echo "---"
curl -X GET "$BASE_URL/dfsp/providers/EMAIL_WALLET" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

# Test 4: Get MOBILE_WALLET provider type
echo "4️⃣  GET /dfsp/providers/MOBILE_WALLET - Get MOBILE_WALLET provider type"
echo "---"
curl -X GET "$BASE_URL/dfsp/providers/MOBILE_WALLET" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

# Test 5: Get all BANK values (banks)
echo "5️⃣  GET /dfsp/providers/BANK/values - Get all banks"
echo "---"
curl -X GET "$BASE_URL/dfsp/providers/BANK/values" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

# Test 6: Get all EMAIL_WALLET values
echo "6️⃣  GET /dfsp/providers/EMAIL_WALLET/values - Get all email wallets"
echo "---"
curl -X GET "$BASE_URL/dfsp/providers/EMAIL_WALLET/values" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

# Test 7: Get all MOBILE_WALLET values
echo "7️⃣  GET /dfsp/providers/MOBILE_WALLET/values - Get all mobile wallets"
echo "---"
curl -X GET "$BASE_URL/dfsp/providers/MOBILE_WALLET/values" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

# Test 8: Get provider value by code (ICIC)
echo "8️⃣  GET /dfsp/values/ICIC - Get ICICI Bank by code"
echo "---"
curl -X GET "$BASE_URL/dfsp/values/ICIC" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

# Test 9: Get provider value by code (GMAIL)
echo "9️⃣  GET /dfsp/values/GMAIL - Get Gmail Wallet by code"
echo "---"
curl -X GET "$BASE_URL/dfsp/values/GMAIL" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

# Test 10: Get children (branches) of ICICI Bank (parent_id=1)
echo "🔟 GET /dfsp/values/1/children - Get ICICI Bank branches"
echo "---"
curl -X GET "$BASE_URL/dfsp/values/1/children" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

# Test 11: Get children (branches) of HDFC Bank (parent_id=2)
echo "1️⃣1️⃣  GET /dfsp/values/2/children - Get HDFC Bank branches"
echo "---"
curl -X GET "$BASE_URL/dfsp/values/2/children" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n\n"

echo "=========================================="
echo "Testing Complete!"
echo "=========================================="
