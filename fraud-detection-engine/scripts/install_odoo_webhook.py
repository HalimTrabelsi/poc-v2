"""Install a real-time fraud detection webhook into OpenG2P (Odoo).

Creates a base.automation record that fires whenever a registrant (beneficiary)
is created or updated in res_partner. The action calls the fraud engine webhook
so the beneficiary is scored within seconds of being saved — no polling lag.

Usage (run once, from outside Docker):
    python scripts/install_odoo_webhook.py

Or inside the fraud-engine container (fraud-engine sees Odoo at openg2p-odoo:8069):
    python scripts/install_odoo_webhook.py --odoo-url http://openg2p-odoo:8069

Requirements:
    pip install xmlrpc  (stdlib — no extra install needed)
"""
import argparse
import logging
import xmlrpc.client

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── defaults ──────────────────────────────────────────────────────────────────
DEFAULT_ODOO_URL   = "http://localhost:8069"
DEFAULT_ODOO_DB    = "openg2p"
DEFAULT_ODOO_USER  = "admin"
DEFAULT_ODOO_PASS  = "admin"

# The URL the Odoo container uses to reach the fraud engine.
# Inside Docker both services share the openg2p-internal network.
DEFAULT_FRAUD_ENGINE_URL = "http://fraud-engine:8000"
DEFAULT_WEBHOOK_SECRET   = "dev-secret-change-in-prod"

# ── Odoo Python action code ────────────────────────────────────────────────────
# This snippet runs inside Odoo's server action context.
# `records` is the res.partner recordset that was just saved.
_ACTION_CODE = '''\
import requests, logging
_log = logging.getLogger("fraud_webhook")

FRAUD_ENGINE_URL = "{fraud_engine_url}"
WEBHOOK_SECRET   = "{webhook_secret}"

for partner in records:
    if not partner.is_registrant:
        continue
    try:
        r = requests.post(
            f"{{FRAUD_ENGINE_URL}}/api/v1/webhook/beneficiary-saved",
            json={{"partner_id": partner.id, "event": "saved"}},
            headers={{"X-Webhook-Secret": WEBHOOK_SECRET}},
            timeout=5,
        )
        _log.info("Fraud webhook: partner_id=%d status=%d", partner.id, r.status_code)
    except Exception as exc:
        _log.warning("Fraud webhook failed for partner_id=%d: %s", partner.id, exc)
'''


def _connect(odoo_url: str, db: str, user: str, password: str):
    common = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/common")
    uid = common.authenticate(db, user, password, {})
    if not uid:
        raise RuntimeError(f"Odoo authentication failed for user '{user}' on db '{db}'")
    models = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/object")
    return uid, models


def _get_partner_model_id(models, db, uid, password):
    ids = models.execute_kw(db, uid, password, "ir.model", "search",
                            [[["model", "=", "res.partner"]]])
    if not ids:
        raise RuntimeError("ir.model for res.partner not found in Odoo")
    return ids[0]


def _automation_exists(models, db, uid, password, name: str) -> int | None:
    ids = models.execute_kw(db, uid, password, "base.automation", "search",
                            [[["name", "=", name]]])
    return ids[0] if ids else None


def install(
    odoo_url: str,
    db: str,
    user: str,
    password: str,
    fraud_engine_url: str,
    webhook_secret: str,
) -> int:
    logger.info("Connecting to Odoo at %s (db=%s user=%s)…", odoo_url, db, user)
    uid, models = _connect(odoo_url, db, user, password)
    logger.info("Authenticated as uid=%d", uid)

    model_id = _get_partner_model_id(models, db, uid, password)
    action_name = "Fraud Detection — Score on Beneficiary Save"

    existing_id = _automation_exists(models, db, uid, password, action_name)
    if existing_id:
        logger.info("Automation already exists (id=%d). Updating code…", existing_id)
        models.execute_kw(db, uid, password, "base.automation", "write", [[existing_id], {
            "code": _ACTION_CODE.format(
                fraud_engine_url=fraud_engine_url,
                webhook_secret=webhook_secret,
            ),
        }])
        return existing_id

    action_id = models.execute_kw(db, uid, password, "base.automation", "create", [{
        "name": action_name,
        "model_id": model_id,
        "state": "code",                  # Python code action
        "trigger": "on_write_or_create",  # fires on create AND write
        "filter_domain": "[['is_registrant','=',True]]",
        "filter_pre_domain": False,
        "active": True,
        "code": _ACTION_CODE.format(
            fraud_engine_url=fraud_engine_url,
            webhook_secret=webhook_secret,
        ),
    }])

    logger.info(
        "Automated action created (id=%d): '%s'", action_id, action_name
    )
    logger.info(
        "From now on, saving any registrant in OpenG2P triggers fraud scoring within seconds."
    )
    return action_id


def parse_args():
    p = argparse.ArgumentParser(description="Install fraud webhook into OpenG2P/Odoo")
    p.add_argument("--odoo-url",        default=DEFAULT_ODOO_URL)
    p.add_argument("--db",              default=DEFAULT_ODOO_DB)
    p.add_argument("--user",            default=DEFAULT_ODOO_USER)
    p.add_argument("--password",        default=DEFAULT_ODOO_PASS)
    p.add_argument("--fraud-engine-url",default=DEFAULT_FRAUD_ENGINE_URL)
    p.add_argument("--webhook-secret",  default=DEFAULT_WEBHOOK_SECRET)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    action_id = install(
        odoo_url=args.odoo_url,
        db=args.db,
        user=args.user,
        password=args.password,
        fraud_engine_url=args.fraud_engine_url,
        webhook_secret=args.webhook_secret,
    )
    print(f"\nDone. Automation ID: {action_id}")
    print("Test it: open http://localhost:8069 → create/save any Individual registrant.")
    print("Watch logs: docker logs fraud-engine -f --tail 50")
