"""Install a PostgreSQL NOTIFY trigger on OpenG2P's res_partner table.

When a registrant (is_registrant=true) is inserted or updated, the trigger fires:
    NOTIFY fraud_queue, '<partner_id>'

The fraud engine's scanner service listens on this channel and immediately
scores the beneficiary — zero polling lag, no Odoo module required.

Usage (run once):
    python scripts/install_pg_trigger.py
"""
import logging
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OPENG2P_DB_URL = "postgresql://odoo:openg2p@localhost:5432/openg2p"

TRIGGER_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION notify_fraud_queue()
RETURNS trigger AS $$
BEGIN
    -- Only fire for registrants (beneficiaries), not companies or system users
    IF COALESCE(NEW.is_registrant, false) = true THEN
        PERFORM pg_notify('fraud_queue', NEW.id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

TRIGGER_SQL = """
DROP TRIGGER IF EXISTS trg_fraud_queue ON res_partner;
CREATE TRIGGER trg_fraud_queue
    AFTER INSERT OR UPDATE ON res_partner
    FOR EACH ROW
    EXECUTE FUNCTION notify_fraud_queue();
"""

VERIFY_SQL = """
SELECT trigger_name, event_manipulation, action_timing
FROM information_schema.triggers
WHERE event_object_table = 'res_partner'
  AND trigger_name = 'trg_fraud_queue';
"""


def install(db_url: str = OPENG2P_DB_URL) -> None:
    engine = create_engine(db_url)
    with engine.begin() as conn:
        logger.info("Creating notify_fraud_queue() trigger function…")
        conn.execute(text(TRIGGER_FUNCTION_SQL))

        logger.info("Attaching trigger trg_fraud_queue to res_partner…")
        conn.execute(text(TRIGGER_SQL))

        rows = conn.execute(text(VERIFY_SQL)).fetchall()
        if rows:
            logger.info("Trigger installed: %s", rows)
        else:
            logger.error("Trigger not found after install — check DB permissions.")
            sys.exit(1)

    logger.info(
        "Done. The fraud engine scanner now listens on channel 'fraud_queue' "
        "and scores any registrant within 1-2 seconds of being saved in OpenG2P."
    )


if __name__ == "__main__":
    install()
