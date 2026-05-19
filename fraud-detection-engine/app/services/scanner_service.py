"""Background scanner: real-time LISTEN/NOTIFY + periodic poll fallback.

Primary path (instant):
  PostgreSQL LISTEN on channel 'fraud_queue' — the trg_fraud_queue trigger fires
  NOTIFY whenever a registrant is inserted or updated in res_partner. The
  listener scores the beneficiary within ~1 second.

Fallback path (catch-all):
  Every 60 s the scanner polls OpenG2P for any unscored registrants, covering
  cases where the trigger fires before the listener is ready, or for existing
  beneficiaries that pre-date the trigger.

Install the trigger once with:
    python scripts/install_pg_trigger.py
"""
import logging
import select
import threading
import time

import psycopg2
import psycopg2.extensions
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Connection string used by psycopg2 directly for LISTEN (needs raw connection)
_PG_DSN: str | None = None


def _get_pg_dsn() -> str:
    global _PG_DSN
    if _PG_DSN is None:
        from app.config import settings
        # Convert SQLAlchemy URL → psycopg2 DSN
        url = str(settings.openg2p_db_url)
        _PG_DSN = url.replace("postgresql+psycopg2://", "postgresql://").replace(
            "postgresql://", "host=", 1
        )
        # Use psycopg2's own DSN parsing via from_url helper
        _PG_DSN = str(settings.openg2p_db_url)
    return _PG_DSN


class BeneficiaryScanner:
    """Real-time beneficiary scanner combining pg LISTEN + periodic poll fallback."""

    POLL_INTERVAL = 60          # seconds between full-table catch-up scans
    LISTEN_TIMEOUT = 5          # seconds select() waits before checking _running

    def __init__(self) -> None:
        self._running = False
        self._scored_ids: set[str] = set()
        self._lock = threading.Lock()
        self._listen_thread: threading.Thread | None = None
        self._poll_thread: threading.Thread | None = None

    # ── public API ──────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True

        self._listen_thread = threading.Thread(
            target=self._listen_loop, daemon=True, name="FraudScanner-Listen"
        )
        self._poll_thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="FraudScanner-Poll"
        )
        self._listen_thread.start()
        self._poll_thread.start()
        logger.info(
            "BeneficiaryScanner started — LISTEN thread + %ds poll fallback",
            self.POLL_INTERVAL,
        )

    def stop(self) -> None:
        self._running = False
        logger.info("BeneficiaryScanner stopping.")

    def scan_now(self) -> dict:
        """Trigger an immediate full scan. Called from the API."""
        return self._run_poll_scan()

    # ── LISTEN/NOTIFY path ──────────────────────────────────────────────────

    def _listen_loop(self) -> None:
        """Open a raw psycopg2 connection and LISTEN on 'fraud_queue'."""
        while self._running:
            conn = None
            try:
                from app.config import settings
                dsn = str(settings.openg2p_db_url)
                conn = psycopg2.connect(dsn)
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                with conn.cursor() as cur:
                    cur.execute("LISTEN fraud_queue;")
                logger.info("LISTEN active on channel 'fraud_queue'")

                while self._running:
                    ready = select.select([conn], [], [], self.LISTEN_TIMEOUT)
                    if ready[0]:
                        conn.poll()
                        while conn.notifies:
                            notify = conn.notifies.pop(0)
                            partner_id = notify.payload.strip()
                            if partner_id:
                                logger.info(
                                    "NOTIFY received: partner_id=%s — scoring now", partner_id
                                )
                                threading.Thread(
                                    target=self._score_one,
                                    args=(partner_id,),
                                    daemon=True,
                                ).start()

            except Exception as exc:
                logger.warning("LISTEN loop error (%s) — reconnecting in 10 s", exc)
                time.sleep(10)
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    # ── periodic poll path ──────────────────────────────────────────────────

    def _poll_loop(self) -> None:
        while self._running:
            try:
                result = self._run_poll_scan()
                if result["scored"] > 0:
                    logger.info(
                        "Poll-scan: scored %d new (CRITICAL=%d HIGH=%d)",
                        result["scored"],
                        result.get("CRITICAL", 0),
                        result.get("HIGH", 0),
                    )
            except Exception:
                logger.exception("Poll-scan error — will retry in %ds", self.POLL_INTERVAL)
            time.sleep(self.POLL_INTERVAL)

    def _run_poll_scan(self) -> dict:
        from app.data.connector import OpenG2PConnector
        from app.data.repository import FraudCaseRepository

        connector = OpenG2PConnector()
        repo = FraudCaseRepository()

        scored_in_db = {str(r) for r in repo.get_all_scored_beneficiary_ids()}
        with self._lock:
            already_known = self._scored_ids | scored_in_db

        with connector.get_session() as session:
            rows = session.execute(text("""
                SELECT p.id FROM res_partner p
                WHERE COALESCE(p.active, true) = true
                  AND COALESCE(p.is_company, false) = false
                  AND COALESCE(p.is_registrant, false) = true
                  AND p.name NOT IN (
                      'My Company', 'Administrator', 'Public user',
                      'Default User Template', 'OdooBot'
                  )
                ORDER BY p.id DESC
                LIMIT 500
            """)).fetchall()

        all_ids = [str(r[0]) for r in rows]
        new_ids = [bid for bid in all_ids if bid not in already_known]

        summary: dict = {
            "found": len(all_ids), "new": len(new_ids), "scored": 0,
            "errors": 0, "CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0,
        }
        for bid in new_ids:
            result = self._score_one(bid)
            if result:
                risk = result.get("risk_level", "LOW")
                summary["scored"] += 1
                summary[risk] = summary.get(risk, 0) + 1
            else:
                summary["errors"] += 1

        return summary

    # ── shared scoring ──────────────────────────────────────────────────────

    def _score_one(self, beneficiary_id: str) -> dict | None:
        with self._lock:
            if beneficiary_id in self._scored_ids:
                return None            # already in-flight from NOTIFY

        try:
            from app.services.decision_service import DecisionOrchestrator
            result = DecisionOrchestrator().score_beneficiary(beneficiary_id)
            with self._lock:
                self._scored_ids.add(beneficiary_id)
            logger.info(
                "Scored partner_id=%s risk=%s score=%.3f",
                beneficiary_id,
                result.get("risk_level"),
                result.get("final_score", 0),
            )
            return result
        except Exception as exc:
            logger.warning("Scoring failed for partner_id=%s: %s", beneficiary_id, exc)
            return None


# Module-level singleton started by the FastAPI lifespan
_scanner: BeneficiaryScanner | None = None


def get_scanner() -> BeneficiaryScanner:
    global _scanner
    if _scanner is None:
        _scanner = BeneficiaryScanner()
    return _scanner
