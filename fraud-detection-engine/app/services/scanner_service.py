"""Background scanner: detects new OpenG2P beneficiaries and scores them automatically."""
import logging
import threading
import time
from typing import Callable

from sqlalchemy import text

logger = logging.getLogger(__name__)


class BeneficiaryScanner:
    """Poll OpenG2P for unscored beneficiaries and run the fraud pipeline on each.

    Runs in a daemon thread so it does not block the FastAPI process.
    """

    def __init__(self, poll_interval_seconds: int = 30) -> None:
        self._interval = poll_interval_seconds
        self._running = False
        self._thread: threading.Thread | None = None
        self._scored_ids: set[str] = set()
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the background polling thread."""
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="BeneficiaryScanner")
        self._thread.start()
        logger.info("BeneficiaryScanner started (interval=%ds)", self._interval)

    def stop(self) -> None:
        self._running = False
        logger.info("BeneficiaryScanner stopped.")

    def scan_now(self) -> dict:
        """Trigger an immediate scan and return summary. Called from the API."""
        return self._run_scan()

    def _loop(self) -> None:
        while self._running:
            try:
                result = self._run_scan()
                if result["scored"] > 0:
                    logger.info(
                        "Auto-scan: scored %d new beneficiaries (CRITICAL=%d HIGH=%d)",
                        result["scored"], result.get("CRITICAL", 0), result.get("HIGH", 0),
                    )
            except Exception:
                logger.exception("Scanner loop error — will retry in %ds", self._interval)
            time.sleep(self._interval)

    def _run_scan(self) -> dict:
        from app.data.connector import OpenG2PConnector
        from app.data.repository import FraudCaseRepository
        from app.services.decision_service import DecisionOrchestrator

        connector = OpenG2PConnector()
        repo = FraudCaseRepository()
        orchestrator = DecisionOrchestrator()

        # Load already-scored IDs from fraud_db to avoid re-scoring
        scored_in_db = {str(r) for r in repo.get_all_scored_beneficiary_ids()}
        with self._lock:
            already_known = self._scored_ids | scored_in_db

        # Fetch all registrant IDs from OpenG2P
        with connector.get_session() as session:
            rows = session.execute(text("""
                SELECT p.id
                FROM res_partner p
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

        summary: dict = {"found": len(all_ids), "new": len(new_ids), "scored": 0,
                         "errors": 0, "CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}

        for bid in new_ids:
            try:
                result = orchestrator.score_beneficiary(bid)
                risk = result.get("risk_level", "LOW")
                summary["scored"] += 1
                summary[risk] = summary.get(risk, 0) + 1
                with self._lock:
                    self._scored_ids.add(bid)
            except Exception as exc:
                logger.warning("Failed to score beneficiary %s: %s", bid, exc)
                summary["errors"] += 1

        return summary


# Module-level singleton started by the FastAPI lifespan
_scanner: BeneficiaryScanner | None = None


def get_scanner() -> BeneficiaryScanner:
    global _scanner
    if _scanner is None:
        _scanner = BeneficiaryScanner(poll_interval_seconds=30)
    return _scanner
