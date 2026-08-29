"""Repository pattern for persisting fraud decisions to the feature store."""
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.config import settings

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fraud_cases (
    case_id         TEXT PRIMARY KEY,
    beneficiary_id  TEXT NOT NULL,
    final_score     REAL NOT NULL,
    rule_score      REAL,
    ml_score        REAL,
    graph_score     REAL,
    risk_level      TEXT NOT NULL,
    recommendation  TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'OPEN',
    rules_triggered JSONB,
    top_features    JSONB,
    features        JSONB,
    explanation     TEXT,
    llm_explanation TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ
);
ALTER TABLE fraud_cases ADD COLUMN IF NOT EXISTS llm_explanation TEXT;
ALTER TABLE fraud_cases ADD COLUMN IF NOT EXISTS country_code TEXT;
CREATE INDEX IF NOT EXISTS idx_fc_beneficiary ON fraud_cases(beneficiary_id);
CREATE INDEX IF NOT EXISTS idx_fc_risk_level  ON fraud_cases(risk_level);
CREATE INDEX IF NOT EXISTS idx_fc_status      ON fraud_cases(status);
CREATE INDEX IF NOT EXISTS idx_fc_country     ON fraud_cases(country_code);

CREATE TABLE IF NOT EXISTS fraud_feedback (
    id              SERIAL PRIMARY KEY,
    case_id         TEXT NOT NULL REFERENCES fraud_cases(case_id),
    beneficiary_id  TEXT NOT NULL,
    verdict         TEXT NOT NULL CHECK (verdict IN ('confirmed_fraud','false_positive','uncertain')),
    notes           TEXT,
    investigator    TEXT,
    original_score  REAL,
    original_risk   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ff_case      ON fraud_feedback(case_id);
CREATE INDEX IF NOT EXISTS idx_ff_verdict   ON fraud_feedback(verdict);
CREATE INDEX IF NOT EXISTS idx_ff_created   ON fraud_feedback(created_at);
"""


class FraudCaseRepository:
    """SQLAlchemy Core repository for the fraud_cases table.

    Connects to the internal feature store (not OpenG2P), which the API
    owns and may write to freely.
    """

    def __init__(self, db_url: Optional[str] = None) -> None:
        url = db_url or settings.feature_store_url
        self._engine: Engine = create_engine(
            url, pool_size=3, max_overflow=5, pool_pre_ping=True
        )
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the fraud_cases table if it does not already exist."""
        try:
            with self._engine.begin() as conn:
                for stmt in _CREATE_TABLE_SQL.strip().split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        conn.execute(text(stmt))
        except Exception as exc:
            logger.warning("Could not ensure fraud_cases table: %s", exc)

    def save_decision(self, decision: dict) -> str:
        """Persist a fraud decision and return the generated case_id."""
        import json

        case_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        stmt = text("""
            INSERT INTO fraud_cases
                (case_id, beneficiary_id, final_score, rule_score, ml_score,
                 graph_score, risk_level, recommendation, status,
                 rules_triggered, top_features, features, explanation,
                 country_code, created_at)
            VALUES
                (:case_id, :beneficiary_id, :final_score, :rule_score, :ml_score,
                 :graph_score, :risk_level, :recommendation, 'OPEN',
                 :rules_triggered, :top_features, :features, :explanation,
                 :country_code, :created_at)
            ON CONFLICT (case_id) DO NOTHING
        """)

        # Serialize features, stripping non-serialisable objects (e.g. ML model)
        raw_features = decision.get("features") or {}
        safe_features = {
            k: v for k, v in raw_features.items()
            if isinstance(v, (int, float, str, bool, type(None)))
        }

        params = {
            "case_id": case_id,
            "beneficiary_id": str(decision.get("beneficiary_id", "")),
            "final_score": float(decision.get("final_score", 0.0)),
            "rule_score": float(decision.get("rule_score", 0.0)),
            "ml_score": float(decision.get("ml_score", 0.0)),
            "graph_score": float(decision.get("graph_score", 0.0)),
            "risk_level": str(decision.get("risk_level", "LOW")),
            "recommendation": str(decision.get("recommendation", "CLEAR")),
            "rules_triggered": json.dumps(decision.get("rules_triggered", [])),
            "top_features": json.dumps(decision.get("top_features", [])),
            "features": json.dumps(safe_features),
            "explanation": str(decision.get("explanation", "")),
            "country_code": decision.get("country_code") or None,
            "created_at": now,
        }

        try:
            with self._engine.begin() as conn:
                conn.execute(stmt, params)
        except Exception as exc:
            logger.error("Failed to save fraud decision: %s", exc)

        return case_id

    def get_case(self, case_id: str) -> Optional[dict]:
        """Return a single case dict or None if not found."""
        stmt = text("SELECT * FROM fraud_cases WHERE case_id = :case_id")
        try:
            with self._engine.connect() as conn:
                row = conn.execute(stmt, {"case_id": case_id}).mappings().fetchone()
                return dict(row) if row else None
        except Exception as exc:
            logger.error("get_case failed: %s", exc)
            return None

    def list_cases(
        self,
        risk_levels: Optional[list[str]] = None,
        statuses: Optional[list[str]] = None,
        limit: int = 50,
    ) -> list[dict]:
        """Return matching cases ordered by creation time descending."""
        conditions = []
        params: dict[str, Any] = {"limit": limit}

        if risk_levels:
            conditions.append("risk_level = ANY(:risk_levels)")
            params["risk_levels"] = risk_levels
        if statuses:
            conditions.append("status = ANY(:statuses)")
            params["statuses"] = statuses

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        stmt = text(f"SELECT * FROM fraud_cases {where} ORDER BY created_at DESC LIMIT :limit")

        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt, params).mappings().fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("list_cases failed: %s", exc)
            return []

    def update_case_status(
        self, case_id: str, status: str, notes: Optional[str] = None
    ) -> bool:
        """Set a new status and append notes. Returns True on success."""
        stmt = text("""
            UPDATE fraud_cases
            SET status = :status,
                notes  = COALESCE(:notes, notes),
                updated_at = NOW()
            WHERE case_id = :case_id
        """)
        try:
            with self._engine.begin() as conn:
                result = conn.execute(stmt, {"case_id": case_id, "status": status, "notes": notes})
                return result.rowcount > 0
        except Exception as exc:
            logger.error("update_case_status failed: %s", exc)
            return False

    def update_llm_explanation(self, case_id: str, text_explanation: str) -> bool:
        """Persist an LLM-generated explanation. Idempotent — overwrites prior text."""
        stmt = text("""
            UPDATE fraud_cases
            SET llm_explanation = :exp, updated_at = NOW()
            WHERE case_id = :case_id
        """)
        try:
            with self._engine.begin() as conn:
                result = conn.execute(stmt, {"case_id": case_id, "exp": text_explanation})
                return result.rowcount > 0
        except Exception as exc:
            logger.error("update_llm_explanation failed: %s", exc)
            return False

    def get_all_scored_beneficiary_ids(self) -> list[str]:
        """Return every beneficiary_id that already has a case in the DB."""
        stmt = text("SELECT DISTINCT beneficiary_id FROM fraud_cases")
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).fetchall()
                return [r[0] for r in rows]
        except Exception as exc:
            logger.error("get_all_scored_beneficiary_ids failed: %s", exc)
            return []

    def get_country_stats(self) -> list[dict]:
        """Return per-country scan counts and risk breakdown.

        Grouped by the country_code each case was scored under (see
        app.core.country_reference) — lets the dashboard show "how many
        beneficiaries were scanned under each deployment country" and
        their risk-level split, in one query.
        """
        stmt = text("""
            SELECT
                COALESCE(country_code, 'UNKNOWN') AS country_code,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE risk_level = 'CRITICAL') AS critical,
                COUNT(*) FILTER (WHERE risk_level = 'HIGH')     AS high,
                COUNT(*) FILTER (WHERE risk_level = 'MEDIUM')   AS medium,
                COUNT(*) FILTER (WHERE risk_level = 'LOW')      AS low,
                AVG(final_score) AS avg_score
            FROM fraud_cases
            GROUP BY COALESCE(country_code, 'UNKNOWN')
            ORDER BY total DESC
        """)
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).mappings().fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("get_country_stats failed: %s", exc)
            return []

    def get_beneficiary_history(self, beneficiary_id: str) -> list[dict]:
        """Return all past decisions for a beneficiary, newest first."""
        stmt = text("""
            SELECT * FROM fraud_cases
            WHERE beneficiary_id = :bid
            ORDER BY created_at DESC
        """)
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt, {"bid": beneficiary_id}).mappings().fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("get_beneficiary_history failed: %s", exc)
            return []

    # ── feedback loop ────────────────────────────────────────────────────────

    def save_feedback(
        self, case_id: str, verdict: str, notes: str, investigator: str = "investigator"
    ) -> str:
        """Persist an investigator verdict. Returns the feedback row id."""
        stmt = text("""
            INSERT INTO fraud_feedback
                (case_id, beneficiary_id, verdict, notes, investigator,
                 original_score, original_risk, created_at)
            SELECT
                fc.case_id, fc.beneficiary_id, :verdict, :notes, :investigator,
                fc.final_score, fc.risk_level, NOW()
            FROM fraud_cases fc
            WHERE fc.case_id = :case_id
            RETURNING id
        """)
        try:
            with self._engine.begin() as conn:
                row = conn.execute(
                    stmt,
                    {"case_id": case_id, "verdict": verdict,
                     "notes": notes, "investigator": investigator},
                ).fetchone()
                return str(row[0]) if row else ""
        except Exception as exc:
            logger.error("save_feedback failed: %s", exc)
            return ""

    def get_confirmed_labels(self) -> list[dict]:
        """Return cases with confirmed_fraud or false_positive verdicts + stored features."""
        stmt = text("""
            SELECT ff.verdict, fc.features, fc.final_score, fc.risk_level
            FROM fraud_feedback ff
            JOIN fraud_cases fc ON fc.case_id = ff.case_id
            WHERE ff.verdict IN ('confirmed_fraud', 'false_positive')
            ORDER BY ff.created_at DESC
        """)
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).mappings().fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("get_confirmed_labels failed: %s", exc)
            return []

    def get_feedback_stats(self) -> dict:
        """Return verdict counts and totals for the dashboard."""
        stmt = text("""
            SELECT
                verdict,
                COUNT(*) AS cnt
            FROM fraud_feedback
            GROUP BY verdict
        """)
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).fetchall()
            counts = {r[0]: r[1] for r in rows}
            total = sum(counts.values())
            confirmed = counts.get("confirmed_fraud", 0)
            fp = counts.get("false_positive", 0)
            precision = confirmed / max(confirmed + fp, 1)
            return {
                "total_feedback": total,
                "confirmed_fraud": confirmed,
                "false_positive": fp,
                "uncertain": counts.get("uncertain", 0),
                "estimated_precision": round(precision, 4),
            }
        except Exception as exc:
            logger.error("get_feedback_stats failed: %s", exc)
            return {}

    def get_known_fraud_scores(self) -> dict[str, float]:
        """Return {beneficiary_id: final_score} for every scored beneficiary.

        Used by the graph analyzer to seed PageRank personalization so that
        nodes already confirmed as HIGH/CRITICAL fraudsters act as attractors.
        """
        stmt = text("""
            SELECT DISTINCT ON (beneficiary_id)
                beneficiary_id, final_score
            FROM fraud_cases
            ORDER BY beneficiary_id, created_at DESC
        """)
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).fetchall()
                return {r[0]: float(r[1]) for r in rows}
        except Exception as exc:
            logger.error("get_known_fraud_scores failed: %s", exc)
            return {}
