-- Tables du moteur fraude
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id              SERIAL PRIMARY KEY,
    beneficiary_id  VARCHAR(100) NOT NULL UNIQUE,
    risk_score      FLOAT        NOT NULL DEFAULT 0,
    risk_level      VARCHAR(20)  NOT NULL DEFAULT 'LOW',
    action          VARCHAR(30)  NOT NULL DEFAULT 'CLEAR',
    rule_flags      TEXT,
    explanation     TEXT,
    status          VARCHAR(20)  DEFAULT 'pending',
    created_at      TIMESTAMP    DEFAULT NOW(),
    updated_at      TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_cases (
    id              SERIAL PRIMARY KEY,
    alert_id        INT REFERENCES fraud_alerts(id),
    assigned_to     VARCHAR(100),
    resolution      TEXT,
    notes           TEXT,
    resolved_at     TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_level
    ON fraud_alerts(risk_level);
CREATE INDEX IF NOT EXISTS idx_alerts_status
    ON fraud_alerts(status);
CREATE INDEX IF NOT EXISTS idx_alerts_score
    ON fraud_alerts(risk_score DESC);