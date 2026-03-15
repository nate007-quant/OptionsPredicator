-- Regime v1 additive columns for SQLite predictions table
ALTER TABLE predictions ADD COLUMN regime_label TEXT;
ALTER TABLE predictions ADD COLUMN regime_confidence REAL;
ALTER TABLE predictions ADD COLUMN regime_version TEXT;
