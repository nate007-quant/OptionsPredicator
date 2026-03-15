#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
from dataclasses import dataclass
from typing import Any

import psycopg


@dataclass
class TableSpec:
    name: str
    create_sql: str
    pk_cols: list[str]


TABLE_SPECS: list[TableSpec] = [
    # core prediction/local-state tables (legacy sqlite)
    TableSpec(
        name="predictions",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.predictions (
          id BIGINT PRIMARY KEY,
          timestamp TEXT NOT NULL,
          ticker TEXT NOT NULL,
          expiration_date TEXT NOT NULL,
          source_snapshot_file TEXT NOT NULL,
          source_snapshot_hash TEXT NOT NULL,
          chart_file TEXT,
          spot_price REAL NOT NULL,
          signals_used TEXT NOT NULL,
          chart_description TEXT,
          predicted_direction TEXT NOT NULL,
          predicted_magnitude REAL NOT NULL,
          confidence REAL NOT NULL,
          strategy_suggested TEXT NOT NULL,
          reasoning TEXT NOT NULL,
          prompt_version TEXT NOT NULL,
          model_used TEXT NOT NULL,
          model_provider TEXT NOT NULL,
          routing_reason TEXT NOT NULL,
          price_at_prediction REAL,
          price_at_outcome REAL,
          actual_move REAL,
          result TEXT,
          pnl_simulated REAL,
          outcome_notes TEXT,
          scored_at TEXT,
          observed_ts_utc TEXT,
          outcome_ts_utc TEXT,
          features_version TEXT,
          features_json TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_predictions_hash_prompt_model ON {schema}.predictions(source_snapshot_hash, prompt_version, model_used);
        CREATE INDEX IF NOT EXISTS idx_predictions_timestamp ON {schema}.predictions(timestamp);
        CREATE INDEX IF NOT EXISTS idx_predictions_result_null ON {schema}.predictions(result);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="performance_summary",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.performance_summary (
          id BIGINT PRIMARY KEY,
          generated_at TEXT NOT NULL,
          total_predictions INTEGER NOT NULL,
          total_scored INTEGER NOT NULL,
          overall_accuracy REAL,
          summary_json TEXT NOT NULL
        );
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="system_events",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.system_events (
          id BIGINT PRIMARY KEY,
          timestamp TEXT NOT NULL,
          level TEXT NOT NULL,
          component TEXT NOT NULL,
          event TEXT NOT NULL,
          message TEXT NOT NULL,
          snapshot_hash TEXT,
          model_used TEXT,
          details_json TEXT NOT NULL
        );
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="model_usage",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.model_usage (
          id BIGINT PRIMARY KEY,
          ts_utc TEXT NOT NULL,
          observed_ts_utc TEXT,
          snapshot_hash TEXT,
          kind TEXT NOT NULL,
          model_used TEXT,
          model_provider TEXT,
          prompt_chars INTEGER,
          output_chars INTEGER,
          latency_ms INTEGER,
          input_tokens INTEGER,
          output_tokens INTEGER,
          total_tokens INTEGER,
          est_input_tokens INTEGER NOT NULL,
          est_output_tokens INTEGER NOT NULL,
          est_total_tokens INTEGER NOT NULL
        );
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="eod_predictions",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.eod_predictions (
          trade_day TEXT NOT NULL,
          asof_minutes INTEGER NOT NULL,
          levels_asof_snapshot_index INTEGER NOT NULL,
          model_version TEXT NOT NULL,
          created_at_utc TEXT NOT NULL,
          open_price REAL,
          early_end_price REAL,
          close_price REAL,
          levels_json TEXT,
          features_version TEXT,
          features_json TEXT,
          pred_dir TEXT,
          pred_conf REAL,
          pred_move_pts REAL,
          p_action REAL,
          event_probs_json TEXT,
          label_dir TEXT,
          label_move_pts REAL,
          label_band_pts REAL,
          label_events_json TEXT,
          scored_at TEXT,
          PRIMARY KEY (trade_day, asof_minutes, levels_asof_snapshot_index, model_version)
        );
        """,
        pk_cols=["trade_day", "asof_minutes", "levels_asof_snapshot_index", "model_version"],
    ),
    TableSpec(
        name="schema_migrations",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.schema_migrations (
          id BIGINT PRIMARY KEY,
          filename TEXT NOT NULL UNIQUE,
          checksum_sha256 TEXT NOT NULL,
          applied_at_utc TEXT NOT NULL
        );
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="backtest_sampler_sessions",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.backtest_sampler_sessions (
          id BIGINT PRIMARY KEY,
          strategy_key TEXT NOT NULL,
          schema_version INTEGER NOT NULL,
          created_at_utc TEXT NOT NULL,
          started_at_utc TEXT NULL,
          stopped_at_utc TEXT NULL,
          status TEXT NOT NULL,
          base_params_json TEXT NOT NULL,
          search_plan_json TEXT NOT NULL,
          seed BIGINT NULL,
          runs_completed INTEGER NOT NULL DEFAULT 0,
          duplicates_skipped INTEGER NOT NULL DEFAULT 0,
          runs_failed INTEGER NOT NULL DEFAULT 0,
          cancel_requested INTEGER NOT NULL DEFAULT 0,
          last_run_id BIGINT NULL,
          precheck_rejected INTEGER NOT NULL DEFAULT 0,
          last_activity_at_utc TEXT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sampler_sessions_status ON {schema}.backtest_sampler_sessions(status, created_at_utc DESC);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="portfolio_defs",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.portfolio_defs (
          id BIGINT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          legs_json TEXT NOT NULL,
          execution_mode TEXT NOT NULL DEFAULT 'independent',
          group_start_day TEXT NULL,
          group_end_day TEXT NULL,
          paired_environment TEXT NOT NULL DEFAULT 'sandbox',
          paired_account_label TEXT NULL,
          signal_engine_enabled INTEGER NOT NULL DEFAULT 0,
          signal_last_poll_utc TEXT NULL,
          signal_last_emit_utc TEXT NULL,
          signal_last_error TEXT NULL,
          signal_last_source_ts TEXT NULL,
          created_at_utc TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_defs_updated ON {schema}.portfolio_defs(updated_at_utc DESC);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="portfolio_backtest_sessions",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.portfolio_backtest_sessions (
          id BIGINT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          started_at_utc TEXT NULL,
          stopped_at_utc TEXT NULL,
          status TEXT NOT NULL,
          legs_json TEXT NOT NULL,
          legs_total INTEGER NOT NULL,
          legs_completed INTEGER NOT NULL DEFAULT 0,
          legs_failed INTEGER NOT NULL DEFAULT 0,
          cancel_requested INTEGER NOT NULL DEFAULT 0,
          last_activity_at_utc TEXT NULL,
          combined_summary_json TEXT NULL,
          combined_equity_json TEXT NULL,
          legs_summaries_json TEXT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_sessions_status ON {schema}.portfolio_backtest_sessions(status, created_at_utc DESC);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="portfolio_group_runs",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.portfolio_group_runs (
          id BIGINT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          started_at_utc TEXT NULL,
          stopped_at_utc TEXT NULL,
          status TEXT NOT NULL,
          portfolio_ids_json TEXT NOT NULL,
          portfolios_total INTEGER NOT NULL,
          portfolios_completed INTEGER NOT NULL DEFAULT 0,
          portfolios_failed INTEGER NOT NULL DEFAULT 0,
          cancel_requested INTEGER NOT NULL DEFAULT 0,
          last_activity_at_utc TEXT NULL,
          group_summary_json TEXT NULL,
          group_equity_json TEXT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_group_runs_status ON {schema}.portfolio_group_runs(status, created_at_utc DESC);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="portfolio_group_run_portfolios",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.portfolio_group_run_portfolios (
          id BIGINT PRIMARY KEY,
          group_run_id BIGINT NOT NULL,
          portfolio_id BIGINT NOT NULL,
          portfolio_name TEXT NOT NULL,
          status TEXT NOT NULL,
          error TEXT NULL,
          combined_summary_json TEXT NULL,
          combined_equity_json TEXT NULL,
          legs_summaries_json TEXT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_group_run_portfolios ON {schema}.portfolio_group_run_portfolios(group_run_id, portfolio_id);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="parameter_groups",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.parameter_groups (
          id BIGINT PRIMARY KEY,
          name TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'Draft',
          tags_json TEXT NOT NULL DEFAULT '[]',
          comment TEXT,
          run_ids_json TEXT NOT NULL DEFAULT '[]',
          portfolio_ids_json TEXT NOT NULL DEFAULT '[]',
          created_at_utc TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL,
          archived INTEGER NOT NULL DEFAULT 0
        );
        """,
        pk_cols=["id"],
    ),
    # execution subsystem
    TableSpec(
        name="execution_account_controls",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.execution_account_controls (
          account_id TEXT PRIMARY KEY,
          enabled INTEGER NOT NULL DEFAULT 1,
          updated_at_utc TEXT NOT NULL
        );
        """,
        pk_cols=["account_id"],
    ),
    TableSpec(
        name="execution_intents",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.execution_intents (
          id BIGINT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL,
          environment TEXT NOT NULL,
          broker_name TEXT NOT NULL,
          status TEXT NOT NULL,
          strategy_key TEXT,
          symbol TEXT,
          candidate_ref TEXT,
          idempotency_key TEXT NOT NULL,
          intent_payload_json TEXT NOT NULL,
          error TEXT,
          broker_external_id TEXT,
          precheck_status TEXT,
          precheck_payload_json TEXT,
          risk_gate_status TEXT,
          quarantine_reason TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_execution_intents_idempotency_key ON {schema}.execution_intents(idempotency_key);
        CREATE INDEX IF NOT EXISTS idx_execution_intents_status ON {schema}.execution_intents(status, created_at_utc DESC);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="trade_runs",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.trade_runs (
          id BIGINT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL,
          environment TEXT NOT NULL,
          broker_name TEXT NOT NULL,
          execution_intent_id BIGINT,
          status TEXT NOT NULL,
          underlying TEXT,
          side TEXT,
          qty INTEGER,
          entry_order_id TEXT,
          exit_order_id TEXT,
          opened_at_utc TEXT,
          closed_at_utc TEXT,
          open_reason TEXT,
          close_reason TEXT,
          pnl_realized_usd REAL,
          pnl_unrealized_usd REAL,
          run_payload_json TEXT,
          complex_exit_order_id TEXT,
          protection_state TEXT,
          circuit_breaker_flag INTEGER NOT NULL DEFAULT 0,
          close_mode TEXT,
          degraded_protection_mode INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_trade_runs_status ON {schema}.trade_runs(status, created_at_utc DESC);
        CREATE INDEX IF NOT EXISTS idx_trade_runs_open ON {schema}.trade_runs(environment, status);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="risk_session_state",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.risk_session_state (
          id BIGINT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL,
          environment TEXT NOT NULL,
          broker_name TEXT NOT NULL,
          session_day_local TEXT NOT NULL,
          session_tz TEXT NOT NULL,
          realized_pnl_usd REAL NOT NULL DEFAULT 0,
          unrealized_pnl_usd REAL NOT NULL DEFAULT 0,
          max_daily_loss_usd REAL NOT NULL DEFAULT 300,
          block_new_entries INTEGER NOT NULL DEFAULT 0,
          reason TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_risk_session_state_key ON {schema}.risk_session_state(environment, broker_name, session_day_local);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="reprice_policy",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.reprice_policy (
          id BIGINT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL,
          environment TEXT NOT NULL,
          underlying TEXT NOT NULL DEFAULT 'SPX',
          max_attempts INTEGER NOT NULL DEFAULT 3,
          step REAL NOT NULL DEFAULT 0.05,
          interval_seconds INTEGER NOT NULL DEFAULT 25,
          max_total_concession REAL NOT NULL DEFAULT 0.15,
          enabled INTEGER NOT NULL DEFAULT 1
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_reprice_policy_env_underlying ON {schema}.reprice_policy(environment, underlying);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="audit_log",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.audit_log (
          id BIGINT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          environment TEXT NOT NULL,
          actor TEXT NOT NULL,
          action TEXT NOT NULL,
          entity_type TEXT,
          entity_id TEXT,
          details_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_audit_log_created ON {schema}.audit_log(created_at_utc DESC);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="broker_reconciliation_log",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.broker_reconciliation_log (
          id BIGINT PRIMARY KEY,
          snapshot_ts TEXT NOT NULL,
          environment TEXT NOT NULL,
          broker_name TEXT NOT NULL,
          open_orders_json TEXT NOT NULL,
          open_positions_json TEXT NOT NULL,
          diff_json TEXT,
          resolved_bool INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_broker_reconciliation_log_snapshot ON {schema}.broker_reconciliation_log(environment, broker_name, snapshot_ts DESC);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="incident_events",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.incident_events (
          id BIGINT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          environment TEXT NOT NULL,
          broker_name TEXT NOT NULL,
          severity TEXT NOT NULL,
          incident_type TEXT NOT NULL,
          trade_run_id BIGINT,
          execution_intent_id BIGINT,
          details_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_incident_events_created ON {schema}.incident_events(environment, broker_name, created_at_utc DESC);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="order_events",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.order_events (
          id BIGINT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          environment TEXT NOT NULL,
          broker_name TEXT NOT NULL,
          trade_run_id BIGINT,
          execution_intent_id BIGINT,
          order_id TEXT,
          event_type TEXT NOT NULL,
          status TEXT,
          raw_payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_order_events_created ON {schema}.order_events(created_at_utc DESC);
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="position_events",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.position_events (
          id BIGINT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          environment TEXT NOT NULL,
          broker_name TEXT NOT NULL,
          trade_run_id BIGINT,
          position_key TEXT,
          event_type TEXT NOT NULL,
          qty REAL,
          price REAL,
          pnl_unrealized_usd REAL,
          pnl_realized_usd REAL,
          raw_payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_position_events_created ON {schema}.position_events(created_at_utc DESC);
        """,
        pk_cols=["id"],
    ),
    # tuning/control tables (dashboard dependency)
    TableSpec(
        name="tuning_profiles",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.tuning_profiles (
          name TEXT PRIMARY KEY,
          config_json TEXT NOT NULL,
          built_in INTEGER NOT NULL DEFAULT 0,
          version INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """,
        pk_cols=["name"],
    ),
    TableSpec(
        name="tuning_config_versions",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.tuning_config_versions (
          version BIGINT PRIMARY KEY,
          config_json TEXT NOT NULL,
          actor TEXT,
          action TEXT,
          created_at TEXT NOT NULL
        );
        """,
        pk_cols=["version"],
    ),
    TableSpec(
        name="tuning_config_state",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.tuning_config_state (
          id INTEGER PRIMARY KEY,
          current_version BIGINT,
          current_config_json TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          updated_by TEXT
        );
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="tuning_audit_log",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.tuning_audit_log (
          id BIGINT PRIMARY KEY,
          ts TEXT NOT NULL,
          actor TEXT,
          action TEXT NOT NULL,
          old_values_json TEXT,
          new_values_json TEXT,
          result TEXT NOT NULL,
          error_detail TEXT,
          meta_json TEXT
        );
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="tuning_job_state",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.tuning_job_state (
          id INTEGER PRIMARY KEY,
          job_id TEXT,
          action TEXT,
          status TEXT,
          progress TEXT,
          started_at TEXT,
          finished_at TEXT,
          duration_sec REAL,
          error_text TEXT
        );
        """,
        pk_cols=["id"],
    ),
    TableSpec(
        name="tuning_settings",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.tuning_settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """,
        pk_cols=["key"],
    ),
    TableSpec(
        name="storage_metrics_samples",
        create_sql="""
        CREATE TABLE IF NOT EXISTS {schema}.storage_metrics_samples (
          id BIGINT PRIMARY KEY,
          sample_minute_utc TEXT NOT NULL,
          db_bytes BIGINT NOT NULL DEFAULT 0,
          logs_bytes BIGINT NOT NULL DEFAULT 0,
          data_root_bytes BIGINT NOT NULL DEFAULT 0,
          disk_total_bytes BIGINT,
          disk_free_bytes BIGINT,
          disk_used_bytes BIGINT,
          postgres_bytes BIGINT,
          timescale_bytes BIGINT,
          postgres_db_name TEXT,
          timescale_db_name TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_storage_metrics_samples_minute ON {schema}.storage_metrics_samples(sample_minute_utc);
        """,
        pk_cols=["id"],
    ),
]


def _sqlite_columns(con: sqlite3.Connection, table: str) -> list[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows]


def _pg_columns(cur: psycopg.Cursor, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [str(r[0]) for r in cur.fetchall()]


def _read_sqlite_rows(con: sqlite3.Connection, table: str, cols: list[str]) -> list[tuple[Any, ...]]:
    if not cols:
        return []
    qcols = ", ".join(cols)
    return con.execute(f"SELECT {qcols} FROM {table}").fetchall()


def _upsert_rows(cur: psycopg.Cursor, schema: str, table: str, cols: list[str], pk_cols: list[str], rows: list[tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    col_sql = ", ".join(cols)
    ph = ", ".join(["%s"] * len(cols))
    conflict = ", ".join(pk_cols)
    upd_cols = [c for c in cols if c not in pk_cols]
    if upd_cols:
        set_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in upd_cols])
        sql = f"INSERT INTO {schema}.{table} ({col_sql}) VALUES ({ph}) ON CONFLICT ({conflict}) DO UPDATE SET {set_sql}"
    else:
        sql = f"INSERT INTO {schema}.{table} ({col_sql}) VALUES ({ph}) ON CONFLICT ({conflict}) DO NOTHING"
    cur.executemany(sql, rows)
    return len(rows)


def _set_sequence(cur: psycopg.Cursor, schema: str, table: str, id_col: str = "id") -> None:
    cur.execute(
        """
        SELECT pg_get_serial_sequence(%s, %s)
        """,
        (f"{schema}.{table}", id_col),
    )
    r = cur.fetchone()
    seq = r[0] if r else None
    if not seq:
        return
    cur.execute(f"SELECT COALESCE(MAX({id_col}),0) FROM {schema}.{table}")
    mx = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT setval(%s, %s, true)", (seq, mx if mx > 0 else 1))



def _ensure_id_sequence_default(cur: psycopg.Cursor, schema: str, table: str) -> None:
    cur.execute(
        """
        SELECT column_default
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name='id'
        """,
        (schema, table),
    )
    r = cur.fetchone()
    if not r:
        return
    if r[0]:
        return
    seq = f"{table}_id_seq"
    cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {schema}.{seq}")
    cur.execute(f"ALTER TABLE {schema}.{table} ALTER COLUMN id SET DEFAULT nextval('{schema}.{seq}'::regclass)")
    cur.execute(f"SELECT COALESCE(MAX(id),0) FROM {schema}.{table}")
    mx = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT setval('{schema}.{seq}'::regclass, %s, true)", (mx if mx > 0 else 1,))


def main() -> None:
    ap = argparse.ArgumentParser(description="Migrate sqlite app-state tables to Postgres (id-preserving)")
    ap.add_argument("--sqlite", default="/mnt/options_ai/database/predictions.db")
    ap.add_argument("--pg", default=os.getenv("BACKTEST_DATABASE_URL", ""))
    ap.add_argument("--schema", default="spx")
    ap.add_argument("--apply", action="store_true", help="perform writes (default is dry-run)")
    args = ap.parse_args()

    if not args.pg:
        raise SystemExit("--pg (or BACKTEST_DATABASE_URL) is required")

    sq = sqlite3.connect(args.sqlite)
    sq.row_factory = sqlite3.Row

    with psycopg.connect(args.pg) as pg:
        with pg.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")
            for spec in TABLE_SPECS:
                for stmt in [s.strip() for s in spec.create_sql.format(schema=args.schema).split(";") if s.strip()]:
                    cur.execute(stmt)
                if 'id' in spec.pk_cols:
                    _ensure_id_sequence_default(cur, args.schema, spec.name)
        pg.commit()

        summary: list[tuple[str, int, int, str]] = []

        for spec in TABLE_SPECS:
            with pg.cursor() as cur:
                s_cols = _sqlite_columns(sq, spec.name)
                p_cols = _pg_columns(cur, args.schema, spec.name)
                cols = [c for c in s_cols if c in p_cols]
                rows = _read_sqlite_rows(sq, spec.name, cols)

                inserted = 0
                if args.apply:
                    inserted = _upsert_rows(cur, args.schema, spec.name, cols, spec.pk_cols, rows)
                    if "id" in spec.pk_cols and "id" in cols:
                        _ensure_id_sequence_default(cur, args.schema, spec.name)
                        _set_sequence(cur, args.schema, spec.name, "id")
                    pg.commit()

                cur.execute(f"SELECT COUNT(*) FROM {args.schema}.{spec.name}")
                pg_count = int(cur.fetchone()[0])
                summary.append((spec.name, len(rows), pg_count, "applied" if args.apply else "dry-run"))
                print(f"[{spec.name}] sqlite={len(rows)} pg={pg_count} mode={'apply' if args.apply else 'dry-run'}")

        print("\nSummary")
        for t, sct, pct, mode in summary:
            print(f"- {t}: sqlite={sct} pg={pct} ({mode})")


if __name__ == "__main__":
    main()
