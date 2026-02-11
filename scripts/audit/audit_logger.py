"""
JNE Audit Logger — Simplified module for 3 audit deliverables.

Tables populated:
  1. audit.data_traceability  — per-AWB data journey
  2. audit.batch_log          — per-batch process logs
  3. audit.change_log         — immutable change records (LSN)
  4. audit.transformation_log — supporting transform results
"""

import logging
from datetime import datetime
from contextlib import contextmanager
from sqlalchemy import text

logger = logging.getLogger(__name__)


class AuditLogger:
    """Audit logger for the JNE data pipeline."""

    def __init__(self, engine):
        self.engine = engine

    # ----------------------------------------------------------------
    # 1. TRACEABILITY
    # ----------------------------------------------------------------

    def log_trace(self, source_stage, target_stage, awb_number=None,
                  transformation_logic=None, batch_id=None,
                  record_count=None, metadata=None):
        """Log a data journey step (traceability)."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO audit.data_traceability
                        (awb_number, source_stage, target_stage,
                         transformation_logic, batch_id, record_count, metadata)
                    VALUES (:awb, :src, :tgt, :logic, :bid, :cnt, :meta)
                    RETURNING audit_id
                """), {
                    'awb': awb_number, 'src': source_stage, 'tgt': target_stage,
                    'logic': transformation_logic, 'bid': batch_id,
                    'cnt': record_count, 'meta': metadata,
                })
                return result.fetchone()[0]
        except Exception as e:
            logger.warning(f"Audit trace failed (non-fatal): {e}")
            return None

    # ----------------------------------------------------------------
    # 2. LOGS MONITORING
    # ----------------------------------------------------------------

    def start_batch(self, application, pipeline_stage, record_count_in=0):
        """Start a monitored batch. Returns batch_id."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO audit.batch_log
                        (application, pipeline_stage, record_count_in, status)
                    VALUES (:app, :stage, :cnt, 'RUNNING')
                    RETURNING batch_id
                """), {
                    'app': application, 'stage': pipeline_stage, 'cnt': record_count_in,
                })
                batch_id = result.fetchone()[0]
                logger.info(f"Audit batch started: {application} (ID {batch_id})")
                return batch_id
        except Exception as e:
            logger.warning(f"Audit start_batch failed (non-fatal): {e}")
            return None

    def complete_batch(self, batch_id, record_count_out, status='SUCCESS',
                       error_details=None):
        """Complete a monitored batch."""
        if batch_id is None:
            return
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    UPDATE audit.batch_log
                    SET completed_at = CURRENT_TIMESTAMP,
                        duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)),
                        record_count_out = :cnt,
                        status = :status,
                        error_details = :err
                    WHERE batch_id = :bid
                """), {
                    'bid': batch_id, 'cnt': record_count_out,
                    'status': status, 'err': error_details,
                })
                logger.info(f"Audit batch {batch_id} completed: {status} ({record_count_out} rows)")
        except Exception as e:
            logger.warning(f"Audit complete_batch failed (non-fatal): {e}")

    # ----------------------------------------------------------------
    # 3. INTEGRITY
    # ----------------------------------------------------------------

    def log_change(self, awb_number, status_after, system_action, metadata=None):
        """Log an immutable change record (integrity)."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO audit.change_log
                        (awb_number, status_after, system_action, metadata)
                    VALUES (:awb, :status, :action, :meta)
                    RETURNING log_id
                """), {
                    'awb': awb_number, 'status': status_after,
                    'action': system_action, 'meta': metadata,
                })
                return result.fetchone()[0]
        except Exception as e:
            logger.warning(f"Audit log_change failed (non-fatal): {e}")
            return None

    def backfill_change_log(self):
        """
        Backfill change_log from raw.CMS_DSTATUS.
        Each row in DSTATUS is a status change for a CNOTE.
        Clears previous backfill entries before re-inserting to avoid duplicates.
        """
        try:
            with self.engine.begin() as conn:
                # Clear previous backfill entries
                conn.execute(text(
                    "DELETE FROM audit.change_log WHERE system_action = 'Legacy System (Backfill)'"
                ))
                result = conn.execute(text("""
                    INSERT INTO audit.change_log
                        (awb_number, status_after, system_action, captured_at)
                    SELECT
                        CAST(dstatus_cnote_no AS VARCHAR(50)),
                        dstatus_status,
                        'Legacy System (Backfill)',
                        COALESCE(
                            CASE WHEN dstatus_status_date IS NOT NULL AND dstatus_status_date > 0
                                 THEN to_timestamp(dstatus_status_date) END,
                            CASE WHEN create_date IS NOT NULL AND create_date > 0
                                 THEN to_timestamp(create_date) END,
                            CURRENT_TIMESTAMP
                        )
                    FROM raw.cms_dstatus
                    WHERE dstatus_cnote_no IS NOT NULL
                """))
                logger.info(f"Backfilled {result.rowcount} change_log entries from CMS_DSTATUS")
                return result.rowcount
        except Exception as e:
            logger.warning(f"Backfill change_log failed (non-fatal): {e}")
            return 0

    def backfill_traceability(self):
        """
        Backfill per-AWB traceability entries from unified_shipments.
        Creates trace records showing each AWB's journey: raw -> staging -> transformed.
        Clears previous backfill entries before re-inserting.
        """
        try:
            with self.engine.begin() as conn:
                # Clear previous backfill entries
                conn.execute(text(
                    "DELETE FROM audit.data_traceability "
                    "WHERE transformation_logic LIKE 'Backfill:%'"
                ))
                # Insert per-AWB trace: raw -> staging (unification)
                result1 = conn.execute(text("""
                    INSERT INTO audit.data_traceability
                        (awb_number, source_stage, target_stage,
                         transformation_logic, record_count)
                    SELECT
                        CAST(cnote_no AS VARCHAR(50)),
                        'PostgreSQL (raw.cms_cnote)',
                        'PostgreSQL (staging.unified_shipments)',
                        'Backfill: 36-table SQL unification with manifest pivot',
                        1
                    FROM staging.unified_shipments
                    WHERE cnote_no IS NOT NULL
                """))
                logger.info(f"Backfilled {result1.rowcount} traceability entries (raw -> staging)")

                # Insert per-AWB trace: staging -> transformed
                result2 = conn.execute(text("""
                    INSERT INTO audit.data_traceability
                        (awb_number, source_stage, target_stage,
                         transformation_logic, record_count)
                    SELECT
                        CAST(cnote_no AS VARCHAR(50)),
                        'PostgreSQL (staging.unified_shipments)',
                        'PostgreSQL (transformed.unified_shipments)',
                        'Backfill: Pandas date standardization, DQ flags, manifest counts',
                        1
                    FROM transformed.unified_shipments
                    WHERE cnote_no IS NOT NULL
                """))
                logger.info(f"Backfilled {result2.rowcount} traceability entries (staging -> transformed)")

                total = result1.rowcount + result2.rowcount
                return total
        except Exception as e:
            logger.warning(f"Backfill traceability failed (non-fatal): {e}")
            return 0

    # ----------------------------------------------------------------
    # SUPPORTING: Transformation log
    # ----------------------------------------------------------------

    def log_transformation(self, table_name, row_count, status):
        """Log a transformation result (used by transform_tables.py)."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO audit.transformation_log (table_name, row_count, status)
                    VALUES (:tbl, :cnt, :status)
                """), {'tbl': table_name, 'cnt': row_count, 'status': status})
        except Exception as e:
            logger.warning(f"Audit log_transformation failed (non-fatal): {e}")

    # ----------------------------------------------------------------
    # QUERIES (for monitoring DAG)
    # ----------------------------------------------------------------

    def get_pipeline_health(self):
        """Get pipeline health summary from audit.v_pipeline_health."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM audit.v_pipeline_health"))
                rows = result.fetchall()
                cols = result.keys()
                return [dict(zip(cols, row)) for row in rows]
        except Exception as e:
            logger.warning(f"get_pipeline_health failed: {e}")
            return []

    def get_recent_failures(self):
        """Get recent batch failures."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM audit.v_recent_failures"))
                rows = result.fetchall()
                cols = result.keys()
                return [dict(zip(cols, row)) for row in rows]
        except Exception as e:
            logger.warning(f"get_recent_failures failed: {e}")
            return []


@contextmanager
def audited_batch(engine, application, pipeline_stage, record_count_in=0):
    """
    Context manager for audited pipeline batches.

    Usage:
        with audited_batch(engine, "Airflow (Unification)", "DATA_UNIFICATION", 10108) as batch:
            # do work
            batch['record_count_out'] = 10108
    """
    audit = AuditLogger(engine)
    batch_id = audit.start_batch(application, pipeline_stage, record_count_in)
    batch = {'batch_id': batch_id, 'record_count_out': 0, 'status': 'SUCCESS', 'error': None}
    try:
        yield batch
    except Exception as e:
        batch['status'] = 'FAILED'
        batch['error'] = str(e)
        raise
    finally:
        audit.complete_batch(
            batch_id,
            batch['record_count_out'],
            batch['status'],
            batch['error'],
        )
