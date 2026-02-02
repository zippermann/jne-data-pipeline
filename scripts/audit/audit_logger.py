"""
JNE Audit Logger Module - SQLAlchemy 2.0 Compatible
Comprehensive logging system for pipeline traceability, monitoring, and integrity
"""

import logging
import traceback
from datetime import datetime
from typing import Optional, Dict, Any, List
import json
import socket
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AuditLogger:
    """
    Comprehensive audit logging system implementing:
    1. Traceability: Data journey tracking
    2. Logs Monitoring: Pipeline health monitoring
    3. Integrity: Immutable change history
    """
    
    def __init__(self, connection_string: str):
        """
        Initialize audit logger with database connection
        
        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
        self.hostname = socket.gethostname()
        logger.info(f"AuditLogger initialized for host: {self.hostname}")
    
    # ================================================================
    # COMPONENT 1: TRACEABILITY (Data Journey Tracking)
    # ================================================================
    
    def log_lineage(
        self,
        source_table: str,
        target_table: str,
        operation_type: str,
        source_record_id: Optional[str] = None,
        target_record_id: Optional[str] = None,
        transformation_name: Optional[str] = None,
        record_count: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Log data lineage for traceability"""
        try:
            query = text("""
                INSERT INTO audit.data_lineage (
                    source_table, source_record_id, target_table, target_record_id,
                    operation_type, transformation_name, record_count, metadata
                ) VALUES (
                    :source_table, :source_record_id, :target_table, :target_record_id,
                    :operation_type, :transformation_name, :record_count, :metadata
                ) RETURNING lineage_id
            """)
            
            with self.engine.begin() as conn:
                result = conn.execute(query, {
                    'source_table': source_table,
                    'source_record_id': source_record_id,
                    'target_table': target_table,
                    'target_record_id': target_record_id,
                    'operation_type': operation_type,
                    'transformation_name': transformation_name,
                    'record_count': record_count,
                    'metadata': json.dumps(metadata) if metadata else None
                })
                lineage_id = result.fetchone()[0]
                logger.info(f"Logged lineage: {source_table} -> {target_table} ({operation_type})")
                return lineage_id
                
        except Exception as e:
            logger.error(f"Failed to log lineage: {e}")
            raise
    
    # ================================================================
    # COMPONENT 2: LOGS MONITORING (Pipeline Health)
    # ================================================================
    
    def start_job(
        self,
        job_name: str,
        job_type: str,
        pipeline_stage: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> int:
        """Start ETL job logging"""
        try:
            query = text("""
                SELECT audit.start_etl_job(
                    :job_name, :job_type, :pipeline_stage, :parameters
                )
            """)
            
            with self.engine.begin() as conn:
                result = conn.execute(query, {
                    'job_name': job_name,
                    'job_type': job_type,
                    'pipeline_stage': pipeline_stage,
                    'parameters': json.dumps(parameters) if parameters else None
                })
                job_log_id = result.fetchone()[0]
                logger.info(f"Started job: {job_name} (ID: {job_log_id})")
                return job_log_id
                
        except Exception as e:
            logger.error(f"Failed to start job: {e}")
            raise
    
    def complete_job(
        self,
        job_log_id: int,
        status: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Complete ETL job logging"""
        try:
            query = text("""
                SELECT audit.complete_etl_job(
                    :job_log_id, :status, :records_processed, :records_success,
                    :records_failed, :error_message, :error_details
                )
            """)
            
            with self.engine.begin() as conn:
                conn.execute(query, {
                    'job_log_id': job_log_id,
                    'status': status,
                    'records_processed': records_processed,
                    'records_success': records_success,
                    'records_failed': records_failed,
                    'error_message': error_message,
                    'error_details': json.dumps(error_details) if error_details else None
                })
                logger.info(f"Completed job ID {job_log_id} with status: {status}")
                
        except Exception as e:
            logger.error(f"Failed to complete job: {e}")
            raise
    
    def update_job_progress(
        self,
        job_log_id: int,
        records_processed: int,
        records_success: int = 0,
        records_failed: int = 0
    ) -> None:
        """Update job progress (for long-running jobs)"""
        try:
            query = text("""
                UPDATE audit.etl_job_log
                SET records_processed = :records_processed,
                    records_success = :records_success,
                    records_failed = :records_failed
                WHERE job_log_id = :job_log_id
            """)
            
            with self.engine.begin() as conn:
                conn.execute(query, {
                    'job_log_id': job_log_id,
                    'records_processed': records_processed,
                    'records_success': records_success,
                    'records_failed': records_failed
                })
                
        except Exception as e:
            logger.error(f"Failed to update job progress: {e}")
    
    # ================================================================
    # COMPONENT 3: INTEGRITY (Data Quality & Transformations)
    # ================================================================
    
    def log_quality_check(
        self,
        job_log_id: int,
        check_name: str,
        check_type: str,
        table_name: str,
        column_name: Optional[str] = None,
        check_query: Optional[str] = None,
        expected_value: Optional[str] = None,
        actual_value: Optional[str] = None,
        records_checked: Optional[int] = None,
        records_passed: Optional[int] = None,
        records_failed: Optional[int] = None,
        status: str = 'PASS',
        error_details: Optional[Dict[str, Any]] = None
    ) -> int:
        """Log data quality check results"""
        try:
            # Calculate pass percentage
            pass_percentage = None
            if records_checked and records_checked > 0 and records_passed is not None:
                pass_percentage = (records_passed / records_checked) * 100
            
            query = text("""
                INSERT INTO audit.data_quality_log (
                    job_log_id, check_name, check_type, table_name, column_name,
                    check_query, expected_value, actual_value, records_checked,
                    records_passed, records_failed, pass_percentage, status, error_details
                ) VALUES (
                    :job_log_id, :check_name, :check_type, :table_name, :column_name,
                    :check_query, :expected_value, :actual_value, :records_checked,
                    :records_passed, :records_failed, :pass_percentage, :status, :error_details
                ) RETURNING quality_log_id
            """)
            
            with self.engine.begin() as conn:
                result = conn.execute(query, {
                    'job_log_id': job_log_id,
                    'check_name': check_name,
                    'check_type': check_type,
                    'table_name': table_name,
                    'column_name': column_name,
                    'check_query': check_query,
                    'expected_value': expected_value,
                    'actual_value': actual_value,
                    'records_checked': records_checked,
                    'records_passed': records_passed,
                    'records_failed': records_failed,
                    'pass_percentage': pass_percentage,
                    'status': status,
                    'error_details': json.dumps(error_details) if error_details else None
                })
                quality_log_id = result.fetchone()[0]
                logger.info(f"Logged quality check: {check_name} on {table_name} - {status}")
                return quality_log_id
                
        except Exception as e:
            logger.error(f"Failed to log quality check: {e}")
            raise
    
    def log_transformation(
        self,
        job_log_id: int,
        transformation_name: str,
        transformation_type: str,
        source_table: str,
        target_table: str,
        transformation_logic: Optional[str] = None,
        records_before: Optional[int] = None,
        records_after: Optional[int] = None,
        records_inserted: int = 0,
        records_updated: int = 0,
        records_deleted: int = 0,
        columns_affected: Optional[List[str]] = None,
        transformation_rules: Optional[Dict[str, Any]] = None,
        version: Optional[str] = None
    ) -> int:
        """Log transformation for audit trail"""
        try:
            query = text("""
                INSERT INTO audit.transformation_history (
                    job_log_id, transformation_name, transformation_type, source_table,
                    target_table, transformation_logic, records_before, records_after,
                    records_inserted, records_updated, records_deleted, columns_affected,
                    transformation_rules, version
                ) VALUES (
                    :job_log_id, :transformation_name, :transformation_type, :source_table,
                    :target_table, :transformation_logic, :records_before, :records_after,
                    :records_inserted, :records_updated, :records_deleted, :columns_affected,
                    :transformation_rules, :version
                ) RETURNING transform_id
            """)
            
            with self.engine.begin() as conn:
                result = conn.execute(query, {
                    'job_log_id': job_log_id,
                    'transformation_name': transformation_name,
                    'transformation_type': transformation_type,
                    'source_table': source_table,
                    'target_table': target_table,
                    'transformation_logic': transformation_logic,
                    'records_before': records_before,
                    'records_after': records_after,
                    'records_inserted': records_inserted,
                    'records_updated': records_updated,
                    'records_deleted': records_deleted,
                    'columns_affected': columns_affected,
                    'transformation_rules': json.dumps(transformation_rules) if transformation_rules else None,
                    'version': version
                })
                transform_id = result.fetchone()[0]
                logger.info(f"Logged transformation: {transformation_name}")
                return transform_id
                
        except Exception as e:
            logger.error(f"Failed to log transformation: {e}")
            raise
    
    # ================================================================
    # UTILITY METHODS
    # ================================================================
    
    def log_user_activity(
        self,
        user_name: str,
        activity_type: str,
        table_name: Optional[str] = None,
        action_description: Optional[str] = None,
        query_executed: Optional[str] = None,
        rows_affected: Optional[int] = None
    ) -> int:
        """Log user activity for compliance"""
        try:
            query = text("""
                INSERT INTO audit.user_activity (
                    user_name, activity_type, table_name, action_description,
                    query_executed, rows_affected
                ) VALUES (
                    :user_name, :activity_type, :table_name, :action_description,
                    :query_executed, :rows_affected
                ) RETURNING activity_id
            """)
            
            with self.engine.begin() as conn:
                result = conn.execute(query, {
                    'user_name': user_name,
                    'activity_type': activity_type,
                    'table_name': table_name,
                    'action_description': action_description,
                    'query_executed': query_executed,
                    'rows_affected': rows_affected
                })
                activity_id = result.fetchone()[0]
                return activity_id
                
        except Exception as e:
            logger.error(f"Failed to log user activity: {e}")
            raise
    
    def get_pipeline_health(self) -> Dict[str, Any]:
        """Get current pipeline health summary"""
        try:
            query = text("SELECT * FROM audit.v_pipeline_health_summary")
            
            with self.engine.connect() as conn:
                result = conn.execute(query)
                rows = result.fetchall()
                
                health_data = []
                for row in rows:
                    health_data.append({
                        'pipeline_stage': row[0],
                        'job_type': row[1],
                        'total_jobs': row[2],
                        'successful_jobs': row[3],
                        'failed_jobs': row[4],
                        'partial_jobs': row[5],
                        'total_records_processed': row[6],
                        'total_records_failed': row[7],
                        'avg_duration_seconds': float(row[8]) if row[8] else None,
                        'last_run_time': row[9].isoformat() if row[9] else None
                    })
                
                return {'pipeline_health': health_data}
                
        except Exception as e:
            logger.error(f"Failed to get pipeline health: {e}")
            return {'error': str(e)}
    
    def get_data_quality_scorecard(self) -> Dict[str, Any]:
        """Get data quality scorecard"""
        try:
            query = text("SELECT * FROM audit.v_data_quality_scorecard")
            
            with self.engine.connect() as conn:
                result = conn.execute(query)
                rows = result.fetchall()
                
                scorecard_data = []
                for row in rows:
                    scorecard_data.append({
                        'table_name': row[0],
                        'check_type': row[1],
                        'total_checks': row[2],
                        'passed_checks': row[3],
                        'failed_checks': row[4],
                        'warning_checks': row[5],
                        'avg_pass_percentage': float(row[6]) if row[6] else None,
                        'last_check_time': row[7].isoformat() if row[7] else None
                    })
                
                return {'quality_scorecard': scorecard_data}
                
        except Exception as e:
            logger.error(f"Failed to get quality scorecard: {e}")
            return {'error': str(e)}


# ================================================================
# CONTEXT MANAGER FOR JOB EXECUTION
# ================================================================

class AuditedJob:
    """
    Context manager for automatic job audit logging
    
    Usage:
        with AuditedJob(audit_logger, "Load Data", "LOAD", "DATA_UNIFICATION") as job:
            # Do work
            job.increment_processed(100)
            job.increment_success(95)
            job.increment_failed(5)
    """
    
    def __init__(
        self,
        audit_logger: AuditLogger,
        job_name: str,
        job_type: str,
        pipeline_stage: str,
        parameters: Optional[Dict[str, Any]] = None
    ):
        self.audit_logger = audit_logger
        self.job_name = job_name
        self.job_type = job_type
        self.pipeline_stage = pipeline_stage
        self.parameters = parameters
        self.job_log_id = None
        self.records_processed = 0
        self.records_success = 0
        self.records_failed = 0
        self.status = 'SUCCESS'
        self.error_message = None
        self.error_details = None
    
    def __enter__(self):
        """Start the job"""
        self.job_log_id = self.audit_logger.start_job(
            self.job_name,
            self.job_type,
            self.pipeline_stage,
            self.parameters
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Complete the job, handling exceptions"""
        if exc_type is not None:
            self.status = 'FAILED'
            self.error_message = str(exc_val)
            self.error_details = {
                'exception_type': exc_type.__name__,
                'traceback': traceback.format_exc()
            }
        
        self.audit_logger.complete_job(
            self.job_log_id,
            self.status,
            self.records_processed,
            self.records_success,
            self.records_failed,
            self.error_message,
            self.error_details
        )
        
        # Don't suppress the exception
        return False
    
    def increment_processed(self, count: int = 1):
        """Increment processed record count"""
        self.records_processed += count
    
    def increment_success(self, count: int = 1):
        """Increment success record count"""
        self.records_success += count
    
    def increment_failed(self, count: int = 1):
        """Increment failed record count"""
        self.records_failed += count
    
    def set_status(self, status: str):
        """Set job status"""
        self.status = status
