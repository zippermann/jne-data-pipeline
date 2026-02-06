"""
JNE Shipment Tracking - Usage Examples
Shows how to integrate shipment tracking into different parts of the pipeline

This can be used by:
- Kafka CDC consumers
- Airflow transformation tasks
- Manual status updates
"""

from sqlalchemy import create_engine, text
import json
from datetime import datetime

# Connection
DB_CONN = "postgresql://jne_user:jne_secure_password_2024@localhost:5432/jne_dashboard"


class ShipmentTracker:
    """
    Track shipment positions through the pipeline.
    Logs immutable records for audit compliance.
    """
    
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
    
    def log_status(self, awb_number: str, status: str, system_action: str,
                   source_table: str = None, location_code: str = None,
                   location_name: str = None, metadata: dict = None) -> int:
        """
        Log a shipment status change.
        
        Args:
            awb_number: JNE AWB/CNOTE number (e.g., 'JNE554433')
            status: New status (e.g., 'Arrived at Hub', 'Sorted', 'Out for Delivery')
            system_action: What triggered this change:
                - 'Kafka CDC' - Real-time change capture
                - 'Airflow: Final Write to PostgreSQL' - Batch processing
                - 'CSV Load' - Initial data load
                - 'Manual Update' - User intervention
            source_table: Which table this came from (e.g., 'cms_cnote', 'cms_dstatus')
            location_code: Hub/branch code (e.g., 'JKT01', 'BDG02')
            location_name: Hub/branch name (e.g., 'Jakarta Hub', 'Bandung Gateway')
            metadata: Additional JSON data
        
        Returns:
            log_id of the tracking record
        """
        try:
            query = text("""
                SELECT audit.log_shipment_status(
                    :awb, :status, :action, :source, NULL, :loc_code, :loc_name, :meta
                )
            """)
            
            with self.engine.begin() as conn:
                result = conn.execute(query, {
                    'awb': awb_number,
                    'status': status,
                    'action': system_action,
                    'source': source_table,
                    'loc_code': location_code,
                    'loc_name': location_name,
                    'meta': json.dumps(metadata) if metadata else None
                })
                return result.fetchone()[0]
        except Exception as e:
            print(f"Error logging shipment status: {e}")
            return None
    
    def get_journey(self, awb_number: str) -> list:
        """Get complete journey timeline for a shipment"""
        query = text("SELECT * FROM audit.get_shipment_journey(:awb)")
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {'awb': awb_number})
            return [dict(row._mapping) for row in result]
    
    def get_current_status(self, awb_number: str) -> dict:
        """Get current status of a shipment"""
        query = text("""
            SELECT * FROM audit.v_shipment_current_status 
            WHERE awb_number = :awb
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {'awb': awb_number})
            row = result.fetchone()
            return dict(row._mapping) if row else None
    
    def bulk_log_from_table(self, source_table: str, awb_column: str, 
                            status_column: str, system_action: str,
                            location_column: str = None, limit: int = None):
        """
        Bulk log status changes from a source table.
        Useful for initial data load or batch processing.
        """
        query = f"""
            SELECT DISTINCT {awb_column} as awb, {status_column} as status
            {f', {location_column} as location' if location_column else ''}
            FROM raw.{source_table}
            WHERE {awb_column} IS NOT NULL
            {f'LIMIT {limit}' if limit else ''}
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
        
        logged = 0
        for row in rows:
            awb = row[0]
            status = row[1]
            location = row[2] if location_column else None
            
            self.log_status(
                awb_number=str(awb),
                status=str(status) if status else 'Unknown',
                system_action=system_action,
                source_table=source_table,
                location_code=str(location) if location else None
            )
            logged += 1
        
        return logged


# ============================================================
# USAGE EXAMPLES
# ============================================================

def example_kafka_cdc():
    """Example: Log status changes from Kafka CDC consumer"""
    tracker = ShipmentTracker(DB_CONN)
    
    # Simulating Kafka message processing
    kafka_message = {
        'awb': 'JNE554433',
        'new_status': 'Arrived at Hub',
        'hub_code': 'JKT01',
        'hub_name': 'Jakarta Hub',
        'timestamp': '2026-02-04T14:00:02'
    }
    
    tracker.log_status(
        awb_number=kafka_message['awb'],
        status=kafka_message['new_status'],
        system_action='Kafka CDC',
        location_code=kafka_message['hub_code'],
        location_name=kafka_message['hub_name'],
        metadata={'kafka_timestamp': kafka_message['timestamp']}
    )
    print(f"✓ Logged: {kafka_message['awb']} -> {kafka_message['new_status']}")


def example_airflow_batch():
    """Example: Log status changes from Airflow batch processing"""
    tracker = ShipmentTracker(DB_CONN)
    
    # After transformation completes
    processed_shipments = [
        ('JNE554433', 'Sorted'),
        ('JNE554434', 'Out for Delivery'),
        ('JNE554435', 'Delivered'),
    ]
    
    for awb, status in processed_shipments:
        tracker.log_status(
            awb_number=awb,
            status=status,
            system_action='Airflow: Final Write to PostgreSQL',
            source_table='transformed.cms_cnote'
        )
    print(f"✓ Logged {len(processed_shipments)} status changes from Airflow")


def example_get_journey():
    """Example: Query shipment journey"""
    tracker = ShipmentTracker(DB_CONN)
    
    journey = tracker.get_journey('JNE554433')
    
    print("\nShipment Journey for JNE554433:")
    print("-" * 60)
    for stage in journey:
        print(f"Stage {stage['stage_number']}: {stage['status']}")
        print(f"  Location: {stage['location']}")
        print(f"  Time: {stage['captured_at']}")
        print(f"  Action: {stage['system_action']}")
        if stage['time_in_status']:
            print(f"  Duration: {stage['time_in_status']}")
        print()


def example_bulk_load():
    """Example: Bulk load status from CMS_DSTATUS table"""
    tracker = ShipmentTracker(DB_CONN)
    
    # Load statuses from CMS_DSTATUS
    logged = tracker.bulk_log_from_table(
        source_table='cms_dstatus',
        awb_column='dstatus_cnote_no',
        status_column='dstatus_desc',  # or appropriate status column
        system_action='CSV Load: Initial Import',
        limit=100  # Start with 100 for testing
    )
    print(f"✓ Bulk logged {logged} shipment statuses")


if __name__ == '__main__':
    print("JNE Shipment Tracking Examples")
    print("=" * 60)
    
    # Uncomment to run examples:
    # example_kafka_cdc()
    # example_airflow_batch()
    # example_get_journey()
    # example_bulk_load()
    
    print("\nEdit this file and uncomment examples to test.")
    print("Make sure to run 04-shipment-tracking-audit.sql first!")
