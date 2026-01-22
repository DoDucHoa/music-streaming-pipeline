"""
BigQuery Sink Module
Handles writing streaming data to Google BigQuery
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from typing import Dict, Any, Optional
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.config_loader import get_config

logger = get_logger()


class BigQuerySink:
    """Handles writing streaming data to BigQuery"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize BigQuery sink
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        
        # BigQuery configuration
        self.project_id = config.get('gcp_project_id')
        self.table = config.get('bigquery_table')
        self.temp_bucket = config.get('temp_gcs_bucket', '')
        self.write_method = config.get('write_method', 'direct')
        
        # Streaming configuration
        self.checkpoint_location = config.get('checkpoint_location', '/opt/spark/checkpoints')
        self.trigger_interval = config.get('trigger_interval', '30 seconds')
        
        # Dry run mode (for testing without writing)
        self.dry_run = config.get('dry_run', False)
    
    def write_to_bigquery_batch(self, df: DataFrame, batch_id: int) -> None:
        """
        Write batch of data to BigQuery using foreachBatch
        
        Args:
            df: DataFrame batch to write
            batch_id: Batch identifier
        """
        try:
            if df.count() == 0:
                self.logger.info(f"Batch {batch_id}: No records to write")
                return
            
            record_count = df.count()
            self.logger.info(f"📊 Batch {batch_id}: Writing {record_count} records to BigQuery...")
            
            if self.dry_run:
                self.logger.info(f"🧪 DRY RUN MODE: Would write {record_count} records")
                df.show(5, truncate=False)
                return
            
            # Write to BigQuery
            (
                df.write
                .format("bigquery")
                .option("table", self.table)
                .option("temporaryGcsBucket", self.temp_bucket) if self.temp_bucket else df.write
                .mode("append")
                .save()
            )
            
            self.logger.info(f"✅ Batch {batch_id}: Successfully wrote {record_count} records to BigQuery")
            
        except Exception as e:
            self.logger.error(f"❌ Batch {batch_id}: Failed to write to BigQuery: {str(e)}")
            self.logger.exception("Full error details:", exc_info=True)
            raise
    
    def write_stream(self, df: DataFrame, query_name: str = "bigquery_sink") -> StreamingQuery:
        """
        Write streaming DataFrame to BigQuery
        
        Args:
            df: Streaming DataFrame to write
            query_name: Name for the streaming query
            
        Returns:
            StreamingQuery object
        """
        self.logger.log_bigquery_connection(self.table)
        self.logger.info(f"Checkpoint location: {self.checkpoint_location}")
        self.logger.info(f"Trigger interval: {self.trigger_interval}")
        
        if self.dry_run:
            self.logger.info("🧪 Running in DRY RUN mode - data will not be written to BigQuery")
        
        try:
            # Use foreachBatch for more control and better error handling
            streaming_query = (
                df.writeStream
                .outputMode("append")
                .foreachBatch(self.write_to_bigquery_batch)
                .option("checkpointLocation", self.checkpoint_location)
                .trigger(processingTime=self.trigger_interval)
                .queryName(query_name)
                .start()
            )
            
            self.logger.info(f"✅ Streaming query '{query_name}' started successfully")
            
            return streaming_query
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start streaming query: {str(e)}")
            raise
    
    def write_stream_console(self, df: DataFrame, query_name: str = "console_debug") -> StreamingQuery:
        """
        Write streaming DataFrame to console for debugging
        
        Args:
            df: Streaming DataFrame
            query_name: Name for the query
            
        Returns:
            StreamingQuery object
        """
        self.logger.info("📺 Starting console output for debugging...")
        
        streaming_query = (
            df.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 10)
            .trigger(processingTime=self.trigger_interval)
            .queryName(query_name)
            .start()
        )
        
        return streaming_query
    
    def monitor_query(self, query: StreamingQuery) -> None:
        """
        Monitor streaming query progress
        
        Args:
            query: StreamingQuery to monitor
        """
        try:
            while query.isActive:
                # Get query status
                status = query.status
                
                if status['isDataAvailable']:
                    progress = query.lastProgress
                    if progress:
                        batch_id = progress.get('batchId', 'unknown')
                        num_input_rows = progress.get('numInputRows', 0)
                        processing_time = progress.get('durationMs', {}).get('addBatch', 0)
                        
                        self.logger.log_streaming_metrics(
                            batch_id=batch_id,
                            records_processed=num_input_rows,
                            processing_time_ms=processing_time,
                            records_written=num_input_rows  # Assuming all written successfully
                        )
                
                # Wait before next check
                query.awaitTermination(60)  # Check every 60 seconds
                
        except KeyboardInterrupt:
            self.logger.info("🛑 Received interrupt signal, stopping query...")
            query.stop()
        except Exception as e:
            self.logger.error(f"❌ Error monitoring query: {str(e)}")
            query.stop()
            raise


def create_bigquery_sink(spark: SparkSession) -> BigQuerySink:
    """
    Factory function to create BigQuerySink instance
    
    Args:
        spark: SparkSession instance
        
    Returns:
        Configured BigQuerySink instance
    """
    config_loader = get_config()
    
    # Prepare configuration dictionary
    config = {
        'gcp_project_id': config_loader.get('gcp.project_id'),
        'bigquery_table': config_loader.get_bigquery_table(),
        'temp_gcs_bucket': config_loader.get('spark.bigquery.temp_gcs_bucket', ''),
        'write_method': config_loader.get('spark.bigquery.write_method', 'direct'),
        'checkpoint_location': config_loader.get_checkpoint_location(),
        'trigger_interval': config_loader.get_trigger_interval(),
        'dry_run': config_loader.is_dry_run()
    }
    
    return BigQuerySink(spark, config)


if __name__ == "__main__":
    print("Testing BigQuery Sink...")
    print("Note: This requires a running Spark session and BigQuery credentials")
    print("Run this as part of the main application")
