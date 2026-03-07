"""
Logging Utility
Provides structured logging for the Spark Streaming application
"""

import logging
import sys
from typing import Optional
from datetime import datetime


class SparkLogger:
    """Custom logger for Spark Streaming application"""
    
    def __init__(self, name: str = "MusicStreamingPipeline", level: str = "INFO"):
        """
        Initialize logger
        
        Args:
            name: Logger name
            level: Logging level (DEBUG, INFO, WARN, ERROR)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()
    
    def _setup_handlers(self) -> None:
        """Setup console and file handlers with formatting"""
        # Console handler with colored output
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Detailed format for console
        console_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        
        self.logger.addHandler(console_handler)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message"""
        self.logger.info(self._format_message(message, kwargs))
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message"""
        self.logger.debug(self._format_message(message, kwargs))
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message"""
        self.logger.warning(self._format_message(message, kwargs))
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message"""
        self.logger.error(self._format_message(message, kwargs))
    
    def exception(self, message: str, exc_info: bool = True) -> None:
        """Log exception with traceback"""
        self.logger.exception(message, exc_info=exc_info)
    
    def _format_message(self, message: str, context: dict) -> str:
        """
        Format log message with context
        
        Args:
            message: Log message
            context: Additional context as key-value pairs
            
        Returns:
            Formatted message
        """
        if context:
            context_str = " | ".join(f"{k}={v}" for k, v in context.items())
            return f"{message} | {context_str}"
        return message
    
    def log_streaming_metrics(
        self,
        batch_id: int,
        records_processed: int,
        processing_time_ms: float,
        records_written: int = 0,
        errors: int = 0
    ) -> None:
        """
        Log streaming batch metrics
        
        Args:
            batch_id: Batch identifier
            records_processed: Number of records processed
            processing_time_ms: Processing time in milliseconds
            records_written: Number of records written to sink
            errors: Number of errors encountered
        """
        throughput = records_processed / (processing_time_ms / 1000) if processing_time_ms > 0 else 0
        
        self.info(
            f"Batch completed",
            batch_id=batch_id,
            records_processed=records_processed,
            records_written=records_written,
            errors=errors,
            processing_time_ms=f"{processing_time_ms:.2f}",
            throughput_per_sec=f"{throughput:.2f}"
        )
    
    def log_startup(self, config: dict) -> None:
        """Log application startup information"""
        self.info("=" * 80)
        self.info("🚀 Music Streaming Pipeline - Spark Application Starting")
        self.info("=" * 80)
        self.info(f"Timestamp: {datetime.now().isoformat()}")
        self.info(f"Application: {config.get('app_name', 'Unknown')}")
        self.info(f"Kafka Topic: {config.get('kafka_topic', 'Unknown')}")
        self.info(f"BigQuery Table: {config.get('bigquery_table', 'Unknown')}")
        self.info(f"Trigger Interval: {config.get('trigger_interval', 'Unknown')}")
        self.info("=" * 80)
    
    def log_shutdown(self, reason: Optional[str] = None) -> None:
        """Log application shutdown"""
        self.info("=" * 80)
        self.info("🛑 Music Streaming Pipeline - Spark Application Shutting Down")
        if reason:
            self.info(f"Reason: {reason}")
        self.info(f"Timestamp: {datetime.now().isoformat()}")
        self.info("=" * 80)
    
    def log_kafka_connection(self, bootstrap_servers: str, topic: str) -> None:
        """Log Kafka connection information"""
        self.info("📡 Connecting to Kafka", 
                 bootstrap_servers=bootstrap_servers,
                 topic=topic)
    
    def log_bigquery_connection(self, table: str) -> None:
        """Log BigQuery connection information"""
        self.info("📊 Connecting to BigQuery",
                 table=table)
    
    def log_data_quality_check(
        self,
        total_records: int,
        valid_records: int,
        invalid_records: int,
        null_page: int = 0,
        null_ts: int = 0
    ) -> None:
        """Log data quality check results"""
        self.info("✅ Data quality check completed",
                 total_records=total_records,
                 valid_records=valid_records,
                 invalid_records=invalid_records,
                 null_page=null_page,
                 null_ts=null_ts)
    
    def log_error_batch(self, batch_id: int, error_msg: str) -> None:
        """Log batch processing error"""
        self.error(f"❌ Error processing batch {batch_id}: {error_msg}")


# Singleton instance
_logger_instance: Optional[SparkLogger] = None


def get_logger(name: str = "MusicStreamingPipeline", level: str = "INFO") -> SparkLogger:
    """
    Get or create logger singleton instance
    
    Args:
        name: Logger name
        level: Logging level
        
    Returns:
        SparkLogger instance
    """
    global _logger_instance
    
    if _logger_instance is None:
        _logger_instance = SparkLogger(name, level)
    
    return _logger_instance


if __name__ == "__main__":
    # Test logger
    logger = get_logger()
    
    logger.info("Testing logger")
    logger.debug("Debug message")
    logger.warning("Warning message")
    logger.error("Error message")
    
    logger.log_streaming_metrics(
        batch_id=1,
        records_processed=1000,
        processing_time_ms=500,
        records_written=950,
        errors=50
    )
    
    logger.log_startup({
        'app_name': 'MusicStreamingPipeline',
        'kafka_topic': 'music-streaming-events',
        'bigquery_table': 'project.dataset.table',
        'trigger_interval': '30 seconds'
    })
