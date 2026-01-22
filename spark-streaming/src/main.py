"""
Music Streaming Pipeline - Spark Structured Streaming Application
Main entry point for the real-time data processing pipeline
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.config_loader import get_config
from utils.logger import get_logger
from processors.kafka_consumer import create_kafka_consumer
from processors.transformers import create_transformer
from processors.bigquery_sink import create_bigquery_sink


def create_spark_session(app_name: str, gcp_project_id: str, credentials_path: str) -> SparkSession:
    """
    Create and configure Spark session with BigQuery connector
    
    Args:
        app_name: Application name
        gcp_project_id: GCP project ID
        credentials_path: Path to GCP service account key
        
    Returns:
        Configured SparkSession
    """
    logger = get_logger()
    logger.info("🔧 Creating Spark session...")
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2")
        .config("spark.sql.streaming.checkpointLocation", "/opt/spark/checkpoints")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    
    # Set GCP credentials
    spark.conf.set("credentialsFile", credentials_path)
    spark.conf.set("parentProject", gcp_project_id)
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"✅ Spark session created: {spark.version}")
    return spark


def main():
    """Main application logic"""
    
    # Initialize logger
    logger = get_logger()
    
    try:
        # Load configuration
        logger.info("📋 Loading configuration...")
        config = get_config()
        
        # Log startup
        startup_config = {
            'app_name': config.get('spark.app_name', 'MusicStreamingPipeline'),
            'kafka_topic': config.get_kafka_topic(),
            'bigquery_table': config.get_bigquery_table(),
            'trigger_interval': config.get_trigger_interval()
        }
        logger.log_startup(startup_config)
        
        # Create Spark session
        spark = create_spark_session(
            app_name=config.get('spark.app_name', 'MusicStreamingPipeline'),
            gcp_project_id=config.get('gcp.project_id'),
            credentials_path=config.get('gcp.credentials_path')
        )
        
        # Create Kafka consumer
        logger.info("📡 Setting up Kafka consumer...")
        kafka_consumer = create_kafka_consumer(spark)
        streaming_df = kafka_consumer.get_streaming_dataframe()
        
        # Create transformer
        logger.info("🔄 Setting up data transformer...")
        transformer = create_transformer(config.get_processing_config())
        
        # Apply transformations
        transformed_df = transformer.transform(streaming_df)
        
        # Select columns for BigQuery
        final_df = transformer.select_bigquery_columns(transformed_df)
        
        # Create BigQuery sink
        logger.info("📊 Setting up BigQuery sink...")
        bigquery_sink = create_bigquery_sink(spark)
        
        # Start streaming query
        logger.info("🚀 Starting streaming query...")
        streaming_query = bigquery_sink.write_stream(
            final_df,
            query_name="music_streaming_to_bigquery"
        )
        
        # Monitor query
        logger.info("👀 Monitoring streaming query...")
        logger.info("Press Ctrl+C to stop the application")
        logger.info("=" * 80)
        
        bigquery_sink.monitor_query(streaming_query)
        
    except KeyboardInterrupt:
        logger.log_shutdown("User interrupt (Ctrl+C)")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Application error: {str(e)}")
        logger.exception("Full error details:", exc_info=True)
        logger.log_shutdown(f"Error: {str(e)}")
        sys.exit(1)
    
    finally:
        # Cleanup
        try:
            if 'spark' in locals():
                logger.info("🧹 Cleaning up Spark session...")
                spark.stop()
        except:
            pass


if __name__ == "__main__":
    main()
