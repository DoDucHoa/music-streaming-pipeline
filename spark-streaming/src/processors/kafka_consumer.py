"""
Kafka Consumer Module
Handles reading streaming data from Kafka topics
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, IntegerType
from typing import Dict, Any
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.config_loader import get_config

logger = get_logger()


class KafkaConsumer:
    """Handles Kafka streaming consumption"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize Kafka consumer
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.bootstrap_servers = config.get('kafka_bootstrap_servers', 'kafka:29092')
        self.topic = config.get('kafka_topic', 'music-streaming-events')
        self.logger = logger
    
    def get_event_schema(self) -> StructType:
        """
        Define schema for incoming events from eventsim
        
        Returns:
            StructType schema matching eventsim output
        """
        return StructType([
            # Event identifiers
            StructField("sessionId", StringType(), True),
            
            # User information
            StructField("userId", StringType(), True),  # Nullable for guest users
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("level", StringType(), True),  # free or paid
            
            # Authentication
            StructField("auth", StringType(), True),  # Guest, Logged In, Logged Out, Cancelled
            StructField("registration", LongType(), True),  # Unix timestamp in milliseconds
            
            # Event details
            StructField("page", StringType(), True),
            StructField("method", StringType(), True),
            StructField("status", IntegerType(), True),
            
            # Song information (only for NextSong events)
            StructField("artist", StringType(), True),
            StructField("song", StringType(), True),
            StructField("length", FloatType(), True),
            
            # Location & Device
            StructField("location", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", StringType(), True),
            StructField("lat", FloatType(), True),
            StructField("lon", FloatType(), True),
            StructField("userAgent", StringType(), True),
            
            # Session info
            StructField("itemInSession", IntegerType(), True),
            
            # Timestamp
            StructField("ts", LongType(), True),  # Unix timestamp in milliseconds
        ])
    
    def create_kafka_stream(self) -> DataFrame:
        """
        Create streaming DataFrame from Kafka
        
        Returns:
            Streaming DataFrame with Kafka messages
        """
        self.logger.log_kafka_connection(self.bootstrap_servers, self.topic)
        
        try:
            # Read from Kafka
            kafka_df = (
                self.spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.bootstrap_servers)
                .option("subscribe", self.topic)
                .option("startingOffsets", "earliest")  # For development, use 'latest' in production
                .option("failOnDataLoss", "false")  # Handle potential data loss gracefully
                .option("kafka.session.timeout.ms", "30000")
                .option("kafka.request.timeout.ms", "40000")
                .option("maxOffsetsPerTrigger", self.config.get('max_offsets_per_trigger', 10000))
                .load()
            )
            
            self.logger.info("✅ Kafka stream created successfully",
                           topic=self.topic,
                           servers=self.bootstrap_servers)
            
            return kafka_df
            
        except Exception as e:
            self.logger.error(f"❌ Failed to create Kafka stream: {str(e)}")
            raise
    
    def parse_kafka_messages(self, kafka_df: DataFrame) -> DataFrame:
        """
        Parse JSON messages from Kafka
        
        Args:
            kafka_df: Raw Kafka DataFrame
            
        Returns:
            Parsed DataFrame with event schema
        """
        event_schema = self.get_event_schema()
        
        # Parse JSON value from Kafka
        parsed_df = (
            kafka_df
            .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
            .select(
                from_json(col("json_value"), event_schema).alias("event"),
                col("kafka_timestamp")
            )
            .select("event.*", "kafka_timestamp")
        )
        
        self.logger.info("✅ Kafka messages parsed with schema")
        
        return parsed_df
    
    def get_streaming_dataframe(self) -> DataFrame:
        """
        Get complete streaming DataFrame with parsed events
        
        Returns:
            Streaming DataFrame ready for processing
        """
        # Create Kafka stream
        kafka_df = self.create_kafka_stream()
        
        # Parse JSON messages
        parsed_df = self.parse_kafka_messages(kafka_df)
        
        return parsed_df


def create_kafka_consumer(spark: SparkSession) -> KafkaConsumer:
    """
    Factory function to create KafkaConsumer instance
    
    Args:
        spark: SparkSession instance
        
    Returns:
        Configured KafkaConsumer instance
    """
    config_loader = get_config()
    
    # Prepare configuration dictionary
    config = {
        'kafka_bootstrap_servers': config_loader.get_kafka_bootstrap_servers(),
        'kafka_topic': config_loader.get_kafka_topic(),
        'max_offsets_per_trigger': config_loader.get('spark.streaming.max_offsets_per_trigger', 10000)
    }
    
    return KafkaConsumer(spark, config)


if __name__ == "__main__":
    # Test Kafka consumer (requires running Spark session)
    print("Testing Kafka Consumer...")
    print("Note: This requires a running Spark session and Kafka broker")
    print("Run this as part of the main application")
