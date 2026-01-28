"""
Data Transformers Module
Handles data transformations and cleaning for streaming events
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, udf, current_timestamp, to_date, lit, when,
    trim, split, regexp_extract
)
from pyspark.sql.types import StringType, TimestampType, DateType
from typing import Dict, Any, List
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.helpers import generate_event_id, extract_location_components

logger = get_logger()


class EventTransformer:
    """Handles data transformations for streaming events"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer
        
        Args:
            config: Processing configuration
        """
        self.config = config
        self.logger = logger
        
        # Get configuration
        self.data_quality_config = config.get('data_quality', {})
        self.transformations_config = config.get('transformations', {})
        self.filters_config = config.get('filters', {})
    
    def apply_data_quality_checks(self, df: DataFrame) -> DataFrame:
        """
        Apply data quality checks and filtering
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        initial_count = df.count() if self.config.get('log_counts', False) else 0
        
        # Drop records with null page (required field)
        if self.data_quality_config.get('drop_null_page', True):
            df = df.filter(col("page").isNotNull())
        
        # Drop records with null timestamp (required field)
        if self.data_quality_config.get('drop_null_ts', True):
            df = df.filter(col("ts").isNotNull())
        
        # Note: We keep null userId (guest users are valid)
        
        if self.config.get('log_counts', False):
            final_count = df.count()
            self.logger.info(f"Data quality: {initial_count} → {final_count} records")
        
        return df
    
    def apply_event_filters(self, df: DataFrame) -> DataFrame:
        """
        Apply event-level filters
        
        Args:
            df: Input DataFrame
            
        Returns:
            Filtered DataFrame
        """
        # Exclude specific pages
        exclude_pages = self.filters_config.get('exclude_pages', [])
        if exclude_pages:
            for page in exclude_pages:
                df = df.filter(col("page") != page)
            self.logger.info(f"Excluded pages: {exclude_pages}")
        
        # Include only specific pages (if specified)
        include_pages = self.filters_config.get('include_pages', [])
        if include_pages:
            df = df.filter(col("page").isin(include_pages))
            self.logger.info(f"Included only pages: {include_pages}")
        
        return df
    
    def convert_timestamps(self, df: DataFrame) -> DataFrame:
        """
        Convert millisecond timestamps to proper timestamp types
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with converted timestamps
        """
        # Convert ts from milliseconds to timestamp
        df = df.withColumn(
            "ts",
            (col("ts") / 1000).cast(TimestampType())
        )
        
        # Convert registration from milliseconds to timestamp (if exists)
        df = df.withColumn(
            "registration",
            when(col("registration").isNotNull(),
                 (col("registration") / 1000).cast(TimestampType())
            ).otherwise(None)
        )
        
        return df
    
    def add_processing_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add processing metadata columns
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata columns
        """
        # Add event_id (UUID)
        if self.transformations_config.get('add_event_id', True):
            generate_id_udf = udf(lambda: generate_event_id(), StringType())
            df = df.withColumn("event_id", generate_id_udf())
        
        # Add event_type based on kafka topic (if available)
        if self.transformations_config.get('add_event_type', True) and 'kafka_topic' in df.columns:
            df = df.withColumn(
                "event_type",
                when(col("kafka_topic") == "listen_events", "song_play")
                .when(col("kafka_topic") == "page_view_events", "page_view")
                .when(col("kafka_topic") == "auth_events", "authentication")
                .when(col("kafka_topic") == "status_change_events", "status_change")
                .otherwise("unknown")
            )
        
        # Add processing timestamp
        if self.transformations_config.get('add_processing_timestamp', True):
            df = df.withColumn("processed_at", current_timestamp())
        
        # Add processing date (for partitioning)
        if self.transformations_config.get('add_processing_date', True):
            df = df.withColumn("processing_date", to_date(col("ts")))
        
        return df
    
    def parse_location_fields(self, df: DataFrame) -> DataFrame:
        """
        Parse and enhance location fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with parsed location fields
        """
        if not self.transformations_config.get('extract_location', True):
            return df
        
        # Extract city and state from location string if not already present
        # Location format: "San Francisco-Oakland-Hayward, CA"
        
        # Only extract if city/state are null
        df = df.withColumn(
            "city",
            when(col("city").isNull() & col("location").isNotNull(),
                 trim(split(col("location"), ",").getItem(0))
            ).otherwise(col("city"))
        )
        
        df = df.withColumn(
            "state",
            when(col("state").isNull() & col("location").isNotNull(),
                 trim(split(col("location"), ",").getItem(1))
            ).otherwise(col("state"))
        )
        
        return df
    
    def clean_string_fields(self, df: DataFrame) -> DataFrame:
        """
        Clean and normalize string fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with cleaned strings
        """
        string_fields = [
            "userId", "firstName", "lastName", "gender", "level",
            "auth", "page", "method", "artist", "song",
            "location", "city", "state", "zip", "userAgent"
        ]
        
        for field in string_fields:
            if field in df.columns:
                df = df.withColumn(
                    field,
                    when(col(field).isNotNull(), trim(col(field))).otherwise(None)
                )
        
        return df
    
    def add_raw_event_json(self, df: DataFrame) -> DataFrame:
        """
        Add raw event as JSON string for debugging
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with raw_event column
        """
        # For now, we'll skip this to keep things simple
        # In production, you might want to store original JSON
        df = df.withColumn("raw_event", lit(None).cast(StringType()))
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to the DataFrame
        
        Args:
            df: Raw input DataFrame
            
        Returns:
            Fully transformed DataFrame ready for BigQuery
        """
        self.logger.info("🔄 Starting data transformations...")
        
        # 1. Data quality checks
        df = self.apply_data_quality_checks(df)
        
        # 2. Event filters
        df = self.apply_event_filters(df)
        
        # 3. Convert timestamps
        df = self.convert_timestamps(df)
        
        # 4. Parse location fields
        df = self.parse_location_fields(df)
        
        # 5. Clean string fields
        df = self.clean_string_fields(df)
        
        # 6. Add processing metadata
        df = self.add_processing_metadata(df)
        
        # 7. Add raw event JSON (optional)
        df = self.add_raw_event_json(df)
        
        self.logger.info("✅ Data transformations completed")
        
        return df
    
    def select_bigquery_columns(self, df: DataFrame) -> DataFrame:
        """
        Select and order columns to match BigQuery schema
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame with columns matching BigQuery schema
        """
        columns = [
            # Event identifiers
            "event_id",
            "sessionId",
            
            # User information
            "userId",
            "firstName",
            "lastName",
            "gender",
            "level",
            
            # Authentication
            "auth",
            "registration",
            
            # Event details
            "page",
            "method",
            "status",
            
            # Song information
            "artist",
            "song",
            "length",
            
            # Location & Device
            "location",
            "city",
            "state",
            "zip",
            "lat",
            "lon",
            "userAgent",
            
            # Timestamp & Session
            "ts",
            "itemInSession",
            
            # Processing metadata
            "processed_at",
            "processing_date",
            
            # Raw event
            "raw_event"
        ]
        
        # Select only columns that exist
        existing_columns = [c for c in columns if c in df.columns]
        
        return df.select(*existing_columns)


def create_transformer(config: Dict[str, Any]) -> EventTransformer:
    """
    Factory function to create EventTransformer
    
    Args:
        config: Processing configuration
        
    Returns:
        EventTransformer instance
    """
    return EventTransformer(config)


if __name__ == "__main__":
    print("Testing Event Transformer...")
    print("Note: This requires a Spark DataFrame to test")
    print("Run this as part of the main application")
