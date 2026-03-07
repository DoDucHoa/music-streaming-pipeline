"""
Configuration Loader Utility
Loads and validates configuration from YAML file
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigLoader:
    """Loads and manages configuration from YAML files"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration loader
        
        Args:
            config_path: Path to config file. If None, uses default path
        """
        self.config_path = config_path or self._get_default_config_path()
        self.config: Dict[str, Any] = {}
        self._load_config()
        self._validate_config()
    
    def _get_default_config_path(self) -> str:
        """Get default configuration file path"""
        # Try multiple potential locations
        possible_paths = [
            "/opt/spark/config/config.yaml",  # Docker container path
            "./config/config.yaml",  # Local development
            "../config/config.yaml",  # When running from src/
            "../../config/config.yaml",  # When running from src/utils/
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        # If no config found, use default Docker path
        return "/opt/spark/config/config.yaml"
    
    def _load_config(self) -> None:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            
            # Handle empty config file
            if self.config is None:
                self.config = {}
            
            print(f"✅ Configuration loaded from: {self.config_path}")
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Configuration file not found at: {self.config_path}"
            )
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML format: {e}")
    
    def _validate_config(self) -> None:
        """Validate required configuration keys"""
        required_keys = [
            'gcp',
            'kafka',
            'spark',
            'processing'
        ]
        
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required config section: {key}")
        
        # Validate GCP configuration
        gcp_config = self.config['gcp']
        if gcp_config['project_id'] == 'YOUR_PROJECT_ID':
            raise ValueError(
                "GCP project_id not configured! "
                "Please update config.yaml with your GCP project ID"
            )
        
        # Validate credentials path
        creds_path = gcp_config.get('credentials_path')
        if creds_path and not os.path.exists(creds_path):
            print(f"⚠️  Warning: Credentials file not found at: {creds_path}")
            print("   Please ensure the file exists before running the application")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-notation key
        
        Args:
            key: Configuration key (e.g., 'gcp.project_id')
            default: Default value if key not found
            
        Returns:
            Configuration value
            
        Example:
            >>> config.get('gcp.project_id')
            'my-project-123'
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_gcp_config(self) -> Dict[str, Any]:
        """Get GCP configuration"""
        return self.config.get('gcp', {})
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka configuration"""
        return self.config.get('kafka', {})
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration"""
        return self.config.get('spark', {})
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Get processing configuration"""
        return self.config.get('processing', {})
    
    def get_kafka_bootstrap_servers(self) -> str:
        """Get Kafka bootstrap servers"""
        return self.get('kafka.bootstrap_servers', 'kafka:29092')
    
    def get_kafka_topic_config(self) -> Dict[str, Any]:
        """
        Get Kafka topic configuration
        
        Returns:
            Dictionary with topic mode and topic list/pattern
        """
        kafka_config = self.get_kafka_config()
        topic_mode = kafka_config.get('topic_mode', 'single')
        
        topics_config = kafka_config.get('topics', {})
        
        return {
            'topic_mode': topic_mode,
            'topics_single': topics_config.get('single', 'listen_events'),
            'topics_multiple': topics_config.get('multiple', ['listen_events']),
            'topics_pattern': topics_config.get('pattern', '.*_events')
        }
    
    def get_kafka_topic(self) -> str:
        """
        Get Kafka topic name (for backward compatibility)
        Returns comma-separated topics for multiple mode
        """
        topic_config = self.get_kafka_topic_config()
        mode = topic_config['topic_mode']
        
        if mode == 'multiple':
            return ','.join(topic_config['topics_multiple'])
        elif mode == 'pattern':
            return topic_config['topics_pattern']
        else:
            return topic_config['topics_single']
    
    def get_bigquery_table(self) -> str:
        """
        Get fully qualified BigQuery table name
        
        Returns:
            Table name in format: project.dataset.table
        """
        project_id = self.get('gcp.project_id')
        dataset_id = self.get('gcp.dataset_id')
        table_id = self.get('gcp.table_id')
        return f"{project_id}.{dataset_id}.{table_id}"
    
    def get_checkpoint_location(self) -> str:
        """Get Spark checkpoint location"""
        return self.get('spark.streaming.checkpoint_location', '/opt/spark/checkpoints')
    
    def get_trigger_interval(self) -> str:
        """Get streaming trigger interval"""
        return self.get('spark.streaming.trigger_interval', '30 seconds')
    
    def is_debug_mode(self) -> bool:
        """Check if debug mode is enabled"""
        return self.get('dev.debug_mode', False)
    
    def is_dry_run(self) -> bool:
        """Check if dry run mode is enabled"""
        return self.get('dev.dry_run', False)
    
    def __repr__(self) -> str:
        return f"ConfigLoader(config_path='{self.config_path}')"


# Singleton instance
_config_instance: Optional[ConfigLoader] = None


def get_config(config_path: Optional[str] = None) -> ConfigLoader:
    """
    Get or create configuration singleton instance
    
    Args:
        config_path: Optional path to config file
        
    Returns:
        ConfigLoader instance
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = ConfigLoader(config_path)
    
    return _config_instance


if __name__ == "__main__":
    # Test configuration loading
    try:
        config = get_config()
        print("\n📋 Configuration Summary:")
        print(f"  GCP Project: {config.get('gcp.project_id')}")
        print(f"  BigQuery Table: {config.get_bigquery_table()}")
        print(f"  Kafka Topic: {config.get_kafka_topic()}")
        print(f"  Kafka Servers: {config.get_kafka_bootstrap_servers()}")
        print(f"  Trigger Interval: {config.get_trigger_interval()}")
        print(f"  Debug Mode: {config.is_debug_mode()}")
        print("✅ Configuration loaded successfully!\n")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
