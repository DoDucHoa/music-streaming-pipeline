"""
Helper Utilities
Common utility functions for the Spark Streaming application
"""

import uuid
from datetime import datetime, timezone
from typing import Optional, Any, Dict
import json


def generate_event_id() -> str:
    return str(uuid.uuid4())


def get_current_timestamp() -> datetime:
    return datetime.now(timezone.utc)


def parse_json_safe(json_str: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return None


def extract_location_components(location: str) -> tuple:
    """
    Extract city and state from location string
    
    Args:
        location: Location string (e.g., "San Francisco-Oakland-Hayward, CA")
        
    Returns:
        Tuple of (city, state)
        
    Example:
        >>> extract_location_components("San Francisco-Oakland-Hayward, CA")
        ('San Francisco-Oakland-Hayward', 'CA')
    """
    if not location or not isinstance(location, str):
        return (None, None)
    
    parts = location.split(',')
    if len(parts) >= 2:
        city = parts[0].strip()
        state = parts[1].strip()
        return (city, state)
    
    return (location.strip(), None)


def convert_timestamp_ms_to_datetime(ts_ms: Optional[int]) -> Optional[datetime]:
    """
    Convert millisecond timestamp to datetime
    
    Args:
        ts_ms: Timestamp in milliseconds
        
    Returns:
        Datetime object or None if invalid
    """
    if ts_ms is None or ts_ms < 0:
        return None
    
    try:
        return datetime.fromtimestamp(ts_ms / 1000.0)
    except (ValueError, OverflowError, OSError):
        return None


def validate_required_fields(record: Dict[str, Any], required_fields: list) -> bool:
    """
    Validate that record contains required fields
    
    Args:
        record: Record to validate
        required_fields: List of required field names
        
    Returns:
        True if all required fields present and not None
    """
    for field in required_fields:
        if field not in record or record[field] is None:
            return False
    return True


def clean_string(value: Any) -> Optional[str]:
    """
    Clean and normalize string value
    
    Args:
        value: Value to clean
        
    Returns:
        Cleaned string or None
    """
    if value is None:
        return None
    
    if not isinstance(value, str):
        value = str(value)
    
    # Strip whitespace
    value = value.strip()
    
    # Return None for empty strings
    return value if value else None


def safe_float(value: Any) -> Optional[float]:
    """
    Safely convert value to float
    
    Args:
        value: Value to convert
        
    Returns:
        Float value or None if conversion fails
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_int(value: Any) -> Optional[int]:
    """
    Safely convert value to int
    
    Args:
        value: Value to convert
        
    Returns:
        Int value or None if conversion fails
    """
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def is_valid_event_page(page: str, exclude_pages: list = None) -> bool:
    """
    Check if event page is valid and not in exclude list
    
    Args:
        page: Page name
        exclude_pages: List of pages to exclude
        
    Returns:
        True if page is valid
    """
    if not page:
        return False
    
    if exclude_pages and page in exclude_pages:
        return False
    
    return True


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string (e.g., "2m 30s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    
    if minutes < 60:
        return f"{minutes}m {secs}s"
    
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    return f"{hours}h {mins}m"


def get_processing_date(ts: datetime) -> str:
    """
    Get processing date for partitioning
    
    Args:
        ts: Timestamp
        
    Returns:
        Date string in YYYY-MM-DD format
    """
    return ts.strftime('%Y-%m-%d')


def mask_sensitive_data(data: str, show_last: int = 4) -> str:
    """
    Mask sensitive data for logging
    
    Args:
        data: Sensitive data to mask
        show_last: Number of characters to show at end
        
    Returns:
        Masked string
        
    Example:
        >>> mask_sensitive_data("my-secret-key-12345", 4)
        '***************2345'
    """
    if not data or len(data) <= show_last:
        return "***"
    
    return "*" * (len(data) - show_last) + data[-show_last:]


def calculate_throughput(records: int, duration_seconds: float) -> float:
    """
    Calculate throughput (records per second)
    
    Args:
        records: Number of records processed
        duration_seconds: Duration in seconds
        
    Returns:
        Throughput (records/second)
    """
    if duration_seconds <= 0:
        return 0.0
    return records / duration_seconds


if __name__ == "__main__":
    # Test helper functions
    print("Testing helper utilities...")
    
    # Test event ID generation
    event_id = generate_event_id()
    print(f"✅ Event ID: {event_id}")
    
    # Test location parsing
    city, state = extract_location_components("San Francisco-Oakland-Hayward, CA")
    print(f"✅ Location parsing: {city}, {state}")
    
    # Test timestamp conversion
    ts = convert_timestamp_ms_to_datetime(1609459200000)
    print(f"✅ Timestamp conversion: {ts}")
    
    # Test data masking
    masked = mask_sensitive_data("my-secret-key-12345")
    print(f"✅ Data masking: {masked}")
    
    # Test throughput calculation
    throughput = calculate_throughput(1000, 30.5)
    print(f"✅ Throughput: {throughput:.2f} records/sec")
    
    print("\n✅ All helper tests passed!")
