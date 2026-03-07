-- BigQuery Table Schema for Raw Music Streaming Events
-- Table: music_streaming_data.raw_events
-- This table stores all raw events from the music streaming application

CREATE TABLE IF NOT EXISTS `music_streaming_data.raw_events` (
  -- Event identifiers
  event_id STRING NOT NULL,
  session_id STRING,
  
  -- User information
  userId STRING,
  firstName STRING,
  lastName STRING,
  gender STRING,
  level STRING,  -- free or paid
  
  -- Authentication & Session
  auth STRING,  -- Guest, Logged In, Logged Out, Cancelled
  registration TIMESTAMP,
  
  -- Event details
  page STRING NOT NULL,  -- NextSong, Home, Login, Logout, etc.
  method STRING,  -- GET, PUT, POST
  status INT64,  -- HTTP status code (200, 404, etc.)
  
  -- Song information (only for NextSong events)
  artist STRING,
  song STRING,
  length FLOAT64,  -- song duration in seconds
  
  -- Location & Device
  location STRING,
  city STRING,
  state STRING,
  zip STRING,
  lat FLOAT64,
  lon FLOAT64,
  userAgent STRING,
  
  -- Timestamp
  ts TIMESTAMP NOT NULL,  -- Event timestamp
  itemInSession INT64,  -- Item number in current session
  
  -- Processing metadata
  processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  processing_date DATE,  -- For partitioning
  
  -- Raw event JSON (for debugging)
  raw_event STRING
)
PARTITION BY DATE(ts)
CLUSTER BY userId, page, level
OPTIONS(
  description="Raw events from music streaming application (eventsim)",
  labels=[("source", "eventsim"), ("env", "dev"), ("pipeline", "spark-streaming")]
);

-- Create a view for song play events only
CREATE OR REPLACE VIEW `music_streaming_data.song_plays` AS
SELECT
  event_id,
  session_id,
  userId,
  firstName,
  lastName,
  level,
  artist,
  song,
  length,
  location,
  city,
  state,
  userAgent,
  ts,
  itemInSession
FROM
  `music_streaming_data.raw_events`
WHERE
  page = 'NextSong'
  AND artist IS NOT NULL
  AND song IS NOT NULL;

-- Add comments for documentation
ALTER TABLE `music_streaming_data.raw_events`
SET OPTIONS (
  description = "Raw events from music streaming application. Partitioned by event date for efficient querying. Clustered by userId, page, and level for optimal performance."
);
