{{
  config(
    materialized='view',
    tags=['staging']
  )
}}

/*
  Staging Model: All Events
  
  Purpose:
    Clean and standardize raw events from Spark streaming pipeline.
    This is the foundation layer that all downstream models depend on.
  
  Source:
    music_streaming_data.raw_events (written by Spark Streaming)
  
  Transformations:
    - Rename columns to snake_case for consistency
    - Cast data types explicitly
    - Add derived fields (event_date, event_hour)
    - Filter out invalid records
    - Trim string fields
*/

WITH source AS (
  SELECT * FROM {{ source('music_streaming', 'raw_events') }}
),

cleaned AS (
  SELECT
    -- Event identifiers
    event_id,
    session_id,
    
    -- User information (rename to snake_case)
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level AS subscription_level,
    
    -- Authentication & Session
    auth AS auth_status,
    registration AS registration_timestamp,
    
    -- Event details
    page AS page_name,
    method AS http_method,
    status AS http_status,
    
    -- Song information
    TRIM(artist) AS artist_name,
    TRIM(song) AS song_title,
    length AS song_duration_seconds,
    
    -- Location & Device
    TRIM(location) AS location_full,
    TRIM(city) AS city,
    TRIM(state) AS state,
    zip AS zip_code,
    lat AS latitude,
    lon AS longitude,
    userAgent AS user_agent,
    
    -- Timestamp fields
    ts AS event_timestamp,
    DATE(ts) AS event_date,
    EXTRACT(HOUR FROM ts) AS event_hour,
    EXTRACT(DAYOFWEEK FROM ts) AS day_of_week,  -- 1=Sunday, 7=Saturday
    
    -- Session context
    itemInSession AS item_in_session,
    
    -- Processing metadata
    processed_at AS spark_processed_at,
    processing_date AS spark_processing_date
    
  FROM source
  
  -- Data quality filters
  WHERE 
    event_id IS NOT NULL
    AND ts IS NOT NULL
    AND page IS NOT NULL
)

SELECT * FROM cleaned
